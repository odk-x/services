/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.sync.service;

import java.util.Arrays;
import java.util.List;

import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.common.android.application.AppAwareApplication;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.core.R;
import org.opendatakit.sync.service.data.SynchronizationResult;
import org.opendatakit.sync.service.data.SynchronizationResult.Status;
import org.opendatakit.sync.service.data.TableResult;
import org.opendatakit.sync.service.exceptions.InvalidAuthTokenException;
import org.opendatakit.sync.service.exceptions.NoAppNameSpecifiedException;

import android.app.Service;
import org.opendatakit.sync.service.logic.AggregateSynchronizer;
import org.opendatakit.sync.service.logic.ProcessAppAndTableLevelChanges;
import org.opendatakit.sync.service.logic.ProcessRowDataChanges;
import org.opendatakit.sync.service.logic.Synchronizer;

public class AppSynchronizer {

  private static final String TAG = AppSynchronizer.class.getSimpleName();

  private final Service service;
  private final String appName;
  private final GlobalSyncNotificationManager globalNotifManager;

  private SyncStatus status;
  private Thread curThread;
  private SyncTask curTask;
  private SyncNotification syncProgress;

  AppSynchronizer(Service srvc, String appName, GlobalSyncNotificationManager notificationManager) {
    this.service = srvc;
    this.appName = appName;
    this.status = SyncStatus.INIT;
    this.curThread = null;
    this.globalNotifManager = notificationManager;
    this.syncProgress = new SyncNotification(srvc, appName);
  }

  public boolean synchronize(boolean push, SyncAttachmentState attachmentState) {
    if (curThread == null || (!curThread.isAlive() || curThread.isInterrupted())) {
      curTask = new SyncTask(((AppAwareApplication) service.getApplication()), push,
          attachmentState);
      curThread = new Thread(curTask);
      status = SyncStatus.SYNCING;
      curThread.start();
      return true;
    }
    return false;
  }

  public SyncStatus getStatus() {
    return status;
  }

  public String getSyncUpdateText() {
    return syncProgress.getUpdateText();
  }

  public SyncProgressState getProgressState() {
    return syncProgress.getProgressState();
  }

  private class SyncTask implements Runnable {

    private AppAwareApplication application;
    private boolean push;
    private SyncAttachmentState attachmentState;

    public SyncTask(AppAwareApplication application, boolean push, SyncAttachmentState attachmentState) {
      this.application = application;
      this.push = push;
      this.attachmentState = attachmentState;
    }

    @Override
    public void run() {

      try {

        globalNotifManager.startingSync(appName);
        syncProgress.updateNotification(SyncProgressState.STARTING,
            application.getString(R.string.sync_starting), 100, 0, false);
        sync(syncProgress);
      } catch (NoAppNameSpecifiedException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        status = SyncStatus.NETWORK_ERROR;
        syncProgress.updateNotification(SyncProgressState.ERROR, "There were failures...", 100, 0,
            false);
      } finally {
        SyncExecutionContext.refreshActivityUINeeded(appName);
        try {
          globalNotifManager.stoppingSync(appName);
        } catch (NoAppNameSpecifiedException e) {
          // impossible to get here
        }
      }

    }

    private void sync(SyncNotification syncProgress) {

    //  PropertiesSingleton props = CommonToolProperties.get(this.application, appName);
      
      try {
        WebLogger.getLogger(appName).i(TAG, "APPNAME IN SERVICE: " + appName);

        // TODO: should use the APK manager to search for org.opendatakit.N
        // packages, and collect N:V strings e.g., 'survey:1', 'tables:1',
        // 'scan:1' etc. where V is the > 100's digit of the version code.
        // The javascript API and file representation are the 100's and
        // higher place in the versionCode. N is the next package in the
        // package chain.
        // TODO: Future: Add config option to specify a list of other APK
        // prefixes to the set of APKs to discover (e.g., for 3rd party
        // app support).
        //
        // NOTE: server limits this string to 10 characters

        SynchronizationResult syncResult = new SynchronizationResult();

        SyncExecutionContext sharedContext = new SyncExecutionContext( application,
            appName, syncProgress, syncResult);

        Synchronizer synchronizer = new AggregateSynchronizer(sharedContext);

        sharedContext.setSynchronizer(synchronizer);

        ProcessAppAndTableLevelChanges appAndTableLevelProcessor = new ProcessAppAndTableLevelChanges(sharedContext);
        
        ProcessRowDataChanges rowDataProcessor = new ProcessRowDataChanges(sharedContext);

        status = SyncStatus.SYNCING;
        ODKFileUtils.assertDirectoryStructure(appName);

        // sync the app-level files, table schemas and table-level files
        List<TableResource> workingListOfTables = appAndTableLevelProcessor.synchronizeConfigurationAndContent(push);
        
        if (syncResult.getAppLevelStatus() != Status.SUCCESS) {
          WebLogger.getLogger(appName).e(TAG, "Abandoning data row update -- app-level sync was not successful!");
        } else {
          // and now sync the data rows. This does not proceed if there
          // was an app-level sync failure or if the particular tableId
          // experienced a table-level sync failure in the preceeding step.
  
          rowDataProcessor.synchronizeDataRowsAndAttachments(workingListOfTables, attachmentState);
        }

        boolean authProblems = false;

        String reason = "none";
        // examine results
        if (syncResult.getAppLevelStatus() != Status.SUCCESS) {
          authProblems = (syncResult.getAppLevelStatus() == Status.AUTH_EXCEPTION);
          reason = "overall results";
          status = SyncStatus.NETWORK_ERROR;
        }

        int attachmentsFailed = 0;
        for (TableResult result : syncResult.getTableResults()) {
          SynchronizationResult.Status tableStatus = result.getStatus();
          // TODO: decide how to handle the status
          if (tableStatus != Status.SUCCESS) {
            if (tableStatus == Status.AUTH_EXCEPTION) {
              authProblems = true;
            } else if (tableStatus == Status.TABLE_PENDING_ATTACHMENTS) {
              ++attachmentsFailed;
              continue;
            } else if (tableStatus == Status.TABLE_CONTAINS_CHECKPOINTS
                || tableStatus == Status.TABLE_CONTAINS_CONFLICTS
                || tableStatus == Status.TABLE_REQUIRES_APP_LEVEL_SYNC) {
              status = SyncStatus.CONFLICT_RESOLUTION;
            } else {
              status = SyncStatus.NETWORK_ERROR;
              reason = "table " + result.getTableDisplayName();
            }
          }
        }

        if (authProblems) {
          throw new InvalidAuthTokenException("Synthesized");
        }

        // if rows aren't successful, fail.
        if (status != SyncStatus.SYNCING && status != SyncStatus.CONFLICT_RESOLUTION) {
          syncProgress.finalErrorNotification("There were failures. Status: " + status + " Reason:"
              + reason);
          return;
        }

        // success
        if (status != SyncStatus.CONFLICT_RESOLUTION) {
          status = (attachmentsFailed > 0) ? SyncStatus.SYNC_COMPLETE_PENDING_ATTACHMENTS :
              SyncStatus.SYNC_COMPLETE;
          syncProgress.clearNotification(attachmentsFailed);
        } else {
          syncProgress.finalErrorNotification("Conflicts exist.  Please resolve.");
        }

        WebLogger.getLogger(appName).i(TAG,
            "[SyncThread] timestamp: " + System.currentTimeMillis());
      } catch (InvalidAuthTokenException e) {
        SyncExecutionContext.invalidateAuthToken(application, appName);
        status = SyncStatus.AUTH_RESOLUTION;
        syncProgress.finalErrorNotification("Account Re-Authorization Required");
      } catch (Exception e) {
        WebLogger.getLogger(appName).i(
            TAG,
            "[exception during synchronization. stack trace:\n"
                + Arrays.toString(e.getStackTrace()));
        String msg = e.getLocalizedMessage();
        if (msg == null) {
          msg = e.getMessage();
        }
        if (msg == null) {
          msg = e.toString();
        }
        status = SyncStatus.NETWORK_ERROR;
        syncProgress.finalErrorNotification("Failed Sync: " + msg);
      }
    }

  }
}
