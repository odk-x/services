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

import android.app.Service;

import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.common.android.application.AppAwareApplication;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.sync.service.exceptions.InvalidAuthTokenException;
import org.opendatakit.sync.service.exceptions.NoAppNameSpecifiedException;
import org.opendatakit.sync.service.logic.AggregateSynchronizer;
import org.opendatakit.sync.service.logic.ProcessAppAndTableLevelChanges;
import org.opendatakit.sync.service.logic.ProcessRowDataChanges;
import org.opendatakit.sync.service.logic.Synchronizer;

import java.util.Arrays;
import java.util.List;

public class AppSynchronizer {

  private static final String TAG = AppSynchronizer.class.getSimpleName();

  private final Service service;
  private final String appName;
  private final GlobalSyncNotificationManager globalNotifManager;

  private SyncStatus status;
  private Thread curThread;
  private SyncTask curTask;
  private SyncNotification syncProgress;
  private SyncOverallResult syncResult;

  AppSynchronizer(Service srvc, String appName, GlobalSyncNotificationManager notificationManager) {
    this.service = srvc;
    this.appName = appName;
    this.status = SyncStatus.INIT;
    this.curThread = null;
    this.globalNotifManager = notificationManager;
    this.syncProgress = new SyncNotification(srvc, appName);
    this.syncResult = new SyncOverallResult();
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

  public SyncOverallResult getSyncResult() {
    return syncResult;
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

        SyncExecutionContext sharedContext = new SyncExecutionContext( application,
            appName, syncProgress, syncResult);

        Synchronizer synchronizer = new AggregateSynchronizer(sharedContext);

        sharedContext.setSynchronizer(synchronizer);

        ProcessAppAndTableLevelChanges appAndTableLevelProcessor = new ProcessAppAndTableLevelChanges(sharedContext);
        
        ProcessRowDataChanges rowDataProcessor = new ProcessRowDataChanges(sharedContext);

        boolean authProblems = false;
        int tablesWithProblems = 0;
        status = SyncStatus.SYNCING;
        ODKFileUtils.assertDirectoryStructure(appName);

        // sync the app-level files, table schemas and table-level files
        List<TableResource> workingListOfTables = appAndTableLevelProcessor.synchronizeConfigurationAndContent(push);
        
        if (syncResult.getAppLevelSyncOutcome() != SyncOutcome.SUCCESS) {
          authProblems = (syncResult.getAppLevelSyncOutcome() == SyncOutcome.AUTH_EXCEPTION);
          // TODO: split out types of exceptions to distinguish network vs local
          status = SyncStatus.NETWORK_ERROR;
          WebLogger.getLogger(appName).e(TAG, "Abandoning data row update -- app-level sync was not successful!");
        } else {
          // and now sync the data rows. This does not proceed if there
          // was an app-level sync failure or if the particular tableId
          // experienced a table-level sync failure in the preceeding step.
  
          rowDataProcessor.synchronizeDataRowsAndAttachments(workingListOfTables, attachmentState);
        }

        int attachmentsFailed = 0;
        for (TableLevelResult result : syncResult.getTableLevelResults()) {
          SyncOutcome tableStatus = result.getSyncOutcome();
          switch (tableStatus) {
          case SUCCESS:
            break;
          case WORKING:
          case EXCEPTION:
          case FAILURE:
            // TODO: split out the types of exceptions to distinguish network vs local
            status = SyncStatus.NETWORK_ERROR;
            ++tablesWithProblems;
            break;
          case AUTH_EXCEPTION:
            // this will trump everything...
            authProblems = true;
            break;
          case TABLE_PENDING_ATTACHMENTS:
            ++attachmentsFailed;
            break;
          case TABLE_REQUIRES_APP_LEVEL_SYNC:
            // TODO: communicate this better with different status value (e.g., resync)
            status = SyncStatus.FILE_ERROR;
            ++tablesWithProblems;
            break;
          case TABLE_DOES_NOT_EXIST_ON_SERVER:
          case TABLE_CONTAINS_CHECKPOINTS:
          case TABLE_CONTAINS_CONFLICTS:
            status = SyncStatus.CONFLICT_RESOLUTION;
            ++tablesWithProblems;
            break;
          }
        }

        // if everything succeeded, the overall state will still be SYNCING
        // determine the final status.
        if (status == SyncStatus.SYNCING) {
          status = (attachmentsFailed > 0) ? SyncStatus.SYNC_COMPLETE_PENDING_ATTACHMENTS :
              SyncStatus.SYNC_COMPLETE;
        }

        // and if anywhere in the process, we had an auth failure, flag that as the cause.
        if (authProblems) {
          status = SyncStatus.AUTH_RESOLUTION;
        }

        // stop the in-progress notification and report an overall success/failure
        switch ( status ) {
        case NETWORK_ERROR:
          syncProgress.finalErrorNotification("Network Error. Please verify your server URL.");
          break;
        default:
        case FILE_ERROR:
          syncProgress.finalErrorNotification("Unknown Error. Please contact opendatakit@.");
          break;
        case AUTH_RESOLUTION:
          syncProgress.finalErrorNotification("Authentication Error. Please verify credentials.");
          break;
        case CONFLICT_RESOLUTION:
          syncProgress.finalConflictNotification(tablesWithProblems);
          break;
        case SYNC_COMPLETE:
          syncProgress.clearNotification(attachmentsFailed);
          break;
        case SYNC_COMPLETE_PENDING_ATTACHMENTS:
          syncProgress.clearNotification(attachmentsFailed);
          break;
        }

        WebLogger.getLogger(appName).i(TAG,
            "[SyncThread] timestamp: " + System.currentTimeMillis());
      } catch (InvalidAuthTokenException e) {
        status = SyncStatus.AUTH_RESOLUTION;
        syncProgress.finalErrorNotification("Authentication Error. Please verify credentials.");
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
        // TODO: improve the identification of the type of the error (network vs local)
        status = SyncStatus.NETWORK_ERROR;
        syncProgress.finalErrorNotification("Failed Sync: " + msg);
      }
    }

  }
}
