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
package org.opendatakit.services.sync.service;

import android.content.Context;

import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.exceptions.AccessDeniedException;
import org.opendatakit.services.sync.service.exceptions.NoAppNameSpecifiedException;
import org.opendatakit.services.sync.service.logic.AggregateSynchronizer;
import org.opendatakit.services.sync.service.logic.ProcessAppAndTableLevelChanges;
import org.opendatakit.services.sync.service.logic.ProcessRowDataOrchestrateChanges;
import org.opendatakit.services.sync.service.logic.Synchronizer;
import org.opendatakit.services.sync.service.logic.SynchronizerFactory;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncOutcome;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.sync.service.SyncProgressEvent;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.SyncStatus;
import org.opendatakit.sync.service.TableLevelResult;
import org.opendatakit.utilities.ODKFileUtils;

import java.util.HashMap;
import java.util.List;

public class AppSynchronizer {

  private static final String TAG = AppSynchronizer.class.getSimpleName();

  private final Context context;
  private final String versionCodeString;
  private final String appName;
  private final GlobalSyncNotificationManager globalNotificationManager;

  private SyncStatus status;
  private Long threadStartTime = null;
  private Long threadEndTime = null;
  private Thread curThread;
  private SyncTask curTask;
  private SyncProgressTracker syncProgressTracker;
  private SyncOverallResult syncResult;

  public AppSynchronizer(Context context, String versionCodeString, String appName,
                  GlobalSyncNotificationManager globalNotificationManager) {
    this.context = context;
    this.versionCodeString = versionCodeString;
    this.appName = appName;
    this.status = SyncStatus.NONE;
    this.curThread = null;
    this.globalNotificationManager = globalNotificationManager;
    this.syncProgressTracker = new SyncProgressTracker(this.context, this.globalNotificationManager, appName);
    this.syncResult = new SyncOverallResult();
  }

  /**
   * This API is ONLY used in the website codebase.
   *
   * @param push
   * @param attachmentState
   */
  public synchronized void directSynchronize(boolean push, SyncAttachmentState attachmentState) {
    curThread = Thread.currentThread();
    curTask = new SyncTask(context, versionCodeString, push, attachmentState);
    threadStartTime = System.currentTimeMillis();
    status = SyncStatus.SYNCING;
    curTask.run();
  }

  public synchronized boolean synchronize(boolean push, SyncAttachmentState attachmentState) {
    if (curThread == null) {
      curTask = new SyncTask(context, versionCodeString, push, attachmentState);
      threadStartTime = System.currentTimeMillis();
      curThread = new Thread(curTask);
      status = SyncStatus.SYNCING;
      curThread.start();
      return true;
    }
    return false;
  }

  public synchronized boolean verifyServerSettings() {
    if (curThread == null) {
      curTask = new SyncTask(context, versionCodeString);
      threadStartTime = System.currentTimeMillis();
      curThread = new Thread(curTask);
      status = SyncStatus.SYNCING;
      curThread.start();
      return true;
    }
    return false;
  }

  private synchronized boolean isRunning() {
    // if the thread is alive, we are running
    if ( curThread != null && curThread.isAlive() ) {
      return true;
    }

    // when we are just starting a worker thread, the progress state is null
    // and the status in SYNCING. (see synchronize(), above).
    if (getSyncProgressEvent().progressState == null && status == SyncStatus.SYNCING) {
      return true;
    }

    // one of the above two conditions will be true if a sync request
    // is running. Otherwise, it is not running.
    return false;
  }

  /**
   * @return true if there is no reason to keep this information around. i.e., no sync action
   * was performed or the action was performed a long time ago.
   */
  public boolean isReapable() {
    // a sync action is actively running
    if ( isRunning() ) {
      return false;
    }

    // there was a sync action and it ended less than a RETENTION_PERIOD ago.
    if ( (threadEndTime != null) &&
         (System.currentTimeMillis() < threadEndTime + GlobalSyncNotificationManager.RETENTION_PERIOD) ) {
      return false;
    }

    // otherwise, no action or a stale one -- we can reap ourselves!
    return true;
  }

  public SyncStatus getStatus() {
    return status;
  }

  public SyncProgressEvent getSyncProgressEvent() {
    return syncProgressTracker.getProgressStatus();
  }

  public SyncOverallResult getSyncResult() {
    return syncResult;
  }

  private class SyncTask implements Runnable {

    private final Context context;
    private final String versionCodeString;
    private final boolean onlyVerifySettings;
    private final boolean push;
    private final SyncAttachmentState attachmentState;

    public SyncTask(Context context, String versionCodeString) {
      this.context = context;
      this.versionCodeString = versionCodeString;
      this.onlyVerifySettings = true;
      this.push = false;
      this.attachmentState = SyncAttachmentState.NONE;
    }

    public SyncTask(Context context, String versionCodeString, boolean push, SyncAttachmentState attachmentState) {
      this.context = context;
      this.versionCodeString = versionCodeString;
      this.onlyVerifySettings = false;
      this.push = push;
      this.attachmentState = attachmentState;
    }

    @Override
    public void run() {

      try {
        globalNotificationManager.startingSync(appName);
        syncProgressTracker.updateNotification(SyncProgressState.STARTING,
            context.getString(R.string.sync_starting), 100, 0, false);
        if ( onlyVerifySettings ) {
          verifySettings();
        } else {
          sync();
        }
      } catch (Exception e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        status = SyncStatus.DEVICE_ERROR;
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_device_internal_error));
      } finally {
        try {
          globalNotificationManager.stoppingSync(appName);
        } catch (NoAppNameSpecifiedException e) {
          // impossible to get here
        }
        threadEndTime = System.currentTimeMillis();
      }

    }

    private SyncStatus resolveOutcome(SyncOutcome outcome) {
      SyncStatus status = SyncStatus.SYNCING;
      switch (outcome) {
      case WORKING:
      case SUCCESS:
        break;
      case FAILURE:
        status = SyncStatus.DEVICE_ERROR;
        break;
      case ACCESS_DENIED_EXCEPTION:
      case ACCESS_DENIED_REAUTH_EXCEPTION:
        status = SyncStatus.AUTHENTICATION_ERROR;
        break;
      case BAD_CLIENT_CONFIG_EXCEPTION:
      case NOT_OPEN_DATA_KIT_SERVER_EXCEPTION:
      case UNEXPECTED_REDIRECT_EXCEPTION:
        status = SyncStatus.SERVER_IS_NOT_ODK_SERVER;
        break;
      case LOCAL_DATABASE_EXCEPTION:
        status = SyncStatus.DEVICE_ERROR;
        break;
      case NETWORK_TRANSMISSION_EXCEPTION:
        status = SyncStatus.NETWORK_TRANSPORT_ERROR;
        break;
      case INCOMPATIBLE_SERVER_VERSION_EXCEPTION:
        status = SyncStatus.REQUEST_OR_PROTOCOL_ERROR;
        break;
      case INTERNAL_SERVER_FAILURE_EXCEPTION:
        status = SyncStatus.SERVER_INTERNAL_ERROR;
        break;
      case APPNAME_DOES_NOT_EXIST_ON_SERVER:
        status = SyncStatus.APPNAME_NOT_SUPPORTED_BY_SERVER;
        break;
      case NO_APP_LEVEL_FILES_ON_SERVER_TO_SYNC:
      case CLIENT_VERSION_FILES_DO_NOT_EXIST_ON_SERVER:
      case NO_TABLES_ON_SERVER_TO_SYNC:
      case INCOMPLETE_SERVER_CONFIG_MISSING_FILE_BODY:
        status = SyncStatus.SERVER_MISSING_CONFIG_FILES;
        break;
      case NO_LOCAL_TABLES_TO_RESET_ON_SERVER:
        status = SyncStatus.SERVER_RESET_FAILED_DEVICE_HAS_NO_CONFIG_FILES;
        break;
      case TABLE_REQUIRES_APP_LEVEL_SYNC:
      case TABLE_DOES_NOT_EXIST_ON_SERVER:
      case TABLE_SCHEMA_COLUMN_DEFINITION_MISMATCH:
        status = SyncStatus.RESYNC_BECAUSE_CONFIG_HAS_BEEN_RESET_ERROR;
        break;
      case TABLE_CONTAINS_CHECKPOINTS:
      case TABLE_CONTAINS_CONFLICTS:
        status = SyncStatus.CONFLICT_RESOLUTION;
        break;
      case TABLE_PENDING_ATTACHMENTS:
        status = SyncStatus.SYNC_COMPLETE_PENDING_ATTACHMENTS;
      }

      return status;
    }

    private void sync() {

      SyncExecutionContext sharedContext = null;
      try {
        WebLogger.getLogger(appName).i(TAG, "APPNAME IN SERVICE: " + appName);
        WebLogger.getLogger(appName).i(TAG, "[SyncThread] begin SYNCING timestamp: " + System.currentTimeMillis());

        status = SyncStatus.SYNCING;
        ODKFileUtils.assertDirectoryStructure(appName);

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

        sharedContext = new SyncExecutionContext(context, versionCodeString,
                                                  appName, syncProgressTracker, syncResult);

        sharedContext.setSynchronizer(SynchronizerFactory.create(sharedContext));

        ProcessAppAndTableLevelChanges appAndTableLevelProcessor = new ProcessAppAndTableLevelChanges(
                sharedContext);

        ProcessRowDataOrchestrateChanges rowDataProcessor = new ProcessRowDataOrchestrateChanges(sharedContext);

        List<TableResource> workingListOfTables = null;
        try {
          // sync the app-level files, table schemas and table-level files
          workingListOfTables = appAndTableLevelProcessor.synchronizeConfigurationAndContent(push);
        } catch (ServicesAvailabilityException e) {
          WebLogger.getLogger(appName).printStackTrace(e);
          if (syncResult.getAppLevelSyncOutcome() == SyncOutcome.WORKING) {
            syncResult.setAppLevelSyncOutcome(SyncOutcome.LOCAL_DATABASE_EXCEPTION);
          }
        }

        if (syncResult.getAppLevelSyncOutcome() != SyncOutcome.SUCCESS) {
          WebLogger.getLogger(appName)
                  .e(TAG, "Abandoning data row update -- app-level sync was not successful!");
        } else if (workingListOfTables != null) {
          // and now sync the data rows. This does not proceed if there
          // was an app-level sync failure or if the particular tableId
          // experienced a table-level sync failure in the preceeding step.

          try {
            rowDataProcessor.synchronizeDataRowsAndAttachments(workingListOfTables, attachmentState);
          } catch (ServicesAvailabilityException e) {
            WebLogger.getLogger(appName).printStackTrace(e);
          } finally {
            for (TableLevelResult tlr : syncResult.getTableLevelResults()) {
              if (tlr.getSyncOutcome() == SyncOutcome.WORKING) {
                WebLogger.getLogger(appName).e(TAG, "Abandoning data row update " + tlr.getTableId()
                        + " -- exception aborts processing!");
                tlr.setSyncOutcome(SyncOutcome.FAILURE);
              }
            }
          }
        }
      } catch (AccessDeniedException e) {
        syncResult.setAppLevelSyncOutcome(SyncOutcome.ACCESS_DENIED_REAUTH_EXCEPTION);
        WebLogger.getLogger(appName)
                .e(TAG, "Abandoning data row update -- app-level sync was not successful!");
      }

      WebLogger.getLogger(appName).i(TAG,
              "[SyncThread] work completed (begin SyncStatus determination) timestamp: " + System.currentTimeMillis());

      // OK. At this point, we have completed the sync. We need to update
      // SyncStatus to reflect the overall outcome.
      if (syncResult.getAppLevelSyncOutcome() == SyncOutcome.WORKING) {
        // this should have been resolved to a final outcome by now.
        syncResult.setAppLevelSyncOutcome(SyncOutcome.FAILURE);
      }

      // try to determine the overall status of the sync.
      // This will begin with the app-level sync outcome.
      int tablesWithProblems = 0;
      int attachmentsFailed = 0;
      SyncStatus finalStatus = resolveOutcome(syncResult.getAppLevelSyncOutcome());
      for (TableLevelResult result : syncResult.getTableLevelResults()) {
        SyncOutcome tableOutcome = result.getSyncOutcome();
        if (tableOutcome == SyncOutcome.WORKING) {
          result.setSyncOutcome(SyncOutcome.FAILURE);
          tableOutcome = SyncOutcome.FAILURE;
        }
        // determine the status from this table..
        SyncStatus tableStatus = resolveOutcome(tableOutcome);
        switch (tableStatus) {
          default:
            ++tablesWithProblems;
            break;
          case SYNCING:
          case SYNC_COMPLETE:
            tableStatus = SyncStatus.SYNCING;
            break;
          case SYNC_COMPLETE_PENDING_ATTACHMENTS:
            ++attachmentsFailed;
        }

        // update the overall status to the first error we find.
        // track pending attachments, but allow more serious errors
        // to override that status outcome.
        if ((finalStatus == SyncStatus.SYNCING || finalStatus == SyncStatus.SYNC_COMPLETE_PENDING_ATTACHMENTS)
                && (tableStatus != SyncStatus.SYNCING)) {
          finalStatus = tableStatus;
        }
      }

      if (finalStatus == SyncStatus.SYNCING) {
        status = SyncStatus.SYNC_COMPLETE;
      } else {
        status = finalStatus;
      }

      // Only attempt to write a device status record if we are not the anonymous
      // user, the server is an ODK server, its appName matches ours,
      // accepts our authentication, and there isn't a network transport error
      // (which might indicate a network login screen).
      if ((!sharedContext.getAuthenticationType().equals(
              sharedContext.getString(R.string.credential_type_none))) &&
          !( status == SyncStatus.SERVER_IS_NOT_ODK_SERVER ||
             status == SyncStatus.APPNAME_NOT_SUPPORTED_BY_SERVER ||
            status == SyncStatus.AUTHENTICATION_ERROR ||
            status == SyncStatus.NETWORK_TRANSPORT_ERROR )) {
        HashMap<String, Object> deviceInfo = sharedContext.getDeviceInfo();
        deviceInfo.put("status", status.name());
        try {
          sharedContext.getSynchronizer().publishDeviceInformation(deviceInfo);
        } catch (Exception e) {
          WebLogger.getLogger(appName).printStackTrace(e);
          WebLogger.getLogger(appName).e(TAG, "Unable to publish device info to server");
        }
      }

      // stop the in-progress notification and report an overall success/failure
      setFinalNotification(status, false, tablesWithProblems, attachmentsFailed);
    }

  private void verifySettings() {

    try {
      WebLogger.getLogger(appName).i(TAG, "APPNAME IN SERVICE: " + appName);
      WebLogger.getLogger(appName).i(TAG, "[SyncThread] begin VERIFYING timestamp: " + System.currentTimeMillis());

      status = SyncStatus.SYNCING;
      ODKFileUtils.assertDirectoryStructure(appName);

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

      SyncExecutionContext sharedContext = new SyncExecutionContext(context,
          versionCodeString, appName, syncProgressTracker, syncResult);

      Synchronizer synchronizer = new AggregateSynchronizer(sharedContext);

      sharedContext.setSynchronizer(synchronizer);

      ProcessAppAndTableLevelChanges appAndTableLevelProcessor = new ProcessAppAndTableLevelChanges(
              sharedContext);

      try {
        // sync the app-level files, table schemas and table-level files
        appAndTableLevelProcessor.verifyServerConfiguration();
      } catch (ServicesAvailabilityException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        if (syncResult.getAppLevelSyncOutcome() == SyncOutcome.WORKING) {
          syncResult.setAppLevelSyncOutcome(SyncOutcome.LOCAL_DATABASE_EXCEPTION);
        }
      }

    } catch (AccessDeniedException e) {
      syncResult.setAppLevelSyncOutcome(SyncOutcome.ACCESS_DENIED_REAUTH_EXCEPTION);
      WebLogger.getLogger(appName)
              .e(TAG, "Abandoning data row update -- app-level sync was not successful!");
    }

    WebLogger.getLogger(appName).i(TAG,
            "[SyncThread] work completed (begin SyncStatus determination) timestamp: " + System.currentTimeMillis());

    // OK. At this point, we have completed the sync. We need to update
    // SyncStatus to reflect the overall outcome.
    if (syncResult.getAppLevelSyncOutcome() == SyncOutcome.WORKING) {
      // this should have been resolved to a final outcome by now.
      syncResult.setAppLevelSyncOutcome(SyncOutcome.SUCCESS);
    }

    // try to determine the overall status of the sync.
    // This will begin with the app-level sync outcome.
    int tablesWithProblems = 0;
    int attachmentsFailed = 0;
    SyncStatus finalStatus = resolveOutcome(syncResult.getAppLevelSyncOutcome());

    if (finalStatus == SyncStatus.SYNCING) {
      status = SyncStatus.SYNC_COMPLETE;
    } else {
      status = finalStatus;
    }

    setFinalNotification(status, true, 0, 0);
  }

  private void setFinalNotification(SyncStatus status, boolean onlyVerify, int tablesWithProblems, int attachmentsFailed) {
      // stop the in-progress notification and report an overall success/failure
      switch (status) {
      case
          /** earlier sync ended with socket or lower level transport or protocol error (e.g., 300's) */ NETWORK_TRANSPORT_ERROR:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_network_transport_error));
        break;
      case
          /** earlier sync ended with Authorization denied (authentication and/or access) error */ AUTHENTICATION_ERROR:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_authentication_error));
        break;
      case
          /** earlier sync ended with a 500 error from server */ SERVER_INTERNAL_ERROR:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_internal_server_error));
        break;
      case
          /** the server is not an ODK Server - bad client config */ SERVER_IS_NOT_ODK_SERVER:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_bad_gateway_or_client_config));
        break;
      case
          /** earlier sync ended with a 400 error that wasn't Authorization denied */ REQUEST_OR_PROTOCOL_ERROR:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_request_or_protocol_error));
        break;
      case /** no earlier sync and no active sync */ NONE:
      case /** active sync -- get SyncProgressEvent to see current status */ SYNCING:
      case
          /** error accessing or updating database */ DEVICE_ERROR:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_device_internal_error));
        break;
      case
          /** the server is not configured for this appName -- Site Admin / Preferences */ APPNAME_NOT_SUPPORTED_BY_SERVER:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_appname_not_supported_by_server));
        break;
      case
          /** the server does not have any configuration, or no configuration for this client version */ SERVER_MISSING_CONFIG_FILES:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_server_missing_config_files));
        break;
      case
          /** the device does not have any configuration to push to server */ SERVER_RESET_FAILED_DEVICE_HAS_NO_CONFIG_FILES:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_server_reset_failed_device_has_no_config_files));
        break;
      case
          /** while a sync was in progress, another device reset the app config, requiring a restart of
           * our sync */ RESYNC_BECAUSE_CONFIG_HAS_BEEN_RESET_ERROR:
        syncProgressTracker.finalErrorNotification(context.getString(R.string.sync_status_resync_because_config_has_been_reset_error, appName));
        break;
      case
          /** earlier sync ended with one or more tables containing row conflicts or checkpoint rows */ CONFLICT_RESOLUTION:
        syncProgressTracker.finalConflictNotification(tablesWithProblems);
        break;
      case
          /** earlier sync ended successfully without conflicts and all row-level attachments sync'd */ SYNC_COMPLETE:
        if ( onlyVerify ) {
          syncProgressTracker.clearVerificationNotification();
        } else {
          syncProgressTracker.clearNotification(attachmentsFailed);
        }
        break;
      case
          /** earlier sync ended successfully without conflicts but needs row-level attachments sync'd */ SYNC_COMPLETE_PENDING_ATTACHMENTS:
        syncProgressTracker.clearNotification(attachmentsFailed);
        break;
      }
    }
  }
}
