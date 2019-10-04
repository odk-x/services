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

import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.sync.service.SyncProgressEvent;
import org.opendatakit.sync.service.SyncProgressState;

public final class SyncProgressTracker {
  // Used for logging
  private static final String TAG = SyncProgressTracker.class.getSimpleName();

  private final Context cntxt;
  private final String appName;
  private final GlobalSyncNotificationManager notificationManager;
  private SyncProgressEvent progressStatus;

  public SyncProgressTracker(Context context, GlobalSyncNotificationManager notificationManager,
                             String appName) {
    this.cntxt = context;
    this.appName = appName;
    this.notificationManager = notificationManager;
    this.progressStatus = new SyncProgressEvent(null, SyncProgressState.INACTIVE, -1, 0);
  }

  public synchronized void updateNotification(SyncProgressState pgrState, String text,
      int maxProgress, int progress, boolean indeterminateProgress) {
    this.progressStatus = new SyncProgressEvent(text, pgrState, progress, maxProgress);

    notificationManager.updateNotification(appName, text, maxProgress, progress, indeterminateProgress);
    WebLogger.getLogger(appName)
        .i(TAG, "Update SYNC Notification -" + appName + " TEXT:" + text + " PROG:" + progress);
  }

  public synchronized SyncProgressEvent getProgressStatus() {
    return progressStatus;
  }

  public synchronized void finalErrorNotification(String text) {
    this.progressStatus = new SyncProgressEvent(text, SyncProgressState.FINISHED, -1, 0);

    notificationManager.finalErrorNotification(appName, text);
    WebLogger.getLogger(appName).e(TAG, "FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }

  public synchronized void finalConflictNotification(int tablesWithProblems) {
    String text = cntxt.getString(R.string.sync_notification_conflicts_text, tablesWithProblems);
    this.progressStatus = new SyncProgressEvent(text, SyncProgressState.FINISHED, -1, 0);

    notificationManager.finalConflictNotification(appName, text);
    WebLogger.getLogger(appName).w(TAG, "FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }

  public synchronized void clearNotification(int pendingAttachments) {
    String title;
    String text;
    if (pendingAttachments > 0) {
      text = cntxt.getString(R.string.sync_notification_success_pending_attachments_text,
          pendingAttachments);
      title = cntxt.getString(R.string.sync_notification_success_pending_attachments, appName);
    } else {
      text = cntxt.getString(R.string.sync_notification_success_complete_text);
      title = cntxt.getString(R.string.sync_notification_success_complete, appName);
    }
    this.progressStatus = new SyncProgressEvent(text, SyncProgressState.FINISHED, -1, 0);

    notificationManager.clearNotification(appName, title, text);
    WebLogger.getLogger(appName).i(TAG, "FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }

  public synchronized void clearVerificationNotification() {
    String text = cntxt.getString(R.string.sync_notification_success_verify_complete_text);
    String title = cntxt.getString(R.string.sync_notification_success_verify_complete, appName);
    this.progressStatus = new SyncProgressEvent(text, SyncProgressState.FINISHED, -1, 0);

    notificationManager.clearVerificationNotification(appName, title, text);
    WebLogger.getLogger(appName).i(TAG, "FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }
}
