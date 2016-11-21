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

import org.opendatakit.logging.WebLogger;

import android.app.Notification;
import android.app.NotificationManager;
import android.content.Context;
import org.opendatakit.services.R;

public final class SyncNotification {
  private static final String LOGTAG = SyncNotification.class.getSimpleName();

  private final Context cntxt;
  private final String appName;
  private final NotificationManager notificationManager;

  private SyncProgressEvent progressStatus;
  private final Notification.Builder builder;

  public SyncNotification(Context context, String appName) {
    this.cntxt = context;
    this.appName = appName;
    this.notificationManager = (NotificationManager) cntxt
        .getSystemService(Context.NOTIFICATION_SERVICE);
    this.progressStatus = new SyncProgressEvent(0, null, SyncProgressState.INACTIVE, -1, 0);
    this.builder = new Notification.Builder(cntxt);
  }

  public synchronized void updateNotification(SyncProgressState pgrState, String text,
      int maxProgress, int progress, boolean indeterminateProgress) {
    int messageNum = 0;
    this.progressStatus = new SyncProgressEvent(messageNum, text, pgrState, progress, maxProgress);
    builder.setContentTitle(cntxt.getString(R.string.sync_notification_syncing, appName)).setContentText(text).setAutoCancel(false)
        .setOngoing(true);
    builder.setSmallIcon(android.R.drawable.ic_popup_sync);
    builder.setProgress(maxProgress, progress, indeterminateProgress);
    Notification syncNotif = builder.build();

    notificationManager.notify(appName, messageNum, syncNotif);
    WebLogger.getLogger(appName).i(
        LOGTAG,
        messageNum + " Update SYNC Notification -" + appName + " TEXT:" + text + " PROG:"
            + progress);

  }

  public synchronized SyncProgressEvent getProgressStatus() {
    return progressStatus;
  }

  public synchronized void finalErrorNotification(String text) {
    int messageNum = 0;
    this.progressStatus = new SyncProgressEvent(messageNum, text, SyncProgressState.FINISHED, -1, 0);
    Notification.Builder finalBuilder = new Notification.Builder(cntxt);
    finalBuilder.setContentTitle(cntxt.getString(R.string.sync_notification_failure, appName)).setContentText(text)
        .setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(R.drawable.ic_error_white_24dp);

    Notification syncNotif = finalBuilder.build();

    notificationManager.notify(appName, messageNum, syncNotif);
    WebLogger.getLogger(appName).e(LOGTAG,
        messageNum + " FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }

  public synchronized void finalConflictNotification(int tablesWithProblems) {
    int messageNum = 0;
    String text = cntxt.getString(R.string.sync_notification_conflicts_text, tablesWithProblems);
    this.progressStatus = new SyncProgressEvent(messageNum, text, SyncProgressState.FINISHED, -1, 0);
    Notification.Builder finalBuilder = new Notification.Builder(cntxt);
    finalBuilder.setContentTitle(cntxt.getString(R.string.sync_notification_conflicts, appName)).setContentText(text)
        .setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(R.drawable.ic_warning_white_24dp);

    Notification syncNotif = finalBuilder.build();

    notificationManager.notify(appName, messageNum, syncNotif);
    WebLogger.getLogger(appName).w(LOGTAG,
        messageNum + " FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }

  public synchronized void clearNotification(int pendingAttachments) {
    Notification.Builder finalBuilder = new Notification.Builder(cntxt);
    int messageNum = 0;
    String text;
    if ( pendingAttachments > 0 ) {
      text = cntxt.getString(R.string.sync_notification_success_pending_attachments_text, pendingAttachments);
      finalBuilder.setContentTitle(cntxt.getString(R.string.sync_notification_success_pending_attachments, appName)).setContentText(text)
              .setAutoCancel(true).setOngoing(false);
      finalBuilder.setSmallIcon(R.drawable.ic_done_white_24dp);
    } else {
      text = cntxt.getString(R.string.sync_notification_success_complete_text);
      finalBuilder.setContentTitle(cntxt.getString(R.string.sync_notification_success_complete, appName)).setContentText(text)
              .setAutoCancel(true).setOngoing(false);
      finalBuilder.setSmallIcon(R.drawable.ic_done_white_24dp);
    }
    this.progressStatus = new SyncProgressEvent(messageNum, text, SyncProgressState.FINISHED, -1, 0);

    Notification syncNotif = finalBuilder.build();

    notificationManager.notify(appName, messageNum, syncNotif);
    WebLogger.getLogger(appName).i(LOGTAG,
        messageNum + " FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }


  public synchronized void clearVerificationNotification() {
    Notification.Builder finalBuilder = new Notification.Builder(cntxt);
    int messageNum = 0;
    String text;
    text = cntxt.getString(R.string.sync_notification_success_verify_complete_text);
    finalBuilder.setContentTitle(cntxt.getString(R.string.sync_notification_success_verify_complete, appName)).setContentText(text)
            .setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(R.drawable.ic_done_white_24dp);
    this.progressStatus = new SyncProgressEvent(messageNum, text, SyncProgressState.FINISHED, -1, 0);

    Notification syncNotif = finalBuilder.build();

    notificationManager.notify(appName, messageNum, syncNotif);
    WebLogger.getLogger(appName).i(LOGTAG,
            messageNum + " FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }
}
