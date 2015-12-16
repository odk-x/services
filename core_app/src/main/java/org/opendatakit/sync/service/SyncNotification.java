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

import org.opendatakit.common.android.utilities.WebLogger;

import android.app.Notification;
import android.app.NotificationManager;
import android.content.Context;

public final class SyncNotification {
  private static final String LOGTAG = SyncNotification.class.getSimpleName();

  private final Context cntxt;
  private final String appName;
  private final NotificationManager notificationManager;

  private int messageNum;
  private String updateText;
  private SyncProgressState progressState;
  private Notification.Builder builder;

  public SyncNotification(Context context, String appName) {
    this.cntxt = context;
    this.appName = appName;
    this.notificationManager = (NotificationManager) cntxt
        .getSystemService(Context.NOTIFICATION_SERVICE);
    this.messageNum = 0;
    this.updateText = null;
    this.progressState = null;
    this.builder = new Notification.Builder(cntxt);
  }

  public synchronized void updateNotification(SyncProgressState pgrState, String text,
      int maxProgress, int progress, boolean indeterminateProgress) {
    this.progressState = pgrState;
    this.updateText = text;
    builder.setContentTitle("ODK syncing " + appName).setContentText(text).setAutoCancel(false)
        .setOngoing(true);
    builder.setSmallIcon(android.R.drawable.ic_popup_sync);
    builder.setProgress(maxProgress, progress, indeterminateProgress);
    Notification syncNotif = builder.getNotification();

    notificationManager.notify(appName, messageNum, syncNotif);
    WebLogger.getLogger(appName).i(
        LOGTAG,
        messageNum + " Update SYNC Notification -" + appName + " TEXT:" + text + " PROG:"
            + progress);

  }

  public synchronized String getUpdateText() {
    return updateText;
  }

  public synchronized SyncProgressState getProgressState() {
    return progressState;
  }

  public synchronized void finalErrorNotification(String text) {
    this.progressState = SyncProgressState.ERROR;
    this.updateText = text;
    Notification.Builder finalBuilder = new Notification.Builder(cntxt);
    finalBuilder.setContentTitle("ODK SYNC ERROR " + appName).setContentText(text)
        .setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(android.R.drawable.ic_dialog_alert);

    Notification syncNotif = finalBuilder.getNotification();

    notificationManager.notify(appName, messageNum, syncNotif);
    WebLogger.getLogger(appName).e(LOGTAG,
        messageNum + " FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }

  public synchronized void clearNotification() {
    this.progressState = SyncProgressState.COMPLETE;
    this.updateText = "Sync Completed Successfully";

    Notification.Builder finalBuilder = new Notification.Builder(cntxt);
    finalBuilder.setContentTitle("ODK SYNC SUCESS " + appName).setContentText(updateText)
        .setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(android.R.drawable.ic_dialog_alert);

    Notification syncNotif = finalBuilder.getNotification();

    notificationManager.notify(appName, messageNum, syncNotif);
  }

}
