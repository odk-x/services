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

import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.exceptions.NoAppNameSpecifiedException;

import java.util.ArrayList;
import java.util.List;

public final class GlobalSyncNotificationManagerImpl implements GlobalSyncNotificationManager {

  private static final int UNIQUE_ID = 1337;
  private static final int UPDATE_NOTIFICATION_ID = 0;

  private final Service service;
  private final NotificationManager manager;
  private final boolean test;

  private boolean displayNotification;

  private List<AppSyncStatus> statusList = new ArrayList<AppSyncStatus>();

  public GlobalSyncNotificationManagerImpl(Service service) {
    this.test = false;
    this.service = service;
    this.manager = (NotificationManager) service.getSystemService(Context.NOTIFICATION_SERVICE);
    this.displayNotification = false;
  }

  public GlobalSyncNotificationManagerImpl(Service service, boolean test) {
    this.test = test;
    this.service = service;
    this.manager = (NotificationManager) service.getSystemService(Context.NOTIFICATION_SERVICE);
    this.displayNotification = false;
  }

  public synchronized void startingSync(String appName) throws NoAppNameSpecifiedException {
    AppSyncStatus appStatus = getAppStatus(appName);
    appStatus.setSyncing(true);
    update();
  }

  public synchronized void stoppingSync(String appName) throws NoAppNameSpecifiedException {
    AppSyncStatus appStatus = getAppStatus(appName);
    appStatus.setSyncing(false);
    update();
  }

  private synchronized boolean isDisplayingNotification() {
    return displayNotification;
  }

  private AppSyncStatus getAppStatus(String appName) throws NoAppNameSpecifiedException {
    if (appName == null) {
      throw new NoAppNameSpecifiedException("Cannot update NotificationManager without appName");
    }

    AppSyncStatus appStatus = null;

    // see if manager already knows about the app
    for (AppSyncStatus status : statusList) {
      if (status.getAppName().equals(appName)) {
        appStatus = status;
      }
    }

    // if manager does not know about app, create it
    if (appStatus == null) {
      appStatus = new AppSyncStatus(appName);
      statusList.add(appStatus);
    }
    return appStatus;
  }

  private void update() {
    // check if NotificationManager should be displaying notification
    boolean shouldDisplay = false;
    for (AppSyncStatus status : statusList) {
      if (status.isSyncing()) {
        shouldDisplay = true;
      }
    }

    // if should and actual do not match fix
    if (shouldDisplay && !displayNotification) {
      createNotification();
    } else if (!shouldDisplay && displayNotification) {
      removeNotification();
    }

    if ( shouldDisplay != displayNotification ) {
      throw new IllegalStateException("unexpected dissonant state");
    }
  }

  private void createNotification() {
    // The intent to launch when the user clicks the expanded notification
    // Intent tmpIntent = new Intent(service, SyncActivity.class);
    Intent tmpIntent = new Intent(Intent.ACTION_VIEW);
    tmpIntent.setClassName("org.opendatakit.sync", "org.opendatakit.services.sync.actions.activities.SyncActivity");
    tmpIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
    PendingIntent pendIntent = PendingIntent.getActivity(service.getApplicationContext(), 0,
        tmpIntent, 0);

    Notification.Builder builder = new Notification.Builder(service);
    builder.setTicker("ODK Syncing").setContentTitle("ODK Sync")
        .setContentText("ODK is syncing an Application").setWhen(System.currentTimeMillis())
        .setAutoCancel(false).setOngoing(true).setContentIntent(pendIntent)
            .setSmallIcon(R.drawable.odk_services);

    Notification runningNotification = builder.build();
    runningNotification.flags |= Notification.FLAG_NO_CLEAR;

    if (!test) {
      service.startForeground(UNIQUE_ID, runningNotification);
    }
    displayNotification = true;
  }

  private void removeNotification() {
    if (!test) {
      service.stopForeground(true);
    }
    displayNotification = false;
  }

  private void notify( String appName, int id, Notification syncNotif) {
    manager.notify(appName, id, syncNotif);
  }

  public synchronized void updateNotification( String appName, String text,
                                              int maxProgress, int progress, boolean indeterminateProgress) {
    Notification.Builder builder = new Notification.Builder(service);
    builder.setContentTitle(service.getString(R.string.sync_notification_syncing, appName))
        .setContentText(text).setAutoCancel(false).setOngoing(true);
    builder.setSmallIcon(android.R.drawable.ic_popup_sync);
    builder.setProgress(maxProgress, progress, indeterminateProgress);

    Notification syncNotif = builder.build();
    notify(appName, UPDATE_NOTIFICATION_ID, syncNotif);
  }

  public synchronized void finalErrorNotification(String appName, String text) {
    Notification.Builder finalBuilder = new Notification.Builder(service);
    finalBuilder.setContentTitle(service.getString(R.string.sync_notification_failure, appName))
        .setContentText(text).setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(R.drawable.ic_error_white_24dp);

    Notification syncNotif = finalBuilder.build();
    notify(appName, UPDATE_NOTIFICATION_ID, syncNotif);
  }

  public synchronized void finalConflictNotification(String appName, String text) {
    Notification.Builder finalBuilder = new Notification.Builder(service);
    finalBuilder.setContentTitle(service.getString(R.string.sync_notification_conflicts, appName))
        .setContentText(text).setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(R.drawable.ic_warning_white_24dp);

    Notification syncNotif = finalBuilder.build();

    notify(appName, UPDATE_NOTIFICATION_ID, syncNotif);
  }

  public synchronized void clearNotification(String appName, String title, String text) {
    Notification.Builder finalBuilder = new Notification.Builder(service);
      finalBuilder.setContentTitle(title)
          .setContentText(text).setAutoCancel(true).setOngoing(false);
      finalBuilder.setSmallIcon(R.drawable.ic_done_white_24dp);

    Notification syncNotif = finalBuilder.build();

    notify(appName, UPDATE_NOTIFICATION_ID, syncNotif);
  }

  public synchronized void clearVerificationNotification(String appName, String title, String text) {
    Notification.Builder finalBuilder = new Notification.Builder(service);
    finalBuilder.setContentTitle(title)
        .setContentText(text).setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(R.drawable.ic_done_white_24dp);

    Notification syncNotif = finalBuilder.build();

    notify(appName, UPDATE_NOTIFICATION_ID, syncNotif);
  }

  private final class AppSyncStatus {
    private final String appName;
    private boolean syncing;

    AppSyncStatus(String appName) {
      this.appName = appName;
      this.syncing = false;
    }

    public String getAppName() {
      return appName;
    }

    public void setSyncing(boolean syncing) {
      this.syncing = syncing;
    }

    public boolean isSyncing() {
      return syncing;
    }

  }
}
