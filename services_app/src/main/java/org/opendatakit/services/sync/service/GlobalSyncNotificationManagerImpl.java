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
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.os.Build;

import androidx.annotation.NonNull;
import androidx.core.app.NotificationCompat;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.services.R;
import org.opendatakit.services.resolve.conflict.AllConflictsResolutionActivity;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.sync.service.exceptions.NoAppNameSpecifiedException;

import java.util.ArrayList;
import java.util.List;

public final class GlobalSyncNotificationManagerImpl implements GlobalSyncNotificationManager {

  private static final int UNIQUE_ID = 1337;
  private static final String SYNC_NOTIFICATION_CHANNEL_ID_PREFIX = "global_sync_";

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
    update(appName);
  }

  public synchronized void stoppingSync(String appName) throws NoAppNameSpecifiedException {
    AppSyncStatus appStatus = getAppStatus(appName);
    appStatus.setSyncing(false);
    update(appName);
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

  private void update(String appName) {
    // check if NotificationManager should be displaying notification
    boolean shouldDisplay = false;
    for (AppSyncStatus status : statusList) {
      if (status.isSyncing()) {
        shouldDisplay = true;
      }
    }

    // if should and actual do not match fix
    if (shouldDisplay && !displayNotification) {
      createNotification(appName);
    } else if (!shouldDisplay && displayNotification) {
      removeNotification();
    }

    if ( shouldDisplay != displayNotification ) {
      throw new IllegalStateException("unexpected dissonant state");
    }
  }

  private void createNotification(String appName) {
    // The intent to launch when the user clicks the expanded notification
    // Intent tmpIntent = new Intent(service, SyncActivity.class);
    Intent tmpIntent = new Intent(Intent.ACTION_VIEW);
    tmpIntent.setClassName("org.opendatakit.sync", "org.opendatakit.services.sync.actions.activities.SyncActivity");
    tmpIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
    PendingIntent pendIntent = PendingIntent.getActivity(service.getApplicationContext(), 0,
        tmpIntent, 0);

    createSyncNotificationChannel(appName);
    Notification runningNotification = new NotificationCompat
        .Builder(service, getNotificationChannelId(appName))
        .setTicker(service.getString(R.string.sync_ticker))
        .setContentTitle(service.getString(R.string.app_name))
        .setContentText(service.getString(R.string.sync_foreground_text))
        .setWhen(System.currentTimeMillis())
        .setAutoCancel(false)
        .setOngoing(true)
        .setContentIntent(pendIntent)
        .setSmallIcon(R.drawable.odk_services)
        .build();

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
    createSyncNotificationChannel(appName);

    NotificationCompat.Builder builder = new NotificationCompat
        .Builder(service, getNotificationChannelId(appName))
        .setContentTitle(service.getString(R.string.sync_notification_syncing, appName))
        .setContentText(text)
        .setAutoCancel(false)
        .setOngoing(true)
        .setSmallIcon(android.R.drawable.ic_popup_sync)
        .setProgress(maxProgress, progress, indeterminateProgress)
        .setCategory(NotificationCompat.CATEGORY_PROGRESS)
        .addAction(
            android.R.drawable.ic_popup_sync,
            service.getString(R.string.sync_notification_launch_sync_progress_intent),
            createPendingIntentForSyncActivity(appName)
        );

    notify(appName, SYNC_NOTIFICATION_ID, builder.build());
  }


  public synchronized void finalErrorNotification(String appName, String text) {
    createSyncNotificationChannel(appName);

    NotificationCompat.Builder finalBuilder = new NotificationCompat
        .Builder(service, getNotificationChannelId(appName))
        .setContentTitle(service.getString(R.string.sync_notification_failure, appName))
        .setContentText(text)
        .setAutoCancel(true)
        .setOngoing(false)
        .setSmallIcon(R.drawable.ic_error_white_24dp)
        .setCategory(NotificationCompat.CATEGORY_ERROR);

    // setup the launch sync activity pending intent
    PendingIntent pendingIntent = createPendingIntentForSyncActivity(appName);
    finalBuilder.addAction(R.drawable.ic_error_white_24dp, text, pendingIntent);

    notify(appName, SYNC_NOTIFICATION_ID, finalBuilder.build());
  }

  public synchronized void finalConflictNotification(String appName, String text) {
    createSyncNotificationChannel(appName);

    NotificationCompat.Builder finalBuilder = new NotificationCompat
        .Builder(service, getNotificationChannelId(appName))
        .setContentTitle(service.getString(R.string.sync_notification_conflicts, appName))
        .setContentText(text)
        .setAutoCancel(true)
        .setOngoing(false)
        .setSmallIcon(R.drawable.ic_warning_white_24dp)
        .setCategory(NotificationCompat.CATEGORY_ERROR);

    // setup the launch resolve conflicts activity pending intent
    PendingIntent pendingIntent = createPendingIntentForAllConflictsActivity(appName);
    finalBuilder.addAction(R.drawable.ic_warning_white_24dp, text, pendingIntent);

    notify(appName, SYNC_NOTIFICATION_ID, finalBuilder.build());
  }

  public synchronized void clearNotification(String appName, String title, String text) {
    createSyncNotificationChannel(appName);

    NotificationCompat.Builder finalBuilder = new NotificationCompat
        .Builder(service, getNotificationChannelId(appName))
        .setContentTitle(title)
        .setContentText(text)
        .setAutoCancel(true)
        .setOngoing(false)
        .setSmallIcon(R.drawable.ic_done_white_24dp)
        .setCategory(NotificationCompat.CATEGORY_PROGRESS);

    // setup the launch sync activity pending intent
    PendingIntent pendingIntent = createPendingIntentForSyncActivity(appName);
    finalBuilder.addAction(R.drawable.ic_done_white_24dp, text, pendingIntent);

    // setup clearing sync state when user dismisses the notification
    pendingIntent = createSyncClearIntent(appName);
    finalBuilder.setDeleteIntent(pendingIntent);


    notify(appName, SYNC_NOTIFICATION_ID, finalBuilder.build());
  }

  public synchronized void clearVerificationNotification(String appName, String title, String text) {
    createSyncNotificationChannel(appName);

    NotificationCompat.Builder finalBuilder = new NotificationCompat
        .Builder(service, getNotificationChannelId(appName))
        .setContentTitle(title)
        .setContentText(text)
        .setAutoCancel(true)
        .setOngoing(false)
        .setSmallIcon(R.drawable.ic_done_white_24dp)
        .setCategory(NotificationCompat.CATEGORY_PROGRESS);

    // setup the launch sync activity pending intent
    PendingIntent pendingIntent = createPendingIntentForVerifyActivity(appName);
    finalBuilder.addAction(R.drawable.ic_done_white_24dp, text, pendingIntent);

    // setup clearing sync state when user dismisses the notification
    pendingIntent = createSyncClearIntent(appName);
    finalBuilder.setDeleteIntent(pendingIntent);

    notify(appName, SYNC_NOTIFICATION_ID, finalBuilder.build());
  }

  @NonNull private PendingIntent createPendingIntentForVerifyActivity(String appName) {
    // setup the launch of VerifyServerSettings activity pending intent
    Intent i = new Intent(service, VerifyServerSettingsActivity.class);
    i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_REORDER_TO_FRONT |
        Intent.FLAG_FROM_BACKGROUND);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
    return PendingIntent.getActivity(service, 0, i, 0);
  }

  @NonNull private PendingIntent createPendingIntentForSyncActivity(String appName) {
    // setup the launch sync activity pending intent
    Intent i = new Intent(service, SyncActivity.class);
    i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_REORDER_TO_FRONT |
        Intent.FLAG_FROM_BACKGROUND);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
    return PendingIntent.getActivity(service, 0, i, 0);
  }


  @NonNull private PendingIntent createPendingIntentForAllConflictsActivity(String appName) {
    // setup the launch conflict resolution activity pending intent
    Intent i = new Intent(service, AllConflictsResolutionActivity.class);
    i.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_REORDER_TO_FRONT |
        Intent.FLAG_FROM_BACKGROUND);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
    return PendingIntent.getActivity(service, 0, i, 0);
  }

  @NonNull private PendingIntent createSyncClearIntent(String appName) {
    Intent i = new Intent(service, ClearSuccessfulSyncService.class);
    i.setFlags(Intent.FLAG_FROM_BACKGROUND);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
    return PendingIntent.getService(service, 0, i,0);

  }

  private void createSyncNotificationChannel(@NonNull String appName) {
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
      NotificationChannel channel = new NotificationChannel(
          getNotificationChannelId(appName),
          service.getString(R.string.sync_notification_channel_name, appName),
          NotificationManager.IMPORTANCE_LOW
      );

      channel.setLockscreenVisibility(Notification.VISIBILITY_PUBLIC);
      channel.enableVibration(false);

      manager.createNotificationChannel(channel);
    }
  }

  @NonNull
  private String getNotificationChannelId(@NonNull String appName) {
    return SYNC_NOTIFICATION_CHANNEL_ID_PREFIX + appName;
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
