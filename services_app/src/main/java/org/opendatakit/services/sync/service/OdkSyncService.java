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

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;

import org.opendatakit.application.IToolAware;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.sync.service.SyncProgressEvent;
import org.opendatakit.sync.service.SyncStatus;
import org.opendatakit.utilities.ODKFileUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OdkSyncService extends Service {

  /**
   * change to true expression if you want to debug the Sync service
   */
  public static boolean possiblyWaitForSyncServiceDebugger() {
    if ( false ) {
      android.os.Debug.waitForDebugger();
      int len = "for setting breakpoint".length();
      return true;
    }
    return false;
  }

  // Used for logging
  private static final String TAG = OdkSyncService.class.getSimpleName();

  private IOdkSyncServiceInterfaceImpl serviceInterface;
  private GlobalSyncNotificationManager notificationManager;
  private ScheduledExecutorService shutdownTester;

  // NOTE: syncs is used for synchronized(syncs) {...} wraps when accessing
  // all of: syncs, lastStartId, shutdownActorNotYetStarted, isBound
  private final Map<String, AppSynchronizer> syncs = new HashMap<>();
  private Integer lastStartId = null;
  private boolean shutdownActorNotYetStarted = true;
  private boolean isBound = true;

  // Scheduled Executor Runnable that runs every RETENTION_PERIOD / 5 intervals
  // to determine whether or not the service can be shut down.
  private final Runnable shutdownActor = new Runnable() {
    @Override public void run() {
      boolean canShutdown = true;
      Integer savedLastStartId;

      synchronized (syncs) {
        savedLastStartId = lastStartId;
        if (isBound) {
          canShutdown = false;
        } else {
          for (AppSynchronizer sync : syncs.values()) {
            if (!sync.isReapable()) {
              canShutdown = false;
              break;
            }
          }
        }
      }

      if ( canShutdown ) {
        WebLogger.getLogger(ODKFileUtils.getOdkDefaultAppName()).i(TAG,
            "Sync Service shutdownActor -- calling stopSelf");
        stopSelf(savedLastStartId);
      } else {
        WebLogger.getLogger(ODKFileUtils.getOdkDefaultAppName()).i(TAG,
            "Sync Service shutdownActor -- conditions not met to shut down service");
      }

    }
  };

  /**
   * Invokes startService() on the Sync service and schedules a
   * periodic shutdownActor Runnable (above) to determine when to
   * shut down the service.
   *
   * @param shutdownActorNotYetStarted
   */
  private void startShutdownActor(boolean shutdownActorNotYetStarted) {
    if ( shutdownActorNotYetStarted ) {
      // prevent service from being terminated by Android
      Intent bind_intent = new Intent();
      bind_intent.setClassName(IntentConsts.Sync.APPLICATION_NAME,
          IntentConsts.Sync.SYNC_SERVICE_CLASS);
      startService(bind_intent);
      // and schedule a periodic executor to test whether it is safe to stop.
      shutdownTester.scheduleWithFixedDelay(shutdownActor,
          GlobalSyncNotificationManager.RETENTION_PERIOD / 5,
          GlobalSyncNotificationManager.RETENTION_PERIOD / 5, TimeUnit.MILLISECONDS);
    }
  }

  @Override public int onStartCommand(Intent intent, int flags, int startId) {
    //android.os.Debug.waitForDebugger();
    WebLogger.getLogger(ODKFileUtils.getOdkDefaultAppName()).i(TAG,
        "Sync Service onStartCommand startId: " + startId);

    boolean wasShutdownActorNotYetStarted;
    synchronized (syncs) {
      lastStartId = startId;
      wasShutdownActorNotYetStarted = shutdownActorNotYetStarted;
      shutdownActorNotYetStarted = false;
    }

    if (wasShutdownActorNotYetStarted) {
      startShutdownActor(wasShutdownActorNotYetStarted);
    }

    return START_STICKY;
  }

  @Override
  public void onCreate() {
    serviceInterface = new IOdkSyncServiceInterfaceImpl(this);
    notificationManager = new GlobalSyncNotificationManagerImpl(this);
    shutdownTester = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public IBinder onBind(Intent intent) {
    WebLogger.getLogger(ODKFileUtils.getOdkDefaultAppName()).i(TAG,
        "Sync Service onBind new client");

    possiblyWaitForSyncServiceDebugger();

    boolean wasShutdownActorNotYetStarted;
    synchronized (syncs) {
      wasShutdownActorNotYetStarted = shutdownActorNotYetStarted;
      shutdownActorNotYetStarted = false;
      isBound = true;
    }

    if (wasShutdownActorNotYetStarted) {
      startShutdownActor(wasShutdownActorNotYetStarted);
    }

    return serviceInterface;
  }

  @Override public boolean onUnbind(Intent intent) {
    WebLogger.getLogger(ODKFileUtils.getOdkDefaultAppName()).i(TAG,
        "Sync Service onUnbind -- no more bound interfaces");

    synchronized (syncs) {
      isBound = false;
    }

    return super.onUnbind(intent);
  }

  @Override
  public void onDestroy() {
    WebLogger.getLogger(ODKFileUtils.getOdkDefaultAppName()).i(TAG,
        "Sync Service is shutting down");
    shutdownTester.shutdownNow();
    try {
      shutdownTester.awaitTermination(100L, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private AppSynchronizer getSync(String appName) {
    synchronized (syncs) {
      AppSynchronizer sync = syncs.get(appName);
      if (sync == null) {
        sync = new AppSynchronizer(this.getApplicationContext(),
            ((IToolAware) this.getApplication()).getVersionCodeString(),
            appName,
            notificationManager);
        syncs.put(appName, sync);
      }
      return sync;

    }
  }

  public boolean resetServer(String appName, SyncAttachmentState attachmentState) {
    AppSynchronizer sync = getSync(appName);
    return sync.synchronize(true, attachmentState);
  }

  public boolean synchronizeWithServer(String appName, SyncAttachmentState attachmentState) {
    AppSynchronizer sync = getSync(appName);
    return sync.synchronize(false, attachmentState);
  }

  public SyncStatus getStatus(String appName) {
    AppSynchronizer sync = getSync(appName);
    return sync.getStatus();
  }

  public SyncProgressEvent getSyncProgress(String appName) {
    AppSynchronizer sync = getSync(appName);
    return sync.getSyncProgressEvent();
  }

  public SyncOverallResult getSyncResult(String appName) {
    AppSynchronizer sync = getSync(appName);
    if ( sync.getStatus() == SyncStatus.NONE || sync.getStatus() == SyncStatus.SYNCING ) {
      return null;
    } else {
      return sync.getSyncResult();
    }
  }

  public boolean clearAppSynchronizer(String appName) {
    AppSynchronizer appSync = syncs.remove(appName);
    return appSync != null;
  }

  public boolean verifyServerSettings(String appName) {
    AppSynchronizer sync = getSync(appName);
    return sync.verifyServerSettings();
  }

}
