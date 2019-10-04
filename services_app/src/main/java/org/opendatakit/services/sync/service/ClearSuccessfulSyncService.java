package org.opendatakit.services.sync.service;

import android.app.IntentService;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;

import androidx.annotation.Nullable;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.sync.service.IOdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncStatus;

/**
 * Created by wrb on 2/11/2018.
 */
public class ClearSuccessfulSyncService extends IntentService implements ServiceConnection {

   private static final String TAG = ClearSuccessfulSyncService.class.getSimpleName();

   private String appName;

   private final Object interfaceGuard;

   // interfaceGuard guards access to all of the following...
   private IOdkSyncServiceInterface odkSyncInterfaceGuarded;
   private boolean mBoundGuarded;
   // end guarded access.

   public ClearSuccessfulSyncService() {
      super(ClearSuccessfulSyncService.class.getName());
      interfaceGuard = new Object();
      odkSyncInterfaceGuarded = null;
      mBoundGuarded = false;
   }

   @Override protected void onHandleIntent(@Nullable Intent intent) {
      appName = intent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);

      // Do this in on resume so that if we resolve a row it will be refreshed
      // when we come back.
      if (appName == null) {
         Log.e(TAG, IntentConsts.INTENT_KEY_APP_NAME + " [onHandleIntent] appName not supplied on "
             + "intent");
         return;
      }

      try {
         WebLogger.getLogger(appName).i(TAG, "[onHandleIntent] Attempting bind to sync service");
         Intent bind_intent = new Intent();
         bind_intent.setClassName(IntentConsts.Sync.APPLICATION_NAME,
             IntentConsts.Sync.SYNC_SERVICE_CLASS);
         bindService(bind_intent, this, Context.BIND_AUTO_CREATE);
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Override public void onServiceConnected(ComponentName name, IBinder service) {
      if (!name.getClassName().equals(IntentConsts.Sync.SYNC_SERVICE_CLASS)) {
         WebLogger.getLogger(appName).e(TAG, "[onServiceConnected] Unrecognized service");
         return;
      }

      synchronized (interfaceGuard) {
         odkSyncInterfaceGuarded = (service == null) ?
             null :
             IOdkSyncServiceInterface.Stub.asInterface(service);
         mBoundGuarded = (odkSyncInterfaceGuarded != null);
      }
      WebLogger.getLogger(appName).i(TAG, "[onServiceConnected] Bound to sync service");

      // clear sync status to NONE if in a non-error state
      try {
         SyncStatus status = odkSyncInterfaceGuarded.getSyncStatus(appName);
         if (status == SyncStatus.SYNC_COMPLETE
             || status == SyncStatus.SYNC_COMPLETE_PENDING_ATTACHMENTS) {
            WebLogger.getLogger(appName).i(TAG,
                "Sync was successful and user cancelled global notification therefore"
                    + "clearing AppSynchronizer state");
            odkSyncInterfaceGuarded.clearAppSynchronizer(appName);
         }
      } catch (RemoteException e) {
         e.printStackTrace();
      } finally {
         if (mBoundGuarded) {
            unbindService(this);
         }
      }
   }

   @Override public void onServiceDisconnected(ComponentName name) {
      WebLogger.getLogger(appName).i(TAG, "[onServiceDisconnected] Unbound to sync service");
      synchronized (interfaceGuard) {
         odkSyncInterfaceGuarded = null;
         mBoundGuarded = false;
      }
      WebLogger.getLogger(appName)
          .i(TAG, "[onServiceDisconnected] Stopping background " + "intent service");
      this.stopSelf();
   }
}
