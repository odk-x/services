package org.opendatakit.database.service;

import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.utilities.WebLogger;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

public class OdkDatabaseService extends Service {

  public static final String LOGTAG = OdkDatabaseService.class.getSimpleName();

  private OdkDatabaseServiceInterface servInterface;
  
  @Override
  public void onCreate() {
    super.onCreate();
    servInterface = new OdkDatabaseServiceInterface(this);
  }

  @Override
  public IBinder onBind(Intent intent) {
    Log.i(LOGTAG, "onBind -- returning interface.");
    return servInterface; 
  }

  @Override
  public boolean onUnbind(Intent intent) {
    // TODO Auto-generated method stub
    super.onUnbind(intent);
    Log.i(LOGTAG, "onUnbind -- releasing interface.");
    // release all non-group instances
    DatabaseFactory.get().releaseAllDatabaseNonGroupNonInternalInstances(getApplicationContext());
    // this may be too aggressive, but ensures that WebLogger is released.
    WebLogger.closeAll();
    return false;
  }
  
  @Override
  public synchronized void onDestroy() {
    Log.w(LOGTAG, "onDestroy -- shutting down worker (zero interfaces)!");
    super.onDestroy();
    // release all non-group instances
    DatabaseFactory.get().releaseAllDatabaseNonGroupNonInternalInstances(getApplicationContext());
    // this may be too aggressive, but ensures that WebLogger is released.
    WebLogger.closeAll();
  }

}
