package org.opendatakit.database.service;

import java.util.HashMap;
import java.util.Map;

import org.sqlite.database.sqlite.SQLiteDatabase;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

public class OdkDatabaseService extends Service {

  public static final String LOGTAG = OdkDatabaseService.class.getSimpleName();

  private OdkDatabaseServiceInterface servInterface;

  private Map<String,SQLiteDatabase> dbHandles = new HashMap<String,SQLiteDatabase>();
  
  void addActiveDatabase(String appName, SQLiteDatabase db) {
    dbHandles.put(appName, db);
  }
  
  void removeActiveDatabase(String appName) {
    dbHandles.remove(appName);
  }
  
  SQLiteDatabase getActiveDatabase(String appName) {
    SQLiteDatabase db = dbHandles.get(appName);
    return db;
  }
  
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

  public synchronized void appNameDied(String appName) {
    SQLiteDatabase db = getActiveDatabase(appName);
    
    if ( db != null ) {
      db.close();
    }
    removeActiveDatabase(appName);
  }
  
  @Override
  public synchronized void onDestroy() {
    Log.w(LOGTAG, "onDestroy -- shutting down worker (zero interfaces)!");

    // and release any transactions we are holding...
    for (SQLiteDatabase db : dbHandles.values() ) {
      db.close();
    }
    dbHandles.clear();
    
    Log.i(LOGTAG, "onDestroy - done");
    super.onDestroy();
  }

}
