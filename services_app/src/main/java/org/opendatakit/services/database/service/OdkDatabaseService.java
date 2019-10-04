/*
 * Copyright (C) 2015 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.services.database.service;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

import org.opendatakit.database.service.DbChunk;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class OdkDatabaseService extends Service {

  private static final String LOGTAG = OdkDatabaseService.class.getSimpleName();

  // A place to store pieces of large tables or other return values that won't fit across the
  // AIDL call
  private Map<UUID, DbChunk> parceledChunks;

  /**
   * change to true expression if you want to debug the database service
   */
  public static void possiblyWaitForDatabaseServiceDebugger() {
    if ( false ) {
      android.os.Debug.waitForDebugger();
      int len = "for setting breakpoint".length();
    }
  }

  private OdkDatabaseServiceInterface servInterface;
  
  @Override
  public void onCreate() {
    super.onCreate();
    parceledChunks = new HashMap<>();
    servInterface = new OdkDatabaseServiceInterface(this);
    AndroidConnectFactory.configure();
  }

  @Override
  public IBinder onBind(Intent intent) {
    possiblyWaitForDatabaseServiceDebugger();
    Log.i(LOGTAG, "onBind -- returning interface.");

    if (parceledChunks == null) {
      parceledChunks = new HashMap<>();
    }

    return servInterface; 
  }

  @Override
  public boolean onUnbind(Intent intent) {
    // TODO Auto-generated method stub
    super.onUnbind(intent);
    Log.i(LOGTAG, "onUnbind -- releasing interface.");
    // release all non-group instances
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeAllDatabaseServiceConnections();
    // this may be too aggressive, but ensures that WebLogger is released.
    WebLogger.closeAll();

    parceledChunks = null;

    return false;
  }
  
  @Override
  public synchronized void onDestroy() {
    Log.w(LOGTAG, "onDestroy -- shutting down worker (zero interfaces)!");
    super.onDestroy();
    // release all non-group instances
    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeAllDatabaseServiceConnections();
    // this may be too aggressive, but ensures that WebLogger is released.
    WebLogger.closeAll();

    parceledChunks = null;
  }

  /**
   * Cache the extra data for a return value that exceeds the 1MB limit of an AIDL call.
   *
   * @param parceledChunk The extra data to be stored
   */
  public void putParceledChunk(DbChunk parceledChunk) {
    if (parceledChunk == null) {
      Log.w(LOGTAG, "Attempted to store a null chunk");
      return;
    }

    parceledChunks.put(parceledChunk.getThisID(), parceledChunk);
  }

  /**
   * Cache the extra data for a return value that exceeds the 1MB limit of an AIDL call.
   *
   * @param chunkList The extra data to be stored
   */
  public void putParceledChunks(List<DbChunk> chunkList) {
    if (chunkList == null) {
      Log.e(LOGTAG, "Attempted to store a null chunk list");
      return;
    }

    for(DbChunk chunk: chunkList) {
      parceledChunks.put(chunk.getThisID(), chunk);
    }
  }

  /**
   * Retrieve a cached chunk
   *
   * @param id The look up key
   * @return The chunk
   */
  public DbChunk getParceledChunk(UUID id) {
    return parceledChunks.get(id);
  }

  /**
   * Retrieve and remove a cached chunk
   *
   * @param id The look up key
   * @return The chunk
   */
  public DbChunk removeParceledChunk(UUID id) {
    return parceledChunks.remove(id);
  }

}
