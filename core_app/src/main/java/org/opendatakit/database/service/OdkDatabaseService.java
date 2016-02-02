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

package org.opendatakit.database.service;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

import org.opendatakit.common.android.database.AndroidConvConnectFactory;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.core.application.Core;

public class OdkDatabaseService extends Service {

  public static final String LOGTAG = OdkDatabaseService.class.getSimpleName();

  /**
   * change to true expression if you want to debug the database service
   */
  public static void possiblyWaitForDatabaseServiceDebugger() {
    if ( false) {
      android.os.Debug.waitForDebugger();
      int len = new String("for setting breakpoint").length();
    }
  }

  private OdkDatabaseServiceInterface servInterface;
  
  @Override
  public void onCreate() {
    super.onCreate();
    servInterface = new OdkDatabaseServiceInterface(this);
    AndroidConvConnectFactory.configure();

  }

  @Override
  public IBinder onBind(Intent intent) {
    possiblyWaitForDatabaseServiceDebugger();
    Log.i(LOGTAG, "onBind -- returning interface.");
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
  }

}
