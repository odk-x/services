/*
 * Copyright (C) 2014 University of Washington
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

package org.opendatakit.core.application;

import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.core.R;

import android.app.Application;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager.NameNotFoundException;
import android.util.Log;

public class Core extends Application {

  public static final String LOGTAG = Core.class.getSimpleName();
  
  /**
   * Set this to true if you want to attach a debugger to any or all 
   * of the services. The debugger is attached in the onBind() call.
   * This affects the OdkDbShimService, OdkWebkitServerService and
   * Sync services.
   * If true, then tables on server are dropped if not present on device.
   */
  private boolean debugService = false;
  
  private int sessionCount = 0;
  
  private static Core singleton = null;

  public static Core getInstance() {
    return singleton;
  }

  public boolean shouldWaitForDebugger() {
    return debugService;
  }
  
  public String getVersionCodeString() {
    try {
      PackageInfo pinfo;
      pinfo = getPackageManager().getPackageInfo(getPackageName(), 0);
      int versionNumber = pinfo.versionCode;
      return Integer.toString(versionNumber);
    } catch (NameNotFoundException e) {
      e.printStackTrace();
      return "";
    }
  }

  public String getVersionedAppName() {
    String versionDetail = "";
    try {
      PackageInfo pinfo;
      pinfo = getPackageManager().getPackageInfo(getPackageName(), 0);
      int versionNumber = pinfo.versionCode;
      String versionName = pinfo.versionName;
      versionDetail = " " + versionName + " (rev " + versionNumber + ")";
    } catch (NameNotFoundException e) {
      e.printStackTrace();
    }
    return getString(R.string.app_name) + versionDetail;
  }

  /**
   * Creates required directories on the SDCard (or other external storage)
   *
   * @return true if there are tables present
   * @throws RuntimeException
   *           if there is no SDCard or the directory exists as a non directory
   */
  public static void createODKDirs(String appName) throws RuntimeException {

    ODKFileUtils.verifyExternalStorageAvailability();

    ODKFileUtils.assertDirectoryStructure(appName);
  }

  @Override
  public void onCreate() {
    singleton = this;
    super.onCreate();
    Log.i(LOGTAG, "onCreate");
    PropertiesSingleton.setStartCoreServices(this);
  }

  @Override
  public void onTerminate() {
    Log.i(LOGTAG, "onTerminate");
    WebLogger.closeAll();
    super.onTerminate();
  }

}
