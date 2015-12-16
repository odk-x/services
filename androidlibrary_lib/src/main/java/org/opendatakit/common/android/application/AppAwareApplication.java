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

package org.opendatakit.common.android.application;

import android.app.Application;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager.NameNotFoundException;
import android.content.res.Configuration;
import android.util.Log;
import org.opendatakit.common.android.logic.CommonToolProperties;
import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;

/**
 * Move some of the functionality of CommonApplication up into androidlibrary
 * so that it can be shared with Core Services. License reader task is moved
 * out of global state and into the AboutMenuFragment static state.
 *
 * @author mitchellsundt@gmail.com
 */
public abstract class AppAwareApplication extends Application {

  private static final String t = "AppAwareApplication";

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

  public AppAwareApplication() {
    super();
  }

  /**
   * Return the resource id for the friendly name of this application.
   *
   * @return
   */
  public abstract int getApkDisplayNameResourceId();

  @Override
  public void onCreate() {
    super.onCreate();
  }

  @Override
  public void onConfigurationChanged(Configuration newConfig) {
    super.onConfigurationChanged(newConfig);
    Log.i(t, "onConfigurationChanged");
  }

  @Override
  public void onTerminate() {
    WebLogger.closeAll();
    super.onTerminate();
    Log.i(t, "onTerminate");
  }

  public int getQuestionFontsize(String appName) {
    PropertiesSingleton props = CommonToolProperties.get(this, appName);
    Integer question_font = props.getIntegerProperty(CommonToolProperties.KEY_FONT_SIZE);
    int questionFontsize = (question_font == null) ? CommonToolProperties.DEFAULT_FONT_SIZE : question_font;
    return questionFontsize;
  }

  /**
   * The tool name is the name of the package after the org.opendatakit. prefix.
   * 
   * @return the tool name.
   */
  public String getToolName() {
    String packageName = getPackageName();
    String[] parts = packageName.split("\\.");
    return parts[2];
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

  public String getVersionDetail() {
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
    return versionDetail;
  }

  public String getVersionedAppName() {
    String versionDetail = this.getVersionDetail();
    return getString(getApkDisplayNameResourceId()) + versionDetail;
  }
}
