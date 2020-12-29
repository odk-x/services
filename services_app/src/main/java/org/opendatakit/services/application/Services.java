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

package org.opendatakit.services.application;

import android.content.Context;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.content.res.Configuration;
import android.util.Log;

import androidx.multidex.MultiDexApplication;

import com.google.firebase.analytics.FirebaseAnalytics;

import org.opendatakit.application.IToolAware;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;

import java.lang.ref.WeakReference;

public final class Services extends MultiDexApplication implements IToolAware {

  private static final String t = Services.class.getSimpleName();

  private FirebaseAnalytics analytics;
  private static WeakReference<Services> singleton = null;

  @Override public int getApkDisplayNameResourceId() {
    return R.string.app_name;
  }

   @Override
  public void onCreate() {
    if (singleton == null) singleton = new WeakReference<>(this);
    super.onCreate();

    analytics = FirebaseAnalytics.getInstance(this);
    analytics.logEvent(FirebaseAnalytics.Event.APP_OPEN, null);
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

  /**
   * The tool name is the name of the package after the org.opendatakit. prefix.
   *
   * @return the tool name.
   */
  @Override
  public String getToolName() {
    String packageName = getPackageName();
    String[] parts = packageName.split("\\.");
    return parts[2];
  }

  @Override
  public String getVersionCodeString() {
    try {
      PackageInfo pinfo;
      pinfo = getPackageManager().getPackageInfo(getPackageName(), 0);
      int versionNumber = pinfo.versionCode;
      return Integer.toString(versionNumber);
    } catch (PackageManager.NameNotFoundException e) {
      e.printStackTrace();
      return "";
    }
  }

  @Override
  public String getVersionDetail() {
    String versionDetail = "";
    try {
      PackageInfo pinfo;
      pinfo = getPackageManager().getPackageInfo(getPackageName(), 0);
      String versionName = pinfo.versionName;
      versionDetail = " " + versionName;
    } catch (PackageManager.NameNotFoundException e) {
      e.printStackTrace();
    }
    return versionDetail;
  }

  @Override
  public String getVersionedToolName() {
    String versionDetail = this.getVersionDetail();
    return getString(getApkDisplayNameResourceId()) + versionDetail;
  }

  @Deprecated public static Context _please_dont_use_getInstance() {
    return singleton.get();
  }
}
