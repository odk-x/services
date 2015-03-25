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

package org.opendatakit.common.android.logic;

import java.util.TreeMap;

import android.content.Context;

public abstract class PropertiesSingletonFactory {

  private final TreeMap<String,String> mGeneralDefaults; 
  private final TreeMap<String,String> mAdminDefaults;

  private String gAppName = null;
  private PropertiesSingleton gSingleton = null;
  
  protected PropertiesSingletonFactory(TreeMap<String,String> generalDefaults, TreeMap<String,String> adminDefaults) {
    
    mGeneralDefaults = generalDefaults;
    mAdminDefaults = adminDefaults;
  }

  /**
   * This should be called shortly before accessing the settings.
   * The individual get/set/contains/remove functionality does not
   * check for prior changes to properties, but this does.
   * 
   * @param context
   * @param appName
   * @param toolName
   * @return
   */
  public synchronized PropertiesSingleton getSingleton(Context context, String appName) {
    if (appName == null || appName.length() == 0) {
      throw new IllegalArgumentException("Unexpectedly null or empty appName");
    }

    if ( gSingleton == null || gAppName == null || !gAppName.equals(appName) ) {
      gSingleton = new PropertiesSingleton(context, appName, mGeneralDefaults, mAdminDefaults);
      gAppName = appName;
    }
    gSingleton.setCurrentContext(context);
    return gSingleton;
  }

}
