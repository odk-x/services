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

import org.opendatakit.androidlibrary.R;

import android.content.Context;

public class CommonToolProperties {

  public static final int DEFAULT_FONT_SIZE = 16;
  /****************************************************************
   * CommonToolPropertiesSingletonFactory (opendatakit.properties)
   */
  
  /*******************
   * General Settings
   */

  // server identity
  /** ODK 2.0 server URL */
  public static final String KEY_SYNC_SERVER_URL = "common.sync_server_url";
  /** ODK 1.x server URL */
  public static final String KEY_LEGACY_SERVER_URL = "common.legacy_server_url";
  
  // account identity
  /** gmail account */
  public static final String KEY_ACCOUNT = "common.account";
  /** ODK Aggregate username */
  public static final String KEY_USERNAME = "common.username";

  // general settings
  public static final String KEY_FONT_SIZE = "common.font_size";

  /*******************
   * Admin Settings 
   */
  public static final String KEY_CHANGE_SYNC_SERVER = "common.change_sync_server";
  public static final String KEY_CHANGE_LEGACY_SERVER = "common.change_legacy_server";
  
  public static final String KEY_CHANGE_GOOGLE_ACCOUNT = "common.change_google_account";
  public static final String KEY_CHANGE_USERNAME = "common.change_username";
  public static final String KEY_CHANGE_PASSWORD = "common.change_password";
  public static final String KEY_STORE_PASSWORD = "common.store_password";
  
  public static final String KEY_CHANGE_FONT_SIZE = "common.change_font_size";


  /***********************************************************************
   * Secure properties (always move into appName-secure location).
   * e.g., authentication codes and passwords.
   */

  /*******************
   * General Settings
   */
  
  /** gmail account OAuth 2.0 token */
  public static final String KEY_AUTH = "common.auth";
  /** ODK Aggregate password */
  public static final String KEY_PASSWORD = "common.password";
  /** Admin Settings password */
  public static final String KEY_ADMIN_PW = "common.admin_pw";
  
  public static void accumulateProperties( Context context, 
      TreeMap<String,String> plainProperties, TreeMap<String,String> secureProperties) {
    
    // Set default values as necessary
    
    // the properties managed through the general settings pages.
    
    plainProperties.put(KEY_SYNC_SERVER_URL, 
       context.getString(R.string.default_sync_server_url));
    plainProperties.put(KEY_LEGACY_SERVER_URL,
        context.getString(R.string.default_legacy_server_url));
    plainProperties.put(KEY_ACCOUNT, "");
    plainProperties.put(KEY_USERNAME, "");
    plainProperties.put(KEY_FONT_SIZE, Integer.toString(DEFAULT_FONT_SIZE));

    // the properties that are managed through the admin settings pages.
   
    plainProperties.put(KEY_CHANGE_SYNC_SERVER, "true");
    plainProperties.put(KEY_CHANGE_LEGACY_SERVER, "true");
    
    plainProperties.put(KEY_CHANGE_GOOGLE_ACCOUNT, "true");
    plainProperties.put(KEY_CHANGE_USERNAME, "true");
    plainProperties.put(KEY_CHANGE_PASSWORD, "true");
    
    plainProperties.put(KEY_CHANGE_FONT_SIZE, "true");

    // handle the secure properties. If these are in the incoming property file,
    // remove them and move them into the secure properties area.
    //
    secureProperties.put(KEY_AUTH, "");
    secureProperties.put(KEY_PASSWORD, "");
    secureProperties.put(KEY_ADMIN_PW, "");
  }

  private static class CommonPropertiesSingletonFactory extends PropertiesSingletonFactory {

    private CommonPropertiesSingletonFactory(TreeMap<String,String> generalDefaults, TreeMap<String,String> adminDefaults) {
      super(generalDefaults, adminDefaults);
    }
  }
  
  private static CommonPropertiesSingletonFactory factory = null;
  
  public static synchronized PropertiesSingleton get(Context context, String appName) {
    if ( factory == null ) {
      TreeMap<String,String> plainProperties = new TreeMap<String,String>();
      TreeMap<String,String> secureProperties = new TreeMap<String,String>();
      
      CommonToolProperties.accumulateProperties(context, plainProperties, secureProperties);
      
      factory = new CommonPropertiesSingletonFactory(plainProperties, secureProperties);
    }
    return factory.getSingleton(context, appName);
  }

}
