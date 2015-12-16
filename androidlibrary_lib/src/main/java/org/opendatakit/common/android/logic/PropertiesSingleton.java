/*
 * Copyright (C) 2013-2014 University of Washington
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

import android.content.Context;
import android.content.SharedPreferences;
import android.util.Log;
import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

public class PropertiesSingleton {

  private static final String t = "PropertiesSingleton";

  private static final String PROPERTIES_FILENAME = "app.properties";

  private static boolean isMocked = false;

  private final String mAppName;
  private long lastModified = 0L;
  private final TreeMap<String, String> mGeneralDefaults;
  private final TreeMap<String, String> mSecureDefaults;

  private Properties mProps;
  private Context mBaseContext;

  private boolean isSecureProperty(String propertyName) {
    return mSecureDefaults.containsKey(propertyName);
  }

  void setCurrentContext(Context context) {
    try {
      mBaseContext = context;
      if (isModified()) {
        readProperties();
      }
    } catch (Exception e) {
      if (isMocked) {
        mBaseContext = context;
      } else {
        boolean faked = false;
        Context app = context.getApplicationContext();
        Class classObj = app.getClass();
        String appName = classObj.getSimpleName();
        while (!appName.equals("CommonApplication")) {
          classObj = classObj.getSuperclass();
          if (classObj == null)
            break;
          appName = classObj.getSimpleName();
        }

        if (classObj != null) {
          try {
            Class<?>[] argClassList = new Class[] {};
            Method m = classObj.getDeclaredMethod("isMocked", argClassList);
            Object[] argList = new Object[] {};
            Object o = m.invoke(null, argList);
            if (((Boolean) o).booleanValue()) {
              mBaseContext = context;
              isMocked = true;
              faked = true;
            }
          } catch (Exception e1) {
          }
        }
        if (!faked) {
          e.printStackTrace();
          throw new IllegalStateException("ODK Core Services must be installed!");
        }
      }
    }
  }

  private static SharedPreferences getSharedPreferences(Context context) {
    try {
      return context.getSharedPreferences(context.getPackageName(), Context.MODE_PRIVATE
            | Context.MODE_MULTI_PROCESS);
    } catch ( Exception e ) {
     Log.e("PropertiesSingleton", "Unable to access SharedPreferences!");
     return null;
    }
  }

  public boolean containsKey(String propertyName) {
    if (isSecureProperty(propertyName)) {
      // this needs to be stored in a protected area
      SharedPreferences sharedPreferences = getSharedPreferences(mBaseContext);
      return (sharedPreferences == null) ? false :
        sharedPreferences.contains(mAppName + "_" + propertyName);
    } else {
      return mProps.containsKey(propertyName);
    }
  }

  /**
   * Accesses the given propertyName. This may be stored in SharedPreferences or
   * in the PROPERTIES_FILENAME in the config/assets directory.
   * 
   * @param propertyName
   * @return null or the string value
   */
  public String getProperty(String propertyName) {
    if (isSecureProperty(propertyName)) {
      // this needs to be stored in a protected area
      SharedPreferences sharedPreferences = getSharedPreferences(mBaseContext);
      return (sharedPreferences == null) ? null : 
        sharedPreferences.getString(mAppName + "_" + propertyName, null);
    } else {
      return mProps.getProperty(propertyName);
    }
  }

  /**
   * Accesses the given propertyName. This may be stored in SharedPreferences or
   * in the PROPERTIES_FILENAME in the config/assets directory.
   * 
   * If the value is not specified, null or an empty string, a null value is
   * returned. Boolean.TRUE is returned if the value is "true", otherwise
   * Boolean.FALSE is returned.
   * 
   * @param propertyName
   * @return null or boolean true/false
   */
  public Boolean getBooleanProperty(String propertyName) {
    Boolean booleanSetting = Boolean.TRUE;
    String value = getProperty(propertyName);
    if (value == null || value.length() == 0) {
      return null;
    }

    if (!"true".equalsIgnoreCase(value)) {
      booleanSetting = Boolean.FALSE;
    }

    return booleanSetting;
  }

  public void setBooleanProperty(String propertyName, boolean value) {
    setProperty(propertyName, Boolean.toString(value));
  }

  /**
   * Accesses the given propertyName. This may be stored in SharedPreferences or
   * in the PROPERTIES_FILENAME in the config/assets directory.
   * 
   * If the value is not specified, null or an empty string, or if the value
   * cannot be parsed as an integer, then null is return. Otherwise, the integer
   * value is returned.
   * 
   * @param propertyName
   * @return
   */
  public Integer getIntegerProperty(String propertyName) {
    String value = getProperty(propertyName);
    if (value == null) {
      return null;
    }
    try {
      int v = Integer.parseInt(value);
      return v;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public void setIntegerProperty(String propertyName, int value) {
    setProperty(propertyName, Integer.toString(value));
  }

  /**
   * Caller is responsible for calling writeProperties() to persist this value
   * to disk.
   * 
   * @param propertyName
   */
  public void removeProperty(String propertyName) {
    if (isSecureProperty(propertyName)) {
      // this needs to be stored in a protected area
      SharedPreferences sharedPreferences = getSharedPreferences(mBaseContext);
      if ( sharedPreferences != null ) {
        sharedPreferences.edit().remove(mAppName + "_" + propertyName).commit();
      } else {
        throw new IllegalStateException("Unable to remove SharedPreferences");
      }
    } else {
      mProps.remove(propertyName);
    }
  }

  /**
   * Caller is responsible for calling writeProperties() to persist this value
   * to disk.
   * 
   * @param propertyName
   * @param value
   */
  public void setProperty(String propertyName, String value) {
    if (isSecureProperty(propertyName)) {
      // this needs to be stored in a protected area
      SharedPreferences sharedPreferences = getSharedPreferences(mBaseContext);
      if ( sharedPreferences != null ) {
        sharedPreferences.edit().putString(mAppName + "_" + propertyName, value).commit();
      } else {
        throw new IllegalStateException("Unable to write SharedPreferences");
      }
    } else {
      if (isModified()) {
        readProperties();
      }
      mProps.setProperty(propertyName, value);
      writeProperties();
    }
  }

  private static String coreStartPropertyName() {
    return "/core_startTime";
  }

  /**
   * Indicate that the core services APK has started
   *
   * @param context
   */
  public static void setStartCoreServices(Context context) {
    // this needs to be stored in a protected area
    SharedPreferences sharedPreferences = getSharedPreferences(context);
    if ( sharedPreferences != null ) {
      sharedPreferences
        .edit()
        .putString(coreStartPropertyName(),
            TableConstants.nanoSecondsFromMillis(System.currentTimeMillis())).commit();
    } else {
      throw new IllegalStateException("Unable to write SharedPreferences");
    }
  }

  private String toolInitializationPropertyName(String toolName) {
    return mAppName + "/" + toolName + "_shouldInitialize";
  }

  /**
   * Determine whether or not the initialization task for the given toolName
   * should be run.
   *
   * @param toolName
   *          (e.g., survey, tables, scan, etc.)
   */
  public boolean shouldRunInitializationTask(String toolName) {
    Boolean booleanSetting = Boolean.TRUE;
    // this needs to be stored in a protected area
    SharedPreferences sharedPreferences = getSharedPreferences(mBaseContext);
    if ( sharedPreferences == null ) {
      throw new IllegalStateException("Unable to access SharedPreferences");
    }
    
    String value = sharedPreferences.getString(toolInitializationPropertyName(toolName), null);
    if (value == null || value.length() == 0) {
      return booleanSetting;
    }

    String coreValue = sharedPreferences.getString(coreStartPropertyName(), null);
    if (coreValue == null || coreValue.length() == 0) {
      return booleanSetting;
    }

    return value.compareTo(coreValue) <= 0;
  }

  /**
   * Indicate that the initialization task for this given tool has been run.
   *
   * @param toolName
   */
  public void clearRunInitializationTask(String toolName) {
    // this needs to be stored in a protected area
    SharedPreferences sharedPreferences = getSharedPreferences(mBaseContext);
    if ( sharedPreferences != null ) {
      sharedPreferences
        .edit()
        .putString(toolInitializationPropertyName(toolName),
            TableConstants.nanoSecondsFromMillis(System.currentTimeMillis())).commit();
    } else {
      throw new IllegalStateException("Unable to write SharedPreferences");
    }
  }

  /**
   * Indicate that the initialization task for this given tool should be run
   * (again).
   *
   * @param toolName
   */
  public void setRunInitializationTask(String toolName) {
    // this needs to be stored in a protected area
    SharedPreferences sharedPreferences = getSharedPreferences(mBaseContext);
    if ( sharedPreferences != null ) {
      sharedPreferences.edit().remove(toolInitializationPropertyName(toolName)).commit();
    } else {
      throw new IllegalStateException("Unable to remove SharedPreferences");
    }
  }

  PropertiesSingleton(Context context, String appName, TreeMap<String, String> plainDefaults,
      TreeMap<String, String> secureDefaults) {
    mAppName = appName;
    mGeneralDefaults = plainDefaults;
    mSecureDefaults = secureDefaults;
    mProps = new Properties();

    // Set default values as necessary
    Properties defaults = new Properties();
    for (TreeMap.Entry<String, String> entry : mGeneralDefaults.entrySet()) {
      defaults.setProperty(entry.getKey(), entry.getValue());
    }

    readProperties();
    setCurrentContext(context);

    boolean dirtyProps = false;
    for (Entry<Object, Object> entry : defaults.entrySet()) {
      if (mProps.containsKey(entry.getKey().toString()) == false) {
        mProps.setProperty(entry.getKey().toString(), entry.getValue().toString());
        dirtyProps = true;
      }
    }

    // strip out the admin password and store it in the app layer.
    for (TreeMap.Entry<String, String> entry : mSecureDefaults.entrySet()) {
      if (mProps.containsKey(entry.getKey())) {
        // NOTE: can't use the static methods because this object is not
        // yet fully created
        SharedPreferences sharedPreferences = getSharedPreferences(mBaseContext);
        if ( sharedPreferences != null ) {
          sharedPreferences.edit()
            .putString(mAppName + "_" + entry.getKey(), entry.getValue())
            .commit();
          mProps.remove(entry.getKey());
          dirtyProps = true;
        } else {
          throw new IllegalStateException("Unable to access SharedPreferences");
        }
      }
    }

    if (dirtyProps) {
      writeProperties();
    }
  }

  private void verifyDirectories() {
    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      ODKFileUtils.assertDirectoryStructure(mAppName);
    } catch (Exception e) {
      Log.e(t, "External storage not available");
      throw new IllegalArgumentException("External storage not available");
    }
  }

  public boolean isModified() {
    File configFile = new File(ODKFileUtils.getAssetsFolder(mAppName), PROPERTIES_FILENAME);

    if (configFile.exists()) {
      return (lastModified != configFile.lastModified());
    } else {
      return false;
    }
  }

  public void readProperties() {
    verifyDirectories();

    FileInputStream configFileInputStream = null;
    try {
      File configFile = new File(ODKFileUtils.getAssetsFolder(mAppName), PROPERTIES_FILENAME);

      if (configFile.exists()) {
        configFileInputStream = new FileInputStream(configFile);

        mProps.loadFromXML(configFileInputStream);
        lastModified = configFile.lastModified();
      }
    } catch (Exception e) {
      WebLogger.getLogger(mAppName).printStackTrace(e);
    } finally {
      if (configFileInputStream != null) {
        try {
          configFileInputStream.close();
        } catch (IOException e) {
          // ignore
          WebLogger.getLogger(mAppName).printStackTrace(e);
        }
      }
    }
  }

  public void writeProperties() {
    verifyDirectories();

    try {
      File tempConfigFile = new File(ODKFileUtils.getAssetsFolder(mAppName), PROPERTIES_FILENAME
          + ".temp");
      FileOutputStream configFileOutputStream = new FileOutputStream(tempConfigFile, false);

      mProps.storeToXML(configFileOutputStream, null, CharEncoding.UTF_8);
      configFileOutputStream.close();

      File configFile = new File(ODKFileUtils.getAssetsFolder(mAppName), PROPERTIES_FILENAME);

      boolean fileSuccess = tempConfigFile.renameTo(configFile);

      if (!fileSuccess) {
        WebLogger.getLogger(mAppName).i(t, "Temporary Config File Rename Failed!");
      } else {
        lastModified = configFile.lastModified();
      }

    } catch (Exception e) {
      WebLogger.getLogger(mAppName).printStackTrace(e);
    }
  }
}
