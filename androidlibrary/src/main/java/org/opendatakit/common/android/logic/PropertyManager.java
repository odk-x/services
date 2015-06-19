/*
 * Copyright (C) 2009-2013 University of Washington
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

import java.util.HashMap;
import java.util.Locale;

import android.content.Context;
import android.net.wifi.WifiInfo;
import android.net.wifi.WifiManager;
import android.provider.Settings;
import android.telephony.TelephonyManager;

/**
 * Used to return device properties to JavaRosa
 *
 * @author Yaw Anokwa (yanokwa@gmail.com)
 * @author mitchellsundt@gmail.com
 */

public class PropertyManager {

  public interface DynamicPropertiesInterface {
    String getUsername();

    String getUserEmail();

    String getAppName();

    String getInstanceDirectory();

    String getUriFragmentNewInstanceFile(String uriDeviceId, String extension);
  };

  private HashMap<String, String> mProperties;

  public final static String DEVICE_ID_PROPERTY = "deviceid"; // imei
  public final static String SUBSCRIBER_ID_PROPERTY = "subscriberid"; // imsi
  public final static String SIM_SERIAL_PROPERTY = "simserial";
  public final static String PHONE_NUMBER_PROPERTY = "phonenumber";

  public final static String OR_DEVICE_ID_PROPERTY = "uri:deviceid"; // imei
  public final static String OR_SUBSCRIBER_ID_PROPERTY = "uri:subscriberid"; // imsi
  public final static String OR_SIM_SERIAL_PROPERTY = "uri:simserial";
  public final static String OR_PHONE_NUMBER_PROPERTY = "uri:phonenumber";

  /**
   * These properties are dynamic and accessed through the
   * DynamicPropertiesInterface. As with all property names,
   * they are compared in a case-insensitive manner.
   */

  // username -- current username
  public final static String USERNAME = "username";
  public final static String OR_USERNAME = "uri:username";
  // email -- current account email
  public final static String EMAIL = "email";
  public final static String OR_EMAIL = "uri:email";
  public final static String APP_NAME = "appName";
  // instanceDirectory -- directory containing media files for current instance
  public final static String INSTANCE_DIRECTORY = "instancedirectory";
  // uriFragmentNewFile -- the appName-relative uri for a non-existent file with the given extension
  public final static String URI_FRAGMENT_NEW_INSTANCE_FILE_WITHOUT_COLON = "urifragmentnewinstancefile";
  public final static String URI_FRAGMENT_NEW_INSTANCE_FILE = URI_FRAGMENT_NEW_INSTANCE_FILE_WITHOUT_COLON + ":";

  /**
   * Constructor used within the Application object to create a singleton of the
   * property manager. Access it through
   * Survey.getInstance().getPropertyManager()
   *
   * @param context
   */
  public PropertyManager(Context context) {

    mProperties = new HashMap<String, String>();
    TelephonyManager mTelephonyManager = (TelephonyManager) context
        .getSystemService(Context.TELEPHONY_SERVICE);

    String deviceId = mTelephonyManager.getDeviceId();
    String orDeviceId = null;
    if (deviceId != null) {
      if ((deviceId.contains("*") || deviceId.contains("000000000000000"))) {
        deviceId = Settings.Secure.getString(context.getContentResolver(),
            Settings.Secure.ANDROID_ID);
        orDeviceId = Settings.Secure.ANDROID_ID + ":" + deviceId;
      } else {
        orDeviceId = "imei:" + deviceId;
      }
    }

    if (deviceId == null) {
      // no SIM -- WiFi only
      // Retrieve WiFiManager
      WifiManager wifi = (WifiManager) context.getSystemService(Context.WIFI_SERVICE);

      // Get WiFi status
      WifiInfo info = wifi.getConnectionInfo();
      if (info != null) {
        deviceId = info.getMacAddress();
        orDeviceId = "mac:" + deviceId;
      }
    }

    // if it is still null, use ANDROID_ID
    if (deviceId == null) {
      deviceId = Settings.Secure
          .getString(context.getContentResolver(), Settings.Secure.ANDROID_ID);
      orDeviceId = Settings.Secure.ANDROID_ID + ":" + deviceId;
    }

    mProperties.put(DEVICE_ID_PROPERTY, deviceId);
    mProperties.put(OR_DEVICE_ID_PROPERTY, orDeviceId);

    String value;

    value = mTelephonyManager.getSubscriberId();
    if (value != null) {
      mProperties.put(SUBSCRIBER_ID_PROPERTY, value);
      mProperties.put(OR_SUBSCRIBER_ID_PROPERTY, "imsi:" + value);
    }
    value = mTelephonyManager.getSimSerialNumber();
    if (value != null) {
      mProperties.put(SIM_SERIAL_PROPERTY, value);
      mProperties.put(OR_SIM_SERIAL_PROPERTY, "simserial:" + value);
    }
    value = mTelephonyManager.getLine1Number();
    if (value != null) {
      mProperties.put(PHONE_NUMBER_PROPERTY, value);
      mProperties.put(OR_PHONE_NUMBER_PROPERTY, "tel:" + value);
    }
  }

  public String getSingularProperty(String rawPropertyName, DynamicPropertiesInterface callback) {

    String propertyName = rawPropertyName.toLowerCase(Locale.ENGLISH);

    // retrieve the dynamic values via the callback...
    if (USERNAME.equals(propertyName)) {
      return callback.getUsername();
    } else if (OR_USERNAME.equals(propertyName)) {
      String value = callback.getUsername();
      if (value == null)
        return null;
      return "username:" + value;
    } else if (EMAIL.equals(propertyName)) {
      return callback.getUserEmail();
    } else if (OR_EMAIL.equals(propertyName)) {
      String value = callback.getUserEmail();
      if (value == null)
        return null;
      return "mailto:" + value;
    } else if (APP_NAME.equals(propertyName)) {
      String value = callback.getAppName();
      if (value == null)
        return null;
      return value;
    } else if (INSTANCE_DIRECTORY.equals(propertyName)) {
      String value = callback.getInstanceDirectory();
      if (value == null)
        return null;
      return value;
    } else if (propertyName.startsWith(URI_FRAGMENT_NEW_INSTANCE_FILE_WITHOUT_COLON)) {
      // grab the requested extension, if any...
      String ext;
      if (propertyName.startsWith(URI_FRAGMENT_NEW_INSTANCE_FILE)) {
        ext = rawPropertyName.substring(URI_FRAGMENT_NEW_INSTANCE_FILE.length());
      } else {
        ext = "";
      }
      String value = callback.getUriFragmentNewInstanceFile(mProperties.get(OR_DEVICE_ID_PROPERTY),ext);
      return value;
    } else {
      return mProperties.get(propertyName);
    }
  }
}
