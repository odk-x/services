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

package org.opendatakit.services.sync.service;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Build;
import android.os.IBinder;
import android.util.Log;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.service.IDbInterface;
import org.opendatakit.database.service.InternalUserDbInterfaceAidlWrapperImpl;
import org.opendatakit.database.service.UserDbInterface;
import org.opendatakit.database.service.UserDbInterfaceImpl;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.properties.PropertyManager;
import org.opendatakit.services.sync.service.logic.Synchronizer;
import org.opendatakit.services.sync.service.logic.Synchronizer.SynchronizerStatus;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.sync.service.SyncOutcome;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.TableLevelResult;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.utilities.NameUtil;
import org.sqlite.database.sqlite.SQLiteException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SyncExecutionContext implements SynchronizerStatus {

  private static final String TAG = "SyncExecutionContext";
  private static final String ACCOUNT_TYPE_G = "com.google";

  private static final int OVERALL_PROGRESS_BAR_LENGTH = 6350400;

  /**
   * The results of the synchronization that we will pass back to the user.
   */
  private final SyncOverallResult mUserResult;

  private int nMajorSyncSteps;
  private int iMajorSyncStep;
  private int GRAINS_PER_MAJOR_SYNC_STEP;
  private final Context application;
  private final String appName;
  private final String userAgent;
  private final String aggregateUri;
  private final String authenticationType;
  private final String username;
  private final String password;
  private final String installationId;
  private final Boolean allowUnsafeAuthentication;

  private final String deviceId;

  private final SyncProgressTracker syncProgressTracker;

  // set this later
  private Synchronizer synchronizer;

  private DbHandle odkDbHandle = null;

  public SyncExecutionContext(Context context, String versionCode, String appName,
      SyncProgressTracker syncProgressTracker,
      SyncOverallResult syncResult) {
    this.application = context;
    this.appName = appName;
    this.userAgent = "Sync " + versionCode + " (gzip)";
    this.syncProgressTracker = syncProgressTracker;
    this.synchronizer = null;
    this.mUserResult = syncResult;

    PropertiesSingleton props = CommonToolProperties.get(context, appName);

    this.aggregateUri = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
    this.authenticationType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
    this.username = props.getProperty(CommonToolProperties.KEY_USERNAME);
    this.password = props.getProperty(CommonToolProperties.KEY_PASSWORD);

    this.installationId = props.getProperty(CommonToolProperties.KEY_INSTALLATION_ID);
    this.allowUnsafeAuthentication = props.getBooleanProperty(CommonToolProperties
        .KEY_ALLOW_NON_SECURE_AUTHENTICATION);

    PropertyManager propertyManager = new PropertyManager(context);
    this.deviceId = propertyManager.getSingularProperty(PropertyManager.OR_DEVICE_ID_PROPERTY,
        null);

    this.nMajorSyncSteps = 1;
    this.GRAINS_PER_MAJOR_SYNC_STEP = (OVERALL_PROGRESS_BAR_LENGTH / nMajorSyncSteps);
    this.iMajorSyncStep = 0;
  }

  public void setSynchronizer(Synchronizer synchronizer) {
    this.synchronizer = synchronizer;
  }

  public String getString(int resId) {
    return application.getString(resId);
  }

  public void signalPropertiesChange() {

    PropertiesSingleton props = CommonToolProperties.get(application, appName);
    props.signalPropertiesChange();
  }

  public void setAppLevelSyncOutcome(SyncOutcome syncOutcome) {
    mUserResult.setAppLevelSyncOutcome(syncOutcome);
  }

  public SyncOutcome getAppLevelSyncOutcome() {
    return mUserResult.getAppLevelSyncOutcome();
  }

  public SyncOutcome exceptionEquivalentOutcome(Throwable e) {
    if ( e instanceof IOException ) {
      // this occurs when JSON parser of response fails
      return (SyncOutcome.INCOMPATIBLE_SERVER_VERSION_EXCEPTION);
    } else if ( e instanceof JsonProcessingException ) {
      // something wrong with Jackson parser / serializer
      return (SyncOutcome.INCOMPATIBLE_SERVER_VERSION_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.AccessDeniedException) {
      return (SyncOutcome.ACCESS_DENIED_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.AccessDeniedReauthException) {
      return (SyncOutcome.ACCESS_DENIED_REAUTH_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.BadClientConfigException) {
      return (SyncOutcome.BAD_CLIENT_CONFIG_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.ClientDetectedVersionMismatchedServerResponseException) {
      return (SyncOutcome.INCOMPATIBLE_SERVER_VERSION_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.ClientDetectedMissingConfigForClientVersionException) {
      return (SyncOutcome.CLIENT_VERSION_FILES_DO_NOT_EXIST_ON_SERVER);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.InternalServerFailureException) {
      return (SyncOutcome.INTERNAL_SERVER_FAILURE_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.NetworkTransmissionException) {
      return (SyncOutcome.NETWORK_TRANSMISSION_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.NotOpenDataKitServerException) {
      return (SyncOutcome.NOT_OPEN_DATA_KIT_SERVER_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.ServerDetectedVersionMismatchedClientRequestException) {
      return (SyncOutcome.INCOMPATIBLE_SERVER_VERSION_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.UnexpectedServerRedirectionStatusCodeException) {
      return (SyncOutcome.UNEXPECTED_REDIRECT_EXCEPTION);
    } else if ( e instanceof IllegalArgumentException ) {
      return (SyncOutcome.LOCAL_DATABASE_EXCEPTION);
    } else if ( e instanceof IllegalStateException ) {
      return (SyncOutcome.LOCAL_DATABASE_EXCEPTION);
    } else if ( e instanceof SQLiteException) {
      return (SyncOutcome.LOCAL_DATABASE_EXCEPTION);
    } else if ( e instanceof ServicesAvailabilityException ) {
      return (SyncOutcome.LOCAL_DATABASE_EXCEPTION);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.SchemaMismatchException) {
      return (SyncOutcome.TABLE_SCHEMA_COLUMN_DEFINITION_MISMATCH);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.ServerDoesNotRecognizeAppNameException) {
      return (SyncOutcome.APPNAME_DOES_NOT_EXIST_ON_SERVER);
    } else if ( e instanceof org.opendatakit.services.sync.service.exceptions.IncompleteServerConfigFileBodyMissingException) {
      return (SyncOutcome.INCOMPLETE_SERVER_CONFIG_MISSING_FILE_BODY);
    } else {
      WebLogger.getLogger(appName).e(TAG, "Unrecognized exception");
      WebLogger.getLogger(appName).printStackTrace(e);
      return (SyncOutcome.FAILURE);
    }
  }

  public TableLevelResult getTableLevelResult(String tableId) {
    return mUserResult.fetchTableLevelResult(tableId);
  }

  public Context getApplication() {
    return application;
  }

  public String getAppName() {
    return this.appName;
  }

  public String getOdkClientApiVersion() {
    return Constants.ODK_CLIENT_API_VERSION;
  }

  public String getUserAgent() {
    return this.userAgent;
  }

  public String getAggregateUri() {
    return this.aggregateUri;
  }

  public Synchronizer getSynchronizer() {
    return synchronizer;
  }

  public String getAuthenticationType() {
    return authenticationType;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getInstallationId() {
    return installationId;
  }

  public boolean getAllowUnsafeAuthentication() {
    if ( allowUnsafeAuthentication == null ) {
      return false;
    } else {
      return allowUnsafeAuthentication;
    }
  }

  public HashMap<String,Object> getDeviceInfo() {
    HashMap<String,Object> deviceInfo = new HashMap<>();
    deviceInfo.put("androidSdkInt", Build.VERSION.SDK_INT);
    deviceInfo.put("androidDevice", Build.DEVICE);
    deviceInfo.put("androidDeviceDisplayString", Build.DISPLAY);
    deviceInfo.put("androidBuildFingerprint", Build.FINGERPRINT);
    deviceInfo.put("androidId", Build.ID);
    deviceInfo.put("androidBrand", Build.BRAND);
    deviceInfo.put("androidManufacturer", Build.MANUFACTURER);
    deviceInfo.put("androidModel", Build.MODEL);
    deviceInfo.put("androidHardware", Build.HARDWARE);
    deviceInfo.put("androidProduct", Build.PRODUCT);
    deviceInfo.put(PropertyManager.OR_DEVICE_ID_PROPERTY, deviceId );
    return deviceInfo;
  }

  public void setUserIdRolesListAndDefaultGroup(String user_id, String rolesList, String
      defaultGroup) {
    PropertiesSingleton props = CommonToolProperties.get(application, appName);

    Map<String,String> properties = new HashMap<String,String>();
    properties.put(CommonToolProperties.KEY_AUTHENTICATED_USER_ID, user_id);
    properties.put(CommonToolProperties.KEY_ROLES_LIST, rolesList);
    properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, defaultGroup);
    props.setProperties(properties);
  }

  public void setUsersList(String value) {
    PropertiesSingleton props = CommonToolProperties.get(application, appName);

    props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_USERS_LIST, value));
  }

  public void setAllToolsToReInitialize() {
    PropertiesSingleton props = CommonToolProperties.get(application, appName);

    props.setAllRunInitializationTasks();
  }

  private int refCount = 1;

  public synchronized DbHandle getDatabase() throws ServicesAvailabilityException {
    if ( odkDbHandle == null ) {
      odkDbHandle = getDatabaseService().openDatabase(appName);
    }
    if ( odkDbHandle == null ) {
      throw new IllegalStateException("Unable to obtain database handle from Services Services!");
    }
    ++refCount;
    return odkDbHandle;
  }

  public synchronized void releaseDatabase(DbHandle odkDbHandle) throws ServicesAvailabilityException {
    if ( odkDbHandle != null ) {
      if ( odkDbHandle != this.odkDbHandle ) {
        throw new IllegalArgumentException("Expected the internal odkDbHandle!");
      }
      --refCount;
      if ( refCount == 0 ) {
        try {
          getDatabaseService().closeDatabase(appName, odkDbHandle);
          this.odkDbHandle = null;
        } catch ( Exception e ) {
          WebLogger.getLogger(appName).printStackTrace(e);
        }
        throw new IllegalStateException("should never get here");
      }
    }
  }

  public String getTableDisplayName(String tableId) throws
      ServicesAvailabilityException {
     PropertiesSingleton props = CommonToolProperties.get(application, appName);

     String locale = props.getUserSelectedDefaultLocale();
     DbHandle db = null;
    try {
      db = getDatabase();

      String rawDisplayName = null;

      List<KeyValueStoreEntry> displayNameList =
          getDatabaseService().getTableMetadata(appName, db, tableId,
              KeyValueStoreConstants.PARTITION_TABLE,
              KeyValueStoreConstants.ASPECT_DEFAULT,
              KeyValueStoreConstants.TABLE_DISPLAY_NAME, null).getEntries();
      if ( displayNameList.size() == 1 ) {
        rawDisplayName = displayNameList.get(0).value;
      }

      if ( rawDisplayName == null ) {
        rawDisplayName = NameUtil.constructSimpleDisplayName(tableId);
      }

      String displayName = LocalizationUtils.getLocalizedDisplayName(appName, tableId,
          locale, rawDisplayName);
      return displayName;
    } finally {
      releaseDatabase(db);
      db = null;
    }
  }

  private class ServiceConnectionWrapper implements ServiceConnection {

    @Override public void onServiceConnected(ComponentName name, IBinder service) {

      if (!name.getClassName().equals(IntentConsts.Database.DATABASE_SERVICE_CLASS)) {
        WebLogger.getLogger(getAppName()).e(TAG, "Unrecognized service");
        return;
      }
      synchronized (odkDbInterfaceBindComplete) {
        try {
          odkDbInterface = (service == null) ? null : new UserDbInterfaceImpl(
              new InternalUserDbInterfaceAidlWrapperImpl(IDbInterface
              .Stub.asInterface(service)));
        } catch (IllegalArgumentException e) {
          odkDbInterface = null;
        }

        active = false;
        odkDbInterfaceBindComplete.notify();
      }
    }

    @Override public void onServiceDisconnected(ComponentName name) {
      synchronized (odkDbInterfaceBindComplete) {
        odkDbInterface = null;
        active = false;
        odkDbInterfaceBindComplete.notify();
      }
    }
  }

  private final ServiceConnectionWrapper odkDbServiceConnection = new ServiceConnectionWrapper();
  private final Object odkDbInterfaceBindComplete = new Object();
  private UserDbInterface odkDbInterface;
  private boolean active = false;


  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   */
  private UserDbInterface invokeBindService() throws InterruptedException {

    Log.i(TAG, "Attempting or polling on bind to Database service");
    Intent bind_intent = new Intent();
    bind_intent.setClassName(IntentConsts.Database.DATABASE_SERVICE_PACKAGE,
        IntentConsts.Database.DATABASE_SERVICE_CLASS);

    synchronized (odkDbInterfaceBindComplete) {
      if ( !active ) {
        active = true;
        application.bindService(bind_intent, odkDbServiceConnection,
            Context.BIND_AUTO_CREATE | Context.BIND_ADJUST_WITH_ACTIVITY);
      }

      odkDbInterfaceBindComplete.wait();

      if (odkDbInterface != null) {
        return odkDbInterface;
      }
    }
    return null;
  }

  public UserDbInterface getDatabaseService() {

    // block waiting for it to be bound...
    for (;;) {
      try {

        synchronized (odkDbInterfaceBindComplete) {
          if (odkDbInterface != null) {
            return odkDbInterface;
          }
        }

        // call method that waits on odkDbInterfaceBindComplete
        // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
        UserDbInterface userDbInterface = invokeBindService();
        if ( userDbInterface != null ) {
          return userDbInterface;
        }

      } catch (InterruptedException e) {
        // expected if we are waiting. Ignore because we log bind attempt if spinning.
      }
    }
  }

  public void resetMajorSyncSteps(int nMajorSyncSteps) {
    this.nMajorSyncSteps = nMajorSyncSteps;
    this.GRAINS_PER_MAJOR_SYNC_STEP = (OVERALL_PROGRESS_BAR_LENGTH / nMajorSyncSteps);
    this.iMajorSyncStep = 0;
  }
  
  public void incMajorSyncStep() {
    ++iMajorSyncStep;
    if ( iMajorSyncStep > nMajorSyncSteps ) {
      iMajorSyncStep = nMajorSyncSteps - 1;
    }
  }
  
  @Override
  public void updateNotification(SyncProgressState state, int textResource, Object[] formatArgVals,
                                 Double progressPercentage, boolean indeterminateProgress) {
    String text = "Bad text resource id: " + textResource + "!";
    String fmt = application.getString(textResource);
    if (fmt != null) {
      if (formatArgVals == null) {
        text = fmt;
      } else {
        text = String.format(fmt, formatArgVals);
      }
    }
    syncProgressTracker.updateNotification(state, text, OVERALL_PROGRESS_BAR_LENGTH, (int) (iMajorSyncStep
        * GRAINS_PER_MAJOR_SYNC_STEP + ((progressPercentage != null) ? (progressPercentage
        * GRAINS_PER_MAJOR_SYNC_STEP / 100.0) : 0.0)), indeterminateProgress);
  }

}
