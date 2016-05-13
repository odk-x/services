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

package org.opendatakit.sync.service;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Build;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.common.android.application.AppAwareApplication;
import org.opendatakit.common.android.logic.CommonToolProperties;
import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.utilities.NameUtil;
import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.DatabaseConsts;
import org.opendatakit.database.OdkDbSerializedInterface;
import org.opendatakit.database.service.KeyValueStoreEntry;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.database.service.OdkDbInterface;
import org.opendatakit.sync.service.data.SynchronizationResult;
import org.opendatakit.sync.service.data.SynchronizationResult.Status;
import org.opendatakit.sync.service.data.TableResult;
import org.opendatakit.sync.service.logic.Synchronizer;
import org.opendatakit.sync.service.logic.Synchronizer.SynchronizerStatus;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SyncExecutionContext implements SynchronizerStatus {

  private static final String TAG = "SyncExecutionContext";
  private static final String ACCOUNT_TYPE_G = "com.google";

  private static final int OVERALL_PROGRESS_BAR_LENGTH = 6350400;
  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
  }

  static final AtomicBoolean refreshRequired = new AtomicBoolean(false);

  public static final void refreshActivityUINeeded(String appName) {
    refreshRequired.set(true);
    WebLogger.getLogger(appName).e(TAG, "FROM UI THREAD: triggering another polling cycle");
  }

  public static void invalidateAuthToken(AppAwareApplication application, String appName) {
    PropertiesSingleton props = CommonToolProperties.get(application, appName);
    AccountManager.get(application).invalidateAuthToken(ACCOUNT_TYPE_G, props.getProperty(CommonToolProperties.KEY_AUTH));
    props.removeProperty(CommonToolProperties.KEY_AUTH);
    props.writeProperties();
    refreshActivityUINeeded(appName);
  }

  /**
   * The results of the synchronization that we will pass back to the user.
   */
  private final SynchronizationResult mUserResult;

  private int nMajorSyncSteps;
  private int iMajorSyncStep;
  private int GRAINS_PER_MAJOR_SYNC_STEP;

  private final AppAwareApplication application;
  private final String appName;
  private final String odkClientApiVersion;
  private final String userAgent;

  private final String aggregateUri;
  private final String authenticationType;
  private final String googleAccount;
  private final String username;
  private final String password;

  private final SyncNotification syncProgress;

  // set this later
  private Synchronizer synchronizer;

  private OdkDbHandle odkDbHandle = null;

  public SyncExecutionContext(AppAwareApplication context, String appName,
      SyncNotification syncProgress,
      SynchronizationResult syncResult) {
    this.application = context;
    this.appName = appName;
    String versionCode = application.getVersionCodeString();
    this.odkClientApiVersion = versionCode.substring(0, versionCode.length() - 2);
    this.userAgent = "Sync " + application.getVersionCodeString() + " (gzip)";
    this.syncProgress = syncProgress;
    this.synchronizer = null;
    this.mUserResult = syncResult;

    PropertiesSingleton props = CommonToolProperties.get(context, appName);

    this.aggregateUri = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
    this.authenticationType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
    this.googleAccount = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
    this.username = props.getProperty(CommonToolProperties.KEY_USERNAME);
    this.password = props.getProperty(CommonToolProperties.KEY_PASSWORD);

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
  
  public void setAppLevelStatus(Status status) {
    mUserResult.setAppLevelStatus(status);
  }
  
  public TableResult getTableResult(String tableId) {
    return mUserResult.getTableResult(tableId);
  }
  
  public String getAppName() {
    return this.appName;
  }

  public String getOdkClientApiVersion() {
    return this.odkClientApiVersion;
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

  public AccountManager getAccountManager() {
    AccountManager accountManager = AccountManager.get(application);
    return accountManager;
  }

  public Account getAccount() {
    Account account = new Account(googleAccount, ACCOUNT_TYPE_G);
    return account;
  }

  public String getAccessToken() {
    PropertiesSingleton props = CommonToolProperties.get(application, appName);

    return props.getProperty(CommonToolProperties.KEY_AUTH);
  }

  public String getAuthenticationType() {
    return authenticationType;
  }

  public String getGoogleAccount() {
    return googleAccount;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  private int refCount = 1;

  public synchronized OdkDbHandle getDatabase() throws RemoteException {
    if ( odkDbHandle == null ) {
      odkDbHandle = getDatabaseService().openDatabase(appName);
    }
    if ( odkDbHandle == null ) {
      throw new IllegalStateException("Unable to obtain database handle from Services Services!");
    }
    ++refCount;
    return odkDbHandle;
  }

  public synchronized void releaseDatabase(OdkDbHandle odkDbHandle) throws RemoteException {
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

  public String getTableDisplayName(String tableId) throws RemoteException {
    OdkDbHandle db = null;
    try {
      db = getDatabase();

      List<KeyValueStoreEntry> displayNameList =
          getDatabaseService().getDBTableMetadata(appName, db, tableId,
              KeyValueStoreConstants.PARTITION_TABLE,
              KeyValueStoreConstants.ASPECT_DEFAULT,
              KeyValueStoreConstants.TABLE_DISPLAY_NAME);
      if ( displayNameList.size() != 1 ) {
        return NameUtil.constructSimpleDisplayName(tableId);
      }

      String rawDisplayName = displayNameList.get(0).value;
      if ( rawDisplayName == null ) {
        return NameUtil.constructSimpleDisplayName(tableId);
      }

      String displayName = ODKDataUtils.getLocalizedDisplayName(rawDisplayName);
      return displayName;
    } finally {
      releaseDatabase(db);
      db = null;
    }
  }

  private class ServiceConnectionWrapper implements ServiceConnection {

    @Override public void onServiceConnected(ComponentName name, IBinder service) {
      if (!name.getClassName().equals(DatabaseConsts.DATABASE_SERVICE_CLASS)) {
        WebLogger.getLogger(getAppName()).e(TAG, "Unrecognized service");
        return;
      }
      synchronized (odkDbInterfaceBindComplete) {
        try {
          odkDbInterface = (service == null) ? null : new OdkDbSerializedInterface(OdkDbInterface
              .Stub.asInterface(service));
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

  private ServiceConnectionWrapper odkDbServiceConnection = new ServiceConnectionWrapper();
  private Object odkDbInterfaceBindComplete = new Object();
  private OdkDbSerializedInterface odkDbInterface;
  private boolean active = false;

  public OdkDbSerializedInterface getDatabaseService() {

    synchronized (odkDbInterfaceBindComplete) {
      if ( odkDbInterface != null ) {
        return odkDbInterface;
      }
    }

    // block waiting for it to be bound...

    Log.i(TAG, "Attempting bind to Database service");
    Intent bind_intent = new Intent();
    bind_intent.setClassName(DatabaseConsts.DATABASE_SERVICE_PACKAGE,
        DatabaseConsts.DATABASE_SERVICE_CLASS);

    for (;;) {
      try {
        synchronized (odkDbInterfaceBindComplete) {
          if ( odkDbInterface != null ) {
            return odkDbInterface;
          }
          if ( !active ) {
            active = true;
            application.bindService(bind_intent, odkDbServiceConnection,
                Context.BIND_AUTO_CREATE | ((Build.VERSION.SDK_INT >= 14) ?
                    Context.BIND_ADJUST_WITH_ACTIVITY :
                    0));
          }

          odkDbInterfaceBindComplete.wait();

          if ( odkDbInterface != null ) {
            return odkDbInterface;
          }
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
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
    syncProgress.updateNotification(state, text, OVERALL_PROGRESS_BAR_LENGTH, (int) (iMajorSyncStep
        * GRAINS_PER_MAJOR_SYNC_STEP + ((progressPercentage != null) ? (progressPercentage
        * GRAINS_PER_MAJOR_SYNC_STEP / 100.0) : 0.0)), indeterminateProgress);
  }

}
