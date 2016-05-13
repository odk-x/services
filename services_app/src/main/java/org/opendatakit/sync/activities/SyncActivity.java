/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.sync.activities;

import android.app.Activity;
import android.app.Fragment;
import android.app.FragmentManager;
import android.app.FragmentTransaction;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Build;
import android.os.Bundle;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;

import android.view.Menu;
import android.view.MenuItem;
import android.widget.Toast;
import org.opendatakit.IntentConsts;
import org.opendatakit.common.android.activities.IAppAwareActivity;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.fragment.AboutMenuFragment;
import org.opendatakit.common.android.logic.CommonToolProperties;
import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.common.android.activities.IOdkAppPropertiesActivity;
import org.opendatakit.common.android.activities.AppPropertiesActivity;
import org.opendatakit.sync.service.OdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.SyncStatus;

/**
 * An activity for handling server conflicts.
 * If an IntentConsts.INTENT_KEY_INSTANCE_ID is provided,
 * opens the row resolution fragment. Otherwise, opens
 * the list resolution fragment.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class SyncActivity extends Activity implements IAppAwareActivity,
    IOdkAppPropertiesActivity, ISyncServiceInterfaceActivity, ServiceConnection {

  private static final String TAG = SyncActivity.class.getSimpleName();

  public static final int AUTHORIZE_ACCOUNT_RESULT_CODE = 1;
  private int SYNC_ACTIVITY_RESULT_CODE = 10;
  private int SETTINGS_ACTIVITY_RESULT_CODE = 100;

  private String mAppName;
  private PropertiesSingleton mProps;

  // odkSyncInterfaceBindComplete guards access to all of the following...
  private Object odkSyncInterfaceBindComplete = new Object();
  private volatile OdkSyncServiceInterface odkSyncInterface;
  private volatile DoSyncActionCallback doSyncActionCallback = null;
  private volatile boolean active = false;
  private volatile boolean deactivate = true;
  // end guarded access.

  @Override public void onServiceConnected(ComponentName name, IBinder service) {
    if (!name.getClassName().equals("org.opendatakit.sync.service.OdkSyncService")) {
      WebLogger.getLogger(getAppName()).e(TAG, "Unrecognized service");
      return;
    }
    boolean isDeactivated = true;
    synchronized (odkSyncInterfaceBindComplete) {
      odkSyncInterface = (service == null) ? null : OdkSyncServiceInterface.Stub.asInterface(service);
      active = false;
      isDeactivated = deactivate;
    }
    if ( !isDeactivated ) {
      verifySyncInterface();
    }
  }

  @Override public void onServiceDisconnected(ComponentName name) {
    boolean isDeactivated = true;
    synchronized (odkSyncInterfaceBindComplete) {
      odkSyncInterface = null;
      active = false;
      isDeactivated = deactivate;
    }
    if ( !isDeactivated ) {
      verifySyncInterface();
    }
  }


  private void verifySyncInterface() {
    OdkSyncServiceInterface syncServiceInterface = null;
    DoSyncActionCallback callback = null;

    synchronized (odkSyncInterfaceBindComplete) {
      if ( odkSyncInterface != null ) {
        syncServiceInterface = odkSyncInterface;
        callback = doSyncActionCallback;
      }
    }

    try {
      if (syncServiceInterface != null && callback != null ) {
        doSyncInterfaceAction(syncServiceInterface, callback);
      }
    } catch ( RemoteException e ) {
      WebLogger.getLogger(getAppName()).printStackTrace(e);
      WebLogger.getLogger(getAppName()).e(TAG, "exception while invoking sync service");
      Toast.makeText(this, "Exception while invoking sync service", Toast.LENGTH_LONG).show();
      syncServiceInterface = null;
    }

    if ( syncServiceInterface == null ) {
      boolean alreadyActive = true;
      synchronized (odkSyncInterfaceBindComplete) {
        alreadyActive = active;
        if (!active) {
          active = true;
        }
      }

      if ( !alreadyActive ) {
        try {
          // Otherwise, set up a bind and attempt to re-tickle...
          WebLogger.getLogger(getAppName()).i(TAG, "Attempting bind to Database service");
          Intent bind_intent = new Intent();
          bind_intent.setClassName(IntentConsts.Sync.APPLICATION_NAME,
              IntentConsts.Sync.SERVICE_NAME);

          bindService(bind_intent, this,
              Context.BIND_AUTO_CREATE | ((Build.VERSION.SDK_INT >= 14) ?
                  Context.BIND_ADJUST_WITH_ACTIVITY :
                  0));
        } catch ( Exception e ) {
          synchronized (odkSyncInterfaceBindComplete) {
            active = false;
          }
        }
      }
    }
  }

  private void doSyncInterfaceAction(OdkSyncServiceInterface syncServiceInterface,
      DoSyncActionCallback doSyncActionCallback) throws RemoteException {
    doSyncActionCallback.doAction(syncServiceInterface);
  }

  /**
   * called by fragments that want to do something on the sync service connection.
   *
   * @param callback
    */
  public void invokeSyncInterfaceAction(DoSyncActionCallback callback) {
    DoSyncActionCallback oldCallback = null;
    boolean isDeactivated = true;
    synchronized (odkSyncInterfaceBindComplete) {
      oldCallback = doSyncActionCallback;
      doSyncActionCallback = callback;
      active = false;
      isDeactivated = deactivate;
    }
    if ( oldCallback != null ) {
//      try {
//        oldCallback.doAction(null);
//      } catch (RemoteException e) {
//        // never happens
//      }
    }
    if ( !isDeactivated ) {
      // and trigger firing of callback
      verifySyncInterface();
    }
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.sync_activity);

    // IMPORTANT NOTE: the Application object is not yet created!

    // Used to ensure that the singleton has been initialized properly
    AndroidConnectFactory.configure();
  }

  @Override
  protected void onResume() {
    super.onResume();

    synchronized (odkSyncInterfaceBindComplete) {
      deactivate = false;
    }

    // Do this in on resume so that if we resolve a row it will be refreshed
    // when we come back.
    mAppName = getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if (mAppName == null) {
      Log.e(TAG, IntentConsts.INTENT_KEY_APP_NAME + " not supplied on intent");
      setResult(Activity.RESULT_CANCELED);
      finish();
      return;
    }

    FragmentManager mgr = getFragmentManager();
    String newFragmentName = null;
    Fragment newFragment = null;

    // we want the list fragment
    newFragmentName = SyncFragment.NAME;
    newFragment = mgr.findFragmentByTag(newFragmentName);
    if ( newFragment == null ) {
      newFragment = new SyncFragment();
    }

    FragmentTransaction trans = mgr.beginTransaction();
    trans.replace(R.id.sync_activity_view, newFragment, newFragmentName);
    trans.commit();
  }

  @Override protected void onPause() {
    super.onPause();
    // release the stale interface (and let it get cleaned up eventually)
    synchronized (odkSyncInterfaceBindComplete) {
      deactivate = true;
      odkSyncInterface = null;
    }
  }

  @Override
  public boolean onCreateOptionsMenu(Menu menu) {
    // Inflate the menu; this adds items to the action bar if it is present.
    getMenuInflater().inflate(R.menu.main, menu);
    return true;
  }

  @Override
  public boolean onOptionsItemSelected(MenuItem item) {
    // Handle action bar item clicks here. The action bar will
    // automatically handle clicks on the Home/Up button, so long
    // as you specify a parent activity in AndroidManifest.xml.
    int id = item.getItemId();
    if (id == R.id.action_sync) {
      Intent i = new Intent(this, SyncActivity.class);
      i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
      startActivityForResult(i, SYNC_ACTIVITY_RESULT_CODE);
      return true;
    }

    if (id == R.id.action_about) {

      FragmentManager mgr = getFragmentManager();
      Fragment newFragment = mgr.findFragmentByTag(AboutMenuFragment.NAME);
      if ( newFragment == null ) {
        newFragment = new AboutMenuFragment();
      }
      FragmentTransaction trans = mgr.beginTransaction();
      trans.replace(R.id.main_activity_view, newFragment, AboutMenuFragment.NAME);
      trans.addToBackStack(AboutMenuFragment.NAME);
      trans.commit();

      return true;
    }

    if (id == R.id.action_settings) {

      Intent intent = new Intent(this, AppPropertiesActivity.class);
      intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
      startActivityForResult(intent, SETTINGS_ACTIVITY_RESULT_CODE);
      return true;
    }
    return super.onOptionsItemSelected(item);
  }

  @Override public String getAppName() {
    return mAppName;
  }

  @Override
  public PropertiesSingleton getProps() {

    if ( mProps == null ) {
      mProps = CommonToolProperties.get(this, mAppName);
    }
    return mProps;
  }
}
