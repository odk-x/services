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
  private final int SYNC_ACTIVITY_RESULT_CODE = 10;
  private final int SETTINGS_ACTIVITY_RESULT_CODE = 100;

  private String mAppName;
  private PropertiesSingleton mProps;

  // odkSyncInterfaceBindComplete guards access to all of the following...
  private OdkSyncServiceInterface odkSyncInterface;
  private boolean mBound = false;
  // end guarded access.

  @Override public void onServiceConnected(ComponentName name, IBinder service) {
    if (!name.getClassName().equals("org.opendatakit.sync.service.OdkSyncService")) {
      WebLogger.getLogger(getAppName()).e(TAG, "[onServiceConnected] Unrecognized service");
      return;
    }

    odkSyncInterface = (service == null) ? null : OdkSyncServiceInterface.Stub.asInterface(service);
    setBound(true);
    WebLogger.getLogger(getAppName()).i(TAG, "[onServiceConnected] Bound to sync service");
  }

  @Override public void onServiceDisconnected(ComponentName name) {
    WebLogger.getLogger(getAppName()).i(TAG, "[onServiceDisconnected] Unbound to sync service");
    setBound(false);
  }

  /**
   * called by fragments that want to do something on the sync service connection.
   *
   * @param callback - callback for fragments that want to use sync service
   */
  public void invokeSyncInterfaceAction(DoSyncActionCallback callback) {
    try {
      boolean bound = getBound();
      if (odkSyncInterface != null && callback != null && bound == true) {
        callback.doAction(odkSyncInterface);
      } else {
        if (callback != null) {
          callback.doAction(odkSyncInterface);
        }
      }
    } catch (RemoteException e) {
      WebLogger.getLogger(getAppName()).printStackTrace(e);
      WebLogger.getLogger(getAppName()).e(TAG, " [invokeSyncInterfaceAction] exception while invoking sync service");
      Toast.makeText(this, " [invokeSyncInterfaceAction] Exception while invoking sync service", Toast.LENGTH_LONG).show();
    }
  }

  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    WebLogger.getLogger(getAppName()).i(TAG, " [onCreate]");
    setContentView(R.layout.sync_activity);

    // IMPORTANT NOTE: the Application object is not yet created!

    // Used to ensure that the singleton has been initialized properly
    AndroidConnectFactory.configure();

    try {
      WebLogger.getLogger(getAppName()).i(TAG, "[onCreate] Attempting bind to sync service");
      Intent bind_intent = new Intent();
      bind_intent.setClassName(IntentConsts.Sync.APPLICATION_NAME,
              IntentConsts.Sync.SERVICE_NAME);
      bindService(bind_intent, this,
              Context.BIND_AUTO_CREATE | ((Build.VERSION.SDK_INT >= 14) ?
                      Context.BIND_ADJUST_WITH_ACTIVITY :
                      0));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void onResume() {
    super.onResume();

    WebLogger.getLogger(getAppName()).i(TAG, " [onResume]");

    // Do this in on resume so that if we resolve a row it will be refreshed
    // when we come back.
    if (getAppName() == null) {
      Log.e(TAG, IntentConsts.INTENT_KEY_APP_NAME + " [onResume] not supplied on intent");
      setResult(Activity.RESULT_CANCELED);
      finish();
      return;
    }

    WebLogger.getLogger(getAppName()).i(TAG, "[onResume] getting SyncFragment");

    FragmentManager mgr = getFragmentManager();
    String newFragmentName;
    Fragment newFragment;

    // we want the list fragment
    newFragmentName = SyncFragment.NAME;
    newFragment = mgr.findFragmentByTag(newFragmentName);
    if ( newFragment == null ) {
      newFragment = new SyncFragment();
      WebLogger.getLogger(getAppName()).i(TAG, "[onResume] creating new SyncFragment");
    }

    FragmentTransaction trans = mgr.beginTransaction();
    trans.replace(R.id.sync_activity_view, newFragment, newFragmentName);
    WebLogger.getLogger(getAppName()).i(TAG, "[onResume] replacing fragment with id " + newFragment.getId());
    trans.commit();
  }

  @Override
  protected void onDestroy() {

    WebLogger.getLogger(getAppName()).i(TAG, " [onDestroy]");

    if (getBound()) {
      unbindService(this);
      setBound(false);
      WebLogger.getLogger(getAppName()).i(TAG, " [onDestroy] Unbound to sync service");
    }

    super.onDestroy();
  }

  @Override public void onBackPressed() {
    super.onBackPressed();
    setResult(RESULT_CANCELED);
    finish();
  }

  @Override
  protected void onPause() {
    super.onPause();

    WebLogger.getLogger(getAppName()).i(TAG, " [onPause]");
  }

  @Override
  protected void onStop() {
    super.onStop();

    WebLogger.getLogger(getAppName()).i(TAG, " [onStop]");
  }

  @Override
  protected void onStart() {
    super.onStart();

    WebLogger.getLogger(getAppName()).i(TAG, " [onStart]");
  }

  @Override
  protected void onRestart() {
    super.onRestart();

    WebLogger.getLogger(getAppName()).i(TAG, " [onRestart]");
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
    if (mAppName == null) {
      mAppName = getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    }
    return mAppName;
  }

  private void setBound (boolean bound) {
    mBound = bound;
  }

  private boolean getBound() {
    return mBound;
  }

  @Override
  public PropertiesSingleton getProps() {

    if ( mProps == null ) {
      mProps = CommonToolProperties.get(this, mAppName);
    }
    return mProps;
  }
}
