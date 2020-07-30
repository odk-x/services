/*
 * Copyright (C) 2017 University of Washington
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

package org.opendatakit.services.sync.actions.activities;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.ComponentName;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Bundle;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentTransaction;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.fragment.AboutMenuFragment;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.resolve.conflict.AllConflictsResolutionActivity;
import org.opendatakit.sync.service.IOdkSyncServiceInterface;
import org.opendatakit.utilities.ODKFileUtils;

/**
 * An activity that lays the foundations of sync funcationality but can be extended to implement
 * different user interfaces on top.
 *
 * Created by jbeorse on 5/31/17.
 */

public abstract class AbsSyncBaseActivity extends AppCompatActivity
    implements IAppAwareActivity, IOdkAppPropertiesActivity, ISyncServiceInterfaceActivity,
    ServiceConnection {

   private static final String TAG = AbsSyncBaseActivity.class.getSimpleName();

   public static final int AUTHORIZE_ACCOUNT_RESULT_CODE = 1;
   protected static final int RESOLVE_CONFLICT_ACTIVITY_RESULT_CODE = 30;
   protected static final int SETTINGS_ACTIVITY_RESULT_CODE = 100;

   protected String mAppName;
   protected PropertiesSingleton mProps;
   private boolean started = false;

   private final Object interfaceGuard = new Object();
   // interfaceGuard guards access to all of the following...
   private IOdkSyncServiceInterface odkSyncInterfaceGuarded;
   private boolean mBoundGuarded = false;
   // end guarded access.

   @Override public void onServiceConnected(ComponentName name, IBinder service) {
      if (!name.getClassName().equals(IntentConsts.Sync.SYNC_SERVICE_CLASS)) {
         WebLogger.getLogger(getAppName()).e(TAG, "[onServiceConnected] Unrecognized service");
         return;
      }

      synchronized (interfaceGuard) {
         odkSyncInterfaceGuarded = (service == null) ?
             null :
             IOdkSyncServiceInterface.Stub.asInterface(service);
         mBoundGuarded = (odkSyncInterfaceGuarded != null);
      }
      WebLogger.getLogger(getAppName()).i(TAG, "[onServiceConnected] Bound to sync service");
   }

   @Override public void onServiceDisconnected(ComponentName name) {
      WebLogger.getLogger(getAppName()).i(TAG, "[onServiceDisconnected] Unbound to sync service");
      synchronized (interfaceGuard) {
         odkSyncInterfaceGuarded = null;
         mBoundGuarded = false;
      }
   }

   /**
    * called by fragments that want to do something on the sync service connection.
    *
    * @param callback - callback for fragments that want to use sync service
    */
   public void invokeSyncInterfaceAction(DoSyncActionCallback callback) {
      try {
         boolean bound;
         IOdkSyncServiceInterface theInterface;
         synchronized (interfaceGuard) {
            theInterface = odkSyncInterfaceGuarded;
            bound = mBoundGuarded;

         }
         if (callback != null) {
            callback.doAction(theInterface);
         }
      } catch (RemoteException e) {
         WebLogger.getLogger(getAppName()).printStackTrace(e);
         WebLogger.getLogger(getAppName())
             .e(TAG, " [invokeSyncInterfaceAction] exception while invoking sync service");
         Toast.makeText(this, " [invokeSyncInterfaceAction] Exception while invoking sync service",
             Toast.LENGTH_LONG).show();
      }
   }

   @Override protected void onCreate(Bundle savedInstanceState) {
      super.onCreate(savedInstanceState);

      WebLogger.getLogger(getAppName()).i(TAG, " [onCreate]");
      setContentView(R.layout.sync_activity);

      // IMPORTANT NOTE: the Application object is not yet created!
      // Used to ensure that the singleton has been initialized properly
      AndroidConnectFactory.configure();

      // Used by app designer grunt task "clean"
      if (getIntent() != null && getIntent().hasExtra("showLogin")) {
         if (savedInstanceState != null && savedInstanceState.containsKey("started")) {
            started = savedInstanceState.getBoolean("started");
         }
         if (!started) {
            started = true;
            Intent i = new Intent(this, LoginActivity.class);
            i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
            startActivity(i);
         }
      }

   }

   @Override protected void onResume() {
      super.onResume();

      // Do this in on resume so that if we resolve a row it will be refreshed
      // when we come back.
      if (getAppName() == null) {
         Log.e(TAG, IntentConsts.INTENT_KEY_APP_NAME + " [onResume] not supplied on intent");
         setResult(Activity.RESULT_CANCELED);
         finish();
         return;
      }

      try {
         WebLogger.getLogger(getAppName()).i(TAG, "[onResume] Attempting bind to sync service");
         Intent bind_intent = new Intent();
         bind_intent.setClassName(IntentConsts.Sync.APPLICATION_NAME,
             IntentConsts.Sync.SYNC_SERVICE_CLASS);
         bindService(bind_intent, this,
             Context.BIND_AUTO_CREATE | Context.BIND_ADJUST_WITH_ACTIVITY);
      } catch (Exception e) {
         e.printStackTrace();
      }

   }

   @Override public void onSaveInstanceState(Bundle outState) {
      super.onSaveInstanceState(outState);
      outState.putBoolean("started", started);
   }

   @Override
   protected void onDestroy() {

      WebLogger.getLogger(getAppName()).i(TAG, " [onDestroy]");

      super.onDestroy();
   }

   @Override
   protected void onPause() {
      super.onPause();


      boolean callUnbind = false;
      synchronized (interfaceGuard) {
         callUnbind = mBoundGuarded;
         odkSyncInterfaceGuarded = null;
         mBoundGuarded = false;
      }

      if (callUnbind) {
         unbindService(this);
         WebLogger.getLogger(getAppName()).i(TAG, " [onPause] Unbound to sync service");
      }

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

   @Override public String getAppName() {
      if (mAppName == null) {
         mAppName = getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
         if(mAppName == null) {
            mAppName = ODKFileUtils.getOdkDefaultAppName();
         }
         Log.i(TAG, mAppName);
      }
      return mAppName;
   }

   @Override
   public boolean onCreateOptionsMenu(Menu menu) {
      // Inflate the menu; this adds items to the action bar if it is present.
      getMenuInflater().inflate(R.menu.main, menu);
      return true;
   }

   @Override
   public boolean onPrepareOptionsMenu(Menu menu) {
      menu.findItem(R.id.action_sync).setVisible(false);
      menu.findItem(R.id.action_verify_server_settings).setVisible(false);
      menu.findItem(R.id.action_change_user).setVisible(false);
      // right?
      return super.onPrepareOptionsMenu(menu);
   }

   @Override
   public boolean onOptionsItemSelected(MenuItem item) {

      // Handle action bar item clicks here. The action bar will
      // automatically handle clicks on the Home/Up button, so long
      // as you specify a parent activity in AndroidManifest.xml.
      int id = item.getItemId();
      if (id == R.id.action_sync) {
         return true;
      }
      if (id == R.id.action_verify_server_settings) {
         return true;
      }

      if (id == R.id.action_resolve_conflict) {
         Intent i = new Intent(this, AllConflictsResolutionActivity.class);
         i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
         startActivityForResult(i, RESOLVE_CONFLICT_ACTIVITY_RESULT_CODE);
         return true;
      }

      if (id == R.id.action_about) {

         FragmentManager mgr = getSupportFragmentManager();
         Fragment newFragment = mgr.findFragmentByTag(AboutMenuFragment.NAME);
         if ( newFragment == null ) {
            newFragment = new AboutMenuFragment();
         }
         FragmentTransaction trans = mgr.beginTransaction();
         trans.replace(R.id.sync_activity_view, newFragment, AboutMenuFragment.NAME);
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

      if (id == R.id.action_change_user) {

         Intent i = new Intent(this, LoginActivity.class);
         i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
         startActivity(i);
         return true;
      }
      return super.onOptionsItemSelected(item);
   }

   @Override public PropertiesSingleton getProps() {
      if (mProps == null) {
         mProps = CommonToolProperties.get(this, getAppName());
      }
      return mProps;
   }

   public static void showAuthenticationErrorDialog(final Activity activity, String message) {
      AlertDialog.Builder builder = new AlertDialog.Builder(activity);
      builder.setTitle(R.string.authentication_error);
      builder.setMessage(message);
      builder.setNeutralButton(android.R.string.ok, new DialogInterface.OnClickListener() {
         public void onClick(DialogInterface dialog, int id) {
            activity.finish();
            dialog.dismiss();
         }
      });
      AlertDialog dialog = builder.create();
      dialog.show();
   }
}
