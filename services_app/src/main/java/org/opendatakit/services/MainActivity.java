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

package org.opendatakit.services;

import android.Manifest;
import android.app.Activity;
import android.app.Fragment;
import android.app.FragmentManager;
import android.app.FragmentTransaction;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.Toast;
import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.fragment.AboutMenuFragment;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.resolve.conflict.AllConflictsResolutionActivity;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.RuntimePermissionUtils;

public class MainActivity extends Activity implements IAppAwareActivity,
    ActivityCompat.OnRequestPermissionsResultCallback {

  // Used for logging
  @SuppressWarnings("unused") private static final String TAG = MainActivity.class.getSimpleName();

  private static final int EXT_STORAGE_REQ_CODE = 0;

  protected static final String[] REQUIRED_PERMISSIONS = new String[] {
      Manifest.permission.READ_EXTERNAL_STORAGE,
      Manifest.permission.WRITE_EXTERNAL_STORAGE
  };

  private int SYNC_ACTIVITY_RESULT_CODE = 10;
  private int VERIFY_SERVER_SETTINGS_ACTIVITY_RESULT_CODE = 20;
  private int RESOLVE_CONFLICT_ACTIVITY_RESULT_CODE = 30;
  private int SETTINGS_ACTIVITY_RESULT_CODE = 100;

  private String mAppName;
  private boolean permissionOnly;

  @Override
  protected void onDestroy() {
    super.onDestroy();
    WebLogger.closeAll();
  }

  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_main);

    // IMPORTANT NOTE: the Application object is not yet created!

    // Used to ensure that the singleton has been initialized properly
    AndroidConnectFactory.configure();

    this.permissionOnly = getIntent().getBooleanExtra(IntentConsts.INTENT_KEY_PERMISSION_ONLY, false);

    if (!RuntimePermissionUtils.checkSelfAllPermission(this, REQUIRED_PERMISSIONS)) {
      ActivityCompat.requestPermissions(
          this,
          REQUIRED_PERMISSIONS,
          EXT_STORAGE_REQ_CODE
      );
    }
  }

  @Override
  protected void onResume() {
    super.onResume();
    // Do this in on resume so that if we resolve a row it will be refreshed
    // when we come back.
    mAppName = getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if (mAppName == null) {
      mAppName = ODKFileUtils.getOdkDefaultAppName();
      //      Log.e(TAG, IntentConsts.INTENT_KEY_APP_NAME + " not supplied on intent");
      //      setResult(Activity.RESULT_CANCELED);
      //      finish();
      //      return;
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

    if (id == R.id.action_verify_server_settings) {
      Intent i = new Intent(this, VerifyServerSettingsActivity.class);
      i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
      startActivityForResult(i, VERIFY_SERVER_SETTINGS_ACTIVITY_RESULT_CODE);
      return true;
    }

    if (id == R.id.action_resolve_conflict) {
      Intent i = new Intent(this, AllConflictsResolutionActivity.class);
      i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
      startActivityForResult(i, RESOLVE_CONFLICT_ACTIVITY_RESULT_CODE);
      return true;
    }

    if (id == R.id.action_about) {

      FragmentManager mgr = getFragmentManager();
      Fragment newFragment = mgr.findFragmentByTag(AboutMenuFragment.NAME);
      if (newFragment == null) {
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

    if (id == R.id.action_change_user) {

      Intent i = new Intent(this, LoginActivity.class);
      i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
      startActivity(i);
      return true;
    }

    return super.onOptionsItemSelected(item);
  }

  @Override
  public String getAppName() {
    return mAppName;
  }

  @Override
  public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
    super.onRequestPermissionsResult(requestCode, permissions, grantResults);

    if (requestCode != EXT_STORAGE_REQ_CODE) {
      return;
    }

    if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
      if (permissionOnly) {
        setResult(Activity.RESULT_OK);
        finish();
      }
      return;
    }

    if (RuntimePermissionUtils.shouldShowAnyPermissionRationale(this, permissions)) {
      RuntimePermissionUtils.createPermissionRationaleDialog(this, requestCode, permissions)
          .setMessage(R.string.write_external_storage_rationale)
          .setNegativeButton(R.string.cancel, new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
              dialog.cancel();
              setResult(Activity.RESULT_CANCELED);
              finish();
            }
          })
          .show();
    } else {
      Toast
          .makeText(this, R.string.write_external_perm_denied, Toast.LENGTH_LONG)
          .show();
      setResult(Activity.RESULT_CANCELED);
      finish();
    }
  }
}