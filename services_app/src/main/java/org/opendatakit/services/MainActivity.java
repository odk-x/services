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
import android.content.DialogInterface;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Paint;
import android.net.Uri;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.core.app.ActivityCompat;
import androidx.core.view.GravityCompat;
import androidx.drawerlayout.widget.DrawerLayout;
import androidx.fragment.app.FragmentManager;

import com.google.android.material.appbar.MaterialToolbar;
import com.google.android.material.navigation.NavigationView;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.preferences.activities.DocumentationWebViewActivity;
import org.opendatakit.services.resolve.conflict.AllConflictsResolutionActivity;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.services.utilities.GoToAboutFragment;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.RuntimePermissionUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MainActivity extends AppCompatActivity implements IAppAwareActivity,
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

  //Defining the Different Views added in the Layout
  private MaterialToolbar toolbar;
  private TextView tvServerUrl, tvUserState, tvUsernameLabel, tvUsername, tvLastSyncTimeLabel, tvLastSyncTime;
  private Button btnSignIn, btnDrawerSignIn;
  private ImageButton btnDrawerClose;
  private NavigationView navView;
  private DrawerLayout drawerLayout;

  PropertiesSingleton props;
  private String UserState;

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

    findViewAndAttachListeners();
  }

  @Override
  public void onBackPressed() {
    int count = getSupportFragmentManager().getBackStackEntryCount();
    if (count == 1) {
      startActivity(new Intent(MainActivity.this,MainActivity.class));
      overridePendingTransition(0, 0);
      finish();
    }
    else
      super.onBackPressed();
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
    updateInterface();
  }

  //  Finding the Different Views from the Layout and attaching OnClickListeners to Buttons and Menu Items
  private void findViewAndAttachListeners(){

    toolbar=findViewById(R.id.toolbarMainActivity);

    tvServerUrl=findViewById(R.id.tvServerUrlMain);
    tvUserState=findViewById(R.id.tvUserStateMain);
    tvUsernameLabel=findViewById(R.id.tvUsernameLabelMain);
    tvUsername=findViewById(R.id.tvUsernameMain);
    tvLastSyncTimeLabel=findViewById(R.id.tvLastSyncTimeLabelMain);
    tvLastSyncTime=findViewById(R.id.tvLastSyncTimeMain);

    btnSignIn=findViewById(R.id.btnSignInMain);

    navView=findViewById(R.id.navViewMain);

    drawerLayout=findViewById(R.id.drawerLayoutMain);

    btnDrawerSignIn=navView.getHeaderView(0).findViewById(R.id.btnDrawerLogin);
    btnDrawerClose=navView.getHeaderView(0).findViewById(R.id.btnDrawerClose);

    toolbar.setOnMenuItemClickListener(new Toolbar.OnMenuItemClickListener() {
      @Override
      public boolean onMenuItemClick(MenuItem item) {
        return onToolbarMenuItemClicked(item);
      }
    });

    navView.setNavigationItemSelectedListener(new NavigationView.OnNavigationItemSelectedListener() {
      @Override
      public boolean onNavigationItemSelected(MenuItem item) {
        return onDrawerMenuItemClicked(item);
      }
    });

    btnDrawerSignIn.setOnClickListener(new View.OnClickListener() {
      @Override
      public void onClick(View v) {
        //  This will open the Sign-In page if the User is Logged Out else would sign out the Current User
        if(UserState.equals(CommonToolProperties.USER_STATE_LOGGED_OUT)){
          onSignInButtonClicked();
        }
        else {
          onSignOutButtonClicked();
        }
      }
    });

    btnDrawerClose.setOnClickListener(new View.OnClickListener() {
      @Override
      public void onClick(View v) {
        //Closing the Navigation Drawer
        drawerLayout.closeDrawer(GravityCompat.START);
      }
    });

    btnSignIn.setOnClickListener(new View.OnClickListener() {
      @Override
      public void onClick(View v) {
        onSignInButtonClicked();
      }
    });
  }

  // Getting the Latest User Properties and Updating the UI according to the User State
  private void updateInterface(){
    props= CommonToolProperties.get(this,mAppName);

    String serverUrl=props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
    UserState=props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE);

    updateDrawerMenu();

    tvServerUrl.setText(serverUrl);
    tvServerUrl.setPaintFlags(tvServerUrl.getPaintFlags() | Paint.UNDERLINE_TEXT_FLAG);

    tvUserState.setText(UserState);

    if(UserState.equals(CommonToolProperties.USER_STATE_LOGGED_OUT)){
      btnSignIn.setVisibility(View.VISIBLE);

      tvUsernameLabel.setVisibility(View.GONE);
      tvUsername.setVisibility(View.GONE);

      tvLastSyncTimeLabel.setVisibility(View.GONE);
      tvLastSyncTime.setVisibility(View.GONE);

      toolbar.getMenu().findItem(R.id.action_sync).setVisible(false);
    }
    else {
      btnSignIn.setVisibility(View.GONE);

      tvLastSyncTimeLabel.setVisibility(View.VISIBLE);
      tvLastSyncTime.setVisibility(View.VISIBLE);

      String timestamp = props.getProperty(CommonToolProperties.KEY_LAST_SYNC_INFO);

      if (timestamp != null) {
        SimpleDateFormat sdf = new SimpleDateFormat(Constants.DATE_TIME_FORMAT);
        String ts = sdf.format(new Date(Long.parseLong(timestamp)));
        tvLastSyncTime.setText(ts);
      }
      else {
        tvLastSyncTime.setText(getResources().getString(R.string.last_sync_not_available));
      }

      toolbar.getMenu().findItem(R.id.action_sync).setVisible(true);

      if (UserState.equals(CommonToolProperties.USER_STATE_ANONYMOUS)) {
        tvUsernameLabel.setVisibility(View.GONE);
        tvUsername.setVisibility(View.GONE);
      }
      else {
        tvUsernameLabel.setVisibility(View.VISIBLE);
        tvUsername.setVisibility(View.VISIBLE);

        String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
        tvUsername.setText(username);
      }
    }
  }

  // Updating the Menu Options according to the Current User State
  private void updateDrawerMenu(){
    Menu menu=navView.getMenu();
    if(UserState.equals(CommonToolProperties.USER_STATE_LOGGED_OUT)){
      menu.findItem(R.id.drawer_resolve_conflict).setVisible(false);
      menu.findItem(R.id.drawer_switch_sign_in_type).setVisible(false);
      menu.findItem(R.id.drawer_update_credentials).setVisible(false);
      btnDrawerSignIn.setText(R.string.drawer_sign_in_button_text);
    }
    else if(UserState.equals(CommonToolProperties.USER_STATE_ANONYMOUS)){
      menu.findItem(R.id.drawer_resolve_conflict).setVisible(true);
      menu.findItem(R.id.drawer_switch_sign_in_type).setVisible(true);
      menu.findItem(R.id.drawer_update_credentials).setVisible(false);
      btnDrawerSignIn.setText(R.string.drawer_sign_out_button_text);
    }
    else {
      menu.findItem(R.id.drawer_resolve_conflict).setVisible(true);
      menu.findItem(R.id.drawer_switch_sign_in_type).setVisible(true);
      menu.findItem(R.id.drawer_update_credentials).setVisible(true);
      btnDrawerSignIn.setText(R.string.drawer_sign_out_button_text);
    }
  }

  // Starting the Login Activity
  private void onSignInButtonClicked(){
    Intent i = new Intent(this, LoginActivity.class);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
    startActivity(i);
  }

  // Signing Out the User
  private void onSignOutButtonClicked(){
    ODKServicesPropertyUtils.clearActiveUser(props);
    drawerLayout.closeDrawer(GravityCompat.START);
    updateInterface();
  }

  // Action on Clicking the Toolbar Menu Item
  private boolean onToolbarMenuItemClicked(MenuItem item){
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

    if (id == R.id.action_settings) {

      Intent intent = new Intent(this, AppPropertiesActivity.class);
      intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
      startActivityForResult(intent, SETTINGS_ACTIVITY_RESULT_CODE);
      return true;
    }

    return false;
  }

  // Action on Clicking the Drawer Menu Item
  private boolean onDrawerMenuItemClicked(MenuItem item){

    drawerLayout.closeDrawer(GravityCompat.START);

    if(item.getItemId()==R.id.drawer_resolve_conflict){
      Intent i = new Intent(this, AllConflictsResolutionActivity.class);
      i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
      startActivityForResult(i, RESOLVE_CONFLICT_ACTIVITY_RESULT_CODE);
      return true;
    }

    if(item.getItemId()==R.id.drawer_about_us){
      FragmentManager mgr = getSupportFragmentManager();
      GoToAboutFragment.GotoAboutFragment(mgr,R.id.main_activity_view);
      item.setVisible(false);
      return true;
    }

    if(item.getItemId()==R.id.drawer_settings){
      Intent intent = new Intent(this, AppPropertiesActivity.class);
      intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
      startActivityForResult(intent, SETTINGS_ACTIVITY_RESULT_CODE);
      return true;
    }

    if(item.getItemId()==R.id.drawer_docs){
      Intent browserIntent = new Intent(
              Intent.ACTION_VIEW,
              Uri.parse(getString(R.string.opendatakit_url))
      );

      if (browserIntent.resolveActivity(this.getApplicationContext().getPackageManager()) != null) {
        startActivity(browserIntent);
      } else {
        Intent i = new Intent(this, DocumentationWebViewActivity.class);
        startActivity(i);
      }

      return true;
    }

    return false;
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