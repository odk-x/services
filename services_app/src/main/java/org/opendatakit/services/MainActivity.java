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
import org.opendatakit.services.sync.actions.activities.UpdateServerSettingsActivity;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.RuntimePermissionUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MainActivity extends AppCompatActivity implements IAppAwareActivity,
        ActivityCompat.OnRequestPermissionsResultCallback {

    /**
     * Class handling actions corresponding to Button Clicks
     */
    private class OnButtonClick implements View.OnClickListener {

        @Override
        public void onClick(View v) {
            if (v.getId() == R.id.btnSignInMain) {
                onSignInButtonClicked();
            } else if (v.getId() == R.id.btnDrawerOpen) {
                drawerLayout.openDrawer(GravityCompat.START);
            } else if (v.getId() == R.id.btnDrawerClose) {
                drawerLayout.closeDrawer(GravityCompat.START);
            } else if (v.getId() == R.id.btnDrawerLogin) {
                if (userState == UserState.LOGGED_OUT) {
                    onSignInButtonClicked();
                } else {
                    onSignOutButtonClicked();
                }
            }
        }
    }

    /**
     * Class handling actions corresponding to Toolbar-Menu Item Click
     */
    private class OnToolbarMenuItemClick implements Toolbar.OnMenuItemClickListener {

        @Override
        public boolean onMenuItemClick(MenuItem item) {
            int id = item.getItemId();

            if (id == R.id.action_sync) {
                Intent i = new Intent(MainActivity.this, SyncActivity.class);
                i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
                startActivityForResult(i, SYNC_ACTIVITY_RESULT_CODE);
                return true;
            } else if (id == R.id.action_verify_server_settings) {
                Intent i = new Intent(MainActivity.this, VerifyServerSettingsActivity.class);
                i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
                startActivityForResult(i, VERIFY_SERVER_SETTINGS_ACTIVITY_RESULT_CODE);
                return true;
            } else if (id == R.id.action_settings) {
                Intent intent = new Intent(MainActivity.this, AppPropertiesActivity.class);
                intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
                startActivityForResult(intent, SETTINGS_ACTIVITY_RESULT_CODE);
                return true;
            }

            return false;
        }
    }

    /**
     * Class handling actions corresponding to Drawer Menu Item Click
     */
    private class OnDrawerMenuItemClick implements NavigationView.OnNavigationItemSelectedListener {

        @Override
        public boolean onNavigationItemSelected(MenuItem item) {
            drawerLayout.closeDrawer(GravityCompat.START);

            if (item.getItemId() == R.id.drawer_resolve_conflict) {
                Intent i = new Intent(MainActivity.this, AllConflictsResolutionActivity.class);
                i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
                startActivityForResult(i, RESOLVE_CONFLICT_ACTIVITY_RESULT_CODE);
                return true;
            } else if (item.getItemId() == R.id.drawer_about_us) {
                Intent intent=new Intent(MainActivity.this, UpdateServerSettingsActivity.class);
                intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
                startActivity(intent);
//                FragmentManager mgr = getSupportFragmentManager();
//                GoToAboutFragment.GotoAboutFragment(mgr, R.id.main_activity_view);
//                item.setVisible(false);
                return true;
            } else if (item.getItemId() == R.id.drawer_settings) {
                Intent intent = new Intent(MainActivity.this, AppPropertiesActivity.class);
                intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
                startActivityForResult(intent, SETTINGS_ACTIVITY_RESULT_CODE);
                return true;
            } else if (item.getItemId() == R.id.drawer_docs) {
                Intent browserIntent = new Intent(
                        Intent.ACTION_VIEW,
                        Uri.parse(getString(R.string.opendatakit_url))
                );

                if (browserIntent.resolveActivity(MainActivity.this.getApplicationContext().getPackageManager()) != null) {
                    startActivity(browserIntent);
                } else {
                    Intent i = new Intent(MainActivity.this, DocumentationWebViewActivity.class);
                    startActivity(i);
                }

                return true;
            } else if (item.getItemId() == R.id.drawer_update_credentials){

                return true;
            }

            return false;
        }
    }

    @SuppressWarnings("unused")
    private static final String TAG = MainActivity.class.getSimpleName();

    private static final int EXT_STORAGE_REQ_CODE = 0;

    protected static final String[] REQUIRED_PERMISSIONS = new String[]{
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
    private NavigationView navView;
    private DrawerLayout drawerLayout;

    private PropertiesSingleton props;
    private UserState userState;

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

        findViewsAndAttachListeners();
    }

    @Override
    public void onBackPressed() {
        int count = getSupportFragmentManager().getBackStackEntryCount();
        if (count == 1) {
            startActivity(new Intent(MainActivity.this, MainActivity.class));
            overridePendingTransition(0, 0);
            finish();
        } else
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

    /**
     * Finding the different views required and attaching onClick Listeners to them
     */
    private void findViewsAndAttachListeners() {
        toolbar = findViewById(R.id.toolbarMainActivity);

        tvServerUrl = findViewById(R.id.tvServerUrlMain);
        tvUserState = findViewById(R.id.tvUserStateMain);
        tvUsernameLabel = findViewById(R.id.tvUsernameLabelMain);
        tvUsername = findViewById(R.id.tvUsernameMain);
        tvLastSyncTimeLabel = findViewById(R.id.tvLastSyncTimeLabelMain);
        tvLastSyncTime = findViewById(R.id.tvLastSyncTimeMain);

        navView = findViewById(R.id.navViewMain);
        drawerLayout = findViewById(R.id.drawerLayoutMain);

        btnSignIn = findViewById(R.id.btnSignInMain);
        btnDrawerSignIn = navView.getHeaderView(0).findViewById(R.id.btnDrawerLogin);

        ImageButton btnDrawerOpen = findViewById(R.id.btnDrawerOpen);
        ImageButton btnDrawerClose = navView.getHeaderView(0).findViewById(R.id.btnDrawerClose);

        toolbar.setOnMenuItemClickListener(new OnToolbarMenuItemClick());
        navView.setNavigationItemSelectedListener(new OnDrawerMenuItemClick());

        OnButtonClick onButtonClick = new OnButtonClick();

        btnSignIn.setOnClickListener(onButtonClick);
        btnDrawerSignIn.setOnClickListener(onButtonClick);

        btnDrawerOpen.setOnClickListener(onButtonClick);
        btnDrawerClose.setOnClickListener(onButtonClick);
    }

    /**
     * Updating the Current User State and performing ui-updates corresponding to the User State
     */
    private void updateInterface() {
        props = CommonToolProperties.get(this, mAppName);
        userState = UserState.valueOf(props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE));

        updateCommonInfo();

        if (userState == UserState.LOGGED_OUT) {
            inLoggedOutState();
        } else if (userState == UserState.ANONYMOUS) {
            inAnonymousState();
        } else {
            inAuthenticatedState();
        }
    }

    /**
     * Actions in the Logged-Out User State
     */
    private void inLoggedOutState() {
        handleViewVisibility(View.VISIBLE, View.GONE, View.GONE, false);
        handleDrawerVisibility(false, false, false);

        tvUserState.setText(R.string.logged_out);
    }

    /**
     * Actions in the Anonymous User State
     */
    private void inAnonymousState() {
        handleViewVisibility(View.GONE, View.GONE, View.VISIBLE, true);
        handleDrawerVisibility(true, true, false);
        displayLastSyncTime();

        tvUserState.setText(R.string.anonymous_user);
    }

    /**
     * Actions in the Authenticated User State
     */
    private void inAuthenticatedState() {
        handleViewVisibility(View.GONE, View.VISIBLE, View.VISIBLE, true);
        handleDrawerVisibility(true, true, true);
        displayLastSyncTime();

        tvUserState.setText(R.string.authenticated_user);

        String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
        tvUsername.setText(username);
    }

    /**
     * Handling the Visibility of Menu Items in the Navigation Drawer
     * It also sets the Text of Sign-In button with respect to the User State
     *
     * @param resolve_visible        : The Visibility of Resolve Conflicts Item
     * @param switch_sign_in_visible : The Visibility of Switch Sign-In Method Item
     * @param update_cred_visible    : The Visibility of Update User Credentials Item
     */
    private void handleDrawerVisibility(boolean resolve_visible, boolean switch_sign_in_visible, boolean update_cred_visible) {
        Menu menu = navView.getMenu();
        menu.findItem(R.id.drawer_resolve_conflict).setVisible(resolve_visible);
        menu.findItem(R.id.drawer_switch_sign_in_type).setVisible(switch_sign_in_visible);
        menu.findItem(R.id.drawer_update_credentials).setVisible(update_cred_visible);
        if (userState == UserState.LOGGED_OUT) {
            btnDrawerSignIn.setText(R.string.drawer_sign_in_button_text);
        } else {
            btnDrawerSignIn.setText(R.string.drawer_sign_out_button_text);
        }
    }

    /**
     * Sets the Visibility of Different Views on the Main Screen
     *
     * @param btnSignInVisible    : The Visibility of the Sign-In Button
     * @param usernameVisible     : The Visibility of the Username
     * @param lastSyncTimeVisible : The Visibility of the Last Sync Time
     * @param syncIconVisible     : The Visibility of the Sync Icon on Toolbar
     */
    private void handleViewVisibility(int btnSignInVisible, int usernameVisible, int lastSyncTimeVisible, boolean syncIconVisible) {
        btnSignIn.setVisibility(btnSignInVisible);
        tvUsernameLabel.setVisibility(usernameVisible);
        tvUsername.setVisibility(usernameVisible);
        tvLastSyncTimeLabel.setVisibility(lastSyncTimeVisible);
        tvLastSyncTime.setVisibility(lastSyncTimeVisible);
        toolbar.getMenu().findItem(R.id.action_sync).setVisible(syncIconVisible);
    }

    /**
     * Updating the Common Information such as Server URL
     */
    private void updateCommonInfo() {
        String serverUrl = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
        tvServerUrl.setText(serverUrl);
        tvServerUrl.setPaintFlags(tvServerUrl.getPaintFlags() | Paint.UNDERLINE_TEXT_FLAG);
    }

    /**
     * Displaying the Last Sync Time
     */
    private void displayLastSyncTime() {
        String timestamp = props.getProperty(CommonToolProperties.KEY_LAST_SYNC_INFO);
        if (timestamp != null) {
            SimpleDateFormat sdf = new SimpleDateFormat(Constants.DATE_TIME_FORMAT);
            String ts = sdf.format(new Date(Long.parseLong(timestamp)));
            tvLastSyncTime.setText(ts);
        } else {
            tvLastSyncTime.setText(getResources().getString(R.string.last_sync_not_available));
        }
    }

    /**
     * Actions on Clicking on the Sign-In Button
     */
    private void onSignInButtonClicked() {
        Intent signInIntent = new Intent(this, LoginActivity.class);
        signInIntent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
        startActivity(signInIntent);
    }

    /**
     * Actions on Clicking on the Sign-Out Button
     */
    private void onSignOutButtonClicked() {
        ODKServicesPropertyUtils.clearActiveUser(props);
        drawerLayout.closeDrawer(GravityCompat.START);
        updateInterface();
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