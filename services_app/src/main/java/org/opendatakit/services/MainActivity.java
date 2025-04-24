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
import android.net.Uri;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.core.app.ActivityCompat;
import androidx.core.splashscreen.SplashScreen;
import androidx.core.view.GravityCompat;
import androidx.drawerlayout.widget.DrawerLayout;
import androidx.lifecycle.LifecycleEventObserver;
import androidx.lifecycle.ViewModelProvider;
import androidx.navigation.NavController;
import androidx.navigation.Navigation;

import com.google.android.material.appbar.MaterialToolbar;
import com.google.android.material.dialog.MaterialAlertDialogBuilder;
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
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.sync.actions.viewModels.AbsSyncViewModel;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.RuntimePermissionUtils;

import java.util.Collections;

public class MainActivity extends AppCompatActivity implements IAppAwareActivity,
        ActivityCompat.OnRequestPermissionsResultCallback {

    /**
     * Class handling actions corresponding to Button Clicks
     */
    private class OnButtonClick implements View.OnClickListener {

        @Override
        public void onClick(View v) {
            if (v.getId() == R.id.btnDrawerOpenMainActivity) {
                drawerLayout.openDrawer(GravityCompat.START);
            } else if (v.getId() == R.id.btnDrawerClose) {
                drawerLayout.closeDrawer(GravityCompat.START);
            } else if (v.getId() == R.id.btnDrawerLogin) {
                if (absSyncViewModel.getUserState() == UserState.LOGGED_OUT) {
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
                navController.navigate(R.id.aboutMenuFragment);
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
            } else if (item.getItemId() == R.id.drawer_update_credentials) {
                Intent signInIntent = new Intent(MainActivity.this, LoginActivity.class);
                signInIntent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
                signInIntent.putExtra(Constants.LOGIN_INTENT_TYPE_KEY, Constants.LOGIN_TYPE_UPDATE_CREDENTIALS);
                startActivity(signInIntent);
                return true;
            } else if (item.getItemId() == R.id.drawer_switch_sign_in_type) {
                Intent signInIntent = new Intent(MainActivity.this, LoginActivity.class);
                signInIntent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
                signInIntent.putExtra(Constants.LOGIN_INTENT_TYPE_KEY, Constants.LOGIN_TYPE_SWITCH_SIGN_IN_TYPE);
                startActivity(signInIntent);
                return true;
            } else if (item.getItemId() == R.id.drawer_server_login) {
                navController.navigate(R.id.updateServerSettingsFragment);
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
    private Button btnDrawerSignIn;
    private NavigationView navView;
    private DrawerLayout drawerLayout;

    private NavController navController;
    private AbsSyncViewModel absSyncViewModel;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        SplashScreen.installSplashScreen(this);
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
        setupViewModelAndNavController();
        handleLifecycleEvents();
    }

    /**
     * Finding the different views required and attaching onClick Listeners to them
     */
    private void findViewsAndAttachListeners() {
        toolbar = findViewById(R.id.toolbarMainActivity);
        navView = findViewById(R.id.navViewMain);
        drawerLayout = findViewById(R.id.drawerLayoutMain);

        btnDrawerSignIn = navView.getHeaderView(0).findViewById(R.id.btnDrawerLogin);

        ImageButton btnDrawerOpen = findViewById(R.id.btnDrawerOpenMainActivity);
        ImageButton btnDrawerClose = navView.getHeaderView(0).findViewById(R.id.btnDrawerClose);

        toolbar.setOnMenuItemClickListener(new OnToolbarMenuItemClick());
        navView.setNavigationItemSelectedListener(new OnDrawerMenuItemClick());

        OnButtonClick onButtonClick = new OnButtonClick();

        btnDrawerSignIn.setOnClickListener(onButtonClick);

        btnDrawerOpen.setOnClickListener(onButtonClick);
        btnDrawerClose.setOnClickListener(onButtonClick);
    }

    private void setupViewModelAndNavController() {
        absSyncViewModel = new ViewModelProvider(this).get(AbsSyncViewModel.class);
        navController = Navigation.findNavController(this, R.id.navHostMain);

        absSyncViewModel.getCurrentUserState().observe(this, userState -> {
            if (userState == UserState.LOGGED_OUT) {
                inLoggedOutState();
            } else if (userState == UserState.ANONYMOUS) {
                inAnonymousState();
            } else {
                inAuthenticatedState();
            }
        });

        absSyncViewModel.checkIsFirstLaunch().observe(this, aBoolean -> {
            if (aBoolean) {
                onFirstLaunch();
            }
        });

        absSyncViewModel.checkIsAnonymousAllowed().observe(this, aBoolean -> {
            if (absSyncViewModel.getUserState() == UserState.AUTHENTICATED_USER) {
                setSwitchSignInEnabled(aBoolean);
            } else
                setSwitchSignInEnabled(true);
        });

        navController.addOnDestinationChangedListener((controller, destination, arguments) -> {
            Menu menu = navView.getMenu();
            menu.findItem(R.id.drawer_about_us).setEnabled(destination.getId() != R.id.aboutMenuFragment);

            if (destination.getId() == R.id.updateServerSettingsFragment) {
                toolbar.setVisibility(View.GONE);
                drawerLayout.setDrawerLockMode(DrawerLayout.LOCK_MODE_LOCKED_CLOSED);
            } else {
                toolbar.setVisibility(View.VISIBLE);
                drawerLayout.setDrawerLockMode(DrawerLayout.LOCK_MODE_UNLOCKED);
            }

            if (mAppName != null)
                updateViewModelWithProps();
        });
    }

    private void handleLifecycleEvents() {
        getLifecycle().addObserver((LifecycleEventObserver) (source, event) -> {
            switch (event) {
                case ON_RESUME: {
                    // Do this in on resume so that if we resolve a row it will be refreshed
                    // when we come back.
                    mAppName = getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
                    if (mAppName == null) {
                        mAppName = ODKFileUtils.getOdkDefaultAppName();
                    }
                    updateViewModelWithProps();
                    break;
                }
                case ON_DESTROY: {
                    WebLogger.closeAll();
                    break;
                }
            }
        });
    }

    public void updateViewModelWithProps() {
        PropertiesSingleton props = CommonToolProperties.get(this, getAppName());

        absSyncViewModel.setAppName(getAppName());

        absSyncViewModel.setIsFirstLaunch(Boolean.parseBoolean(props.getProperty(CommonToolProperties.KEY_FIRST_LAUNCH)));

        absSyncViewModel.setServerUrl(props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL));

        absSyncViewModel.setCurrentUserState(UserState.valueOf(props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE)));
        absSyncViewModel.setUsername(props.getProperty(CommonToolProperties.KEY_USERNAME));

        boolean isAnonymousSignInUsed = Boolean.parseBoolean(props.getProperty(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED));

        if (isAnonymousSignInUsed) {
            boolean isAnonymousAllowed = Boolean.parseBoolean(props.getProperty(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED));
            absSyncViewModel.setIsAnonymousAllowed(isAnonymousAllowed);
        }

        absSyncViewModel.setIsAnonymousSignInUsed(isAnonymousSignInUsed);

        String lastSyncStr = props.getProperty(CommonToolProperties.KEY_LAST_SYNC_INFO);
        if (lastSyncStr != null) {
            absSyncViewModel.setIsLastSyncTimeAvailable(true);
            absSyncViewModel.setLastSyncTime(Long.parseLong(lastSyncStr));
        } else
            absSyncViewModel.setIsLastSyncTimeAvailable(false);
    }

    /**
     * Actions in the Logged-Out User State
     */
    private void inLoggedOutState() {
        handleDrawerVisibility(false, false, false);
        btnDrawerSignIn.setText(R.string.drawer_sign_in_button_text);
        setSyncItemVisible(false);
    }

    /**
     * Actions in the Anonymous User State
     */
    private void inAnonymousState() {
        handleDrawerVisibility(true, true, false);
        btnDrawerSignIn.setText(R.string.drawer_sign_out_button_text);
        setSyncItemVisible(true);
    }

    /**
     * Actions in the Authenticated User State
     */
    private void inAuthenticatedState() {
        handleDrawerVisibility(true, true, true);
        btnDrawerSignIn.setText(R.string.drawer_sign_out_button_text);
        setSyncItemVisible(true);
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
        ODKServicesPropertyUtils.clearActiveUser(CommonToolProperties.get(this, mAppName));
        drawerLayout.closeDrawer(GravityCompat.START);
        updateViewModelWithProps();
    }

    private void setSwitchSignInEnabled(boolean enabled) {
        navView.getMenu().findItem(R.id.drawer_switch_sign_in_type).setEnabled(enabled);
    }

    private void setSyncItemVisible(boolean visible) {
        toolbar.getMenu().findItem(R.id.action_sync).setVisible(visible);
    }

    private void onFirstLaunch() {
        PropertiesSingleton props = CommonToolProperties.get(MainActivity.this, mAppName);
        props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FIRST_LAUNCH, Boolean.toString(false)));

        /**
         * MaterialAlertDialogBuilder is standard for all ODK-X Apps
         * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
         */

        /**
         * New dialog styling
         * MaterialAlertDialogBuilder is standard for all ODK-X Apps
         * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
         * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
         */

        AlertDialog alertDialog = new MaterialAlertDialogBuilder(MainActivity.this,R.style.OdkXAlertDialogStyle)
                .setMessage(R.string.configure_server_settings)
                .setCancelable(false)
                .setPositiveButton(R.string.yes, (dialog, which) -> {
                    dialog.dismiss();
                    navController.navigate(R.id.updateServerSettingsFragment);
                })
                .setNegativeButton(R.string.no, (dialog, which) -> dialog.dismiss()).create();

        alertDialog.setCanceledOnTouchOutside(false);
        alertDialog.show();
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