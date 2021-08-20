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
import android.net.Uri;
import android.os.Bundle;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.Toolbar;
import androidx.core.view.GravityCompat;
import androidx.drawerlayout.widget.DrawerLayout;
import androidx.lifecycle.LifecycleEventObserver;
import androidx.navigation.NavController;
import androidx.navigation.Navigation;

import com.google.android.material.appbar.MaterialToolbar;
import com.google.android.material.navigation.NavigationView;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.preferences.activities.DocumentationWebViewActivity;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.resolve.conflict.AllConflictsResolutionActivity;
import org.opendatakit.services.sync.actions.viewModels.AbsSyncViewModel;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.sync.service.IOdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.utilities.ODKFileUtils;

import java.util.Collections;

/**
 * An activity that lays the foundations of sync funcationality but can be extended to implement
 * different user interfaces on top.
 * <p>
 * Created by jbeorse on 5/31/17.
 */

public abstract class AbsSyncBaseActivity extends AppCompatActivity
        implements IAppAwareActivity, IOdkAppPropertiesActivity, ISyncServiceInterfaceActivity,
        ServiceConnection {

    /**
     * Class handling actions corresponding to Button Clicks
     */
    private class OnButtonClick implements View.OnClickListener {
        @Override
        public void onClick(View v) {
            if (v.getId() == R.id.btnDrawerOpen) {
                drawerLayout.openDrawer(GravityCompat.START);
            } else if (v.getId() == R.id.btnDrawerClose) {
                drawerLayout.closeDrawer(GravityCompat.START);
            } else if (v.getId() == R.id.btnDrawerLogin) {
                if (absSyncViewModel.getUserState() == UserState.LOGGED_OUT) {
                    if (isNotLoginActivity())
                        onSignInButtonClicked();
                    else
                        drawerLayout.closeDrawer(GravityCompat.START);
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

            if (id == R.id.action_settings) {
                Intent intent = new Intent(AbsSyncBaseActivity.this, AppPropertiesActivity.class);
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
                Intent i = new Intent(AbsSyncBaseActivity.this, AllConflictsResolutionActivity.class);
                i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
                startActivityForResult(i, RESOLVE_CONFLICT_ACTIVITY_RESULT_CODE);
                return true;
            } else if (item.getItemId() == R.id.drawer_about_us) {
                navigateToAboutFragment();
                return true;
            } else if (item.getItemId() == R.id.drawer_settings) {
                Intent intent = new Intent(AbsSyncBaseActivity.this, AppPropertiesActivity.class);
                intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
                startActivityForResult(intent, SETTINGS_ACTIVITY_RESULT_CODE);
                return true;
            } else if (item.getItemId() == R.id.drawer_docs) {
                Intent browserIntent = new Intent(
                        Intent.ACTION_VIEW,
                        Uri.parse(getString(R.string.opendatakit_url))
                );

                if (browserIntent.resolveActivity(AbsSyncBaseActivity.this.getApplicationContext().getPackageManager()) != null) {
                    startActivity(browserIntent);
                } else {
                    Intent i = new Intent(AbsSyncBaseActivity.this, DocumentationWebViewActivity.class);
                    startActivity(i);
                }

                return true;
            } else if (item.getItemId() == R.id.drawer_update_credentials) {
                if (isNotLoginActivity()) {
                    Intent signInIntent = new Intent(AbsSyncBaseActivity.this, LoginActivity.class);
                    signInIntent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
                    signInIntent.putExtra(Constants.LOGIN_INTENT_TYPE_KEY, Constants.LOGIN_TYPE_UPDATE_CREDENTIALS);
                    startActivity(signInIntent);
                }
                return true;
            } else if (item.getItemId() == R.id.drawer_switch_sign_in_type) {
                if (isNotLoginActivity()) {
                    Intent signInIntent = new Intent(AbsSyncBaseActivity.this, LoginActivity.class);
                    signInIntent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
                    signInIntent.putExtra(Constants.LOGIN_INTENT_TYPE_KEY, Constants.LOGIN_TYPE_SWITCH_SIGN_IN_TYPE);
                    startActivity(signInIntent);
                }
                return true;
            }

            return false;
        }
    }

    private static final String TAG = AbsSyncBaseActivity.class.getSimpleName();

    public static final int AUTHORIZE_ACCOUNT_RESULT_CODE = 1;
    protected static final int RESOLVE_CONFLICT_ACTIVITY_RESULT_CODE = 30;
    protected static final int SETTINGS_ACTIVITY_RESULT_CODE = 100;

    protected String mAppName;
    protected PropertiesSingleton mProps;

    private final Object interfaceGuard = new Object();
    // interfaceGuard guards access to all of the following...
    private IOdkSyncServiceInterface odkSyncInterfaceGuarded;
    private boolean mBoundGuarded = false;
    // end guarded access.

    private NavigationView navView;
    private DrawerLayout drawerLayout;
    private Button btnDrawerSignIn;

    protected NavController navController;
    protected AbsSyncViewModel absSyncViewModel;

    abstract void initializeViewModelAndNavController();

    abstract void navigateToHomeFragment();

    abstract void navigateToAboutFragment();

    abstract void navigateToUpdateServerSettings();

    abstract boolean isNotLoginActivity();

    abstract boolean isCurrentDestinationAboutFragment();

    @Override
    public void onServiceConnected(ComponentName name, IBinder service) {
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

    @Override
    public void onServiceDisconnected(ComponentName name) {
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

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.sync_activity);

        // IMPORTANT NOTE: the Application object is not yet created!
        // Used to ensure that the singleton has been initialized properly
        AndroidConnectFactory.configure();

        // Used by app designer grunt task "clean"
        if (getIntent() != null && getIntent().hasExtra("showLogin")) {
            if (!absSyncViewModel.getStarted()) {
                absSyncViewModel.setStarted(true);
                Intent i = new Intent(this, LoginActivity.class);
                i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
                startActivity(i);
            }
        }

        findViewsAndAttachListeners();
        setupViewModelAndNavController();
        handleLifecycleEvents();
    }

    /**
     * Finding the different views required and attaching onClick Listeners to them
     */
    protected void findViewsAndAttachListeners() {
        MaterialToolbar toolbar = findViewById(R.id.toolbarSyncActivity);

        navView = findViewById(R.id.navViewSync);
        drawerLayout = findViewById(R.id.drawerLayoutSync);

        btnDrawerSignIn = navView.getHeaderView(0).findViewById(R.id.btnDrawerLogin);

        ImageButton btnDrawerOpen = findViewById(R.id.btnDrawerOpen);
        ImageButton btnDrawerClose = navView.getHeaderView(0).findViewById(R.id.btnDrawerClose);

        OnButtonClick onButtonClick = new OnButtonClick();
        btnDrawerSignIn.setOnClickListener(onButtonClick);
        btnDrawerOpen.setOnClickListener(onButtonClick);
        btnDrawerClose.setOnClickListener(onButtonClick);

        toolbar.setOnMenuItemClickListener(new OnToolbarMenuItemClick());
        navView.setNavigationItemSelectedListener(new OnDrawerMenuItemClick());
    }

    private void setupViewModelAndNavController() {
        navController = Navigation.findNavController(this, R.id.navHostSync);

        initializeViewModelAndNavController();
        updateViewModelWithProps();

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
            if (aBoolean)
                onFirstLaunch();
        });

        navController.addOnDestinationChangedListener((controller, destination, arguments) -> {
            navView.getMenu().findItem(R.id.drawer_about_us).setEnabled(!isCurrentDestinationAboutFragment());

            if (mAppName != null)
                updateViewModelWithProps();
        });
    }

    private void handleLifecycleEvents() {
        getLifecycle().addObserver((LifecycleEventObserver) (source, event) -> {
            switch (event) {
                case ON_CREATE: {
                    WebLogger.getLogger(getAppName()).i(TAG, " [onCreate]");
                    break;
                }
                case ON_START: {
                    WebLogger.getLogger(getAppName()).i(TAG, " [onStart]");
                    break;
                }
                case ON_RESUME: {
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
                        bindService(bind_intent, AbsSyncBaseActivity.this,
                                Context.BIND_AUTO_CREATE | Context.BIND_ADJUST_WITH_ACTIVITY);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (navController.getCurrentDestination() == null)
                        navigateToHomeFragment();

                    updateViewModelWithProps();
                    break;
                }
                case ON_PAUSE: {
                    boolean callUnbind = false;
                    synchronized (interfaceGuard) {
                        callUnbind = mBoundGuarded;
                        odkSyncInterfaceGuarded = null;
                        mBoundGuarded = false;
                    }

                    if (callUnbind) {
                        unbindService(AbsSyncBaseActivity.this);
                        WebLogger.getLogger(getAppName()).i(TAG, " [onPause] Unbound to sync service");
                    }

                    WebLogger.getLogger(getAppName()).i(TAG, " [onPause]");
                }
                case ON_STOP: {
                    WebLogger.getLogger(getAppName()).i(TAG, " [onStop]");
                    break;
                }
                case ON_DESTROY: {
                    WebLogger.getLogger(getAppName()).i(TAG, " [onDestroy]");
                    break;
                }
            }
        });
    }

    private void onFirstLaunch() {
        PropertiesSingleton props = getProps();
        props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FIRST_LAUNCH, Boolean.toString(false)));

        androidx.appcompat.app.AlertDialog alertDialog = new androidx.appcompat.app.AlertDialog
                .Builder(this)
                .setMessage(R.string.configure_server_settings)
                .setCancelable(false)
                .setPositiveButton(R.string.yes, (dialog, which) -> {
                    dialog.dismiss();
                    navigateToUpdateServerSettings();
                })
                .setNegativeButton(R.string.no, (dialog, which) -> dialog.dismiss()).create();

        alertDialog.setCanceledOnTouchOutside(false);
        alertDialog.show();
    }

    /**
     * Actions in the Logged-Out User State
     */
    private void inLoggedOutState() {
        handleDrawerVisibility(false, false, false);
        btnDrawerSignIn.setText(R.string.drawer_sign_in_button_text);
    }

    /**
     * Actions in the Anonymous User State
     */
    private void inAnonymousState() {
        handleDrawerVisibility(true, true, false);
        btnDrawerSignIn.setText(R.string.drawer_sign_out_button_text);
    }

    /**
     * Actions in the Authenticated User State
     */
    private void inAuthenticatedState() {
        handleDrawerVisibility(true, true, true);
        btnDrawerSignIn.setText(R.string.drawer_sign_out_button_text);
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
        ODKServicesPropertyUtils.clearActiveUser(getProps());
        drawerLayout.closeDrawer(GravityCompat.START);
        updateViewModelWithProps();
    }

    @Override
    public String getAppName() {
        if (mAppName == null) {
            mAppName = getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
            if (mAppName == null) {
                mAppName = ODKFileUtils.getOdkDefaultAppName();
            }
            Log.e(TAG, mAppName);
        }
        return mAppName;
    }

    @Override
    public PropertiesSingleton getProps() {
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

    public void updateViewModelWithProps() {
        absSyncViewModel.setAppName(getAppName());

        PropertiesSingleton props = getProps();
        absSyncViewModel.setServerUrl(props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL));
        absSyncViewModel.setIsServerVerified(Boolean.parseBoolean(props.getProperty(CommonToolProperties.KEY_IS_SERVER_VERIFIED)));

        absSyncViewModel.setIsFirstLaunch(Boolean.parseBoolean(props.getProperty(CommonToolProperties.KEY_FIRST_LAUNCH)));

        boolean isAnonymousSignInUsed = Boolean.parseBoolean(props.getProperty(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED));
        absSyncViewModel.setIsAnonymousSignInUsed(isAnonymousSignInUsed);

        if (isAnonymousSignInUsed) {
            boolean isAnonymousAllowed = Boolean.parseBoolean(props.getProperty(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED));
            absSyncViewModel.setIsAnonymousAllowed(isAnonymousAllowed);
        }

        absSyncViewModel.setCurrentUserState(UserState.valueOf(props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE)));
        absSyncViewModel.setUsername(props.getProperty(CommonToolProperties.KEY_USERNAME));

        String userVerifiedStr = props.getProperty(CommonToolProperties.KEY_IS_USER_AUTHENTICATED);
        if (userVerifiedStr != null) {
            absSyncViewModel.setIsUserVerified(Boolean.parseBoolean(userVerifiedStr));
        }

        String lastSyncStr = props.getProperty(CommonToolProperties.KEY_LAST_SYNC_INFO);
        if (lastSyncStr != null) {
            absSyncViewModel.setIsLastSyncTimeAvailable(true);
            absSyncViewModel.setLastSyncTime(Long.parseLong(lastSyncStr));
        } else
            absSyncViewModel.setIsLastSyncTimeAvailable(false);

        if (props.containsKey(CommonToolProperties.KEY_SYNC_ATTACHMENT_STATE) && props.getProperty(CommonToolProperties.KEY_SYNC_ATTACHMENT_STATE) != null) {
            String state = props.getProperty(CommonToolProperties.KEY_SYNC_ATTACHMENT_STATE);
            try {
                absSyncViewModel.updateSyncAttachmentState(SyncAttachmentState.valueOf(state));
            } catch (IllegalArgumentException e) {
                absSyncViewModel.updateSyncAttachmentState(SyncAttachmentState.SYNC);
            }
        }
    }
}
