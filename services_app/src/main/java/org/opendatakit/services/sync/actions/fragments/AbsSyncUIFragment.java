package org.opendatakit.services.sync.actions.fragments;

import android.app.Activity;
import android.app.NotificationManager;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.os.RemoteException;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.lifecycle.LifecycleEventObserver;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.fragment.AlertDialogFragment;
import org.opendatakit.fragment.AlertNProgessMsgFragmentMger;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.sync.actions.activities.AbsSyncBaseActivity;
import org.opendatakit.services.sync.actions.activities.DoSyncActionCallback;
import org.opendatakit.services.sync.actions.activities.ISyncServiceInterfaceActivity;
import org.opendatakit.services.sync.service.GlobalSyncNotificationManager;
import org.opendatakit.sync.service.IOdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncStatus;
import org.opendatakit.utilities.AppNameUtil;

/**
 * Created by wrb on 2/5/2018.
 */

abstract class AbsSyncUIFragment extends Fragment implements AlertDialogFragment.ConfirmAlertDialog {

    private static final String TAG = AbsSyncUIFragment.class.getSimpleName();

    private String mAppName;
    private String alertDialogTag;
    private String progressDialogTag;

    final Handler handler = new Handler();

    private AlertNProgessMsgFragmentMger msgManager;

    abstract void postTaskToAccessSyncService();
    abstract void perhapsEnableButtons();
    abstract void updateInterface();
    abstract void syncCompletedAction(IOdkSyncServiceInterface syncServiceInterface) throws RemoteException;

    AbsSyncUIFragment(String alertDialogTag, String progressDialogTag) {
        this.alertDialogTag = alertDialogTag;
        this.progressDialogTag = progressDialogTag;
    }

    /**
     * Override the Fragment.onAttach() method to get appName
     *
     * @param context
     */
    @Override public void onAttach(Context context) {
        super.onAttach(context);
        mAppName = AppNameUtil.getAppNameFromActivity(getActivity());
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        if(mAppName == null) {
            mAppName = AppNameUtil.getAppNameFromActivity(getActivity());
        }

        if (savedInstanceState != null) {
            msgManager = AlertNProgessMsgFragmentMger
                    .restoreInitMessaging(getAppName(), alertDialogTag, progressDialogTag,
                            savedInstanceState);
        }

        // if message manager was not created from saved state, create fresh
        if (msgManager == null) {
            msgManager = new AlertNProgessMsgFragmentMger(getAppName(), alertDialogTag,
                    progressDialogTag, false, false);
        }

        handleLifecycleEvents();
    }

    protected void handleLifecycleEvents(){
        getLifecycle().addObserver((LifecycleEventObserver) (source, event) -> {
            switch (event){
                case ON_RESUME:{
                    WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onResume]");

                    Intent incomingIntent = getActivity().getIntent();
                    String tmpAppName = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
                    if ( mAppName == null || mAppName.length() == 0 || !mAppName.equals(tmpAppName)) {
                        WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onResume] appName is "
                                + "either null or does not match the current appName of the activity so calling "
                                + "finish");
                        getActivity().setResult(Activity.RESULT_CANCELED);
                        getActivity().finish();
                    }

                    perhapsEnableButtons();
                    handler.postDelayed(new Runnable() {
                        @Override
                        public void run() {
                            msgManager.restoreDialog(getParentFragmentManager(), getId());
                            updateInterface();
                        }
                    }, 100);
                    break;
                }
                case ON_PAUSE:{
                    msgManager.clearDialogsAndRetainCurrentState(getParentFragmentManager());
                    break;
                }
                case ON_DESTROY:{
                    handler.removeCallbacksAndMessages(null);
                    WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onDestroy]");
                    break;
                }
            }
        });

    };

    @Override
    public void onSaveInstanceState(@NonNull Bundle outState) {
        super.onSaveInstanceState(outState);
        if (msgManager != null) {
            msgManager.addStateToSaveStateBundle(outState);
        }
    }

    /**
     * Invoke this at the start of the verify server settings action
     */
    void prepareForSyncAction() {
        // remove any settings for a URL other than the server URL...

        PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();

        String authType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
        if (authType == null) {
            authType = getString(R.string.credential_type_none);
        }

        postTaskToAccessSyncService();
    }

    String getAppName() {
        if (mAppName == null) {
            throw new IllegalStateException("appName not yet initialized");
        }
        return mAppName;
    }

    /**
     * Verifies that the system has the necessary credentials and either creates an error message
     * or a toast to warn the user
     *
     * @param createError if true creates error dialog, if false only shows toast
     *
     * @return true if valid credentials exist, false otherwise
     */
    public boolean areCredentialsConfigured(boolean createError) {
        // verify that we have the necessary credentials
        PropertiesSingleton props = CommonToolProperties.get(getActivity(), getAppName());
        String authType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
        if (getString(R.string.credential_type_none).equals(authType)) {
            return true;
        }
        if (getString(R.string.credential_type_username_password).equals(authType)) {
            String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
            String password = props.getProperty(CommonToolProperties.KEY_PASSWORD);
            if (username == null || username.length() == 0 || password == null
                    || password.length() == 0) {
                if(createError) {
                    AbsSyncBaseActivity.showAuthenticationErrorDialog(getActivity(), getString(R.string.sync_configure_username_password));
                } else {
                    Toast.makeText(getActivity(), getString(R.string.sync_configure_username_password),
                            Toast.LENGTH_LONG).show();
                }
                return false;
            }
            return true;
        }
        if(createError) {
            AbsSyncBaseActivity.showAuthenticationErrorDialog(getActivity(), getString(R.string.sync_configure_credentials));
        } else {
            Toast.makeText(getActivity(), getString(R.string.sync_configure_credentials),
                    Toast.LENGTH_LONG).show();
        }
        return false;
    }

    public void onSyncCompleted() {
        Activity activity = getActivity();
        WebLogger.getLogger(getAppName())
                .i(TAG, "[" + getId() + "] [onSyncCompleted] after getActivity");
        if (activity == null) {
            // we are in transition -- do nothing
            WebLogger.getLogger(getAppName())
                    .i(TAG, "[" + getId() + "] [onSyncCompleted] activity == null = return");
            handler.postDelayed(new Runnable() {
                @Override
                public void run() {
                    onSyncCompleted();
                }
            }, 100);
            return;
        }

        ((ISyncServiceInterfaceActivity) activity)
                .invokeSyncInterfaceAction(new DoSyncActionCallback() {
                    @Override
                    public void doAction(IOdkSyncServiceInterface syncServiceInterface)
                            throws RemoteException {
                        WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onSyncCompleted] called");
                        if (syncServiceInterface != null) {
                            WebLogger.getLogger(getAppName()).i(TAG,
                                    "[" + getId() + "] [onSyncCompleted] and syncServiceInterface is not null");
                            syncCompletedAction(syncServiceInterface);
                            return;
                        } else {
                            WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onSyncCompleted] and syncServiceInterface is null");
                            handler.postDelayed(new Runnable() {
                                @Override
                                public void run() {
                                    onSyncCompleted();
                                }
                            }, 100);
                        }
                    }
                });
    }

    @Override public void okAlertDialog() {
        Activity activity = getActivity();
        if (activity == null) {
            // we are in transition -- do nothing
            WebLogger.getLogger(getAppName())
                    .w(TAG, "[" + getId() + "] [okAlertDialog has been oked] activity == null = return");
            handler.postDelayed(new Runnable() {
                @Override
                public void run() {
                    okAlertDialog();
                }
            }, 100);
            return;
        }

        ((ISyncServiceInterfaceActivity) activity)
                .invokeSyncInterfaceAction(new DoSyncActionCallback() {
                    @Override
                    public void doAction(IOdkSyncServiceInterface syncServiceInterface) throws
                            RemoteException {

                        final SyncStatus status = syncServiceInterface.getSyncStatus(getAppName());

                        if (status == SyncStatus.SYNC_COMPLETE
                                || status == SyncStatus.SYNC_COMPLETE_PENDING_ATTACHMENTS)  {
                            getActivity().setResult(Activity.RESULT_OK);
                        } else {
                            getActivity().setResult(Activity.RESULT_CANCELED);
                        }
                        onSyncCompleted();
                    }
                });
    }

    void removeAnySyncNotification() {
        NotificationManager nm = (NotificationManager) getActivity().getSystemService(Context
                .NOTIFICATION_SERVICE);
        nm.cancel(getAppName(), GlobalSyncNotificationManager.SYNC_NOTIFICATION_ID);
    }

    void createAlertDialog(String title, String message) {
        if(!msgManager.shutdownCalled()) {
            msgManager.createAlertDialog(title, message, getParentFragmentManager(), getId());
        }
    }

    void showProgressDialog(String title, String message, int progressStep, int maxStep) {
        if(!msgManager.shutdownCalled()) {
            FragmentManager fm = getParentFragmentManager();
            msgManager.createProgressDialog(title, message, fm);
            fm.executePendingTransactions();
            msgManager.updateProgressDialogMessage(message, progressStep, maxStep, fm);
        }
    }

    boolean hasDialogBeenCreated() {
        return msgManager.hasDialogBeenCreated();
    }

    void dismissDialogs() {
        FragmentManager fm = getParentFragmentManager();
        msgManager.dismissProgressDialog(fm);
        msgManager.dismissAlertDialog(fm);
    }

    protected PropertiesSingleton getProps(){
        return ((AbsSyncBaseActivity)requireActivity()).getProps();
    }

    protected void updateViewModelWithProps(){
        ((AbsSyncBaseActivity)requireActivity()).updateViewModelWithProps();
    }
}
