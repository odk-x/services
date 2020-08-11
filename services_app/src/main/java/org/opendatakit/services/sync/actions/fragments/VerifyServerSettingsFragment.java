/*
 * Copyright (C) 2016 University of Washington
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
package org.opendatakit.services.sync.actions.fragments;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Environment;
import android.os.RemoteException;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;

import android.widget.Toast;
import androidx.core.content.FileProvider;
import org.opendatakit.httpclientandroidlib.HttpResponse;
import org.opendatakit.httpclientandroidlib.client.HttpClient;
import org.opendatakit.httpclientandroidlib.client.methods.HttpGet;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.BuildConfig;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.sync.actions.VerifyServerSettingsActions;
import org.opendatakit.services.sync.actions.activities.DoSyncActionCallback;
import org.opendatakit.services.sync.actions.activities.ISyncServiceInterfaceActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.sync.service.IOdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.sync.service.SyncProgressEvent;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.SyncStatus;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author mitchellsundt@gmail.com
 */
public class VerifyServerSettingsFragment extends AbsSyncUIFragment {

    private static final String TAG = "VerifyServerSettingsFragment";

    public static final String NAME = "VerifyServerSettingsFragment";
    public static final int ID = R.layout.verify_server_settings_launch_fragment;

    private static final String VERIFY_SERVER_SETTINGS_ACTION = "verifyServerSettingsAction";

    private static final String PROGRESS_DIALOG_TAG = "progressDialogVerifySvr";
    private static final String OUTCOME_DIALOG_TAG = "outcomeDialogVerifySvr";

    private Button startVerifyServerSettings;
    private Button installIoFileManagerBtn;
    private Button installTablesBtn;
    private Button installSurveyBtn;

    private VerifyServerSettingsActions verifyServerSettingsAction = VerifyServerSettingsActions.IDLE;

    public VerifyServerSettingsFragment() {
        super(OUTCOME_DIALOG_TAG, PROGRESS_DIALOG_TAG);
    }


    @Override
    public void onSaveInstanceState(Bundle outState) {
        super.onSaveInstanceState(outState);
        outState.putString(VERIFY_SERVER_SETTINGS_ACTION, verifyServerSettingsAction.name());
    }

    @Override
    public void onActivityCreated(Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);

        Intent incomingIntent = getActivity().getIntent();
        if (savedInstanceState != null && savedInstanceState
                .containsKey(VERIFY_SERVER_SETTINGS_ACTION)) {
            String action = savedInstanceState.getString(VERIFY_SERVER_SETTINGS_ACTION);
            try {
                verifyServerSettingsAction = VerifyServerSettingsActions.valueOf(action);
            } catch (IllegalArgumentException e) {
                verifyServerSettingsAction = VerifyServerSettingsActions.IDLE;
            }
        }
        disableButtons();
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        super.onCreateView(inflater, container, savedInstanceState);

        View view = inflater.inflate(ID, container, false);
        populateTextViewMemberVariablesReferences(view);

        if (savedInstanceState != null && savedInstanceState
                .containsKey(VERIFY_SERVER_SETTINGS_ACTION)) {
            String action = savedInstanceState.getString(VERIFY_SERVER_SETTINGS_ACTION);
            try {
                verifyServerSettingsAction = VerifyServerSettingsActions.valueOf(action);
            } catch (IllegalArgumentException e) {
                verifyServerSettingsAction = VerifyServerSettingsActions.IDLE;
            }
        }

        startVerifyServerSettings = view
                .findViewById(R.id.verify_server_settings_start_button);
        startVerifyServerSettings.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                onClickVerifyServerSettings(v);
            }
        });

        installIoFileManagerBtn = view
                .findViewById(R.id.install_IO_button);
        installIoFileManagerBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                onClickInstallIOFileManager(v);
            }
        });

        return view;
    }


    private void disableButtons() {
        startVerifyServerSettings.setEnabled(false);
    }

    void perhapsEnableButtons() {
        PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
        String url = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
        if (url == null || url.length() == 0) {
            disableButtons();
        } else {
            startVerifyServerSettings.setEnabled(true);
        }
    }


    public void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);

        if (requestCode == VerifyServerSettingsActivity.AUTHORIZE_ACCOUNT_RESULT_CODE) {
            if (resultCode == Activity.RESULT_CANCELED) {
                verifyServerSettingsAction = VerifyServerSettingsActions.IDLE;
            }
            postTaskToAccessSyncService();
        }
    }

    void postTaskToAccessSyncService() {
        WebLogger.getLogger(getAppName()).d(TAG, "[" + getId() + "] [postTaskToAccessSyncService] started");
        Activity activity = getActivity();
        if (activity == null || !hasDialogBeenCreated() || !this.isResumed()) {
            // we are in transition -- do nothing
            WebLogger.getLogger(getAppName())
                    .d(TAG, "[" + getId() + "] [postTaskToAccessSyncService] activity == null");
            handler.postDelayed(new Runnable() {
                @Override
                public void run() {
                    postTaskToAccessSyncService();
                }
            }, 100);

            return;
        }
        ((ISyncServiceInterfaceActivity) activity)
                .invokeSyncInterfaceAction(new DoSyncActionCallback() {
                    @Override
                    public void doAction(IOdkSyncServiceInterface syncServiceInterface)
                            throws RemoteException {
                        if (syncServiceInterface != null) {
                            //          WebLogger.getLogger(getAppName()).d(TAG, "[" + getId() + "] [postTaskToAccessSyncService] syncServiceInterface != null");
                            final SyncStatus status = syncServiceInterface.getSyncStatus(getAppName());
                            final SyncProgressEvent event = syncServiceInterface
                                    .getSyncProgressEvent(getAppName());
                            if (status == SyncStatus.SYNCING) {
                                verifyServerSettingsAction = VerifyServerSettingsActions.MONITOR_VERIFYING;

                                handler.post(new Runnable() {
                                    @Override
                                    public void run() {
                                        showProgressDialog(status, event.progressState, event.progressMessageText,
                                                event.curProgressBar, event.maxProgressBar);
                                    }
                                });
                                return;
                            }

                            switch (verifyServerSettingsAction) {
                                case VERIFY:
                                    syncServiceInterface.verifyServerSettings(getAppName());
                                    verifyServerSettingsAction = VerifyServerSettingsActions.MONITOR_VERIFYING;

                                    handler.post(new Runnable() {
                                        @Override
                                        public void run() {
                                            showProgressDialog(SyncStatus.NONE, null,
                                                    getString(R.string.verify_server_settings_starting), -1, 0);
                                        }
                                    });
                                    break;
                                case IDLE:
                                    if (event.progressState == SyncProgressState.FINISHED) {
                                        final SyncOverallResult result = syncServiceInterface.getSyncResult(getAppName());
                                        handler.post(new Runnable() {
                                            @Override
                                            public void run() {
                                                showOutcomeDialog(status, result);
                                            }
                                        });
                                    }
                                default:
                                    break;
                            }
                        } else {
                            WebLogger.getLogger(getAppName())
                                    .d(TAG, "[" + getId() + "] [postTaskToAccessSyncService] syncServiceInterface == null");
                            // The service is not bound yet so now we need to try again
                            handler.postDelayed(new Runnable() {
                                @Override
                                public void run() {
                                    postTaskToAccessSyncService();
                                }
                            }, 100);
                        }
                    }
                });
    }

    void updateInterface() {
        Activity activity = getActivity();
        if (activity == null || !hasDialogBeenCreated() || !this.isResumed()) {
            // we are in transition -- do nothing
            WebLogger.getLogger(getAppName())
                    .w(TAG, "[" + getId() + "] [updateInterface] activity == null = return");
            handler.postDelayed(new Runnable() {
                @Override
                public void run() {
                    updateInterface();
                }
            }, 100);
            return;
        }
        ((ISyncServiceInterfaceActivity) activity)
                .invokeSyncInterfaceAction(new DoSyncActionCallback() {
                    @Override
                    public void doAction(IOdkSyncServiceInterface syncServiceInterface)
                            throws RemoteException {
                        if (syncServiceInterface != null) {
                            final SyncStatus status = syncServiceInterface.getSyncStatus(getAppName());
                            final SyncProgressEvent event = syncServiceInterface
                                    .getSyncProgressEvent(getAppName());
                            if (status == SyncStatus.SYNCING) {
                                verifyServerSettingsAction = VerifyServerSettingsActions.MONITOR_VERIFYING;

                                handler.post(new Runnable() {
                                    @Override
                                    public void run() {
                                        showProgressDialog(status, event.progressState, event.progressMessageText,
                                                event.curProgressBar, event.maxProgressBar);
                                    }
                                });
                                return;
                            } else {
                                // request completed
                                verifyServerSettingsAction = VerifyServerSettingsActions.IDLE;
                                final SyncOverallResult result = syncServiceInterface.getSyncResult(getAppName());
                                handler.post(new Runnable() {
                                    @Override
                                    public void run() {
                                        if (event.progressState == SyncProgressState.FINISHED) {
                                            showOutcomeDialog(status, result);
                                        }
                                    }
                                });
                                return;
                            }
                        } else {
                            // The service is not bound yet so now we need to try again
                            handler.postDelayed(new Runnable() {
                                @Override
                                public void run() {
                                    updateInterface();
                                }
                            }, 100);
                        }
                    }
                });
    }

    void syncCompletedAction(IOdkSyncServiceInterface syncServiceInterface) throws
            RemoteException {
        removeAnySyncNotification();
        boolean completed = syncServiceInterface.clearAppSynchronizer(getAppName());
        if (!completed) {
            throw new IllegalStateException(
                    "Could not remove AppSynchronizer for " + getAppName());
        }
        getActivity().finish();
    }

    /**
     * Hooked to syncNowButton's onClick in aggregate_activity.xml
     */
    public void onClickVerifyServerSettings(View v) {
        WebLogger.getLogger(getAppName()).d(TAG,
                "[" + getId() + "] [onClickVerifyServerSettings] timestamp: " + System.currentTimeMillis());
        if (areCredentialsConfigured(true)) {
            disableButtons();
            verifyServerSettingsAction = VerifyServerSettingsActions.VERIFY;
            prepareForSyncAction();
        }
    }

    public void onClickInstallIOFileManager(View v) {
        WebLogger.getLogger(getAppName()).d(TAG,
                "[" + getId() + "] [onClickInstallIOFileManager] timestamp: " + System.currentTimeMillis());
        if (areCredentialsConfigured(true)) {
            installIoFileManagerBtn.setEnabled(true);

            new AsyncTask<String, String, String>() {
                String result = "";

                @Override
                protected String doInBackground(String... params) {
                    try {
                      String apkName = params[0];
                      URL url = new URL("https://github.com/odk-x/tables/releases/download/2.1.6/"+apkName);
                      HttpURLConnection c = (HttpURLConnection) url
                              .openConnection();
                      c.setRequestMethod("GET");

                      c.connect();

                      String PATH = Environment.getExternalStorageDirectory()
                              + "/download/";
                      File file = new File(PATH);
                      file.mkdirs();
                      File outputFile = new File(file, apkName);
                      FileOutputStream fos = new FileOutputStream(outputFile);

                      InputStream is = c.getInputStream();

                      byte[] buffer = new byte[1024];
                      int len1 = 0;
                      while ((len1 = is.read(buffer)) != -1) {
                        fos.write(buffer, 0, len1);
                      }
                      fos.close();
                      is.close();
                      Intent intent = new Intent(Intent.ACTION_VIEW);
                      intent.setDataAndType(FileProvider.getUriForFile(getActivity(), "org.opendatakit.services.GenericFileProvider",
                              new File(Environment.getExternalStorageDirectory() + "/download/" + "ODK-X_Tables_v2.1.6.apk")), "application/vnd.android.package-archive");
                      intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
                      intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION);
                      startActivity(intent);
                    } catch (Exception ex) {
                      result="Update error! "+ ex.getMessage();
                      ex.printStackTrace();
                    }
                    return result;
                }

                protected void onPostExecute(String result) {

                    Toast.makeText(getActivity().getApplicationContext(), result,
                            Toast.LENGTH_LONG).show();
                }

                ;
            }.execute("ODK-X_Tables_v2.1.6.apk");

        }
    }


    private void showProgressDialog(SyncStatus status, SyncProgressState progress, String message,
                                    int progressStep, int maxStep) {
        if (getActivity() == null) {
            // we are tearing down or still initializing
            return;
        }
        if (verifyServerSettingsAction == VerifyServerSettingsActions.MONITOR_VERIFYING) {

            disableButtons();

            if (progress == null) {
                progress = SyncProgressState.INACTIVE;
            }

            int id_title = R.string.verifying_server_settings;
            showProgressDialog(getString(id_title), message, progressStep, maxStep);

            if (status == SyncStatus.SYNCING || status == SyncStatus.NONE) {
                handler.postDelayed(new Runnable() {
                    @Override
                    public void run() {
                        updateInterface();
                    }
                }, 150);
            }
        }
    }

    private void showOutcomeDialog(SyncStatus status, SyncOverallResult result) {
        if (getActivity() == null) {
            // we are tearing down or still initializing
            return;
        }
        if (verifyServerSettingsAction == VerifyServerSettingsActions.IDLE) {

            disableButtons();

            String message;
            int id_title;
            switch (status) {
                default:
                    throw new IllegalStateException("Unexpected missing case statement");
                case
                        /** earlier sync ended with socket or lower level transport or protocol error (e.g., 300's) */NETWORK_TRANSPORT_ERROR:
                    id_title = R.string.sync_communications_error;
                    message = getString(R.string.sync_status_network_transport_error);
                    break;
                case
                        /** earlier sync ended with Authorization denied (authentication and/or access) error */AUTHENTICATION_ERROR:
                    id_title = R.string.sync_user_authorization_failure;
                    message = getString(R.string.sync_status_authentication_error);
                    break;
                case
                        /** earlier sync ended with a 500 error from server */SERVER_INTERNAL_ERROR:
                    id_title = R.string.sync_communications_error;
                    message = getString(R.string.sync_status_internal_server_error);
                    break;
                case /** the server is not an ODK Server - bad client config */SERVER_IS_NOT_ODK_SERVER:
                    id_title = R.string.sync_device_configuration_failure;
                    message = getString(R.string.sync_status_bad_gateway_or_client_config);
                    break;
                case
                        /** earlier sync ended with a 400 error that wasn't Authorization denied */REQUEST_OR_PROTOCOL_ERROR:
                    id_title = R.string.sync_communications_error;
                    message = getString(R.string.sync_status_request_or_protocol_error);
                    break;
                case /** no earlier sync and no active sync */NONE:
                case /** active sync -- get SyncProgressEvent to see current status */SYNCING:
                case
                        /** earlier sync ended successfully without conflicts but needs row-level attachments sync'd */SYNC_COMPLETE_PENDING_ATTACHMENTS:
                case
                        /** the server does not have any configuration, or no configuration for this client version */SERVER_MISSING_CONFIG_FILES:
                case
                        /** the device does not have any configuration to push to server */SERVER_RESET_FAILED_DEVICE_HAS_NO_CONFIG_FILES:
                case
                        /** while a sync was in progress, another device reset the app config, requiring a restart of
                         * our sync */RESYNC_BECAUSE_CONFIG_HAS_BEEN_RESET_ERROR:
                case
                        /** earlier sync ended with one or more tables containing row conflicts or checkpoint rows */CONFLICT_RESOLUTION:
                case
                        /** error accessing or updating database */DEVICE_ERROR:
                    id_title = R.string.sync_device_internal_error;
                    message = getString(R.string.sync_status_device_internal_error);
                    break;
                case
                        /** the server is not configured for this appName -- Site Admin / Preferences */APPNAME_NOT_SUPPORTED_BY_SERVER:
                    id_title = R.string.sync_server_configuration_failure;
                    message = getString(R.string.sync_status_appname_not_supported_by_server);
                    break;
                case
                        /** earlier sync ended successfully without conflicts and all row-level attachments sync'd */SYNC_COMPLETE:
                    id_title = R.string.verify_server_setttings_successful;
                    message = getString(R.string.verify_server_setttings_successful_text);
                    break;
            }
            createAlertDialog(getString(id_title), message);
        }
    }
}
