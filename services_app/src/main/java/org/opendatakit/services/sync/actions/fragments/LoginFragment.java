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

import android.accounts.Account;
import android.accounts.AccountManager;
import android.app.Activity;
import android.app.AlertDialog;
import android.app.Fragment;
import android.content.ActivityNotFoundException;
import android.content.ComponentName;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.os.RemoteException;
import android.provider.SyncStateContract;
import android.text.method.PasswordTransformationMethod;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.consts.RequestCodeConsts;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.service.TableHealthInfo;
import org.opendatakit.database.service.TableHealthStatus;
import org.opendatakit.database.service.UserDbInterface;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.services.application.Services;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.LoginActions;
import org.opendatakit.services.sync.actions.SyncActions;
import org.opendatakit.services.sync.actions.activities.*;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;
import org.opendatakit.services.utilities.TableHealthValidator;
import org.opendatakit.sync.service.OdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.sync.service.SyncProgressEvent;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.SyncStatus;

import java.util.*;

/**
 * @author mitchellsundt@gmail.com
 */
public class LoginFragment extends Fragment implements ISyncOutcomeHandler {

   private static final String TAG = "LoginFragment";

   public static final String NAME = "LoginFragment";
   public static final int ID = R.layout.login_fragment;

   private static final String ACCOUNT_TYPE_G = "com.google";

   private static final String LOGIN_ACTION = "loginAction";

   private static final String PROGRESS_DIALOG_TAG = "progressDialog";

   private static final String OUTCOME_DIALOG_TAG = "outcomeDialog";

   private String mAppName;

   private final Handler handler = new Handler();
   private DismissableProgressDialogFragment progressDialog = null;
   private DismissableOutcomeDialogFragment outcomeDialog = null;

   private TextView uriField;
   private TextView accountAuthType;
   private TextView accountIdentity;

   private PropertiesSingleton props;
   private TableHealthValidator healthValidator;

   private EditText usernameEditText;
   private EditText passwordEditText;
   private CheckBox togglePasswordText;
   private Button authenticateNewUser;
   private Button logout;
   private Button cancel;

   private LoginActions loginAction = LoginActions.IDLE;

   @Override
   public void onSaveInstanceState(Bundle outState) {
      super.onSaveInstanceState(outState);
      outState.putString(LOGIN_ACTION, loginAction.name());
   }

   @Override
   public void onActivityCreated(Bundle savedInstanceState) {
      super.onActivityCreated(savedInstanceState);

      Intent incomingIntent = getActivity().getIntent();
      mAppName = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
      if (mAppName == null || mAppName.length() == 0) {
         getActivity().setResult(Activity.RESULT_CANCELED);
         getActivity().finish();
         return;
      }

      if (savedInstanceState != null && savedInstanceState.containsKey(LOGIN_ACTION)) {
         String action = savedInstanceState.getString(LOGIN_ACTION);
         try {
            loginAction = LoginActions.valueOf(action);
         } catch (IllegalArgumentException e) {
            loginAction = LoginActions.IDLE;
         }
      }
      disableButtons();

      healthValidator = new TableHealthValidator(mAppName, getActivity());
   }

   @Override
   public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
      super.onCreateView(inflater, container, savedInstanceState);

      View view = inflater.inflate(ID, container, false);

      props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();

      uriField = (TextView) view.findViewById(R.id.sync_uri_field);
      accountAuthType = (TextView) view.findViewById(R.id.sync_account_auth_label);
      accountIdentity = (TextView) view.findViewById(R.id.sync_account);

      if (savedInstanceState != null && savedInstanceState.containsKey(LOGIN_ACTION)) {
         String action = savedInstanceState.getString(LOGIN_ACTION);
         try {
            loginAction = LoginActions.valueOf(action);
         } catch (IllegalArgumentException e) {
            loginAction = LoginActions.IDLE;
         }
      }

      usernameEditText = (EditText) view.findViewById(R.id.username);
      usernameEditText.setText(props.getProperty(CommonToolProperties.KEY_USERNAME));

      passwordEditText = (EditText) view.findViewById(R.id.pwd_field);

      togglePasswordText = (CheckBox) view.findViewById(R.id.show_pwd);
      togglePasswordText.setOnClickListener(new View.OnClickListener() {
         @Override
         public void onClick(View v) {
            if (togglePasswordText.isChecked()) {
               passwordEditText.setTransformationMethod(null);
            } else {
               passwordEditText.setTransformationMethod(new PasswordTransformationMethod());
            }
         }
      });

      authenticateNewUser = (Button) view.findViewById(R.id.change_user_button);
      authenticateNewUser.setOnClickListener(new View.OnClickListener() {
         @Override
         public void onClick(View v) {
            setNewCredentials();
            refreshCredentialsDisplay();
            verifyServerSettings(v);
         }
      });

      logout = (Button) view.findViewById(R.id.logout_button);
      logout.setOnClickListener(new View.OnClickListener() {
         @Override
         public void onClick(View v) {
            logout();
         }
      });

      cancel = (Button) view.findViewById(R.id.cancel_button);
      cancel.setOnClickListener(new View.OnClickListener() {

         @Override
         public void onClick(View v) {
            getActivity().finish();
         }

      });

      return view;
   }

   private void setNewCredentials() {

      String username = usernameEditText.getText().toString();
      String pw = passwordEditText.getText().toString();

      Map<String, String> properties = new HashMap<String, String>();
      properties.put(CommonToolProperties.KEY_AUTHENTICATION_TYPE,
          getString(R.string.credential_type_username_password));
      properties.put(CommonToolProperties.KEY_USERNAME, username);
      properties.put(CommonToolProperties.KEY_PASSWORD, pw);
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
      properties.put(CommonToolProperties.KEY_USERS_LIST, "");

      props.setProperties(properties);
   }

   private void logout() {
      ODKServicesPropertyUtils.clearActiveUser(props);

      Map<String, String> properties = new HashMap<String, String>();
      properties.put(CommonToolProperties.KEY_AUTHENTICATION_TYPE,
          getString(R.string.credential_type_username_password));
      properties.put(CommonToolProperties.KEY_USERNAME, "");
      properties.put(CommonToolProperties.KEY_AUTH, "");
      properties.put(CommonToolProperties.KEY_PASSWORD, "");

      getActivity().finish();
   }

   protected void refreshCredentialsDisplay() {
      PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
      uriField.setText(props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL));

      String credentialToUse = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
      String[] credentialValues = getResources().getStringArray(R.array.credential_entry_values);
      String[] credentialEntries = getResources().getStringArray(R.array.credential_entries);

      if (credentialToUse == null) {
         credentialToUse = getString(R.string.credential_type_none);
      }

      for (int i = 0; i < credentialValues.length; ++i) {
         if (credentialToUse.equals(credentialValues[i])) {
            if (!credentialToUse.equals(getString(R.string.credential_type_none))) {
               accountAuthType.setText(credentialEntries[i]);
            }
         }
      }

      if (credentialToUse.equals(getString(R.string.credential_type_none))) {
         accountIdentity.setText(getResources().getString(R.string.anonymous));
      } else if (credentialToUse.equals(getString(R.string.credential_type_username_password))) {
         String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
         if (username == null || username.equals("")) {
            accountIdentity.setText(getResources().getString(R.string.no_account));
         } else {
            accountIdentity.setText(username);
         }
      } else if (credentialToUse.equals(getString(R.string.credential_type_google_account))) {
         String googleAccount = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
         if (googleAccount == null || googleAccount.equals("")) {
            accountIdentity.setText(getResources().getString(R.string.no_account));
         } else {
            accountIdentity.setText(googleAccount);
         }
      } else {
         accountIdentity.setText(getResources().getString(R.string.no_account));
      }

      perhapsEnableButtons();
   }

   @Override
   public void onResume() {
      super.onResume();

      WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onResume]");

      Intent incomingIntent = getActivity().getIntent();
      mAppName = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
      if (mAppName == null || mAppName.length() == 0) {
         WebLogger.getLogger(getAppName())
             .i(TAG, "[" + getId() + "] [onResume] mAppName is null so calling finish");
         getActivity().setResult(Activity.RESULT_CANCELED);
         getActivity().finish();
         return;
      }

      healthValidator.verifyTableHealth();
      updateCredentialsUI();
      perhapsEnableButtons();
      updateInterface();
   }

   private void updateCredentialsUI() {
      PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
      uriField.setText(props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL));

      String credentialToUse = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
      String[] credentialValues = getResources().getStringArray(R.array.credential_entry_values);
      String[] credentialEntries = getResources().getStringArray(R.array.credential_entries);

      if (credentialToUse == null) {
         credentialToUse = getString(R.string.credential_type_none);
      }

      for (int i = 0; i < credentialValues.length; ++i) {
         if (credentialToUse.equals(credentialValues[i])) {
            if (!credentialToUse.equals(getString(R.string.credential_type_none))) {
               accountAuthType.setText(credentialEntries[i]);
            }
         }
      }

      String account = ODKServicesPropertyUtils.getActiveUser(props);
      int indexOfColon = account.indexOf(':');
      if (indexOfColon > 0) {
         account = account.substring(indexOfColon + 1);
      }
      if (credentialToUse.equals(getString(R.string.credential_type_none))) {
         accountIdentity.setText(getResources().getString(R.string.anonymous));
      } else if (credentialToUse.equals(getString(R.string.credential_type_username_password))) {
         accountIdentity.setText(account);
      } else if (credentialToUse.equals(getString(R.string.credential_type_google_account))) {
         accountIdentity.setText(account);
      } else {
         accountIdentity.setText(getResources().getString(R.string.no_account));
      }
   }

   private void disableButtons() {
      authenticateNewUser.setEnabled(false);
      logout.setEnabled(false);
      cancel.setEnabled(false);
   }

   private void perhapsEnableButtons() {
      PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
      String url = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
      if (url == null || url.length() == 0) {
         disableButtons();
      } else {
         authenticateNewUser.setEnabled(true);
         logout.setEnabled(true);
         cancel.setEnabled(true);
      }
   }

   AlertDialog.Builder buildOkMessage(String title, String message) {
      AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
      builder.setCancelable(false);
      builder.setPositiveButton(getString(R.string.ok), null);
      builder.setTitle(title);
      builder.setMessage(message);
      return builder;
   }

   /**
    * Invoke this at the start of the verify server settings action
    */
   public void prepareForSyncAction() {
      // remove any settings for a URL other than the server URL...

      PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();

      String authType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
      if (authType == null) {
         authType = getString(R.string.credential_type_none);
      }

      if (getString(R.string.credential_type_google_account).equals(authType)) {
         authenticateGoogleAccount();
      } else {
         tickleInterface();
      }
   }

   /**
    * Hooked up to authorizeAccountButton's onClick in aggregate_activity.xml
    */
   public void authenticateGoogleAccount() {
      WebLogger.getLogger(getAppName())
          .d(TAG, "[" + getId() + "] [authenticateGoogleAccount] invalidated authtoken");
      invalidateAuthToken(getActivity(), getAppName());

      PropertiesSingleton props = CommonToolProperties.get(getActivity(), getAppName());
      Intent i = new Intent(getActivity(), AccountInfoActivity.class);
      Account account = new Account(props.getProperty(CommonToolProperties.KEY_ACCOUNT),
          ACCOUNT_TYPE_G);
      i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
      i.putExtra(AccountInfoActivity.INTENT_EXTRAS_ACCOUNT, account);
      startActivityForResult(i, LoginActivity.AUTHORIZE_ACCOUNT_RESULT_CODE);
   }

   public void onActivityResult(int requestCode, int resultCode, Intent data) {
      super.onActivityResult(requestCode, resultCode, data);

      if (requestCode == LoginActivity.AUTHORIZE_ACCOUNT_RESULT_CODE) {
         if (resultCode == Activity.RESULT_CANCELED) {
            invalidateAuthToken(getActivity(), getAppName());
            loginAction = LoginActions.IDLE;
         }
         tickleInterface();
      } else if (requestCode == RequestCodeConsts.RequestCodes.LAUNCH_CHECKPOINT_RESOLVER ||
          requestCode == RequestCodeConsts.RequestCodes.LAUNCH_CONFLICT_RESOLVER) {
         healthValidator.verifyTableHealth();
      }
   }

   private void tickleInterface() {
      WebLogger.getLogger(getAppName()).d(TAG, "[" + getId() + "] [tickleInterface] started");
      Activity activity = getActivity();
      if (activity == null) {
         // we are in transition -- do nothing
         WebLogger.getLogger(getAppName())
             .d(TAG, "[" + getId() + "] [tickleInterface] activity == null");
         handler.postDelayed(new Runnable() {
            @Override
            public void run() {
               tickleInterface();
            }
         }, 100);

         return;
      }
      ((ISyncServiceInterfaceActivity) activity)
          .invokeSyncInterfaceAction(new DoSyncActionCallback() {
             @Override
             public void doAction(OdkSyncServiceInterface syncServiceInterface)
                 throws RemoteException {
                if (syncServiceInterface != null) {
                   //          WebLogger.getLogger(getAppName()).d(TAG, "[" + getId() + "] [tickleInterface] syncServiceInterface != null");
                   final SyncStatus status = syncServiceInterface.getSyncStatus(getAppName());
                   final SyncProgressEvent event = syncServiceInterface
                       .getSyncProgressEvent(getAppName());
                   WebLogger.getLogger(getAppName()).e(TAG,
                       "tickleInterface status " + status.name() + " login " + "action " + loginAction.name());
                   if (status == SyncStatus.SYNCING) {
                      loginAction = LoginActions.MONITOR_VERIFYING;

                      handler.post(new Runnable() {
                         @Override
                         public void run() {
                            showProgressDialog(status, event.progressState, event.progressMessageText,
                                event.curProgressBar, event.maxProgressBar);
                         }
                      });
                      return;
                   }
                   switch (loginAction) {
                   case VERIFY:
                      syncServiceInterface.verifyServerSettings(getAppName());
                      loginAction = LoginActions.MONITOR_VERIFYING;

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
                       .d(TAG, "[" + getId() + "] [tickleInterface] syncServiceInterface == null");
                   // The service is not bound yet so now we need to try again
                   handler.postDelayed(new Runnable() {
                      @Override
                      public void run() {
                         tickleInterface();
                      }
                   }, 100);
                }
             }
          });
   }

   private void updateInterface() {
      Activity activity = getActivity();
      if (activity == null) {
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
             public void doAction(OdkSyncServiceInterface syncServiceInterface)
                 throws RemoteException {
                if (syncServiceInterface != null) {
                   final SyncStatus status = syncServiceInterface.getSyncStatus(getAppName());
                   final SyncProgressEvent event = syncServiceInterface
                       .getSyncProgressEvent(getAppName());
                   WebLogger.getLogger(getAppName()).e(TAG,
                       "updateInterface status " + status.name() + " login " + "action " + loginAction.name());
                   if (status == SyncStatus.SYNCING) {
                      loginAction = LoginActions.MONITOR_VERIFYING;

                      handler.post(new Runnable() {
                         @Override
                         public void run() {
                            showProgressDialog(status, event.progressState, event.progressMessageText,
                                event.curProgressBar, event.maxProgressBar);
                         }
                      });
                      return;
                   } else if (status != SyncStatus.NONE && loginAction != LoginActions.IDLE) {
                      // request completed
                      loginAction = LoginActions.IDLE;
                      final SyncOverallResult result = syncServiceInterface.getSyncResult(getAppName());
                      handler.post(new Runnable() {
                         @Override
                         public void run() {
                            dismissProgressDialog();
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

   @Override
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
             public void doAction(OdkSyncServiceInterface syncServiceInterface)
                 throws RemoteException {
                WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onSyncCompleted] called");
                if (syncServiceInterface != null) {
                   WebLogger.getLogger(getAppName()).i(TAG,
                       "[" + getId() + "] [onSyncCompleted] and syncServiceInterface is not null");
                   boolean completed = syncServiceInterface.clearAppSynchronizer(getAppName());
                   if (!completed) {
                      throw new IllegalStateException(
                          "Could not remove AppSynchronizer for " + getAppName());
                   }
                   updateCredentialsUI();
                   perhapsEnableButtons();
                   updateInterface();
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

   public boolean areCredentialsConfigured() {
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
            SyncBaseActivity.showAuthenticationErrorDialog(getActivity(), getString(R.string.sync_configure_username_password));
            return false;
         }
         return true;
      }
      if (getString(R.string.credential_type_google_account).equals(authType)) {
         String accountName = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
         if (accountName == null || accountName.length() == 0) {
            SyncBaseActivity.showAuthenticationErrorDialog(getActivity(), getString(R.string.sync_configure_google_account));
            return false;
         }
         return true;
      }
      SyncBaseActivity.showAuthenticationErrorDialog(getActivity(), getString(R.string.sync_configure_credentials));
      return false;
   }

   /**
    * Hooked to syncNowButton's onClick in aggregate_activity.xml
    */
   public void verifyServerSettings(View v) {
      WebLogger.getLogger(getAppName()).d(TAG,
          "[" + getId() + "] [onClickVerifyServerSettings] timestamp: " + System.currentTimeMillis());
      if (areCredentialsConfigured()) {
         disableButtons();
         loginAction = LoginActions.VERIFY;
         prepareForSyncAction();
      }
   }

   public static void invalidateAuthToken(Context context, String appName) {
      PropertiesSingleton props = CommonToolProperties.get(context, appName);
      AccountManager.get(context)
          .invalidateAuthToken(ACCOUNT_TYPE_G, props.getProperty(CommonToolProperties.KEY_AUTH));
      Map<String, String> properties = new HashMap<String, String>();
      properties.put(CommonToolProperties.KEY_AUTH, null);
      properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      properties.put(CommonToolProperties.KEY_USERS_LIST, "");
      props.setProperties(properties);
   }

   @Override
   public void onDestroy() {
      super.onDestroy();
      handler.removeCallbacksAndMessages(null);
      WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onDestroy]");
   }

   private void showProgressDialog(SyncStatus status, SyncProgressState progress, String message,
       int progressStep, int maxStep) {
      if (getActivity() == null) {
         // we are tearing down or still initializing
         return;
      }
      if (loginAction == LoginActions.MONITOR_VERIFYING) {

         disableButtons();

         if (progress == null) {
            progress = SyncProgressState.INACTIVE;
         }

         int id_title = R.string.verifying_server_settings;

         // try to retrieve the active dialog
         Fragment dialog = getFragmentManager().findFragmentByTag(PROGRESS_DIALOG_TAG);

         if (dialog != null && ((DismissableProgressDialogFragment) dialog).getDialog() != null) {
            ((DismissableProgressDialogFragment) dialog).getDialog().setTitle(id_title);
            ((DismissableProgressDialogFragment) dialog).setMessage(message, progressStep, maxStep);
         } else if (progressDialog != null && progressDialog.getDialog() != null) {
            progressDialog.getDialog().setTitle(id_title);
            progressDialog.setMessage(message, progressStep, maxStep);
         } else {
            if (progressDialog != null) {
               dismissProgressDialog();
            }
            progressDialog = DismissableProgressDialogFragment
                .newInstance(getString(id_title), message);

            // If fragment is not visible an exception could be thrown
            // TODO: Investigate a better way to handle this
            try {
               progressDialog.show(getFragmentManager(), PROGRESS_DIALOG_TAG);
            } catch (IllegalStateException ise) {
               ise.printStackTrace();
            }
         }
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

   private void dismissProgressDialog() {
      if (getActivity() == null) {
         // we are tearing down or still initializing
         return;
      }

      // try to retrieve the active dialog
      final Fragment dialog = getFragmentManager().findFragmentByTag(PROGRESS_DIALOG_TAG);

      if (dialog != null && dialog != progressDialog) {
         // the UI may not yet have resolved the showing of the dialog.
         // use a handler to add the dismiss to the end of the queue.
         handler.post(new Runnable() {
            @Override
            public void run() {
               try {
                  ((DismissableProgressDialogFragment) dialog).dismiss();
               } catch (Exception e) {
                  // ignore... we tried!
               }
               perhapsEnableButtons();
            }
         });
      }
      if (progressDialog != null) {
         final DismissableProgressDialogFragment scopedReference = progressDialog;
         progressDialog = null;
         // the UI may not yet have resolved the showing of the dialog.
         // use a handler to add the dismiss to the end of the queue.
         handler.post(new Runnable() {
            @Override
            public void run() {
               try {
                  if (scopedReference != null) {
                     scopedReference.dismiss();
                  }
               } catch (Exception e) {
                  e.printStackTrace();
               }
               perhapsEnableButtons();
            }
         });
      }
   }

   private void showOutcomeDialog(SyncStatus status, SyncOverallResult result) {
      if (getActivity() == null) {
         // we are tearing down or still initializing
         return;
      }
      if (loginAction == LoginActions.IDLE) {

         disableButtons();

         String message;
         int id_title;
         switch (status) {
         default:
            throw new IllegalStateException("Unexpected missing case statement");
         case
             /** earlier sync ended with socket or lower level transport or protocol error (e.g., 300's) */ NETWORK_TRANSPORT_ERROR:
            id_title = R.string.sync_communications_error;
            message = getString(R.string.sync_status_network_transport_error);
            break;
         case
             /** earlier sync ended with Authorization denied (authentication and/or access) error */ AUTHENTICATION_ERROR:
            id_title = R.string.sync_user_authorization_failure;
            message = getString(R.string.sync_status_authentication_error);
            break;
         case
             /** earlier sync ended with a 500 error from server */ SERVER_INTERNAL_ERROR:
            id_title = R.string.sync_communications_error;
            message = getString(R.string.sync_status_internal_server_error);
            break;
         case /** the server is not an ODK Server - bad client config */ SERVER_IS_NOT_ODK_SERVER:
            id_title = R.string.sync_device_configuration_failure;
            message = getString(R.string.sync_status_bad_gateway_or_client_config);
            break;
         case
             /** earlier sync ended with a 400 error that wasn't Authorization denied */ REQUEST_OR_PROTOCOL_ERROR:
            id_title = R.string.sync_communications_error;
            message = getString(R.string.sync_status_request_or_protocol_error);
            break;
         case /** no earlier sync and no active sync */ NONE:
         case /** active sync -- get SyncProgressEvent to see current status */ SYNCING:
         case
             /** earlier sync ended successfully without conflicts but needs row-level attachments sync'd */ SYNC_COMPLETE_PENDING_ATTACHMENTS:
         case
             /** the server does not have any configuration, or no configuration for this client version */ SERVER_MISSING_CONFIG_FILES:
         case
             /** the device does not have any configuration to push to server */ SERVER_RESET_FAILED_DEVICE_HAS_NO_CONFIG_FILES:
         case
             /** while a sync was in progress, another device reset the app config, requiring a restart of
              * our sync */ RESYNC_BECAUSE_CONFIG_HAS_BEEN_RESET_ERROR:
         case
             /** earlier sync ended with one or more tables containing row conflicts or checkpoint rows */ CONFLICT_RESOLUTION:
         case
             /** error accessing or updating database */ DEVICE_ERROR:
            id_title = R.string.sync_device_internal_error;
            message = getString(R.string.sync_status_device_internal_error);
            break;
         case
             /** the server is not configured for this appName -- Site Admin / Preferences */ APPNAME_NOT_SUPPORTED_BY_SERVER:
            id_title = R.string.sync_server_configuration_failure;
            message = getString(R.string.sync_status_appname_not_supported_by_server);
            break;
         case
             /** earlier sync ended successfully without conflicts and all row-level attachments sync'd */ SYNC_COMPLETE:
            id_title = R.string.verify_server_setttings_successful;
            message = getString(R.string.verify_server_setttings_successful_text);
            break;
         }
         // try to retrieve the active dialog
         Fragment dialog = getFragmentManager().findFragmentByTag(OUTCOME_DIALOG_TAG);

         if (dialog != null && ((DismissableOutcomeDialogFragment) dialog).getDialog() != null) {
            ((DismissableOutcomeDialogFragment) dialog).getDialog().setTitle(id_title);
            ((DismissableOutcomeDialogFragment) dialog).setMessage(message);
         } else if (outcomeDialog != null && outcomeDialog.getDialog() != null) {
            outcomeDialog.getDialog().setTitle(id_title);
            outcomeDialog.setMessage(message);
         } else {
            if (outcomeDialog != null) {
               dismissOutcomeDialog();
            }
            outcomeDialog = DismissableOutcomeDialogFragment
                .newInstance(getString(id_title), message, (status == SyncStatus.SYNC_COMPLETE
                    || status == SyncStatus.SYNC_COMPLETE_PENDING_ATTACHMENTS), LoginFragment.NAME);

            // If fragment is not visible an exception could be thrown
            // TODO: Investigate a better way to handle this
            try {
               outcomeDialog.show(getFragmentManager(), OUTCOME_DIALOG_TAG);
            } catch (IllegalStateException ise) {
               ise.printStackTrace();
            }
         }
      }
   }

   private void dismissOutcomeDialog() {
      if (getActivity() == null) {
         // we are tearing down or still initializing
         return;
      }

      // try to retrieve the active dialog
      final Fragment dialog = getFragmentManager().findFragmentByTag(PROGRESS_DIALOG_TAG);

      if (dialog != null && dialog != outcomeDialog) {
         // the UI may not yet have resolved the showing of the dialog.
         // use a handler to add the dismiss to the end of the queue.
         handler.post(new Runnable() {
            @Override
            public void run() {
               try {
                  ((DismissableOutcomeDialogFragment) dialog).dismiss();
               } catch (Exception e) {
                  // ignore... we tried!
               }
               perhapsEnableButtons();
            }
         });
      }
      if (outcomeDialog != null) {
         final DismissableOutcomeDialogFragment scopedReference = outcomeDialog;
         outcomeDialog = null;
         // the UI may not yet have resolved the showing of the dialog.
         // use a handler to add the dismiss to the end of the queue.
         handler.post(new Runnable() {
            @Override
            public void run() {
               try {
                  if (scopedReference != null) {
                     scopedReference.dismiss();
                  }
               } catch (Exception e) {
                  e.printStackTrace();
               }
               perhapsEnableButtons();
            }
         });
      }
   }

   public String getAppName() {
      if (mAppName == null) {
         throw new IllegalStateException("appName not yet initialized");
      }
      return mAppName;
   }
}
