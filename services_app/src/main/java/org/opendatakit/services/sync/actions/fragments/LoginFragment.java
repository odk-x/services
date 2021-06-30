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
import android.graphics.Paint;
import android.os.Bundle;
import android.os.RemoteException;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import com.google.android.material.textfield.TextInputLayout;

import org.opendatakit.consts.RequestCodeConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.sync.actions.LoginActions;
import org.opendatakit.services.sync.actions.activities.DoSyncActionCallback;
import org.opendatakit.services.sync.actions.activities.ISyncServiceInterfaceActivity;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.utilities.TableHealthValidator;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.sync.service.IOdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.sync.service.SyncProgressEvent;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.SyncStatus;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CheckedOutputStream;

/**
 * @author mitchellsundt@gmail.com
 */
public class LoginFragment extends AbsSyncUIFragment {

   private class OnButtonClick implements View.OnClickListener{

      @Override
      public void onClick(View v) {
         if(v.getId()==R.id.btnAnonymousSignInLogin){
            signInAsAnonymousUser();
         }
         else if (v.getId()==R.id.btnUserSignInLogin){
            inSetCredentialsState();
         }
         else if(v.getId()==R.id.btnAuthenticateUserLogin){
            setNewCredentials();
            verifyServerSettings();
         }
      }
   }

   private static final String TAG = "LoginFragment";

   public static final String NAME = "LoginFragment";
   public static final int ID = R.layout.login_fragment;

   private static final String LOGIN_ACTION = "loginAction";

   private static final String PROGRESS_DIALOG_TAG = "progressDialogLogin";
   private static final String OUTCOME_DIALOG_TAG = "outcomeDialogLogin";

   private PropertiesSingleton props;
   private TableHealthValidator healthValidator;

   private LoginActions loginAction = LoginActions.IDLE;

   private TextView tvServerUrl, tvTitle;
   private TextInputLayout inputUsername, inputPassword;
   private Button btnAnonymousSignIn, btnUserSignIn, btnAuthenticateCredentials;

   private UserState userState;

   public LoginFragment() {
      super(OUTCOME_DIALOG_TAG, PROGRESS_DIALOG_TAG);
   }

   @Override
   public void onSaveInstanceState(Bundle outState) {
      super.onSaveInstanceState(outState);
      outState.putString(LOGIN_ACTION, loginAction.name());
   }

   @Override
   public void onActivityCreated(Bundle savedInstanceState) {
      super.onActivityCreated(savedInstanceState);

      Intent incomingIntent = getActivity().getIntent();
      if (savedInstanceState != null && savedInstanceState.containsKey(LOGIN_ACTION)) {
         String action = savedInstanceState.getString(LOGIN_ACTION);
         try {
            loginAction = LoginActions.valueOf(action);
         } catch (IllegalArgumentException e) {
            loginAction = LoginActions.IDLE;
         }
      }
      disableButtons();

      healthValidator = new TableHealthValidator(getAppName(), getActivity());
   }

   @Override
   public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
      super.onCreateView(inflater, container, savedInstanceState);

      View view = inflater.inflate(ID, container, false);

      props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();

      if (savedInstanceState != null && savedInstanceState.containsKey(LOGIN_ACTION)) {
         String action = savedInstanceState.getString(LOGIN_ACTION);
         try {
            loginAction = LoginActions.valueOf(action);
         } catch (IllegalArgumentException e) {
            loginAction = LoginActions.IDLE;
         }
      }

      return view;
   }

   @Override
   public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
      super.onViewCreated(view, savedInstanceState);
      findViewsAndAttachListeners(view);
      inChooseSignInTypeState();
   }

   @Override
   public void onResume() {
      super.onResume();
      updateUserInterface();
   }

   private void findViewsAndAttachListeners(View view){
      tvServerUrl=view.findViewById(R.id.tvServerUrlLogin);
      tvTitle=view.findViewById(R.id.tvTitleLogin);

      inputUsername=view.findViewById(R.id.inputUsernameLogin);
      inputPassword=view.findViewById(R.id.inputPasswordLogin);

      btnAnonymousSignIn=view.findViewById(R.id.btnAnonymousSignInLogin);
      btnUserSignIn=view.findViewById(R.id.btnUserSignInLogin);
      btnAuthenticateCredentials=view.findViewById(R.id.btnAuthenticateUserLogin);

      OnButtonClick onButtonClick=new OnButtonClick();
      btnAnonymousSignIn.setOnClickListener(onButtonClick);
      btnUserSignIn.setOnClickListener(onButtonClick);
      btnAuthenticateCredentials.setOnClickListener(onButtonClick);
   }

   private void updateUserInterface(){
      props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
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

   private void updateCommonInfo(){
      String serverUrl=props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
      tvServerUrl.setText(serverUrl);
      tvServerUrl.setPaintFlags(tvServerUrl.getPaintFlags() | Paint.UNDERLINE_TEXT_FLAG);
   }

   private void inLoggedOutState(){
      tvTitle.setText("Sign In");
      inChooseSignInTypeState();
   }

   private void inAnonymousState(){
      tvTitle.setText("Sign In using Credentials");
      inSetCredentialsState();
   }

   private void inAuthenticatedState(){
      tvTitle.setText("Update Login Credentials");
   }

   private void inChooseSignInTypeState(){
      handleViewVisibility(View.VISIBLE, View.VISIBLE, View.GONE);
   }

   private void inSetCredentialsState(){
      handleViewVisibility(View.GONE, View.GONE, View.VISIBLE);
   }

   private void handleViewVisibility(int anonymousVisible, int authenticatedUserVisible, int setCredentialsVisible){
      btnAnonymousSignIn.setVisibility(anonymousVisible);
      btnUserSignIn.setVisibility(authenticatedUserVisible);

      inputUsername.setVisibility(setCredentialsVisible);
      inputPassword.setVisibility(setCredentialsVisible);
      btnAuthenticateCredentials.setVisibility(setCredentialsVisible);
   }

   private void signInAsAnonymousUser(){
      Map<String,String> properties = new HashMap<String,String>();
      properties.put(CommonToolProperties.KEY_AUTHENTICATION_TYPE, "none");
      properties.put(CommonToolProperties.KEY_CURRENT_USER_STATE, UserState.ANONYMOUS.name());
      properties.put(CommonToolProperties.KEY_USERNAME, "");
      properties.remove(CommonToolProperties.KEY_IS_USER_AUTHENTICATED);
      properties.remove(CommonToolProperties.KEY_LAST_SYNC_INFO);
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
      properties.put(CommonToolProperties.KEY_USERS_LIST, "");
      props.setProperties(properties);

      verifyServerSettings();
   }

   private void setNewCredentials() {
      String username = inputUsername.getEditText().getText().toString();
      String pw = inputPassword.getEditText().getText().toString();

      Map<String, String> properties = new HashMap<>();
      properties.put(CommonToolProperties.KEY_AUTHENTICATION_TYPE, getString(R.string.credential_type_username_password));
      properties.put(CommonToolProperties.KEY_CURRENT_USER_STATE, UserState.AUTHENTICATED_USER.name());
      properties.put(CommonToolProperties.KEY_USERNAME, username);
      properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, Boolean.toString(false));
      properties.remove(CommonToolProperties.KEY_LAST_SYNC_INFO);
      properties.put(CommonToolProperties.KEY_PASSWORD, pw);
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
      properties.put(CommonToolProperties.KEY_USERS_LIST, "");
      props.setProperties(properties);
   }

   /**
    * Hooked to syncNowButton's onClick in aggregate_activity.xml
    */
   public void verifyServerSettings() {
      WebLogger.getLogger(getAppName()).d(TAG,
              "[" + getId() + "] [onClickVerifyServerSettings] timestamp: " + System.currentTimeMillis());
      if (areCredentialsConfigured(true)) {
         disableButtons();
         loginAction = LoginActions.VERIFY;
         prepareForSyncAction();
      }
   }

   private void disableButtons() {
      btnUserSignIn.setEnabled(false);
      btnAnonymousSignIn.setEnabled(false);
      btnAuthenticateCredentials.setEnabled(false);
   }

   void perhapsEnableButtons() {
//      PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
      String url = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
      if (url == null || url.length() == 0) {
         disableButtons();
      } else {
         btnUserSignIn.setEnabled(true);
         btnAnonymousSignIn.setEnabled(true);
         btnAuthenticateCredentials.setEnabled(true);
      }
   }

   public void onActivityResult(int requestCode, int resultCode, Intent data) {
      super.onActivityResult(requestCode, resultCode, data);

      if (requestCode == LoginActivity.AUTHORIZE_ACCOUNT_RESULT_CODE) {
         if (resultCode == Activity.RESULT_CANCELED) {
            loginAction = LoginActions.IDLE;
         }
         postTaskToAccessSyncService();
      } else if (requestCode == RequestCodeConsts.RequestCodes.LAUNCH_CHECKPOINT_RESOLVER ||
          requestCode == RequestCodeConsts.RequestCodes.LAUNCH_CONFLICT_RESOLVER) {
         healthValidator.verifyTableHealth();
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
                   WebLogger.getLogger(getAppName()).e(TAG,
                       "postTaskToAccessSyncService status " + status.name() + " login " + "action " + loginAction.name());
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
         if(activity == null) {
            WebLogger.getLogger(getAppName()).w(TAG, "[" + getId() + "] [updateInterface] activity == null = return");
         } else if(!hasDialogBeenCreated() ) {
            WebLogger.getLogger(getAppName()).w(TAG, "[" + getId() + "] [updateInterface] !msgManager.hasDialogBeenCreated()");
         } else if(!this.isResumed() ) {
            WebLogger.getLogger(getAppName()).w(TAG, "[" + getId() + "] [updateInterface] !this.isResumed()");
         }
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
                      // TODO: figure out if there is a syncStatus that is not covered
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

   void syncCompletedAction(IOdkSyncServiceInterface syncServiceInterface) throws RemoteException {
      removeAnySyncNotification();
      boolean completed = syncServiceInterface.clearAppSynchronizer(getAppName());
      if (!completed) {
         throw new IllegalStateException(
             "Could not remove AppSynchronizer for " + getAppName());
      }
      perhapsEnableButtons();
      updateInterface();
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

         showProgressDialog(getString(id_title), message, progressStep, maxStep);

         if (status == SyncStatus.SYNCING || status == SyncStatus.NONE) {
            handler.postDelayed(new Runnable() {
               @Override public void run() {
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
      if (loginAction == LoginActions.IDLE) {

         disableButtons();
         updateProps(status);

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

         createAlertDialog(getString(id_title), message);
      }
   }

   private void updateProps(SyncStatus status){
      Map<String ,String > properties=new HashMap<>();
      switch (status){
         case AUTHENTICATION_ERROR:{
            properties.put(CommonToolProperties.KEY_IS_SERVER_VERIFIED, Boolean.toString(true));
            properties.put(CommonToolProperties.KEY_LAST_SERVER_VERIFIED_TIME, Long.toString(new Date().getTime()));

            UserState userState=UserState.valueOf(props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE));

            if(userState==UserState.ANONYMOUS){
               properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED, Boolean.toString(true));
               properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED, Boolean.toString(false));
            }
            else {
               properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, Boolean.toString(false));
            }
            break;
         }
         case SERVER_IS_NOT_ODK_SERVER:{

            break;
         }
         case SYNC_COMPLETE:{
            properties.put(CommonToolProperties.KEY_IS_SERVER_VERIFIED, Boolean.toString(true));
            properties.put(CommonToolProperties.KEY_LAST_SERVER_VERIFIED_TIME, Long.toString(new Date().getTime()));

            UserState userState=UserState.valueOf(props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE));

            if(userState==UserState.ANONYMOUS){
               properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED, Boolean.toString(true));
               properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED, Boolean.toString(true));
            }
            else {
               properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, Boolean.toString(true));
            }

            break;
         }
      }
      props.setProperties(properties);
   }
}
