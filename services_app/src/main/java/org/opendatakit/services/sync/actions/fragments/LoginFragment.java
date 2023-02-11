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
import android.content.DialogInterface;
import android.content.Intent;
import android.os.RemoteException;

import androidx.appcompat.app.AlertDialog;
import androidx.lifecycle.Lifecycle;
import androidx.lifecycle.LifecycleEventObserver;
import androidx.lifecycle.ViewModelProvider;
import androidx.navigation.NavController;
import androidx.navigation.Navigation;

import com.google.android.material.dialog.MaterialAlertDialogBuilder;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.consts.RequestCodeConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.LoginActions;
import org.opendatakit.services.sync.actions.activities.DoSyncActionCallback;
import org.opendatakit.services.sync.actions.activities.ISyncServiceInterfaceActivity;
import org.opendatakit.services.sync.actions.activities.LoginActivity;
import org.opendatakit.services.sync.actions.viewModels.LoginViewModel;
import org.opendatakit.services.utilities.Constants;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;
import org.opendatakit.services.utilities.TableHealthValidator;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.sync.service.IOdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.sync.service.SyncProgressEvent;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.SyncStatus;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mitchellsundt@gmail.com
 */
public abstract class LoginFragment extends AbsSyncUIFragment {

   private static final String TAG = "LoginFragment";

   public static final String NAME = "LoginFragment";

   private static final String PROGRESS_DIALOG_TAG = "progressDialogLogin";
   private static final String OUTCOME_DIALOG_TAG = "outcomeDialogLogin";

   private TableHealthValidator healthValidator;

   protected LoginViewModel loginViewModel;
   protected NavController navController;

   public LoginFragment() {
      super(OUTCOME_DIALOG_TAG, PROGRESS_DIALOG_TAG);
   }

   protected void setupViewModelAndNavController(){
      loginViewModel=new ViewModelProvider(requireActivity()).get(LoginViewModel.class);
      navController= Navigation.findNavController(requireView());

      Intent intent=requireActivity().getIntent();
      if(!intent.hasExtra(Constants.LOGIN_INTENT_TYPE_KEY))
         loginViewModel.updateFunctionType(Constants.LOGIN_TYPE_SIGN_IN);
      else
         loginViewModel.updateFunctionType(intent.getStringExtra(Constants.LOGIN_INTENT_TYPE_KEY));
   }

   @Override
   protected void handleLifecycleEvents() {
      super.handleLifecycleEvents();

      requireActivity().getLifecycle().addObserver((LifecycleEventObserver) (source, event) -> {
         if(event== Lifecycle.Event.ON_CREATE){
            disableButtons();
            healthValidator = new TableHealthValidator(getAppName(), getActivity());
         }
      });
   }

   /**
    * Hooked to syncNowButton's onClick in aggregate_activity.xml
    */
   public void verifyServerSettings() {
      WebLogger.getLogger(getAppName()).d(TAG,
              "[" + getId() + "] [onClickVerifyServerSettings] timestamp: " + System.currentTimeMillis());
      if (areCredentialsConfigured(true)) {
         disableButtons();
         loginViewModel.updateLoginAction(LoginActions.VERIFY);
         prepareForSyncAction();
      }
   }

   abstract void disableButtons();

   abstract void perhapsEnableButtons();

   public void onActivityResult(int requestCode, int resultCode, Intent data) {
      super.onActivityResult(requestCode, resultCode, data);

      if (requestCode == LoginActivity.AUTHORIZE_ACCOUNT_RESULT_CODE) {
         if (resultCode == Activity.RESULT_CANCELED) {
            loginViewModel.updateLoginAction(LoginActions.IDLE);
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
                       "postTaskToAccessSyncService status " + status.name() + " login " + "action " + loginViewModel.getCurrentAction().name());
                   if (status == SyncStatus.SYNCING) {
                      loginViewModel.updateLoginAction(LoginActions.MONITOR_VERIFYING);

                      handler.post(new Runnable() {
                         @Override
                         public void run() {
                            showProgressDialog(status, event.progressState, event.progressMessageText,
                                event.curProgressBar, event.maxProgressBar);
                         }
                      });
                      return;
                   }
                   switch (loginViewModel.getCurrentAction()) {
                   case VERIFY:
                      syncServiceInterface.verifyServerSettings(getAppName());
                      loginViewModel.updateLoginAction(LoginActions.MONITOR_VERIFYING);

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
                       "updateInterface status " + status.name() + " login " + "action " + loginViewModel.getCurrentAction().name());
                   if (status == SyncStatus.SYNCING) {
                      loginViewModel.updateLoginAction(LoginActions.MONITOR_VERIFYING);

                      handler.post(new Runnable() {
                         @Override
                         public void run() {
                            showProgressDialog(status, event.progressState, event.progressMessageText,
                                event.curProgressBar, event.maxProgressBar);
                         }
                      });
                      return;
                   } else if (status != SyncStatus.NONE && loginViewModel.getCurrentAction() != LoginActions.IDLE) {
                      // request completed
                      loginViewModel.updateLoginAction(LoginActions.IDLE);
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
      SyncStatus syncStatus = syncServiceInterface.getSyncStatus(getAppName());
      boolean completed = syncServiceInterface.clearAppSynchronizer(getAppName());
      if (!completed) {
         throw new IllegalStateException(
             "Could not remove AppSynchronizer for " + getAppName());
      }
      perhapsEnableButtons();
      updateInterface();
      performActionOnSyncComplete(syncStatus);
   }

   private void showProgressDialog(SyncStatus status, SyncProgressState progress, String message,
       int progressStep, int maxStep) {
      if (getActivity() == null) {
         // we are tearing down or still initializing
         return;
      }
      if (loginViewModel.getCurrentAction() == LoginActions.MONITOR_VERIFYING) {

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
      if (loginViewModel.getCurrentAction() == LoginActions.IDLE) {

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

         createAlertDialog(getString(id_title), message);
      }
   }

   private void performActionOnSyncComplete(SyncStatus syncStatus){
      PropertiesSingleton props = getProps();
      Map<String, String> properties= new HashMap<>();
      switch (syncStatus){
         case SERVER_IS_NOT_ODK_SERVER:{
            properties.putAll(UpdateServerSettingsFragment.getUpdateUrlProperties(loginViewModel.getUrl()));
            props.setProperties(properties);
            updateViewModelWithProps();

            DialogInterface.OnClickListener onClickListener = (dialog, which) -> navController.navigate(R.id.updateServerSettingsFragmentL);
            showAlertDialog(
                    "Server is not an ODK Server",
                    "Would you like to change the Server URL?",
                    onClickListener);
            break;
         }
         case AUTHENTICATION_ERROR:{
            properties.put(CommonToolProperties.KEY_IS_SERVER_VERIFIED, Boolean.toString(true));

            UserState userState = UserState.valueOf(props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE));

            if(userState == UserState.ANONYMOUS){
               properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED, Boolean.toString(true));
               properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED, Boolean.toString(false));
               props.setProperties(properties);
               updateViewModelWithProps();

               DialogInterface.OnClickListener onClickListener = (dialog, which) -> {
                  ODKServicesPropertyUtils.clearActiveUser(getProps());
                  updateViewModelWithProps();
               };

               showAlertDialog("Server Does Not Support Anonymous",
                       "Would you like to logout as Anonymous now?",
                       onClickListener);
            } else {
               properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, Boolean.toString(false));
               props.setProperties(properties);
               updateViewModelWithProps();
            }
            break;
         }
         case SYNC_COMPLETE:{
            properties.put(CommonToolProperties.KEY_IS_SERVER_VERIFIED, Boolean.toString(true));

            UserState userState = UserState.valueOf(props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE));

            if(userState==UserState.ANONYMOUS){
               properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED, Boolean.toString(true));
               properties.put(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED, Boolean.toString(true));
            }
            else {
               properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED, Boolean.toString(true));
            }

            props.setProperties(properties);
            updateViewModelWithProps();
            requireActivity().finish();
            break;
         }
      }
   }

   public void showAlertDialog(String title, String message, DialogInterface.OnClickListener onPositiveButtonClick){

      /**
       * New dialog styling
       * MaterialAlertDialogBuilder is standard for all ODK-X Apps
       * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
       * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
       */

      AlertDialog alertDialog = new MaterialAlertDialogBuilder(requireActivity(),R.style.OdkXAlertDialogStyle)
              .setTitle(title)
              .setMessage(message)
              .setPositiveButton("Yes",onPositiveButtonClick)
              .setNegativeButton("No", (dialog, which) -> dialog.dismiss())
              .setCancelable(true)
              .create();
      alertDialog.setCanceledOnTouchOutside(true);
      alertDialog.show();
   }
}
