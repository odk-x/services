package org.opendatakit.services.sync.actions.fragments;

import android.accounts.AccountManager;
import android.app.Activity;
import android.app.Fragment;
import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.os.RemoteException;
import android.view.View;
import android.widget.TextView;
import android.widget.Toast;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.fragment.DismissableOutcomeDialogFragment.ISyncOutcomeHandler;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.sync.actions.activities.DoSyncActionCallback;
import org.opendatakit.services.sync.actions.activities.ISyncServiceInterfaceActivity;
import org.opendatakit.services.sync.actions.activities.SyncBaseActivity;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;
import org.opendatakit.sync.service.OdkSyncServiceInterface;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wrb on 2/5/2018.
 */

abstract class AbsSyncUIFragment extends Fragment implements ISyncOutcomeHandler {

   private static final String TAG = AbsSyncUIFragment.class.getSimpleName();

   private static final String ACCOUNT_TYPE_G = "com.google";

   private String mAppName;

   final Handler handler = new Handler();
   TextView uriField;
   TextView accountAuthType;
   TextView accountIdentity;


   abstract void postTaskToAccessSyncService();
   abstract void perhapsEnableButtons();
   abstract void updateInterface();
   abstract void syncCompletedAction(OdkSyncServiceInterface syncServiceInterface) throws
       RemoteException;


   /**
    * Obtains the references to the account info and populates the member variables with the
    * references
    *
    * @param view
    */
   void populateTextViewMemberVariablesReferences(View view) {
      uriField = (TextView) view.findViewById(R.id.sync_uri_field);
      accountAuthType = (TextView) view.findViewById(R.id.sync_account_auth_label);
      accountIdentity = (TextView) view.findViewById(R.id.sync_account);
   }

   @Override
   public void onResume() {
      super.onResume();

      WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onResume]");

      Intent incomingIntent = getActivity().getIntent();
      mAppName = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
      if ( mAppName == null || mAppName.length() == 0 ) {
         WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onResume] mAppName is null so calling finish");
         getActivity().setResult(Activity.RESULT_CANCELED);
         getActivity().finish();
         return;
      }

      updateCredentialsUI();
      perhapsEnableButtons();
      updateInterface();
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
   }

   @Override
   public void onDestroy() {
      super.onDestroy();
      handler.removeCallbacksAndMessages(null);
      WebLogger.getLogger(getAppName()).i(TAG, "[" + getId() + "] [onDestroy]");
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

      if (getString(R.string.credential_type_google_account).equals(authType)) {
         //    authenticateGoogleAccount();
      } else {
         postTaskToAccessSyncService();
      }
   }

   String getAppName() {
      if (mAppName == null) {
         throw new IllegalStateException("appName not yet initialized");
      }
      return mAppName;
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
               SyncBaseActivity.showAuthenticationErrorDialog(getActivity(), getString(R.string.sync_configure_username_password));
            } else {
               Toast.makeText(getActivity(), getString(R.string.sync_configure_username_password),
                   Toast.LENGTH_LONG).show();
            }
            return false;
         }
         return true;
      }
      if(createError) {
         SyncBaseActivity.showAuthenticationErrorDialog(getActivity(), getString(R.string.sync_configure_credentials));
      } else {
         Toast.makeText(getActivity(), getString(R.string.sync_configure_credentials),
             Toast.LENGTH_LONG).show();
      }
      return false;
   }

   void updateCredentialsUI() {
      PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
      uriField.setText(props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL));

      String credentialToUse = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
      String[] credentialValues = getResources().getStringArray(R.array.credential_entry_values);
      String[] credentialEntries = getResources().getStringArray(R.array.credential_entries);

      if ( credentialToUse == null ) {
         credentialToUse = getString(R.string.credential_type_none);
      }

      for ( int i = 0 ; i < credentialValues.length ; ++i ) {
         if ( credentialToUse.equals(credentialValues[i]) ) {
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
      if ( credentialToUse.equals(getString(R.string.credential_type_none))) {
         accountIdentity.setText(getResources().getString(R.string.anonymous));
      } else if ( credentialToUse.equals(getString(R.string.credential_type_username_password))) {
         accountIdentity.setText(account);
      } else if ( credentialToUse.equals(getString(R.string.credential_type_google_account))) {
         accountIdentity.setText(account);
      } else {
         accountIdentity.setText(getResources().getString(R.string.no_account));
      }
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
}
