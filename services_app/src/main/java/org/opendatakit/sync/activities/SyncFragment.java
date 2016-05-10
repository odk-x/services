/*
 * Copyright (C) 2015 University of Washington
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
package org.opendatakit.sync.activities;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.app.Activity;
import android.app.AlertDialog;
import android.app.Fragment;
import android.content.ComponentName;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.*;

import org.opendatakit.IntentConsts;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.fragment.ProgressDialogFragment;
import org.opendatakit.common.android.logic.CommonToolProperties;
import org.opendatakit.common.android.logic.PropertiesSingleton;
import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.SyncETagsUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.services.R;
import org.opendatakit.common.android.activities.IOdkAppPropertiesActivity;
import org.opendatakit.sync.service.OdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.sqlite.database.sqlite.SQLiteException;

/**
 * @author mitchellsundt@gmail.com
 */
public class SyncFragment extends Fragment {

  private static final String TAG = "SyncFragment";

  public static final String NAME = "SyncFragment";
  public static final int ID = R.layout.sync_launch_fragment;

  private static final String ACCOUNT_TYPE_G = "com.google";

  private static final String SYNC_ATTACHMENT_TREATMENT = "syncAttachmentState";

  private static final String PROGRESS_DIALOG_TAG = "progressDialog";

  private static enum DialogState {
    Progress, Alert, None
  };

  private String mAppName;

  private Handler handler = new Handler();
  private ProgressDialogFragment progressDialog = null;

  private TextView uriField;
  private TextView accountAuthType;
  private TextView accountIdentity;

  private Spinner syncInstanceAttachmentsSpinner;
  private SyncAttachmentState syncAttachmentState = SyncAttachmentState.SYNC;

  private Button startSync;
  private Button resetServer;

  private enum SyncActions { IDLE, MONITOR_SYNC, MONITOR_RESET_SERVER, SYNC, RESET_SERVER };
  private SyncActions syncAction = SyncActions.IDLE;

  private class ServiceConnectionWrapper implements ServiceConnection {

    @Override public void onServiceConnected(ComponentName name, IBinder service) {
      if (!name.getClassName().equals("org.opendatakit.sync.service.OdkSyncService")) {
        WebLogger.getLogger(getAppName()).e(TAG, "Unrecognized service");
        return;
      }
      synchronized (odkSyncInterfaceBindComplete) {
        odkSyncInterface = (service == null) ? null : OdkSyncServiceInterface.Stub.asInterface(service);
        active = false;
      }
      tickleInterface();
    }

    @Override public void onServiceDisconnected(ComponentName name) {
      synchronized (odkSyncInterfaceBindComplete) {
        odkSyncInterface = null;
        active = false;
      }
      tickleInterface();
    }
  }

  private ServiceConnectionWrapper odkSyncServiceConnection = new ServiceConnectionWrapper();
  private Object odkSyncInterfaceBindComplete = new Object();
  private OdkSyncServiceInterface odkSyncInterface;
  private boolean active = false;

  private void tickleInterface() {
    OdkSyncServiceInterface syncServiceInterface = null;

    synchronized (odkSyncInterfaceBindComplete) {
      if ( odkSyncInterface != null ) {
        syncServiceInterface = odkSyncInterface;
      }
    }

    try {
      if (syncServiceInterface != null) {
        switch (this.syncAction) {
        case SYNC:
          syncServiceInterface.synchronizeWithServer(getAppName(), syncAttachmentState);
          syncAction = SyncActions.MONITOR_SYNC;
          break;
        case RESET_SERVER:
          syncServiceInterface.resetServer(getAppName(), syncAttachmentState);
          syncAction = SyncActions.MONITOR_RESET_SERVER;
          break;
        default:
          break;
        }
      }

      // Otherwise, set up a bind and attempt to re-tickle...
      Log.i(TAG, "Attempting bind to Database service");
      Intent bind_intent = new Intent();
      bind_intent.setClassName("org.opendatakit.services",
          "org.opendatakit.sync.service.OdkSyncService");

      synchronized (odkSyncInterfaceBindComplete) {
        if (!active) {
          active = true;
        }
      }

      getActivity().bindService(bind_intent, odkSyncServiceConnection,
          Context.BIND_AUTO_CREATE | ((Build.VERSION.SDK_INT >= 14) ?
              Context.BIND_ADJUST_WITH_ACTIVITY :
              0));


    } catch ( RemoteException e ) {
      WebLogger.getLogger(getAppName()).printStackTrace(e);
      WebLogger.getLogger(getAppName()).e(TAG, "exception while invoking sync service");
      Toast.makeText(getActivity(), "Exception while invoking sync service", Toast.LENGTH_LONG).show();
    }
  }

  @Override
  public void onSaveInstanceState(Bundle outState) {
    super.onSaveInstanceState(outState);
    outState.putString(SYNC_ATTACHMENT_TREATMENT, syncAttachmentState.name());
  }


  @Override
  public void onActivityCreated(Bundle savedInstanceState) {
    super.onActivityCreated(savedInstanceState);

    Intent incomingIntent = getActivity().getIntent();
    mAppName = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if ( mAppName == null || mAppName.length() == 0 ) {
      getActivity().setResult(Activity.RESULT_CANCELED);
      getActivity().finish();
      return;
    }

    if ( savedInstanceState != null && savedInstanceState.containsKey(SYNC_ATTACHMENT_TREATMENT) ) {
      String treatment = savedInstanceState.getString(SYNC_ATTACHMENT_TREATMENT);
      try {
        syncAttachmentState = SyncAttachmentState.valueOf(treatment);
      } catch ( IllegalArgumentException e ) {
        syncAttachmentState = SyncAttachmentState.SYNC;
      }
    }
    disableButtons();
  }

  @Override
  public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
    super.onCreateView(inflater, container, savedInstanceState);

    View view = inflater.inflate(ID, container, false);
    uriField = (TextView) view.findViewById(R.id.sync_uri_field);
    accountAuthType = (TextView) view.findViewById(R.id.sync_account_auth_label);
    accountIdentity = (TextView) view.findViewById(R.id.sync_account);

    syncInstanceAttachmentsSpinner = (Spinner) view.findViewById(R.id.sync_instance_attachments);

    if ( savedInstanceState != null && savedInstanceState.containsKey(SYNC_ATTACHMENT_TREATMENT) ) {
      String treatment = savedInstanceState.getString(SYNC_ATTACHMENT_TREATMENT);
      try {
        syncAttachmentState = SyncAttachmentState.valueOf(treatment);
      } catch ( IllegalArgumentException e ) {
        syncAttachmentState = SyncAttachmentState.SYNC;
      }
    }

    ArrayAdapter<CharSequence> instanceAttachmentsAdapter = ArrayAdapter.createFromResource(
        getActivity(), R.array.sync_attachment_option_names, android.R.layout.select_dialog_item);
    syncInstanceAttachmentsSpinner.setAdapter(instanceAttachmentsAdapter);

    syncInstanceAttachmentsSpinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
      @Override public void onItemSelected(AdapterView<?> parent, View view, int position,
          long id) {
        String[] syncAttachmentType =
            getResources().getStringArray(R.array.sync_attachment_option_values);
        syncAttachmentState = SyncAttachmentState.valueOf(syncAttachmentType[position]);
      }

      @Override public void onNothingSelected(AdapterView<?> parent) {
        String[] syncAttachmentType =
            getResources().getStringArray(R.array.sync_attachment_option_values);
        syncAttachmentState = SyncAttachmentState.SYNC;
        for ( int i = 0 ; i < syncAttachmentType.length ; ++i ) {
          if ( syncAttachmentType[i].equals(syncAttachmentState.name()) ) {
            syncInstanceAttachmentsSpinner.setSelection(i);
            break;
          }
        }
      }
    });

    startSync = (Button) view.findViewById(R.id.sync_start_button);
    startSync.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        onClickSyncNow(v);
      }
    });
    resetServer = (Button) view.findViewById(R.id.sync_reset_server_button);
    resetServer.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        onClickResetServer(v);
      }
    });

    return view;
  }

  @Override
  public void onResume() {
    super.onResume();

    Intent incomingIntent = getActivity().getIntent();
    mAppName = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if ( mAppName == null || mAppName.length() == 0 ) {
      getActivity().setResult(Activity.RESULT_CANCELED);
      getActivity().finish();
      return;
    }

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
        accountAuthType.setText(credentialEntries[i]);
      }
    }

    if ( credentialToUse.equals(getString(R.string.credential_type_none))) {
      accountIdentity.setText("");
    } else if ( credentialToUse.equals(getString(R.string.credential_type_username_password))) {
      String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
      accountIdentity.setText(username);
    } else if ( credentialToUse.equals(getString(R.string.credential_type_google_account))) {
      String googleAccount = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
      accountIdentity.setText(googleAccount);
    } else {
      accountIdentity.setText("");
    }

    String[] syncAttachmentValues =
        getResources().getStringArray(R.array.sync_attachment_option_values);
    for ( int i = 0 ; i < syncAttachmentValues.length ; ++i ) {
      if ( syncAttachmentState.name().equals(syncAttachmentValues[i]) ) {
        syncInstanceAttachmentsSpinner.setSelection(i);
        break;
      }
    }

    perhapsEnableButtons(props);

    showProgressDialog();
  }

  private void disableButtons() {
    startSync.setEnabled(false);
    resetServer.setEnabled(false);
  }
  private void perhapsEnableButtons(PropertiesSingleton props) {
    String url = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
    if ( url == null || url.length() == 0 ) {
      disableButtons();
    } else {
      startSync.setEnabled(true);
      resetServer.setEnabled(true);
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
   * Invoke this at the start of sync or reset
   */
  public void prepareForSyncAction() {
    // remove any settings for a URL other than the server URL...

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    String verifiedUri = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);

    OdkDbHandle dbHandleName = new OdkDbHandle(ODKDataUtils.genUUID());
    OdkConnectionInterface db = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(getAppName(), dbHandleName);

      SyncETagsUtils utils = new SyncETagsUtils();
      utils.deleteAllSyncETagsExceptForServer(db,
          (verifiedUri == null || verifiedUri.length() == 0) ? null : verifiedUri.toString());
    } catch (SQLiteException e) {
      WebLogger.getLogger(getAppName()).printStackTrace(e);
      WebLogger.getLogger(getAppName())
          .e(TAG, "[onClickSaveSettings][onClick] unable to update database");
      Toast.makeText(getActivity(), "database failure during update", Toast.LENGTH_LONG).show();
    } finally {
      if (db != null) {
        db.releaseReference();
      }
    }

    String authType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
    if ( authType == null ) {
      authType = getString(R.string.credential_type_none);
    }

    if ( getString(R.string.credential_type_google_account).equals(authType)) {
      authenticateGoogleAccount();
    } else {
      tickleInterface();
    }
  }

  /**
   * Hooked up to authorizeAccountButton's onClick in aggregate_activity.xml
   */
  public void authenticateGoogleAccount() {
    WebLogger.getLogger(getAppName()).d(TAG, "[authenticateGoogleAccount] invalidated authtoken");
    invalidateAuthToken(getActivity(), getAppName());

    PropertiesSingleton props = CommonToolProperties.get(getActivity(), getAppName());
    Intent i = new Intent(getActivity(), AccountInfoActivity.class);
    Account account = new Account(props.getProperty(CommonToolProperties.KEY_ACCOUNT), ACCOUNT_TYPE_G);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
    i.putExtra(AccountInfoActivity.INTENT_EXTRAS_ACCOUNT, account);
    startActivityForResult(i, SyncActivity.AUTHORIZE_ACCOUNT_RESULT_CODE);
  }

  public void onActivityResult (int requestCode, int resultCode, Intent data) {
    super.onActivityResult(requestCode, resultCode, data);

    if ( requestCode == SyncActivity.AUTHORIZE_ACCOUNT_RESULT_CODE ) {
      if ( resultCode == Activity.RESULT_CANCELED ) {
        invalidateAuthToken(getActivity(), getAppName());
        syncAction = SyncActions.IDLE;
      }
      tickleInterface();
    }
  }

  /**
   * Hooked to sync_reset_server_button's onClick in sync_launch_fragment.xml
   */
  public void onClickResetServer(View v) {
    WebLogger.getLogger(getAppName()).d(TAG, "in onClickResetServer");
    // ask whether to sync app files and table-level files

    startSync.setEnabled(false);
    resetServer.setEnabled(false);

    // show warning message
    AlertDialog.Builder msg = buildOkMessage(getString(R.string.sync_confirm_reset_app_server),
        getString(R.string.sync_reset_app_server_warning));

    msg.setPositiveButton(getString(R.string.sync_reset), new DialogInterface.OnClickListener() {
      @Override public void onClick(DialogInterface dialog, int which) {
        PropertiesSingleton props = CommonToolProperties.get(getActivity(), getAppName());
        String accountName = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
        WebLogger.getLogger(getAppName())
            .e(TAG, "[onClickResetServer] timestamp: " + System.currentTimeMillis());
        if (accountName == null) {
          Toast.makeText(getActivity(), getString(R.string.sync_choose_account), Toast.LENGTH_SHORT)
              .show();
        } else {
          disableButtons();
          syncAction = SyncActions.RESET_SERVER;
          prepareForSyncAction();
        }
      }
    });

    msg.setNegativeButton(getString(R.string.cancel), null);
    msg.show();
  }

  /**
   * Hooked to syncNowButton's onClick in aggregate_activity.xml
   */
  public void onClickSyncNow(View v) {
    WebLogger.getLogger(getAppName()).d(TAG, "in onClickSyncNow");

    startSync.setEnabled(false);
    resetServer.setEnabled(false);

    // ask whether to sync app files and table-level files
    PropertiesSingleton props = CommonToolProperties.get(getActivity(), getAppName());
    String accountName = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
    WebLogger.getLogger(getAppName()).e(TAG,
        "[onClickSyncNow] timestamp: " + System.currentTimeMillis());
    if (accountName == null) {
      Toast.makeText(getActivity(), getString(R.string.sync_choose_account), Toast.LENGTH_SHORT).show();
    } else {
      disableButtons();
      syncAction = SyncActions.SYNC;
      prepareForSyncAction();
    }
  }

  public static void invalidateAuthToken(Context context, String appName) {
    PropertiesSingleton props = CommonToolProperties.get(context, appName);
    AccountManager.get(context).invalidateAuthToken(ACCOUNT_TYPE_G,
        props.getProperty(CommonToolProperties.KEY_AUTH));
    props.removeProperty(CommonToolProperties.KEY_AUTH);
    props.writeProperties();
  }


  @Override public void onDestroy() {
    super.onDestroy();
  }

  private void showProgressDialog() {
    if ( false ) {
      String progress = "status string";

      disableButtons();

      // try to retrieve the active dialog
      Fragment dialog = getFragmentManager().findFragmentByTag(PROGRESS_DIALOG_TAG);

      if (dialog != null && ((ProgressDialogFragment) dialog).getDialog() != null) {
        ((ProgressDialogFragment) dialog).getDialog().setTitle(R.string.conflict_resolving_all);
        ((ProgressDialogFragment) dialog).setMessage(progress);
      } else if (progressDialog != null && progressDialog.getDialog() != null) {
        progressDialog.getDialog().setTitle(R.string.conflict_resolving_all);
        progressDialog.setMessage(progress);
      } else {
        if (progressDialog != null) {
          dismissProgressDialog();
        }
        progressDialog = ProgressDialogFragment.newInstance(getId(),
            getString(R.string.conflict_resolving_all), progress);
        progressDialog.show(getFragmentManager(), PROGRESS_DIALOG_TAG);
      }
    }
  }

  private void dismissProgressDialog() {
    final Fragment dialog = getFragmentManager().findFragmentByTag(PROGRESS_DIALOG_TAG);
    if (dialog != null && dialog != progressDialog) {
      // the UI may not yet have resolved the showing of the dialog.
      // use a handler to add the dismiss to the end of the queue.
      handler.post(new Runnable() {
        @Override
        public void run() {
          ((ProgressDialogFragment) dialog).dismiss();
        }
      });
    }
    if (progressDialog != null) {
      final ProgressDialogFragment scopedReference = progressDialog;
      progressDialog = null;
      // the UI may not yet have resolved the showing of the dialog.
      // use a handler to add the dismiss to the end of the queue.
      handler.post(new Runnable() {
        @Override public void run() {
          try {
            scopedReference.dismiss();
          } catch (Exception e) {
            // ignore... we tried!
          }
        }
      });
    }
  }

  public String getAppName() {
    if ( mAppName == null ) {
      throw new IllegalStateException("appName not yet initialized");
    }
    return mAppName;
  }

}
