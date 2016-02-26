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
import android.app.*;
import android.content.*;
import android.os.*;
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
import org.opendatakit.core.R;
import org.opendatakit.database.DatabaseConsts;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.database.service.OdkDbInterface;
import org.opendatakit.sync.service.AppSynchronizer;
import org.opendatakit.sync.service.OdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.sqlite.database.sqlite.SQLiteException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mitchellsundt@gmail.com
 */
public class SyncFragment extends Fragment {

  private static final String TAG = "SyncFragment";

  public static final String NAME = "SyncFragment";
  public static final int ID = R.layout.sync_launch_fragment;

  private static final String ACCOUNT_TYPE_G = "com.google";
  private static final String URI_FIELD_EMPTY = "http://";

  private static final String PROGRESS_DIALOG_TAG = "progressDialog";

  private static enum DialogState {
    Progress, Alert, None
  };

  private String mAppName;

  private boolean mAuthorizeSinceCompletion;
  private boolean mAuthorizeAccountSuccessful;

  private Handler handler = new Handler();
  private ProgressDialogFragment progressDialog = null;

  private EditText uriField;
  private Spinner accountListSpinner;

  private Spinner syncInstanceAttachmentsSpinner;
  private SyncAttachmentState syncAttachmentState = SyncAttachmentState.SYNC;

  //private TextView progressState;
  //private TextView progressMessage;

  private Button saveSettings;
  private Button authorizeAccount;
  private Button startSync;
  private Button resetServer;

  private enum SyncActions { IDLE, MONITOR, SYNC, RESET_SERVER };
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
          switch (syncInstanceAttachmentsSpinner.getSelectedItemPosition()) {
          case 0:
            syncAttachmentState = SyncAttachmentState.SYNC;
            break;
          case 1:
            syncAttachmentState = SyncAttachmentState.UPLOAD;
            break;
          case 2:
            syncAttachmentState = SyncAttachmentState.DOWNLOAD;
            break;
          case 3:
            syncAttachmentState = SyncAttachmentState.NONE;
            break;
          default:
            Log.e(TAG, "Invalid sync attachment state spinner");
          }
          syncServiceInterface.synchronize(getAppName(), syncAttachmentState);
          break;
        case RESET_SERVER:
          syncServiceInterface.push(getAppName());
          break;
        case MONITOR:
          break;
        }
      }

      // Otherwise, set up a bind and attempt to re-tickle...
      Log.i(TAG, "Attempting bind to Database service");
      Intent bind_intent = new Intent();
      bind_intent.setClassName("org.opendatakit.core",
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

    disableButtons();
    initializeData();

    mAuthorizeAccountSuccessful = false;
  }

  @Override
  public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
    super.onCreateView(inflater, container, savedInstanceState);

    View view = inflater.inflate(ID, container, false);
    uriField = (EditText) view.findViewById(R.id.sync_uri_field);
    accountListSpinner = (Spinner) view.findViewById(R.id.sync_account_list_spinner);
    syncInstanceAttachmentsSpinner = (Spinner) view.findViewById(R.id.sync_instance_attachments);

    // TODO: Hiding these until we figure out what Sync's UI should be
    //progressState = (TextView) view.findViewById(R.id.sync_progress_state);
    //progressMessage = (TextView) view.findViewById(R.id.sync_progress_message);

    saveSettings = (Button) view.findViewById(R.id.sync_save_settings_button);
    saveSettings.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        onClickSaveSettings(v);
      }
    });

    authorizeAccount = (Button) view.findViewById(R.id.sync_authorize_account_button);
    authorizeAccount.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        onClickAuthorizeAccount(v);
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


    disableButtons();

    return view;
  }

  @Override
  public void onResume() {
    super.onResume();

    showProgressDialog();
  }

  private void disableButtons() {
    mAuthorizeSinceCompletion = false;
    saveSettings.setEnabled(true);
    authorizeAccount.setEnabled(false);
    startSync.setEnabled(false);
    resetServer.setEnabled(false);
  }

  private void initializeData() {
    if ( getActivity() == null ) {
      return;
    }

    PropertiesSingleton props = CommonToolProperties.get(getActivity(), getAppName());
    // Add accounts to spinner
    AccountManager accountManager = AccountManager.get(getActivity());
    Account[] accounts = accountManager.getAccountsByType(ACCOUNT_TYPE_G);
    List<String> accountNames = new ArrayList<String>(accounts.length);
    for (int i = 0; i < accounts.length; i++)
      accountNames.add(accounts[i].name);

    ArrayAdapter<String> accountListAapter = new ArrayAdapter<String>(getActivity(),
        android.R.layout.select_dialog_item, accountNames);
    accountListSpinner.setAdapter(accountListAapter);

    ArrayAdapter<CharSequence> instanceAttachmentsAdapter = ArrayAdapter.createFromResource(
        getActivity(), R.array.sync_attachment_option_names, android.R.layout.select_dialog_item);
    syncInstanceAttachmentsSpinner.setAdapter(instanceAttachmentsAdapter);
    syncInstanceAttachmentsSpinner.setSelection(1);

    // Set saved server url
    String serverUri = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);

    if (serverUri == null)
      uriField.setText(URI_FIELD_EMPTY);
    else
      uriField.setText(serverUri);

    // Set chosen account
    String accountName = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
    if (accountName != null) {
      int index = accountNames.indexOf(accountName);
      accountListSpinner.setSelection(index);
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
   * Hooked up to save settings button in aggregate_activity.xml
   */
  public void onClickSaveSettings(View v) {
    // show warning message
    AlertDialog.Builder msg = buildOkMessage(getString(R.string.sync_confirm_change_settings),
        getString(R.string.sync_change_settings_warning));

    msg.setPositiveButton(getString(R.string.sync_save), new DialogInterface.OnClickListener() {
      @Override public void onClick(DialogInterface dialog, int which) {

        // save fields in preferences
        String uri = uriField.getText().toString();
        if (uri.equals(URI_FIELD_EMPTY)) {
          uri = null;
        }
        String accountName = (String) accountListSpinner.getSelectedItem();

        URI verifiedUri = null;
        if (uri != null) {
          try {
            verifiedUri = new URI(uri);
          } catch (URISyntaxException e) {
            WebLogger.getLogger(getAppName())
                .d(TAG, "[onClickSaveSettings][onClick] invalid server URI: " + uri);
            Toast.makeText(getActivity(), "Invalid server URI: " + uri, Toast.LENGTH_LONG).show();
            return;
          }
        }

        PropertiesSingleton props = CommonToolProperties.get(getActivity(), getAppName());
        props.setProperty(CommonToolProperties.KEY_SYNC_SERVER_URL, uri);
        props.setProperty(CommonToolProperties.KEY_ACCOUNT, accountName);
        props.writeProperties();

        // and remove any settings for a URL other than this...

        OdkDbHandle dbHandleName = new OdkDbHandle(ODKDataUtils.genUUID());
        OdkConnectionInterface db = null;

        try {
          // +1 referenceCount if db is returned (non-null)
          db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
              .getConnection(getAppName(), dbHandleName);

          SyncETagsUtils utils = new SyncETagsUtils();
          utils.deleteAllSyncETagsExceptForServer(db,
              (verifiedUri == null) ? null : verifiedUri.toString());
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

        // SS Oct 15: clear the auth token here.
        // TODO if you change a user you can switch to their privileges
        // without this.
        WebLogger.getLogger(getAppName())
            .d(TAG, "[onClickSaveSettings][onClick] invalidated authtoken");
        invalidateAuthToken(getActivity(), getAppName());

        authorizeAccount.setEnabled((verifiedUri != null));
      }
    });

    msg.setNegativeButton(getString(R.string.cancel), null);
    msg.show();

  }

  /**
   * Hooked up to authorizeAccountButton's onClick in aggregate_activity.xml
   */
  public void onClickAuthorizeAccount(View v) {
    WebLogger.getLogger(getAppName()).d(TAG, "[onClickAuthorizeAccount] invalidated authtoken");
    invalidateAuthToken(getActivity(), getAppName());
    authorizeAccount.setEnabled(false);
    PropertiesSingleton props = CommonToolProperties.get(getActivity(), getAppName());
    Intent i = new Intent(getActivity(), AccountInfoActivity.class);
    Account account = new Account(props.getProperty(CommonToolProperties.KEY_ACCOUNT), ACCOUNT_TYPE_G);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, getAppName());
    i.putExtra(AccountInfoActivity.INTENT_EXTRAS_ACCOUNT, account);
    startActivityForResult(i, SyncActivity.AUTHORIZE_ACCOUNT_RESULT_ID);
  }

  public void onActivityResult (int requestCode, int resultCode, Intent data) {
    super.onActivityResult(requestCode, resultCode, data);

    if ( requestCode == SyncActivity.AUTHORIZE_ACCOUNT_RESULT_ID ) {
      if ( resultCode == Activity.RESULT_CANCELED ) {
        invalidateAuthToken(getActivity(), getAppName());
        authorizeAccount.setEnabled(true);
      } else {
        authorizeAccount.setEnabled(false);
        startSync.setEnabled(true);
        resetServer.setEnabled(true);
      }
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
          tickleInterface();
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
      tickleInterface();
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
