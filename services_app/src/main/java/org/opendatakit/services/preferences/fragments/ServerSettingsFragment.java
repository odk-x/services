/*
 * Copyright (C) 2016 University of Washington
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

package org.opendatakit.services.preferences.fragments;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.app.AlertDialog;
import android.app.Fragment;
import android.app.FragmentTransaction;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.preference.*;
import android.preference.Preference.OnPreferenceChangeListener;
import android.text.InputFilter;
import android.text.Spanned;
import android.widget.Toast;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.preferences.PasswordPreferenceScreen;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.SyncActivity;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ServerSettingsFragment extends PreferenceFragment implements OnPreferenceChangeListener {

  private static final String t = "ServerSettingsFragment";

  private EditTextPreference mServerUrlPreference;
  private ListPreference mSignOnCredentialPreference;
  private EditTextPreference mUsernamePreference;
  private ListPreference mSelectedGoogleAccountPreference;

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();

    addPreferencesFromResource(R.xml.server_preferences);

    // not super safe, but we're just putting in this mode to help
    // administrate
    // would require code to access it
    boolean adminMode;
    adminMode = (this.getArguments() == null) ? false :
        (this.getArguments().containsKey(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE) ?
           this.getArguments().getBoolean(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE) : false);

    String adminPwd = props.getProperty(CommonToolProperties.KEY_ADMIN_PW);
    boolean adminConfigured = (adminPwd != null && adminPwd.length() != 0);

    boolean serverAvailable = !adminConfigured ||
        props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_SYNC_SERVER);

    PreferenceCategory serverCategory = (PreferenceCategory) findPreference(CommonToolProperties.GROUPING_SERVER_CATEGORY);

    // Initialize the Server URL Text Preference
    mServerUrlPreference = (EditTextPreference) findPreference(CommonToolProperties.KEY_SYNC_SERVER_URL);
    if (props.containsKey(CommonToolProperties.KEY_SYNC_SERVER_URL)) {
      String url = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
      mServerUrlPreference.setSummary(url);
      mServerUrlPreference.setText(url);
    }

    mServerUrlPreference
        .setOnPreferenceChangeListener(new OnPreferenceChangeListener() {
          @Override
          public boolean onPreferenceChange(Preference preference,
              Object newValue) {
            String url = newValue.toString();

            // disallow any whitespace
            if ( url.contains(" ") || !url.equals(url.trim()) ) {
              Toast.makeText(getActivity().getApplicationContext(),
                  R.string.url_error_whitespace, Toast.LENGTH_SHORT)
                  .show();
              return false;
            }

            // remove all trailing "/"s
            while (url.endsWith("/")) {
              url = url.substring(0, url.length() - 1);
            }

            boolean isValid = false;
            try {
              new URL(url);
              isValid = true;
            } catch (MalformedURLException e) {
              // ignore
            }

            if (isValid) {
              preference.setSummary(newValue.toString());

              PropertiesSingleton props =
                  ((IOdkAppPropertiesActivity) ServerSettingsFragment.this.getActivity()).getProps();
              Map<String,String> properties = new HashMap<String,String>();
              properties.put(CommonToolProperties.KEY_SYNC_SERVER_URL, newValue.toString());
              properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
              properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
              properties.put(CommonToolProperties.KEY_USERS_LIST, "");
              props.setProperties(properties);
              return true;
            } else {
              Toast.makeText(getActivity().getApplicationContext(),
                  R.string.url_error, Toast.LENGTH_SHORT)
                  .show();
              return false;
            }
          }
        });
    mServerUrlPreference.setSummary(mServerUrlPreference.getText());
    mServerUrlPreference.getEditText().setFilters(
        new InputFilter[] { getReturnFilter() });

    mServerUrlPreference.setEnabled(serverAvailable || adminMode);


    boolean credentialAvailable = !adminConfigured ||
        props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_AUTHENTICATION_TYPE);
    mSignOnCredentialPreference = (ListPreference) findPreference(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
    if (props.containsKey(CommonToolProperties.KEY_AUTHENTICATION_TYPE)) {
      String chosenFontSize = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
      CharSequence entryValues[] = mSignOnCredentialPreference.getEntryValues();
      for (int i = 0; i < entryValues.length; i++) {
        String entry = entryValues[i].toString();
        if (entry.equals(chosenFontSize)) {
          mSignOnCredentialPreference.setValue(entry);
          mSignOnCredentialPreference.setSummary(mSignOnCredentialPreference.getEntries()[i]);
        }
      }
    }

    mSignOnCredentialPreference.setOnPreferenceChangeListener(new OnPreferenceChangeListener() {

      @Override
      public boolean onPreferenceChange(Preference preference, Object newValue) {
        int index = ((ListPreference) preference).findIndexOfValue(newValue.toString());
        String entry = (String) ((ListPreference) preference).getEntries()[index];
        ((ListPreference) preference).setSummary(entry);

        PropertiesSingleton props =
            ((IOdkAppPropertiesActivity) ServerSettingsFragment.this.getActivity()).getProps();
        Map<String,String> properties = new HashMap<String,String>();
        properties.put(CommonToolProperties.KEY_AUTHENTICATION_TYPE, newValue.toString());
        properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
        properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
        properties.put(CommonToolProperties.KEY_USERS_LIST, "");
        props.setProperties(properties);
        return true;
      }
    });

    mSignOnCredentialPreference.setEnabled(credentialAvailable || adminMode);

    //////////////////
    mUsernamePreference = (EditTextPreference) findPreference(CommonToolProperties.KEY_USERNAME);
    if (props.containsKey(CommonToolProperties.KEY_USERNAME)) {
      String user = props.getProperty(CommonToolProperties.KEY_USERNAME);
      mUsernamePreference.setSummary(user);
      mUsernamePreference.setText(user);
    }

    mUsernamePreference.setOnPreferenceChangeListener(this);

    mUsernamePreference.getEditText().setFilters(new InputFilter[] { getReturnFilter() });

    boolean usernamePasswordAvailable = !adminConfigured ||
        props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_USERNAME_PASSWORD);

    mUsernamePreference.setEnabled(usernamePasswordAvailable || adminMode);

    PasswordPreferenceScreen passwordScreen = (PasswordPreferenceScreen) this.findPreference(CommonToolProperties
        .GROUPING_PASSWORD_SCREEN);
    passwordScreen.setCallback(new PasswordPreferenceScreen.PasswordActionCallback() {
      @Override public void showPasswordDialog() {
        // DialogFragment.show() will take care of adding the fragment
        // in a transaction.  We also want to remove any currently showing
        // dialog, so make our own transaction and take care of that here.
        FragmentTransaction ft = getFragmentManager().beginTransaction();
        Fragment prev = getFragmentManager().findFragmentByTag(CommonToolProperties.GROUPING_PASSWORD_SCREEN);
        if (prev != null) {
          ft.remove(prev);
        }

        // Create and show the dialog.
        PasswordDialogFragment newFragment = PasswordDialogFragment.newPasswordDialog(CommonToolProperties.KEY_PASSWORD);
        newFragment.show(ft, CommonToolProperties.GROUPING_PASSWORD_SCREEN);
      }
    });

    passwordScreen.setEnabled((usernamePasswordAvailable || adminMode));

    mSelectedGoogleAccountPreference = (ListPreference) findPreference(CommonToolProperties.KEY_ACCOUNT);

    // get list of google accounts
    final Account[] accounts = AccountManager.get(getActivity().getApplicationContext()).getAccountsByType(
        "com.google");
    ArrayList<String> accountEntries = new ArrayList<String>();
    ArrayList<String> accountValues = new ArrayList<String>();

    for (int i = 0; i < accounts.length; i++) {
      accountEntries.add(accounts[i].name);
      accountValues.add(accounts[i].name);
    }
    accountEntries.add(getString(R.string.no_account));
    accountValues.add("");

    mSelectedGoogleAccountPreference.setEntries(accountEntries.toArray(new String[accountEntries
        .size()]));
    mSelectedGoogleAccountPreference.setEntryValues(accountValues.toArray(new String[accountValues
        .size()]));
    mSelectedGoogleAccountPreference
        .setOnPreferenceChangeListener(new OnPreferenceChangeListener() {

          @Override
          public boolean onPreferenceChange(Preference preference, Object newValue) {
            int index = ((ListPreference) preference).findIndexOfValue(newValue.toString());
            String value = (String) ((ListPreference) preference).getEntryValues()[index];
            preference.setSummary(value);
            PropertiesSingleton props = ((IOdkAppPropertiesActivity)
                ServerSettingsFragment.this.getActivity()).getProps();
            Map<String,String> properties = new HashMap<String,String>();
            properties.put(CommonToolProperties.KEY_ACCOUNT, value);
            properties.put(CommonToolProperties.KEY_AUTH, "");
            properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
            properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
            properties.put(CommonToolProperties.KEY_USERS_LIST, "");
            props.setProperties(properties);
            return true;
          }
        });

    String googleAccount = props.getProperty(CommonToolProperties.KEY_ACCOUNT);
    if ( googleAccount == null ) {
      googleAccount = "";
    }
    boolean found = false;
    for ( int i = 0 ; i < accountValues.size() ; ++i ) {
      if ( googleAccount.equals(accountValues.get(i)) ) {
        mSelectedGoogleAccountPreference.setValue(googleAccount);
        mSelectedGoogleAccountPreference.setSummary(accountEntries.get(i));
        found = true;
      }
    }
    if ( !found ) {
      // clear the account property and authentication status
      Map<String,String> properties = new HashMap<String,String>();
      properties.put(CommonToolProperties.KEY_ACCOUNT, "");
      properties.put(CommonToolProperties.KEY_AUTH, "");
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
      properties.put(CommonToolProperties.KEY_USERS_LIST, "");
      props.setProperties(properties);
      // set to "none"
      mSelectedGoogleAccountPreference.setValue(accountValues.get(accountValues.size()-1));
      mSelectedGoogleAccountPreference.setSummary(accountEntries.get(accountEntries.size()-1));
    }

    Boolean googleAccountAvailable = props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_GOOGLE_ACCOUNT);
    mSelectedGoogleAccountPreference.setEnabled(googleAccountAvailable || adminMode);

    if ( !adminMode &&
        (!serverAvailable || !credentialAvailable || !usernamePasswordAvailable ||
         !googleAccountAvailable) ) {
      serverCategory.setTitle(R.string.server_restrictions_apply);
    }

    verifyTableHealth();
  }

  /**
   * Disallows whitespace from user entry
   *
   * @return
   */
  private InputFilter getWhitespaceFilter() {
    InputFilter whitespaceFilter = new InputFilter() {
      public CharSequence filter(CharSequence source, int start, int end, Spanned dest, int dstart,
          int dend) {
        for (int i = start; i < end; i++) {
          if (Character.isWhitespace(source.charAt(i))) {
            return "";
          }
        }
        return null;
      }
    };
    return whitespaceFilter;
  }

  /**
   * Disallows carriage returns from user entry
   *
   * @return
   */
  private InputFilter getReturnFilter() {
    InputFilter returnFilter = new InputFilter() {
      public CharSequence filter(CharSequence source, int start, int end, Spanned dest, int dstart,
          int dend) {
        for (int i = start; i < end; i++) {
          if (Character.getType((source.charAt(i))) == Character.CONTROL) {
            return "";
          }
        }
        return null;
      }
    };
    return returnFilter;
  }

  /**
   * Generic listener that sets the summary to the newly selected/entered value
   */
  @Override
  public boolean onPreferenceChange(Preference preference, Object newValue) {
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    preference.setSummary((CharSequence) newValue);
    if ( props.containsKey(preference.getKey())) {
      Map<String,String> properties = new HashMap<String,String>();
      properties.put(preference.getKey(), newValue.toString());
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
      properties.put(CommonToolProperties.KEY_USERS_LIST, "");
      props.setProperties(properties);
    } else {
      throw new IllegalStateException("Unexpected case");
    }
    return true;
  }

    private void verifyTableHealth() {
        String appName = ((AppPropertiesActivity) getActivity()).getAppName();

        WebLogger.getLogger(appName).i(t, "[" + getId() + "] [verifyTableHealth]");

        OdkConnectionInterface db = null;
        DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

        List<String> checkpointTables = new LinkedList<>();
        List<String> conflictTables = new LinkedList<>();
        boolean hasChanges = false;

        try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                    .getConnection(appName, dbHandleName);
            List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

            for (String tableId : tableIds) {
                int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);

                if (CursorUtils.getTableHealthIsClean(health)) {
                    continue;
                }

                if (!hasChanges && CursorUtils.getTableHealthHasChanges(health)) {
                    hasChanges = true;
                }

                if (CursorUtils.getTableHealthHasConflicts(health)) {
                    conflictTables.add(tableId);
                }

                if (CursorUtils.getTableHealthHasCheckpoints(health)) {
                    checkpointTables.add(tableId);
                }
            }
        } finally {
            if (db != null) {
                // release the reference...
                // this does not necessarily close the db handle
                // or terminate any pending transaction
                db.releaseReference();
            }
        }

        WebLogger.getLogger(appName).i(t,
                "[" + getId() + "] [verifyTableHealth] " + "summary:\n\tUnsynced changes present: "
                        + hasChanges + "\n\tNumber of conflict rows present: " + conflictTables.size()
                        + "\n\tNumber of checkpoint rows present: " + checkpointTables.size());

        // TODO: Add UI to tell user we're about to resolve checkpoints and conflicts
        // TODO: Deal with conflicts and checkpoints (desktop file)

        if (hasChanges) {
            promptToResolveChanges(appName);
        }
    }

    private void promptToResolveChanges(final String appName) {
        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
        builder.setTitle(R.string.sync_pending_changes);
        builder.setMessage(R.string.resolve_pending_changes);
        builder.setPositiveButton(R.string.ignore_changes, new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int id) {
                dialog.dismiss();
                // Do nothing and continue to login screen
            }
        });
        builder.setNegativeButton(R.string.resolve_with_sync, new DialogInterface.OnClickListener() {
            public void onClick(DialogInterface dialog, int id) {
                dialog.dismiss();

                // Launch the Sync activity to sync your changes.
                // TODO: Can we check if we just came from sync and if so just return to that?
                Intent i = new Intent(getActivity(), SyncActivity.class);
                i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
                startActivity(i);
            }
        });
        AlertDialog dialog = builder.create();
        dialog.show();
    }
}
