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

import android.Manifest;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Bundle;
import android.util.Log;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentActivity;
import androidx.fragment.app.FragmentTransaction;
import androidx.preference.EditTextPreference;
import androidx.preference.ListPreference;
import androidx.preference.Preference;
import androidx.preference.Preference.OnPreferenceChangeListener;
import androidx.preference.PreferenceCategory;
import androidx.preference.PreferenceFragmentCompat;

import com.google.android.material.dialog.MaterialAlertDialogBuilder;
import com.google.zxing.integration.android.IntentIntegrator;
import com.google.zxing.integration.android.IntentResult;

import org.json.JSONException;
import org.json.JSONObject;
import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.consts.RequestCodeConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.PasswordPreferenceScreen;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.sync.actions.fragments.ChooseSignInTypeFragment;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;
import org.opendatakit.services.utilities.TableHealthValidator;
import org.opendatakit.services.utilities.UserState;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static androidx.core.content.ContextCompat.checkSelfPermission;

public class ServerSettingsFragment extends PreferenceFragmentCompat implements
    OnPreferenceChangeListener {

  private static final String t = "ServerSettingsFragment";

  private EditTextPreference mServerUrlPreference;
  private ListPreference mSignOnCredentialPreference;
  private EditTextPreference mUsernamePreference;
  private TableHealthValidator healthValidator;
  private final int PERMISSION_REQUEST_CAMERA_CODE = 1;
  private boolean adminMode;
  private boolean serverAvailable;
  private boolean usernamePasswordAvailable;
  private boolean credentialAvailable;
  protected static final String[] CAMERA_PERMISSION = new String[] {
          Manifest.permission.CAMERA
  };

  @Override
  public void onCreatePreferences(Bundle savedInstanceState, String rootKey){
    setPreferencesFromResource(R.xml.server_preferences, rootKey);
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    setHasOptionsMenu(true);
  }

  @Override
  public void onResume() {
    super.onResume();

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) requireActivity()).getProps();

    String appName = null;
    FragmentActivity activity = requireActivity();
    if(activity instanceof IAppAwareActivity) {
      IAppAwareActivity appPropAct = (IAppAwareActivity) activity;
      appName = appPropAct.getAppName();
    }

    if(appName == null) {
      throw new IllegalStateException("NEED an AppName");
    }

    healthValidator = new TableHealthValidator(appName, getActivity());

    // not super safe, but we're just putting in this mode to help
    // administrate
    // would require code to access it
    adminMode = (this.getArguments() == null) ? false :
        (this.getArguments().containsKey(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE) ?
            this.getArguments().getBoolean(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE) : false);

    String adminPwd = props.getProperty(CommonToolProperties.KEY_ADMIN_PW);
    boolean adminConfigured = (adminPwd != null && adminPwd.length() != 0);

    serverAvailable = !adminConfigured ||
        props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_SYNC_SERVER);

    PreferenceCategory serverCategory = findPreference(CommonToolProperties.GROUPING_SERVER_CATEGORY);

    // Initialize the Server URL Text Preference
    mServerUrlPreference = findPreference(CommonToolProperties.KEY_SYNC_SERVER_URL);
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
            return urlPreferenceChanged(preference,newValue);
          }
        });


    mServerUrlPreference.setSummary(mServerUrlPreference.getText());

    mServerUrlPreference.setEnabled(serverAvailable || adminMode);

    credentialAvailable = !adminConfigured ||
        props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_AUTHENTICATION_TYPE);
    mSignOnCredentialPreference = findPreference(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
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
        return signOnPreferenceChanged(preference,newValue);
      }
    });

    mSignOnCredentialPreference.setEnabled(credentialAvailable || adminMode);

    //////////////////
    mUsernamePreference = findPreference(CommonToolProperties.KEY_USERNAME);
    if (props.containsKey(CommonToolProperties.KEY_USERNAME)) {
      String user = props.getProperty(CommonToolProperties.KEY_USERNAME);
      mUsernamePreference.setSummary(user);
      mUsernamePreference.setText(user);
    }

    mUsernamePreference.setOnPreferenceChangeListener(this);

    usernamePasswordAvailable = !adminConfigured ||
        props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_USERNAME_PASSWORD);

    mUsernamePreference.setEnabled(usernamePasswordAvailable || adminMode);

    PasswordPreferenceScreen passwordScreen = (PasswordPreferenceScreen) this.findPreference(CommonToolProperties
        .GROUPING_PASSWORD_SCREEN);
    passwordScreen.setCallback(new PasswordPreferenceScreen.PasswordActionCallback() {
      @Override public void showPasswordDialog() {
        // DialogFragment.show() will take care of adding the fragment
        // in a transaction.  We also want to remove any currently showing
        // dialog, so make our own transaction and take care of that here.
        FragmentTransaction ft = getParentFragmentManager().beginTransaction();
        Fragment prev = getParentFragmentManager().findFragmentByTag(CommonToolProperties.GROUPING_PASSWORD_SCREEN);
        if (prev != null) {
          ft.remove(prev);
        }
        ft.commit();

        // Create and show the dialog.
        PasswordDialogFragment newFragment = PasswordDialogFragment.newPasswordDialog(CommonToolProperties.KEY_PASSWORD);
        newFragment.show(getParentFragmentManager(), CommonToolProperties.GROUPING_PASSWORD_SCREEN);
      }
    });

    passwordScreen.setEnabled((usernamePasswordAvailable || adminMode));

    if ( !adminMode &&
        (!serverAvailable || !credentialAvailable || !usernamePasswordAvailable) ) {
      serverCategory.setTitle(R.string.server_restrictions_apply);
    }

    healthValidator.verifyTableHealth();
  }

  public boolean urlPreferenceChanged(Preference preference, Object newValue){
    String url = newValue.toString();
    // disallow any whitespace
    if (url.contains(" ") || !url.equals(url.trim())) {
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
      PropertiesSingleton props = ((IOdkAppPropertiesActivity) ServerSettingsFragment.this.getActivity()).getProps();
      props.setProperties(UpdateServerSettingsFragment.getUpdateUrlProperties(newValue.toString()));
      return true;
    } else {
      Toast.makeText(getActivity().getApplicationContext(),
              R.string.url_error, Toast.LENGTH_SHORT)
              .show();
      return false;
    }
  }

  public boolean signOnPreferenceChanged(Preference preference, Object newValue){
    int index = ((ListPreference) preference).findIndexOfValue(newValue.toString());
    String entry = (String) ((ListPreference) preference).getEntries()[index];
    preference.setSummary(entry);
    updatePropertiesSingleton(CommonToolProperties.KEY_AUTHENTICATION_TYPE, newValue.toString());

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) ServerSettingsFragment.this.getActivity()).getProps();
    if(newValue.toString().equals("none")){
      props.setProperties(ChooseSignInTypeFragment.getAnonymousProperties());
    }
    else {
      updatePropertiesSingleton(CommonToolProperties.KEY_CURRENT_USER_STATE, UserState.AUTHENTICATED_USER.name());
    }

    return true;
  }

  @Override
  public void onCreateOptionsMenu(Menu menu, MenuInflater inflater) {
    inflater.inflate(R.menu.server_settings_action_menu, menu);
  }

  private void updatePropertiesSingleton(String key, String value){
    PropertiesSingleton props =
            ((IOdkAppPropertiesActivity) ServerSettingsFragment.this.getActivity()).getProps();
    Map<String,String> properties = new HashMap<String,String>();
    properties.put(key,value);
    properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
    properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
    properties.put(CommonToolProperties.KEY_USERS_LIST, "");
    props.setProperties(properties);
  }

  /**
   * Generic listener that sets the summary to the newly selected/entered value
   */
  @Override
  public boolean onPreferenceChange(Preference preference, Object newValue) {
    // remove whitespace
    String newStr = newValue.toString();
    newStr = newStr.replaceAll("\\r\\n|\\n|\\r", "");

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    preference.setSummary(newStr);
    if ( props.containsKey(preference.getKey())) {
      updatePropertiesSingleton(preference.getKey(), newStr);
    } else {
      throw new IllegalStateException("Unexpected case");
    }
    return true;
  }

  /**
   * Receives result after QrCode scanning.
   */
  @Override
  public void onActivityResult(int requestCode, int resultCode, Intent data) {

    if (requestCode == RequestCodeConsts.RequestCodes.LAUNCH_CHECKPOINT_RESOLVER ||
        requestCode == RequestCodeConsts.RequestCodes.LAUNCH_CONFLICT_RESOLVER) {
      healthValidator.verifyTableHealth();
      return;
    }
       IntentResult result = IntentIntegrator.parseActivityResult(requestCode, resultCode, data);
      if (result != null) {
        if (result.getContents() == null) {
          Toast.makeText(getActivity(), R.string.scanning_cancelled, Toast.LENGTH_SHORT).show();
        } else {
          parseQrCodeResult(result.getContents());
          Log.i("QR code:",result.getContents());
        }
      }
    super.onActivityResult(requestCode, resultCode, data);
  }

  private void parseQrCodeResult(String contents) {
    String TAG =  "pasreQrCodeResult";
    String toastString = "";
    try {
     JSONObject mainObject = new JSONObject(contents);
      try{
        String url = (String)mainObject.get("url");
        if(url!=null &&(serverAvailable||adminMode)){
          urlPreferenceChanged(mServerUrlPreference,url);
          toastString +="URL : "+url+"\n";
        }
      }
      catch (Exception e) {
        toastString +="URL : not found\n";
        Log.i(TAG,"Url not found");
      }
      try{
        String username = (String) mainObject.get("username");
        if(username!=null && (usernamePasswordAvailable || adminMode)){
          onPreferenceChange(mUsernamePreference,username);
         if(credentialAvailable || adminMode) signOnPreferenceChanged(mSignOnCredentialPreference,"username_password");
          toastString = toastString +"Username : "+username+"\n";
        }
      }
      catch (Exception e) {
        toastString +="Sign-on Credential type: anonymous\n";
        toastString +="Username : not found\n";
        if(credentialAvailable || adminMode) signOnPreferenceChanged(mSignOnCredentialPreference,"none");
        Log.i(TAG,"Username not found");
      }

     try{
       String password = (String) mainObject.get("password");
       if(password!=null && (usernamePasswordAvailable || adminMode)){
         updatePassword(password);
         toastString +="Password : "+password;
       }
     }
     catch (Exception e) {
       toastString +="Password : not found";
       Log.i(TAG,"Password not found");
     }

     Toast.makeText(getActivity(), toastString, Toast.LENGTH_SHORT).show();

    } catch (JSONException e) {
      Toast.makeText(getActivity(), R.string.invalid_qr_code, Toast.LENGTH_SHORT).show();
      Log.i(TAG,"Invalid Qr code");
    }

  }

  public void updatePassword(String pw) {
    updatePropertiesSingleton(CommonToolProperties.KEY_PASSWORD, pw);
 }

  @Override
  public boolean onOptionsItemSelected(MenuItem item) {
    if(item.getItemId() == R.id.action_barcode) {
      // When Scan QR icon is clicked.
      if (checkSelfPermission(getActivity(), Manifest.permission.CAMERA)
              == PackageManager.PERMISSION_GRANTED) {
        // Permission is already available, start camera preview
        openBarcodeScanner();
      } else {
        // Permission is missing and must be requested.
        requestCameraPermission();
      }
      return true;
    } else {
        return super.onOptionsItemSelected(item);
    }
  }

  private void openBarcodeScanner() {
    IntentIntegrator
        .forSupportFragment(this)
        .setDesiredBarcodeFormats(IntentIntegrator.QR_CODE_TYPES)
        .setPrompt(getString(R.string.qr_code_scanner_instruction))
        .setCameraId(0)
        .setBeepEnabled(true)
        .setBarcodeImageEnabled(false)
        .initiateScan();
  }

  @Override
  public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions,
                                         @NonNull int[] grantResults) {
    super.onRequestPermissionsResult(requestCode, permissions, grantResults);
    if (requestCode == PERMISSION_REQUEST_CAMERA_CODE) {

      if (grantResults.length == 1 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
        // Permission has been granted. Starting Barcode Scanner.
        openBarcodeScanner();
      }
      else{

        /**
         * New dialog styling
         * MaterialAlertDialogBuilder is standard for all ODK-X Apps
         * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
         * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
         */

        MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(getActivity(),R.style.OdkXAlertDialogStyle);
        builder.setMessage(R.string.camera_permission_rationale)
                .setPositiveButton(R.string.allow, new DialogInterface.OnClickListener() {
                  @Override
                  public void onClick(DialogInterface dialog, int which) {
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                      //For pre Marshmallow devices, this wouldn't be called as they don't need runtime permission.
                      requestPermissions(
                              new String[]{Manifest.permission.CAMERA},
                              PERMISSION_REQUEST_CAMERA_CODE);
                    }
                  }
                })
                .setNegativeButton(R.string.not_now, new DialogInterface.OnClickListener() {
                  @Override
                  public void onClick(DialogInterface dialog, int which) {
                    Toast.makeText(getActivity(), R.string.permission_denied, Toast.LENGTH_SHORT).show();
                    dialog.dismiss();
                  }
                });
        builder.create().show();
      }
    }
   }
  private void requestCameraPermission() {

        if (checkSelfPermission(getActivity(), Manifest.permission.CAMERA)
                != PackageManager.PERMISSION_GRANTED){
          if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            //For pre Marshmallow devices, this wouldn't be called as they don't need runtime permission.
            requestPermissions(
                    CAMERA_PERMISSION,
                    PERMISSION_REQUEST_CAMERA_CODE
        );
      }
    }
    else{
      // Permission has been granted. Starting Barcode Scanner.
      openBarcodeScanner();
    }
  }
}