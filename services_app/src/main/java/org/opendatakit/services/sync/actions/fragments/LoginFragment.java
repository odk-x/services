/*
 * Copyright (C) 2017 University of Washington
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

import android.os.Bundle;
import android.text.method.PasswordTransformationMethod;
import android.view.*;
import android.widget.*;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.sync.actions.VerifyServerSettingsActions;
import org.opendatakit.services.sync.actions.activities.*;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jbeorse on 5/31/17.
 */

public class LoginFragment extends AuthenticationFragment implements ISyncOutcomeHandler {

   private static final String TAG = "LoginFragment";

   public static final String NAME = "LoginFragment";
   public static final int ID = R.layout.login_fragment;

   private PropertiesSingleton props;

   private EditText usernameEditText;
   private EditText passwordEditText;
   private CheckBox togglePasswordText;
   private Button authenticateNewUser;
   private Button logout;
   private Button cancel;


   @Override
   public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
      super.onCreateView(inflater, container, savedInstanceState);

      View view = inflater.inflate(ID, container, false);

      props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();

      uriField = (TextView) view.findViewById(R.id.sync_uri_field);
      accountAuthType = (TextView) view.findViewById(R.id.sync_account_auth_label);
      accountIdentity = (TextView) view.findViewById(R.id.sync_account);

      if (savedInstanceState != null && savedInstanceState.containsKey(VERIFY_SERVER_SETTINGS_ACTION)) {
         String action = savedInstanceState.getString(VERIFY_SERVER_SETTINGS_ACTION);
         try {
            verifyServerSettingsAction = VerifyServerSettingsActions.valueOf(action);
         } catch (IllegalArgumentException e) {
            verifyServerSettingsAction = VerifyServerSettingsActions.IDLE;
         }
      }

      usernameEditText = (EditText) view.findViewById(R.id.username);
      usernameEditText.setText(props.getProperty(CommonToolProperties.KEY_USERNAME));

      passwordEditText = (EditText) view.findViewById(R.id.pwd_field);

      togglePasswordText = (CheckBox) view.findViewById(R.id.show_pwd);
      togglePasswordText.setOnClickListener(new View.OnClickListener() {
         @Override
         public void onClick(View v) {
            if(togglePasswordText.isChecked()) {
               passwordEditText.setTransformationMethod(null);
            } else {
               passwordEditText.setTransformationMethod(new PasswordTransformationMethod());
            }
         }
      });

      authenticateNewUser = (Button) view.findViewById(R.id.change_user_button);
      authenticateNewUser.setOnClickListener(new View.OnClickListener() {
         @Override public void onClick(View v) {
            setNewCredentials();
            verifyServerSettings();
            refreshCredentialsDisplay();
         }
      });

      logout = (Button) view.findViewById(R.id.logout_button);
      logout.setOnClickListener(new View.OnClickListener() {
         @Override public void onClick(View v) {
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

      Map<String,String> properties = new HashMap<String,String>();
      properties.put(CommonToolProperties.KEY_USERNAME, username);
      properties.put(CommonToolProperties.KEY_PASSWORD, pw);
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
      properties.put(CommonToolProperties.KEY_USERS_LIST, "");

      props.setProperties(properties);
   }

   private void logout() {
      ODKServicesPropertyUtils.clearActiveUser(props);
      refreshCredentialsDisplay();
      getActivity().finish();
   }

   protected void disableButtons() {
      authenticateNewUser.setEnabled(false);
      logout.setEnabled(false);
      cancel.setEnabled(false);
   }

   protected void perhapsEnableButtons() {
      PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
      String url = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
      if ( url == null || url.length() == 0 ) {
         disableButtons();
      } else {
         authenticateNewUser.setEnabled(true);
         logout.setEnabled(true);
         cancel.setEnabled(true);
      }
   }
}
