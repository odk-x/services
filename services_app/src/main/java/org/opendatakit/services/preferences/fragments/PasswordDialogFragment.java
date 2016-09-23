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

import android.app.DialogFragment;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;

public class PasswordDialogFragment extends DialogFragment {

  public interface OnChangePassword {
    void passwordChanged();
  }

  private String passwordPropertyName;
  private EditText passwordEditText;
  private EditText verifyEditText;
  private PropertiesSingleton props;
  private OnChangePassword callback;

  /**
   * Create a new instance of PasswordDialogFragment,
   * providing "passwordPropertyName" as an argument.
   */
  static PasswordDialogFragment newPasswordDialog(String passwordPropertyName) {
    PasswordDialogFragment f = new PasswordDialogFragment();

    // Supply num input as an argument.
    Bundle args = new Bundle();
    args.putString("passwordPropertyName", passwordPropertyName);
    f.setArguments(args);

    return f;
  }

  public void setOnChangePasswordCallback(OnChangePassword callback) {
    this.callback = callback;
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    passwordPropertyName = getArguments().getString("passwordPropertyName");
  }

  @Override
  public View onCreateView(LayoutInflater inflater, ViewGroup container,
      Bundle savedInstanceState) {

    final boolean isAdminPassword = passwordPropertyName.equals(CommonToolProperties.KEY_ADMIN_PW);

    View view = inflater.inflate(R.layout.password_dialog_layout, container);

    props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    int fontSize = props.getIntegerProperty(CommonToolProperties.KEY_FONT_SIZE);

    TextView heading = (TextView) view.findViewById(R.id.change_password_heading);
    heading.setText((isAdminPassword ?
        R.string.change_admin_password : R.string.change_server_password));
    passwordEditText = (EditText) view.findViewById(R.id.pwd_field);
    verifyEditText = (EditText) view.findViewById(R.id.verify_field);

    String adminPW = "";
    if (props.getProperty(passwordPropertyName) != null) {
    	adminPW =  props.getProperty(passwordPropertyName);
    }

    // populate the fields if a pw exists
    if (!adminPW.equalsIgnoreCase("")) {
      passwordEditText.setText(adminPW);
      passwordEditText.setSelection(passwordEditText.getText().length());
      verifyEditText.setText(adminPW);
    }

    Button positiveButton = (Button) view.findViewById(R.id.positive_button);
    positiveButton.setOnClickListener(new View.OnClickListener() {
      @Override
      public void onClick(View v) {

        String pw = passwordEditText.getText().toString();
        String ver = verifyEditText.getText().toString();
        
        if (!pw.equalsIgnoreCase("") && !ver.equalsIgnoreCase("") && pw.equals(ver)) {
          // passwords are the same
          props.setProperty(passwordPropertyName, pw);
          if ( !isAdminPassword ) {
            props.setProperty(CommonToolProperties.KEY_ROLES_LIST, "");
            props.setProperty(CommonToolProperties.KEY_USERS_LIST, "");
          }
          props.writeProperties();

          Toast.makeText(PasswordDialogFragment.this.getActivity(),
              R.string.password_changed, Toast.LENGTH_SHORT).show();
          PasswordDialogFragment.this.getDialog().dismiss();
          if ( callback != null ) {
            callback.passwordChanged();
          }
        } else if (pw.equalsIgnoreCase("") && ver.equalsIgnoreCase("")) {
          props.setProperty(passwordPropertyName, "");
          if ( !isAdminPassword ) {
            props.setProperty(CommonToolProperties.KEY_ROLES_LIST, "");
            props.setProperty(CommonToolProperties.KEY_USERS_LIST, "");
          }
          props.writeProperties();

          if ( isAdminPassword ) {
            Toast.makeText(PasswordDialogFragment.this.getActivity(),
                R.string.admin_password_disabled, Toast.LENGTH_SHORT).show();
          }

          PasswordDialogFragment.this.dismiss();
          if ( callback != null ) {
            callback.passwordChanged();
          }
        } else {
          Toast.makeText(PasswordDialogFragment.this.getActivity(),
              R.string.password_mismatch, Toast.LENGTH_SHORT).show();
        }
      }
    });

    Button negativeButton = (Button) view.findViewById(R.id.negative_button);
    negativeButton.setOnClickListener(new View.OnClickListener() {

      @Override
      public void onClick(View v) {
        PasswordDialogFragment.this.dismiss();
      }

    });

    return view;
  }

  @Override public void onViewCreated(View view, Bundle savedInstanceState) {
    super.onViewCreated(view, savedInstanceState);
    // this seems to work to pop the keyboard when the dialog appears
    // i hope this isn't a race condition
    getDialog().getWindow().setSoftInputMode(
        WindowManager.LayoutParams.SOFT_INPUT_STATE_ALWAYS_VISIBLE);
  }
}
