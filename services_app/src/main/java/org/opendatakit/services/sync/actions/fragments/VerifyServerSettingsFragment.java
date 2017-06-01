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


import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;

import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.*;
import org.opendatakit.services.sync.actions.VerifyServerSettingsActions;


/**
 * @author mitchellsundt@gmail.com
 */
public class VerifyServerSettingsFragment extends AuthenticationFragment implements ISyncOutcomeHandler {

  private static final String TAG = "VerifyServerSettingsFragment";

  public static final String NAME = "VerifyServerSettingsFragment";
  public static final int ID = R.layout.verify_server_settings_launch_fragment;

  private Button startVerifyServerSettings;

  @Override
  public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
    super.onCreateView(inflater, container, savedInstanceState);

    View view = inflater.inflate(ID, container, false);
    uriField = (TextView) view.findViewById(R.id.sync_uri_field);
    accountAuthType = (TextView) view.findViewById(R.id.sync_account_auth_label);
    accountIdentity = (TextView) view.findViewById(R.id.sync_account);

    if ( savedInstanceState != null && savedInstanceState.containsKey(VERIFY_SERVER_SETTINGS_ACTION) ) {
      String action = savedInstanceState.getString(VERIFY_SERVER_SETTINGS_ACTION);
      try {
        verifyServerSettingsAction = VerifyServerSettingsActions.valueOf(action);
      } catch ( IllegalArgumentException e ) {
        verifyServerSettingsAction = VerifyServerSettingsActions.IDLE;
      }
    }

    startVerifyServerSettings = (Button) view.findViewById(R.id.verify_server_settings_start_button);
    startVerifyServerSettings.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        verifyServerSettings();
      }
    });

    return view;
  }

  protected void disableButtons() {
    startVerifyServerSettings.setEnabled(false);
  }

  protected void perhapsEnableButtons() {
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    String url = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
    if ( url == null || url.length() == 0 ) {
      disableButtons();
    } else {
      startVerifyServerSettings.setEnabled(true);
    }
  }

}
