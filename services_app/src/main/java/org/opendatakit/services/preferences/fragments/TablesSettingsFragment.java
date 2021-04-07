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

import android.os.Bundle;
import android.view.View;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.preference.CheckBoxPreference;
import androidx.preference.Preference;
import androidx.preference.PreferenceCategory;
import androidx.preference.PreferenceFragmentCompat;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;

import java.util.Collections;

public class TablesSettingsFragment extends PreferenceFragmentCompat {

  private static final String t = "DeviceSettingsFragment";

  private CheckBoxPreference mUseHomeScreenPreference;


  @Override
  public void onCreatePreferences(Bundle savedInstanceState, String rootKey) {
    setPreferencesFromResource(R.xml.tool_tables_preferences, rootKey);
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
  }

  @Override
  public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
    super.onViewCreated(view, savedInstanceState);
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();

    // not super safe, but we're just putting in this mode to help
    // administrate
    // would require code to access it
    boolean adminMode;
    adminMode = (this.getArguments() == null) ? false :
            (this.getArguments().containsKey(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE) ?
                    this.getArguments().getBoolean(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE) : false);

    String adminPwd = props.getProperty(CommonToolProperties.KEY_ADMIN_PW);
    boolean adminConfigured = (adminPwd != null && adminPwd.length() != 0);

    PreferenceCategory deviceCategory = findPreference
            (CommonToolProperties.GROUPING_TOOL_TABLES_CATEGORY);

    Boolean useHomeScreen = props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_USE_HOME_SCREEN);
    useHomeScreen = (useHomeScreen == null) ? false : useHomeScreen;
    boolean useHomeScreenAvailable = !adminConfigured || useHomeScreen;

    mUseHomeScreenPreference = findPreference(CommonToolProperties.KEY_USE_HOME_SCREEN);
    if (props.containsKey(CommonToolProperties.KEY_USE_HOME_SCREEN)) {
      boolean selection = props.getBooleanProperty(CommonToolProperties.KEY_USE_HOME_SCREEN);
      mUseHomeScreenPreference.setChecked(selection);
    }

    mUseHomeScreenPreference.setOnPreferenceChangeListener(new Preference.OnPreferenceChangeListener() {

      @Override
      public boolean onPreferenceChange(Preference preference, Object newValue) {
        PropertiesSingleton props = ((IOdkAppPropertiesActivity)
                TablesSettingsFragment.this.getActivity()).getProps();
        props.setProperties(Collections.singletonMap(preference.getKey(), newValue.toString()));
        return true;
      }
    });

    mUseHomeScreenPreference.setEnabled(useHomeScreenAvailable || adminMode);

    if ( !adminMode && (!useHomeScreenAvailable) ) {
      deviceCategory.setTitle(R.string.tool_tables_restrictions_apply);
    }
  }
}
