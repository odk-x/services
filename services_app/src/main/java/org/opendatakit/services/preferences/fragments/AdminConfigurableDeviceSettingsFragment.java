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
import android.preference.*;
import android.preference.Preference.OnPreferenceChangeListener;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;

public class AdminConfigurableDeviceSettingsFragment extends PreferenceFragment implements
    OnPreferenceChangeListener {

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    addPreferencesFromResource(R.xml.admin_configurable_device_preferences);

    PreferenceScreen prefScreen = this.getPreferenceScreen();

    initializeCheckBoxPreference(prefScreen);
  }

  protected void initializeCheckBoxPreference(PreferenceGroup prefGroup) {

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    for ( int i = 0; i < prefGroup.getPreferenceCount(); i++ ) {
      Preference pref = prefGroup.getPreference(i);
      Class c = pref.getClass();
      if (c == CheckBoxPreference.class) {
        CheckBoxPreference checkBoxPref = (CheckBoxPreference)pref;
        if (props.containsKey(checkBoxPref.getKey())) {
          String checked = props.getProperty(checkBoxPref.getKey());
          if (checked.equals("true")) {
            checkBoxPref.setChecked(true);
          } else {
            checkBoxPref.setChecked(false);
          }
        }
        // Set the listener
        checkBoxPref.setOnPreferenceChangeListener(this);
      } else if (c == PreferenceCategory.class) {
        // Find CheckBoxPreferences in this category
        PreferenceCategory prefCat = (PreferenceCategory)pref;
        initializeCheckBoxPreference(prefCat);
      }
    }
  }

  /**
   * Generic listener that sets the summary to the newly selected/entered value
   */
  @Override
  public boolean onPreferenceChange(Preference preference, Object newValue) {
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    props.setProperty(preference.getKey(), newValue.toString());
    return true;
  }

  @Override
  public void onSaveInstanceState(Bundle outState) {
    super.onSaveInstanceState(outState);
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    props.writeProperties();
  }

  @Override public void onPause() {
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    props.writeProperties();
    super.onPause();
  }
}