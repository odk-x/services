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

import android.arch.lifecycle.Observer;
import android.arch.lifecycle.ViewModelProviders;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.preference.CheckBoxPreference;
import android.support.v7.preference.Preference;
import android.support.v7.preference.Preference.OnPreferenceChangeListener;
import android.support.v7.preference.PreferenceCategory;
import android.support.v7.preference.PreferenceFragmentCompat;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.PreferenceViewModel;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;

import java.util.Collections;

public class TablesSettingsFragment extends PreferenceFragmentCompat {

  private static final String t = "DeviceSettingsFragment";
  private PreferenceViewModel preferenceViewModel;

  @Override
  public void onCreatePreferences(Bundle savedInstanceState, String rootKey) {
    setPreferencesFromResource(R.xml.tool_tables_preferences, rootKey);
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    CheckBoxPreference useHomeScreenPreference =
        (CheckBoxPreference) findPreference(CommonToolProperties.KEY_USE_HOME_SCREEN);

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) requireActivity()).getProps();

    if (props.containsKey(CommonToolProperties.KEY_USE_HOME_SCREEN)) {
      useHomeScreenPreference
          .setChecked(props.getBooleanProperty(CommonToolProperties.KEY_USE_HOME_SCREEN));
    }


    useHomeScreenPreference.setOnPreferenceChangeListener(new OnPreferenceChangeListener() {
      @Override
      public boolean onPreferenceChange(Preference preference, Object newValue) {
        PropertiesSingleton props = ((IOdkAppPropertiesActivity)
            TablesSettingsFragment.this.getActivity()).getProps();
        props.setProperties(Collections.singletonMap(preference.getKey(), newValue.toString()));
        return true;
      }
    });

    preferenceViewModel = ViewModelProviders
        .of(requireActivity())
        .get(PreferenceViewModel.class);

    preferenceViewModel.getAdminMode().observe(this, new Observer<Boolean>() {
      @Override
      public void onChanged(Boolean adminMode) {
        PropertiesSingleton props = ((IOdkAppPropertiesActivity) requireActivity()).getProps();

        Boolean useHomeScreen = props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_USE_HOME_SCREEN);
        useHomeScreen = useHomeScreen != null && useHomeScreen;

        setPrefEnabled(adminMode || useHomeScreen);
      }
    });

    preferenceViewModel.getAdminConfigured().observe(this, new Observer<Boolean>() {
      @Override
      public void onChanged(Boolean adminConfigured) {
        PropertiesSingleton props = ((IOdkAppPropertiesActivity) requireActivity()).getProps();

        Boolean useHomeScreen = props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_USE_HOME_SCREEN);
        useHomeScreen = useHomeScreen != null && useHomeScreen;

//        if (adminConfigured) {
//          if (adminMode) {
//            setEnable
//          } else {
//            if (useHomeScreen) {
//              setEnable
//            } else {
//              disable
//            }
//          }
//        } else {
//          setEnable
//        }

        setPrefEnabled(!adminConfigured || useHomeScreen);
      }
    });
  }

  private void setPrefEnabled(boolean enabled) {
    CheckBoxPreference useHomeScreenPreference =
        (CheckBoxPreference) findPreference(CommonToolProperties.KEY_USE_HOME_SCREEN);
    PreferenceCategory deviceCategory =
        (PreferenceCategory) findPreference(CommonToolProperties.GROUPING_TOOL_TABLES_CATEGORY);

    useHomeScreenPreference.setEnabled(enabled);

    deviceCategory.setTitle(enabled ?
        R.string.tool_tables_settings_summary :
        R.string.tool_tables_restrictions_apply
    );
  }

  @Override
  public void onDestroy() {
    super.onDestroy();

    preferenceViewModel.getAdminMode().removeObservers(this);
    preferenceViewModel.getAdminConfigured().removeObservers(this);
  }
}
