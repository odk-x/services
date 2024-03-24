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

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentTransaction;
import androidx.lifecycle.ViewModelProvider;
import androidx.preference.Preference;
import androidx.preference.Preference.OnPreferenceChangeListener;
import androidx.preference.PreferenceFragmentCompat;

import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.PasswordPreferenceScreen;
import org.opendatakit.services.preferences.PreferenceViewModel;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;

import java.util.Collections;

public class AdminPasswordSettingsFragment extends PreferenceFragmentCompat implements
    OnPreferenceChangeListener,
    PasswordDialogFragment.OnChangePassword {

  private static final String t = "AdminPasswordSettingsFragment";

  private PreferenceViewModel preferenceViewModel;

  @Override
  public void onCreatePreferences(Bundle savedInstanceState, String rootKey)  {
    setPreferencesFromResource(R.xml.admin_password_preferences, rootKey);

    PasswordPreferenceScreen passwordScreen = this.findPreference(CommonToolProperties
        .GROUPING_PASSWORD_SCREEN);
    passwordScreen.setCallback(new PasswordPreferenceScreen.PasswordActionCallback() {
      @Override public void showPasswordDialog() {
        FragmentManager fragmentManager = getParentFragmentManager();

        // DialogFragment.show() will take care of adding the fragment
        // in a transaction.  We also want to remove any currently showing
        // dialog, so make our own transaction and take care of that here.
        FragmentTransaction ft = fragmentManager.beginTransaction();
        Fragment prev = fragmentManager
            .findFragmentByTag(CommonToolProperties.GROUPING_PASSWORD_SCREEN);
        if (prev != null) {
          ft.remove(prev);
        }
        ft.commit();

        // Create and show the dialog.
        PasswordDialogFragment newFragment = PasswordDialogFragment
            .newPasswordDialog(CommonToolProperties.KEY_ADMIN_PW);

        newFragment.setOnChangePasswordCallback(AdminPasswordSettingsFragment.this);
        newFragment.show(fragmentManager, CommonToolProperties.GROUPING_PASSWORD_SCREEN);
      }
    });
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    preferenceViewModel = new ViewModelProvider(requireActivity())
        .get(PreferenceViewModel.class);
  }

  @Override public void onResume() {
    super.onResume();

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();

    PasswordPreferenceScreen passwordScreen =
            this.findPreference(CommonToolProperties.GROUPING_PASSWORD_SCREEN);

    String adminPwd = props.getProperty(CommonToolProperties.KEY_ADMIN_PW);
    if ( adminPwd == null || adminPwd.length() == 0 ) {
      passwordScreen.setSummary(R.string.admin_password_disabled_click_to_set);
    } else {
      passwordScreen.setSummary(R.string.admin_password_settings_summary);
    }
  }

  /**
   * Generic listener that sets the summary to the newly selected/entered value
   */
  @Override
  public boolean onPreferenceChange(Preference preference, Object newValue) {
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    preference.setSummary((CharSequence) newValue);
    props.setProperties(Collections.singletonMap(preference.getKey(), newValue.toString()));
    return true;
  }

  @Override
  public void passwordChanged() {
    PropertiesSingleton props =
        ((IOdkAppPropertiesActivity) AdminPasswordSettingsFragment.this.getActivity()).getProps();

    PasswordPreferenceScreen passwordScreen = AdminPasswordSettingsFragment.this.findPreference(CommonToolProperties
        .GROUPING_PASSWORD_SCREEN);

    String adminPwd = props.getProperty(CommonToolProperties.KEY_ADMIN_PW);
    if ( adminPwd == null || adminPwd.length() == 0 ) {
      passwordScreen.setSummary(R.string.admin_password_disabled_click_to_set);
      preferenceViewModel.setAdminConfigured(false);
      preferenceViewModel.setAdminMode(false);
    } else {
      passwordScreen.setSummary(R.string.admin_password_settings_summary);
      preferenceViewModel.setAdminConfigured(true);
      preferenceViewModel.setAdminMode(true);
    }
  }
}
