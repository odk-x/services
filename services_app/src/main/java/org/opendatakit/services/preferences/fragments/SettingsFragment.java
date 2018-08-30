package org.opendatakit.services.preferences.fragments;

import android.arch.lifecycle.Observer;
import android.arch.lifecycle.ViewModelProviders;
import android.os.Bundle;
import android.support.v7.preference.Preference;
import android.support.v7.preference.PreferenceFragmentCompat;

import org.opendatakit.services.R;
import org.opendatakit.services.preferences.PreferenceViewModel;

public class SettingsFragment extends PreferenceFragmentCompat {
  private PreferenceViewModel preferenceViewModel;

  @Override
  public void onCreatePreferences(Bundle savedInstanceState, String rootKey) {
    setPreferencesFromResource(R.xml.general_preferences, rootKey);
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    preferenceViewModel = ViewModelProviders
        .of(requireActivity())
        .get(PreferenceViewModel.class);

    preferenceViewModel.getAdminMode().observe(this, new Observer<Boolean>() {
      @Override
      public void onChanged(Boolean adminMode) {
        findPreference(getString(R.string.key_admin_server_settings)).setVisible(adminMode);
        findPreference(getString(R.string.key_admin_device_settings)).setVisible(adminMode);
        findPreference(getString(R.string.key_admin_tables_settings)).setVisible(adminMode);

        Boolean adminConfigured = preferenceViewModel.getAdminConfigured().getValue();
        adminConfigured = adminConfigured != null && adminConfigured;

        // enable when admin is configured and had not entered password
        findPreference(getString(R.string.key_admin_general_settings))
            .setVisible(adminConfigured && !adminMode);

        // enable when admin is not configured or in admin mode
        findPreference(getString(R.string.key_admin_password))
            .setVisible(!adminConfigured || adminMode);
      }
    });

    preferenceViewModel.getAdminConfigured().observe(this, new Observer<Boolean>() {
      @Override
      public void onChanged(Boolean adminConfigured) {
        Preference adminPwdPref = findPreference(getString(R.string.key_admin_password));
        if (adminConfigured) {
          adminPwdPref.setTitle(R.string.change_admin_password);
          adminPwdPref.setSummary(R.string.admin_password_enabled);
        } else {
          adminPwdPref.setTitle(R.string.enable_admin_password);
          adminPwdPref.setSummary(R.string.admin_password_disabled);
        }

        Boolean adminMode = preferenceViewModel.getAdminMode().getValue();
        adminMode = adminMode != null && adminMode;
        // enable when admin is configured and had not entered password
        findPreference(getString(R.string.key_admin_general_settings))
            .setVisible(adminConfigured && !adminMode);

        // enable when admin is not configured or in admin mode
        adminPwdPref.setVisible(!adminConfigured || adminMode);
      }
    });
  }
}
