package org.opendatakit.services.preferences.fragments;

import android.arch.lifecycle.MediatorLiveData;
import android.arch.lifecycle.Observer;
import android.arch.lifecycle.ViewModelProviders;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.preference.Preference;
import android.support.v7.preference.PreferenceFragmentCompat;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
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
      public void onChanged(@Nullable Boolean adminMode) {
        findPreference(getString(R.string.key_admin_server_settings)).setVisible(adminMode);
        findPreference(getString(R.string.key_admin_device_settings)).setVisible(adminMode);
        findPreference(getString(R.string.key_admin_tables_settings)).setVisible(adminMode);

        Boolean adminConfigured = preferenceViewModel.getAdminConfigured().getValue();
        adminConfigured = adminConfigured != null && adminConfigured;
        findPreference(getString(R.string.key_admin_general_settings))
            .setVisible(adminConfigured && !adminMode);
      }
    });

    preferenceViewModel.getAdminConfigured().observe(this, new Observer<Boolean>() {
      @Override
      public void onChanged(@Nullable Boolean adminConfigured) {
        Preference preference = findPreference(getString(R.string.key_admin_password));
        if (adminConfigured) {
          preference.setTitle(R.string.change_admin_password);
          preference.setSummary(R.string.admin_password_enabled);
        } else {
          preference.setTitle(R.string.enable_admin_password);
          preference.setSummary(R.string.admin_password_disabled);
        }

        Boolean adminMode = preferenceViewModel.getAdminMode().getValue();
        adminMode = adminMode != null && adminMode;
        findPreference(getString(R.string.key_admin_general_settings))
            .setVisible(adminConfigured && !adminMode);
      }
    });
  }
}
