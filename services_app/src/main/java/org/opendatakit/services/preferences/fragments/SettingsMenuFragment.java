package org.opendatakit.services.preferences.fragments;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;

import androidx.lifecycle.Observer;
import androidx.lifecycle.ViewModelProvider;
import androidx.preference.Preference;
import androidx.preference.PreferenceFragmentCompat;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.PreferenceViewModel;
import org.opendatakit.services.preferences.activities.DocumentationWebViewActivity;

public class SettingsMenuFragment extends PreferenceFragmentCompat {
  private static final int[] PREF_TO_SET_INTENT_APP_NAME = {
      R.string.key_reset_config,
      R.string.key_verify_settings
  };

  private PreferenceViewModel preferenceViewModel;

  @Override
  public void onCreatePreferences(Bundle savedInstanceState, String rootKey) {
    setPreferencesFromResource(R.xml.preferences_menu, rootKey);

    findPreference(getString(R.string.key_documentation))
        .setOnPreferenceClickListener(new Preference.OnPreferenceClickListener() {
          @Override
          public boolean onPreferenceClick(Preference preference) {
            Intent browserIntent = new Intent(
                Intent.ACTION_VIEW,
                Uri.parse(getString(R.string.opendatakit_url))
            );

            if (browserIntent.resolveActivity(requireContext().getPackageManager()) != null) {
              startActivity(browserIntent);
            } else {
              Intent i = new Intent(requireContext(), DocumentationWebViewActivity.class);
              startActivity(i);
            }

            return true;
          }
        });

    findPreference(getString(R.string.key_exit_admin_settings))
            .setOnPreferenceClickListener(new Preference.OnPreferenceClickListener() {
              @Override
              public boolean onPreferenceClick(Preference preference) {
                preferenceViewModel.setAdminMode(false);
                return true;
              }
            });
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    preferenceViewModel = new ViewModelProvider(requireActivity())
        .get(PreferenceViewModel.class);

    preferenceViewModel.getAdminMode().observe(this, new Observer<Boolean>() {
      @Override
      public void onChanged(Boolean adminMode) {
        findPreference(getString(R.string.key_admin_server_settings)).setVisible(adminMode);
        findPreference(getString(R.string.key_exit_admin_settings)).setVisible(adminMode);
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

        findPreference(getString(R.string.key_reset_config))
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
          adminPwdPref.setTitle(R.string.user_restrictions);
          adminPwdPref.setSummary(R.string.enable_user_restrictions);
        }

        Boolean adminMode = preferenceViewModel.getAdminMode().getValue();
        adminMode = adminMode != null && adminMode;
        // enable when admin is configured and had not entered password
        findPreference(getString(R.string.key_admin_general_settings))
            .setVisible(adminConfigured && !adminMode);

        // enable when admin is not configured or in admin mode
        adminPwdPref.setVisible(!adminConfigured || adminMode);

        findPreference(getString(R.string.key_reset_config))
            .setVisible(!adminConfigured || adminMode);
      }
    });
  }

  @Override
  public void onResume() {
    super.onResume();

    String appName = ((IAppAwareActivity) requireActivity()).getAppName();

    for (int key : PREF_TO_SET_INTENT_APP_NAME) {
      findPreference(getString(key))
          .getIntent()
          .putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
    }
  }
}
