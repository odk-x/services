package org.opendatakit.services.preferences.fragments;

import android.os.Bundle;
import android.view.View;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.XmlRes;
import androidx.preference.CheckBoxPreference;
import androidx.preference.Preference;
import androidx.preference.PreferenceCategory;
import androidx.preference.PreferenceFragmentCompat;
import androidx.preference.PreferenceGroup;

import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;

import java.util.Collections;

public abstract class AbsAdminConfigurableSettingsFragment extends PreferenceFragmentCompat
    implements Preference.OnPreferenceChangeListener {
  @XmlRes
  abstract protected int getPreferencesResId();

  @Override
  public void onCreatePreferences(Bundle savedInstanceState, String rootKey) {
    setPreferencesFromResource(getPreferencesResId(), rootKey);
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
  }

  @Override
  public void onViewCreated(@NonNull View view, @Nullable Bundle savedInstanceState) {
    super.onViewCreated(view, savedInstanceState);
    initializeCheckBoxPreference(getPreferenceScreen());
  }

  protected void initializeCheckBoxPreference(PreferenceGroup prefGroup) {

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) requireActivity()).getProps();
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
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) requireActivity()).getProps();
    props.setProperties(Collections.singletonMap(preference.getKey(), newValue.toString()));
    return true;
  }
}
