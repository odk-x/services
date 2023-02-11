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

import android.app.Activity;
import android.content.DialogInterface;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.widget.Toast;

import androidx.appcompat.app.AlertDialog;
import androidx.loader.app.LoaderManager;
import androidx.preference.CheckBoxPreference;
import androidx.preference.ListPreference;
import androidx.preference.Preference;
import androidx.preference.Preference.OnPreferenceChangeListener;
import androidx.preference.PreferenceCategory;
import androidx.preference.PreferenceFragmentCompat;
import androidx.preference.PreferenceScreen;

import com.google.android.material.dialog.MaterialAlertDialogBuilder;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.utilities.MediaUtils;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class DeviceSettingsFragment extends PreferenceFragmentCompat implements
    OnPreferenceChangeListener {

  private static final String t = "DeviceSettingsFragment";
  private static final int LOADER_ID = 3;

  private String mAppName;

  private CommonTranslationsLocaleScreen mDefaultTranslationPreference;
  private ListPreference mFontSizePreference;

  private CheckBoxPreference mShowSplashPreference;
  private PreferenceScreen mSplashPathPreference;

  @Override
  public void onCreatePreferences(Bundle savedInstanceState, String rootKey)  {
    setPreferencesFromResource(R.xml.device_preferences, rootKey);
  }

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    mAppName = ((IAppAwareActivity) requireActivity()).getAppName();
    if (mAppName == null || mAppName.length() == 0) {
      mAppName = ODKFileUtils.getOdkDefaultAppName();
    }

    PropertiesSingleton props = CommonToolProperties.get(this.getActivity(), mAppName);

    // not super safe, but we're just putting in this mode to help
    // administrate
    // would require code to access it
    boolean adminMode;
    adminMode = (this.getArguments() == null) ? false :
        (this.getArguments().containsKey(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE) ?
            this.getArguments().getBoolean(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE) : false);

    String adminPwd = props.getProperty(CommonToolProperties.KEY_ADMIN_PW);
    boolean adminConfigured = (adminPwd != null && adminPwd.length() != 0);

    PreferenceCategory deviceCategory = (PreferenceCategory) findPreference
        (CommonToolProperties.GROUPING_DEVICE_CATEGORY);

    mDefaultTranslationPreference = (CommonTranslationsLocaleScreen) findPreference(CommonToolProperties.KEY_COMMON_TRANSLATIONS_LOCALE);
    final Bundle b = new Bundle();
    b.putString(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
    LoaderManager.getInstance(this).initLoader(LOADER_ID, b, mDefaultTranslationPreference.getLoaderCallback());

    mDefaultTranslationPreference.setOnPreferenceChangeListener(new OnPreferenceChangeListener() {

      @Override
      public boolean onPreferenceChange(Preference preference, Object newValue) {
        String stringValue = newValue.toString();
        int index = ((CommonTranslationsLocaleScreen) preference).findIndexOfValue(stringValue);
        String entry = (String) ((CommonTranslationsLocaleScreen) preference).getEntries()[index];
        preference.setSummary(entry);

        PropertiesSingleton props =
            ((IOdkAppPropertiesActivity) DeviceSettingsFragment.this.getActivity()).getProps();
        if ( stringValue == null || stringValue.length() == 0 || stringValue.equalsIgnoreCase("_")) {
          props.setProperties(Collections.singletonMap(CommonToolProperties
              .KEY_COMMON_TRANSLATIONS_LOCALE, (String) null));
        } else {
          props.setProperties(Collections.singletonMap(CommonToolProperties
              .KEY_COMMON_TRANSLATIONS_LOCALE, stringValue));
        }
        // since the selection changed, we need to change the languages on the tags
        LoaderManager.getInstance(DeviceSettingsFragment.this).restartLoader(LOADER_ID, b,
            mDefaultTranslationPreference.getLoaderCallback());
        return true;
      }
    });

    mDefaultTranslationPreference.setEnabled(true);


    boolean fontAvailable = !adminConfigured ||
        props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_FONT_SIZE);
    mFontSizePreference = (ListPreference) findPreference(CommonToolProperties.KEY_FONT_SIZE);
    if (props.containsKey(CommonToolProperties.KEY_FONT_SIZE)) {
      String chosenFontSize = props.getProperty(CommonToolProperties.KEY_FONT_SIZE);
      CharSequence entryValues[] = mFontSizePreference.getEntryValues();
      for (int i = 0; i < entryValues.length; i++) {
        String entry = entryValues[i].toString();
        if (entry.equals(chosenFontSize)) {
          mFontSizePreference.setValue(entry);
          mFontSizePreference.setSummary(mFontSizePreference.getEntries()[i]);
        }
      }
    }

    mFontSizePreference.setOnPreferenceChangeListener(new OnPreferenceChangeListener() {

      @Override
      public boolean onPreferenceChange(Preference preference, Object newValue) {
        int index = ((ListPreference) preference).findIndexOfValue(newValue.toString());
        String entry = (String) ((ListPreference) preference).getEntries()[index];
        preference.setSummary(entry);

        PropertiesSingleton props =
            ((IOdkAppPropertiesActivity) DeviceSettingsFragment.this.getActivity()).getProps();
        props.setProperties(Collections.singletonMap(CommonToolProperties
            .KEY_FONT_SIZE, newValue.toString()));
        return true;
      }
    });

    mFontSizePreference.setEnabled(fontAvailable || adminMode);

    boolean splashAvailable =  !adminConfigured ||
        props.getBooleanProperty(CommonToolProperties.KEY_CHANGE_SPLASH_SETTINGS);

    mShowSplashPreference = findPreference(CommonToolProperties.KEY_SHOW_SPLASH);
    if (props.containsKey(CommonToolProperties.KEY_SHOW_SPLASH)) {
      boolean checked = props.getBooleanProperty(CommonToolProperties.KEY_SHOW_SPLASH);
      mShowSplashPreference.setChecked(checked);
    }
    mShowSplashPreference.setOnPreferenceChangeListener(new OnPreferenceChangeListener() {

      @Override
      public boolean onPreferenceChange(Preference preference, Object newValue) {
        PropertiesSingleton props =
            ((IOdkAppPropertiesActivity) DeviceSettingsFragment.this.getActivity()).getProps();
        props.setProperties(Collections.singletonMap(CommonToolProperties
            .KEY_SHOW_SPLASH, newValue.toString()));
        return true;
      }
    });

    mShowSplashPreference.setEnabled(adminMode || splashAvailable);


    mSplashPathPreference = findPreference(CommonToolProperties.KEY_SPLASH_PATH);
    if (props.containsKey(CommonToolProperties.KEY_SPLASH_PATH)) {
      mSplashPathPreference.setSummary(props.getProperty(CommonToolProperties.KEY_SPLASH_PATH));
    }
    mSplashPathPreference.setOnPreferenceClickListener(new Preference.OnPreferenceClickListener() {

      private void launchImageChooser() {
        Intent i = new Intent(Intent.ACTION_GET_CONTENT);
        i.setType("image/*");
        DeviceSettingsFragment.this.startActivityForResult(i, AppPropertiesActivity.SPLASH_IMAGE_CHOOSER);
      }

      @Override
      public boolean onPreferenceClick(Preference preference) {
        // if you have a value, you can clear it or select new.
        CharSequence cs = mSplashPathPreference.getSummary();
        if (cs != null && cs.toString().contains("/")) {

          final CharSequence[] items = { getString(R.string.select_another_image),
              getString(R.string.use_odk_default) };


          /**
           * New dialog styling
           * MaterialAlertDialogBuilder is standard for all ODK-X Apps
           * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
           * @params change to **AlertDialogBuilder** in case of any error and remove R.style.... param!
           */

          MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(DeviceSettingsFragment.this.getActivity(),R.style.OdkXAlertDialogStyle );
          builder.setTitle(getString(R.string.change_splash_path));
          builder.setNeutralButton(getString(R.string.cancel),
              new DialogInterface.OnClickListener() {
                @Override
                public void onClick(DialogInterface dialog, int id) {
                  dialog.dismiss();
                }
              });
          builder.setItems(items, new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int item) {
              if (items[item].equals(getString(R.string.select_another_image))) {
                launchImageChooser();
              } else {
                PropertiesSingleton props =
                    ((IOdkAppPropertiesActivity) DeviceSettingsFragment.this.getActivity()).getProps();

                String path = getString(R.string.default_splash_path);
                props.setProperties(Collections.singletonMap(CommonToolProperties
                    .KEY_SPLASH_PATH, path));
                mSplashPathPreference.setSummary(path);
              }
            }
          });
          AlertDialog alert = builder.create();
          alert.show();

        } else {
          launchImageChooser();
        }

        return true;
      }
    });

    mSplashPathPreference.setEnabled(adminMode || splashAvailable);



    if ( !adminMode && (!fontAvailable || !splashAvailable) ) {
      deviceCategory.setTitle(R.string.device_restrictions_apply);
    }
  }

  @Override public void onActivityResult(int requestCode, int resultCode, Intent intent) {
    super.onActivityResult(requestCode, resultCode, intent);

    if (resultCode == Activity.RESULT_CANCELED) {
      // request was canceled, so do nothing
      return;
    }

    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    String appName = props.getAppName();

    switch (requestCode) {
    case AppPropertiesActivity.SPLASH_IMAGE_CHOOSER:
      ////////////////////

    /*
     * We have chosen a saved image from somewhere, but we really want it to be
     * in: /sdcard/odk/instances/[current instance]/something.jpg so we copy it
     * there and insert that copy into the content provider.
     */

      // get gp of chosen file
      Uri selectedMedia = intent.getData();
      String sourceMediaPath = MediaUtils.getPathFromUri(this.getActivity(), selectedMedia, MediaStore.Images.Media.DATA);
      File sourceMedia = new File(sourceMediaPath);
      String extension = sourceMediaPath.substring(sourceMediaPath.lastIndexOf("."));
      File newMedia;

      if ( !ODKFileUtils.isPathUnderAppName(appName, sourceMedia) ) {
        newMedia = ODKFileUtils.asConfigFile(appName, "splash" + extension);
        try {
          ODKFileUtils.copyFile(sourceMedia, newMedia);
        } catch (IOException e) {
          WebLogger.getLogger(appName).e(t, "Failed to copy " + sourceMedia.getAbsolutePath());
          Toast.makeText(this.getActivity(), R.string.splash_media_save_failed, Toast.LENGTH_SHORT).show();
          // keep the image as a captured image so user can choose it.
          return;
        }
        WebLogger.getLogger(appName).i(t, "copied " + sourceMedia.getAbsolutePath() + " to " + newMedia.getAbsolutePath());
      } else {
        newMedia = sourceMedia;
      }

      if (newMedia.exists()) {
        String appRelativePath = ODKFileUtils.asRelativePath(props.getAppName(), newMedia);

        props.setProperties(Collections.singletonMap(CommonToolProperties
            .KEY_SPLASH_PATH, appRelativePath));
        mSplashPathPreference.setSummary(appRelativePath);
      }
    }
  }

  /**
   * Generic listener that sets the summary to the newly selected/entered value
   */
  @Override
  public boolean onPreferenceChange(Preference preference, Object newValue) {
    preference.setSummary((CharSequence) newValue);
    PropertiesSingleton props = ((IOdkAppPropertiesActivity) this.getActivity()).getProps();
    props.setProperties(Collections.singletonMap(preference.getKey(), newValue.toString()));
    return true;
  }
}
