/*
 * Copyright (C) 2017 University of Washington
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

import android.content.Context;
import android.os.Bundle;
import android.util.AttributeSet;

import androidx.loader.app.LoaderManager;
import androidx.loader.content.AsyncTaskLoader;
import androidx.loader.content.Loader;
import androidx.preference.ListPreference;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.utilities.LocalizationUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Holds the list of commonTranslations.js locales and a "default to system locale" option.
 * Uses an AsyncLoaderTask to load the translations.
 *
 * @author mitchellsundt@gmail.com
 */

public class CommonTranslationsLocaleScreen extends ListPreference {
  private static ArrayList<String> savedValues = null;

  public static class LocaleStruct {
    public final String localeName;
    public final String localizedDisplayName;

    LocaleStruct(String localeName, String localizedDisplayName) {
      this.localeName = localeName;
      this.localizedDisplayName = localizedDisplayName;
    }
  }

  public static class LocaleResults {

    final int idxCurrentSelection;
    final String defaultValue;
    final List<LocaleStruct> locales;

    LocaleResults(int idxCurrentSelection, String defaultValue, List<LocaleStruct> locales) {
      this.idxCurrentSelection = idxCurrentSelection;
      this.defaultValue = defaultValue;
      this.locales = locales;
    }

  }

  public static class LocaleLoader extends AsyncTaskLoader<LocaleResults> {

    private final String mAppName;

    public LocaleLoader(Context context, String appName) {
      super(context);
      mAppName = appName;
    }

    /**
     * Handles a request to start the Loader.
     */
    @Override
    protected void onStartLoading() {
      forceLoad();
    }

    @Override
    public LocaleResults loadInBackground() {

      PropertiesSingleton props = CommonToolProperties.get(getContext(), mAppName);
      Locale defaultLocale = Locale.getDefault();
      String systemLocale = defaultLocale.toString();

      List<Map<String, Object>> commonLocales = null;
      String commonLocaleDefault = null;
      try {

        commonLocales = LocalizationUtils.getCommonLocales(props.getAppName());
        commonLocaleDefault = LocalizationUtils.getCommonLocaleDefault(props.getAppName());

      } catch (IOException e) {
        WebLogger.getLogger(mAppName).printStackTrace(e);
        WebLogger.getLogger(mAppName).e("LocaleLoader", "Unable to fetch common translations");
        List<LocaleStruct> localeStructs = new ArrayList<LocaleStruct>();
        localeStructs.add(
            new LocaleStruct("_", getContext().getString(R.string.system_locale, systemLocale)));

        LocaleResults localeResults = new LocaleResults(0, "_", localeStructs);

        return localeResults;
      }

      // current default locale; if null, then use system locale
      String currentDefaultCommonTranslation = props
          .getProperty(CommonToolProperties.KEY_COMMON_TRANSLATIONS_LOCALE);

      String currentLocale = (currentDefaultCommonTranslation == null) ?
          systemLocale :
          currentDefaultCommonTranslation;

      int idxCurrentSelection = -1;
      List<LocaleStruct> localeStructs = new ArrayList<LocaleStruct>();

      localeStructs
          .add(new LocaleStruct("_", getContext().getString(R.string.system_locale, systemLocale)));

      // if we are using the system locale, track that now.
      if (currentDefaultCommonTranslation == null) {
        idxCurrentSelection = 0;
      }

      int idxLocaleDefault = -1;

      if (commonLocales != null) {
        for (Map<String, Object> localeEntry : commonLocales) {
          String localeName = (String) localeEntry.get("name");
          if ( localeName == null ) {
            throw new IllegalStateException("expected name field in locale entry");
          }
          Object obj = localeEntry.get("display");
          if ( obj == null || !(obj instanceof Map)) {
            throw new IllegalStateException("expected display object");
          }
          @SuppressWarnings("unchecked")
          Map<String, Object> displayObject = (Map<String, Object>) obj;
          Object displayLocale = displayObject.get("locale");
          String localization = LocalizationUtils
              .getLocalizationFromMap(props.getAppName(), null, currentLocale, displayLocale);
          if (localization == null) {
            localization = "<<missing localization for localeName " + localeName;
          }

          localeStructs.add(new LocaleStruct(localeName, localization));

          // see if this is a match
          if (currentDefaultCommonTranslation != null && localeName
              .equalsIgnoreCase(currentDefaultCommonTranslation)) {
            idxCurrentSelection = localeStructs.size() - 1;
          }

          if (localeName.equalsIgnoreCase(commonLocaleDefault)) {
            idxLocaleDefault = localeStructs.size() - 1;
          }
        }
      }

      if (idxCurrentSelection == -1) {
        // can't find default locale -- reset to the default locale specified in the common
        // translations file.
        props.setProperties(Collections
            .singletonMap(CommonToolProperties.KEY_COMMON_TRANSLATIONS_LOCALE,
                commonLocaleDefault));
        idxCurrentSelection = idxLocaleDefault;
      }

      LocaleResults localeResults = new LocaleResults(idxCurrentSelection,
          (currentDefaultCommonTranslation == null) ? "_" : currentDefaultCommonTranslation,
          localeStructs);

      if (savedValues == null) {
        savedValues = new ArrayList<>();
        for (LocaleStruct x : localeResults.locales) {
          savedValues.add(x.localizedDisplayName);
        }
      }
      return localeResults;
    }
  }

  public class LocaleLoaderCallback implements LoaderManager.LoaderCallbacks<LocaleResults> {

    @Override
    public Loader<LocaleResults> onCreateLoader(int i, Bundle bundle) {
      return new LocaleLoader(getContext(), bundle.getString(IntentConsts.INTENT_KEY_APP_NAME));
    }

    @Override
    public void onLoadFinished(Loader<LocaleResults> loader, LocaleResults localeResults) {

      String[] localeNames = new String[localeResults.locales.size()];
      String[] localeDisplayNames = new String[localeResults.locales.size()];

      for (int i = 0; i < localeResults.locales.size(); ++i) {
        localeNames[i] = localeResults.locales.get(i).localeName;
        localeDisplayNames[i] = localeResults.locales.get(i).localizedDisplayName;
      }

      CommonTranslationsLocaleScreen.this.setEntryValues(localeNames);
      CommonTranslationsLocaleScreen.this.setEntries(localeDisplayNames);

      if (localeResults.idxCurrentSelection != -1) {
        CommonTranslationsLocaleScreen.this
            .setValue(localeNames[localeResults.idxCurrentSelection]);
        CommonTranslationsLocaleScreen.this
            .setSummary(localeDisplayNames[localeResults.idxCurrentSelection]);
      }

      CommonTranslationsLocaleScreen.this.setDefaultValue(localeResults.defaultValue);
    }

    @Override
    public void onLoaderReset(Loader<LocaleResults> loader) {
    }
  }

  public LoaderManager.LoaderCallbacks<LocaleResults> getLoaderCallback() {
    return new LocaleLoaderCallback();
  }

  /**
   * Called to re-create the list when the screen is rotated. If we don't save the values
   * earlier and restore the saved values, it just makes an empty selection list. It was a very
   * interesting bug to track down
   * <p>
   * Also we can't just not call setEntryValues or setEntries or the whole app crashes
   *
   * @param context the app context (?)
   * @param attrs   a set of attributes (?)
   */
  @SuppressWarnings("unused")
  public CommonTranslationsLocaleScreen(final Context context, AttributeSet attrs) {
    super(context, attrs);
    String[] value = new String[0];
    if (savedValues != null) {
      value = savedValues.toArray(new String[savedValues.size()]);
    }
    this.setEntryValues(value);
    this.setEntries(value);
  }

  public CommonTranslationsLocaleScreen(final Context context) {
    super(context);
    String[] value = new String[0];
    this.setEntryValues(value);
    this.setEntries(value);
  }
}
