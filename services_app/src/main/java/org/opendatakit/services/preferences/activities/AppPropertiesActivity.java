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

package org.opendatakit.services.preferences.activities;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.preference.PreferenceActivity;
import android.support.annotation.StringRes;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.utilities.ODKFileUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * App-level settings activity.  Used across all tools.
 *
 * @author mitchellsundt@gmail.com
 */
public class AppPropertiesActivity extends PreferenceActivity implements IOdkAppPropertiesActivity {

  private static final String t = "AppPropertiesActivity";

  public static final int SPLASH_IMAGE_CHOOSER = 1;

  private static final String SAVED_ADMIN_CONFIGURED = "savedAdminConfigured";
  private String mAppName;
  private boolean mAdminMode;
  private boolean mAdminConfigured;
  private Activity mActivity = this;
  
  private PropertiesSingleton mProps;

  /**
   * Populate the activity with the top-level headers.
   */
  @Override
  public void onBuildHeaders(List<Header> target) {
    List<Header> rawHeaders = new ArrayList<Header>();
    loadHeadersFromResource(R.xml.preferences_headers, rawHeaders);

    for ( Header header : rawHeaders ) {
      // if the properties are only shown in adminMode,
      // then omit them if we are not in admin mode.
      if ( header.fragmentArguments != null ) {
        boolean hasAdminModeRestriction =
          header.fragmentArguments.containsKey(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE);
        boolean hasAdminEnabledRestriction =
          header.fragmentArguments.containsKey(IntentConsts.INTENT_KEY_SETTINGS_ADMIN_ENABLED);

        boolean pass = !(hasAdminEnabledRestriction || hasAdminModeRestriction);
        if ( hasAdminEnabledRestriction ) {
          boolean enabledRequirement = header.fragmentArguments.getBoolean(IntentConsts
              .INTENT_KEY_SETTINGS_ADMIN_ENABLED);
          pass = pass || ( enabledRequirement == mAdminConfigured);
        }

        if ( hasAdminModeRestriction ) {
          boolean modeRestriction = header.fragmentArguments.getBoolean(IntentConsts
              .INTENT_KEY_SETTINGS_IN_ADMIN_MODE);
          pass = pass || ( modeRestriction == mAdminMode );
        }

        if ( !pass ) {
          continue;
        }
      }

      if ( header.id == R.id.general_settings_in_admin_mode ) {
        if ( !mAdminConfigured || mAdminMode ) {
          continue;
        }
      }

      if ( header.id == R.id.clear_configuration_settingss ) {
        // omit this if we have admin mode configured but are not in admin mode
        if ( mAdminConfigured && !mAdminMode ) {
          continue;
        }
      }

      target.add(header);
    }

    for ( Header header : target ) {
      if ( header.id == R.id.general_settings_in_admin_mode ) {
        // TODO: change to challenge for admin password and then
        // TODO: launch the general settings in admin mode.
        Intent intent = new Intent(this, AdminPasswordChallengeActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME,
            this.getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME));
        header.intent = intent;
      }

      if ( header.id == R.id.clear_configuration_settingss ) {
        Intent intent = new Intent(this, ClearAppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME,
            this.getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME));
        header.intent = intent;
      }

      if ( header.id == R.id.verify_server_settingss ) {
        Intent intent = new Intent(this, VerifyServerSettingsActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME,
                this.getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME));
        header.intent = intent;
      }

      if ( header.id == R.id.admin_password_status ) {
        if ( mAdminConfigured ) {
          header.titleRes = R.string.change_admin_password;
          header.summaryRes = R.string.admin_password_enabled;
        } else {
          header.titleRes = R.string.enable_admin_password;
          header.summaryRes = R.string.admin_password_disabled;
        }
      }
      if ( header.fragmentArguments != null ) {
        header.fragmentArguments.putString(IntentConsts.INTENT_KEY_APP_NAME,
            this.getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME));
        if ( mAdminMode ) {
          header.fragmentArguments.putBoolean(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE,
              true);
        } else {
          header.fragmentArguments.remove(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE);
        }
      }
    }
  }

  @Override
  protected boolean isValidFragment(String fragmentName) {
    return true;
  }

  public PropertiesSingleton getProps() {
    return mProps;
  }

  @Override
  protected void onCreate(Bundle savedInstanceState) {

    mAppName = this.getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if (mAppName == null || mAppName.length() == 0) {
      mAppName = ODKFileUtils.getOdkDefaultAppName();
    }

    mProps = CommonToolProperties.get(this, mAppName);

    if ( savedInstanceState != null ) {
      mAdminConfigured = savedInstanceState.getBoolean(SAVED_ADMIN_CONFIGURED);
    } else {
      String adminPwd = mProps.getProperty(CommonToolProperties.KEY_ADMIN_PW);
      mAdminConfigured = (adminPwd != null && adminPwd.length() != 0);
    }

    mAdminMode =
        this.getIntent().getBooleanExtra(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE, false);

    this.getActionBar().setTitle(
        getString((mAdminMode ?
            R.string.action_bar_general_settings_admin_mode :
            R.string.action_bar_general_settings),
            mAppName));

    super.onCreate(savedInstanceState);
  }

  @Override protected void onResume() {
    super.onResume();

    mAppName = this.getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if (mAppName == null || mAppName.length() == 0) {
      mAppName = ODKFileUtils.getOdkDefaultAppName();
    }

    mProps = CommonToolProperties.get(this, mAppName);

    mAdminMode =
        this.getIntent().getBooleanExtra(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE, false);

    String adminPwd = mProps.getProperty(CommonToolProperties.KEY_ADMIN_PW);
    boolean isAdminConfigured = (adminPwd != null && adminPwd.length() != 0);

    boolean shouldInvalidateHeaders = false;
    if ( isAdminConfigured != mAdminConfigured ) {
      mAdminConfigured = isAdminConfigured;
      shouldInvalidateHeaders = true;
    }

    if ( mAdminMode && !mAdminConfigured ) {
      // we disabled admin controls but are in the admin-level
      // settings activity.
      // back out to the non-admin-level settings activity.
      finish();
      return;
    }

    if ( shouldInvalidateHeaders ) {
      invalidateHeaders();
    }
  }

  @Override
  public Intent onBuildStartFragmentIntent(String fragmentName, Bundle args,
      @StringRes int titleRes, int shortTitleRes) {
    Intent toLaunch = super.onBuildStartFragmentIntent(fragmentName, args, titleRes, shortTitleRes);
    toLaunch.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
    if ( mAdminMode ) {
      toLaunch.putExtra(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE, mAdminMode);
    }
    return toLaunch;
  }

  @Override
  protected void onSaveInstanceState(Bundle outState) {
    super.onSaveInstanceState(outState);
    outState.putBoolean(SAVED_ADMIN_CONFIGURED, mAdminConfigured);
  }

  /**
   * If we are exiting the non-privileged settings screen and the user roles are not
   * set, then prompt the user to verify user permissions or lose their changes
   *
   * Otherwise, exit the settings screen.
   */
  @Override
  public void onBackPressed() {
    if ( !mAdminMode ) {
      String authType = mProps.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
      boolean isAnonymous = (authType == null) || (authType.length() == 0) ||
              getString(R.string.credential_type_none).equals(authType);
      if ( mProps.getProperty(CommonToolProperties.KEY_ROLES_LIST).length() == 0 &&
              !isAnonymous ) {

        promptToVerifyCredentials();
        return;
      }
    }
    super.onBackPressed();
  }

  private void promptToVerifyCredentials() {
    AlertDialog.Builder builder = new AlertDialog.Builder(this);
    builder.setTitle(R.string.authenticate_credentials);
    builder.setMessage(R.string.anonymous_warning);
    builder.setPositiveButton(R.string.new_user, new DialogInterface.OnClickListener() {
      public void onClick(DialogInterface dialog, int id) {
        // this will swap to the new activity and close this one
        Intent i = new Intent(mActivity, VerifyServerSettingsActivity.class);
        i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
        startActivity(i);
        dialog.dismiss();
      }
    });
    builder.setNegativeButton(R.string.logout, new DialogInterface.OnClickListener() {
      public void onClick(DialogInterface dialog, int id) {
        finish();
        dialog.dismiss();
      }
    });
    AlertDialog dialog = builder.create();
    dialog.show();
  }

  public String getAppName() {
    return mAppName;
  }

  @Override
  public void onHeaderClick(Header header, int position) {
    super.onHeaderClick(header, position);
    if (header.id == R.id.open_documentation) {
      Intent browserIntent = new Intent(Intent.ACTION_VIEW, Uri.parse(getString(R.string.opendatakit_url)));
      if (browserIntent.resolveActivity(getPackageManager()) != null) {
        startActivity(browserIntent);
      } else {
        Intent i = new Intent(this, DocumentationWebViewActivity.class);
        startActivity(i);
      }
    }
  }
}
