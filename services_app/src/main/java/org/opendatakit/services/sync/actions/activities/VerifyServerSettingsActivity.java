/*
 * Copyright (C) 2016 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.services.sync.actions.activities;

import android.app.Fragment;
import android.app.FragmentManager;
import android.app.FragmentTransaction;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.fragments.VerifyServerSettingsFragment;

/**
 * An activity for verifying the server setings and
 * updating the configured user's permissions.handling server conflicts.
 * If an IntentConsts.INTENT_KEY_INSTANCE_ID is provided,
 * opens the row resolution fragment. Otherwise, opens
 * the list resolution fragment.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class VerifyServerSettingsActivity extends AbsSyncBaseActivity {

  private static final String TAG = VerifyServerSettingsActivity.class.getSimpleName();

  @Override
  protected void onResume() {
    super.onResume();

    WebLogger.getLogger(getAppName()).i(TAG, "[onResume] getting VerifyServerSettingsFragment");

    FragmentManager mgr = getFragmentManager();
    String newFragmentName;
    Fragment newFragment;

    // we want the list fragment
    newFragmentName = VerifyServerSettingsFragment.NAME;
    newFragment = mgr.findFragmentByTag(newFragmentName);
    if ( newFragment == null ) {
      newFragment = new VerifyServerSettingsFragment();
      WebLogger.getLogger(getAppName()).i(TAG, "[onResume] creating new VerifyServerSettingsFragment");
      
      FragmentTransaction trans = mgr.beginTransaction();
      trans.replace(R.id.sync_activity_view, newFragment, newFragmentName);
      WebLogger.getLogger(getAppName()).i(TAG, "[onResume] replacing fragment with id " + newFragment.getId());
      trans.commit();
    }
  }

  @Override public void onBackPressed() {
    PropertiesSingleton props = getProps();
    String authType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
    boolean isAnonymous = (authType == null) || (authType.length() == 0) ||
        getString(R.string.credential_type_none).equals(authType);
    if ( props.getProperty(CommonToolProperties.KEY_ROLES_LIST).length() == 0 &&
        !isAnonymous ) {

      AbsSyncBaseActivity.showAuthenticationErrorDialog(this, getString(R.string.warning_no_user_roles));
      return;
    }
    super.onBackPressed();
  }

}
