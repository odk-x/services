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

import android.app.Activity;
import android.app.Fragment;
import android.app.FragmentManager;
import android.app.FragmentTransaction;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.Bundle;
import android.os.Handler;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.Toast;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.services.preferences.activities.IOdkAppPropertiesActivity;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.fragment.AboutMenuFragment;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.resolve.conflict.AllConflictsResolutionActivity;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.fragments.VerifyServerSettingsFragment;
import org.opendatakit.sync.service.OdkSyncServiceInterface;

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
public class VerifyServerSettingsActivity extends SyncBaseActivity {

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

      Handler h = new Handler();
      Toast.makeText(this, R.string.warning_no_user_roles, Toast.LENGTH_LONG).show();
      h.postDelayed(new Runnable() {
        @Override
        public void run() {
          finish();
        }
      }, Toast.LENGTH_LONG);
      return;
    }
    super.onBackPressed();
  }

}
