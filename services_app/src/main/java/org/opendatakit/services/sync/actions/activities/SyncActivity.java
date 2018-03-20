/*
 * Copyright (C) 2014 University of Washington
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

import android.app.AlertDialog;
import android.app.Fragment;
import android.app.FragmentManager;
import android.app.FragmentTransaction;
import android.content.DialogInterface;
import android.content.Intent;
import android.preference.PreferenceActivity;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.preferences.fragments.ServerSettingsFragment;
import org.opendatakit.services.sync.actions.fragments.SyncFragment;

import java.util.Collections;

/**
 * An activity for syncing the local content with the server.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class SyncActivity extends AbsSyncBaseActivity {

  private static final String TAG = SyncActivity.class.getSimpleName();
  private AlertDialog mDialog;

  @Override
  protected void onResume() {
    super.onResume();

    firstLaunch();
    WebLogger.getLogger(getAppName()).i(TAG, "[onResume] getting SyncFragment");

    FragmentManager mgr = getFragmentManager();
    String newFragmentName;
    Fragment newFragment;

    // we want the list fragment
    newFragmentName = SyncFragment.NAME;
    newFragment = mgr.findFragmentByTag(newFragmentName);
    if ( newFragment == null ) {
      newFragment = new SyncFragment();
      WebLogger.getLogger(getAppName()).i(TAG, "[onResume] creating new SyncFragment");
      
      FragmentTransaction trans = mgr.beginTransaction();
      trans.replace(R.id.sync_activity_view, newFragment, newFragmentName);
      WebLogger.getLogger(getAppName()).i(TAG, "[onResume] replacing fragment with id " + newFragment.getId());
      trans.commit();
    }
  }

  private void firstLaunch() {
    mProps = CommonToolProperties.get(this, mAppName);
    boolean isFirstLaunch = mProps.getBooleanProperty(CommonToolProperties.KEY_FIRST_LAUNCH);
    if (isFirstLaunch) {
      // set first launch to false
      mProps.setProperties(Collections.singletonMap(CommonToolProperties
              .KEY_FIRST_LAUNCH, "false"));
      AlertDialog.Builder builder = new AlertDialog.Builder(this);
      mDialog = builder.setMessage(R.string.configure_server_settings)
              .setCancelable(false)
              .setPositiveButton(R.string.yes, new DialogInterface.OnClickListener() {
                @Override public void onClick(DialogInterface dialog, int which) {
                  mDialog.dismiss();
                  Intent intent = new Intent( SyncActivity.this, AppPropertiesActivity.class );
                  intent.putExtra( PreferenceActivity.EXTRA_SHOW_FRAGMENT, ServerSettingsFragment.class.getName() );
                  intent.putExtra( PreferenceActivity.EXTRA_NO_HEADERS, true );
                  startActivity(intent);
                }
              })
              .setNegativeButton(R.string.no, new DialogInterface.OnClickListener() {
                @Override public void onClick(DialogInterface dialog, int which) {
                  dialog.dismiss();
                }
              }).create();
      mDialog.setCanceledOnTouchOutside(false);
      mDialog.show();
    }
  }

}
