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
import android.content.DialogInterface;
import android.os.Bundle;

import androidx.fragment.app.Fragment;
import androidx.fragment.app.FragmentManager;
import androidx.fragment.app.FragmentTransaction;
import androidx.lifecycle.Lifecycle;
import androidx.lifecycle.LifecycleEventObserver;
import androidx.lifecycle.LifecycleObserver;
import androidx.lifecycle.OnLifecycleEvent;
import androidx.lifecycle.ViewModelProvider;
import androidx.navigation.Navigation;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.fragments.ServerSettingsFragment;
import org.opendatakit.services.sync.actions.fragments.SyncFragment;
import org.opendatakit.services.sync.actions.viewModels.LoginViewModel;
import org.opendatakit.services.sync.actions.viewModels.SyncViewModel;

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
  protected void onCreate(Bundle savedInstanceState) {
    absSyncViewModel=new ViewModelProvider(SyncActivity.this).get(SyncViewModel.class);
    super.onCreate(savedInstanceState);

    navController= Navigation.findNavController(this, R.id.navHostSync);
    navController.setGraph(R.navigation.nav_graph_sync);

    getLifecycle().addObserver((LifecycleEventObserver) (source, event) -> {
      if(event.equals(Lifecycle.Event.ON_RESUME)){
        if(navController.getCurrentDestination()==null){
          navController.navigate(R.id.syncFragment);
        }
      }
    });
  }

  @Override
  protected void onResume() {
    super.onResume();

    firstLaunch();
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

                  getSupportFragmentManager()
                      .beginTransaction()
                      .replace(R.id.sync_activity_view, new ServerSettingsFragment())
                      .addToBackStack(null)
                      .commit();
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
