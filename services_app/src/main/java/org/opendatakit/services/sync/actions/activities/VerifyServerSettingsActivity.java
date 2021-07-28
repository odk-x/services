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
import androidx.navigation.ui.NavigationUI;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.fragments.VerifyServerSettingsFragment;
import org.opendatakit.services.sync.actions.viewModels.VerifyViewModel;

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
  protected void onCreate(Bundle savedInstanceState) {
    absSyncViewModel=new ViewModelProvider(VerifyServerSettingsActivity.this).get(VerifyViewModel.class);
    super.onCreate(savedInstanceState);

    navController=Navigation.findNavController(this, R.id.navHostSync);
    navController.setGraph(R.navigation.nav_graph_verify);

    getLifecycle().addObserver((LifecycleEventObserver) (source, event) -> {
      if(event.equals(Lifecycle.Event.ON_RESUME)){
        if(navController.getCurrentDestination()==null){
          navController.navigate(R.id.verifyServerSettingsFragment);
        }
      }
    });
  }
}
