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

import androidx.lifecycle.ViewModelProvider;

import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.viewModels.VerifyViewModel;

/**
 * An activity for verifying the server setings and
 * updating the configured user's permissions.handling server conflicts.
 * If an IntentConsts.INTENT_KEY_INSTANCE_ID is provided,
 * opens the row resolution fragment. Otherwise, opens
 * the list resolution fragment.
 *
 * @author mitchellsundt@gmail.com
 */
public class VerifyServerSettingsActivity extends AbsSyncBaseActivity {

    @Override
    void initializeViewModelAndNavController() {
        absSyncViewModel = new ViewModelProvider(VerifyServerSettingsActivity.this).get(VerifyViewModel.class);
        navController.setGraph(R.navigation.nav_graph_verify);
    }

    @Override
    void navigateToHomeFragment() {
        navController.navigate(R.id.verifyServerSettingsFragment);
    }

    @Override
    void navigateToAboutFragment() {
        navController.navigate(R.id.aboutMenuFragmentV);
    }

    @Override
    void navigateToUpdateServerSettings() {
        navController.navigate(R.id.updateServerSettingsFragmentV);
    }

    @Override
    boolean isNotLoginActivity() {
        return true;
    }

    @Override
    boolean isCurrentDestinationAboutFragment() {
        if (navController.getCurrentDestination() == null)
            return false;
        return navController.getCurrentDestination().getId() == R.id.aboutMenuFragmentV;
    }

    @Override
    boolean isCurrentDestinationUpdateServerSettings() {
        if (navController.getCurrentDestination() == null)
            return false;
        return navController.getCurrentDestination().getId() == R.id.updateServerSettingsFragmentV;
    }

}
