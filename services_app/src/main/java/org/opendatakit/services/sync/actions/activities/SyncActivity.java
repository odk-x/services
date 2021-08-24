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

import androidx.lifecycle.ViewModelProvider;

import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.viewModels.SyncViewModel;

/**
 * An activity for syncing the local content with the server.
 *
 * @author mitchellsundt@gmail.com
 */
public class SyncActivity extends AbsSyncBaseActivity {

    @Override
    void initializeViewModelAndNavController() {
        absSyncViewModel = new ViewModelProvider(SyncActivity.this).get(SyncViewModel.class);
        navController.setGraph(R.navigation.nav_graph_sync);
    }

    @Override
    void navigateToHomeFragment() {
        navController.navigate(R.id.syncFragment);
    }

    @Override
    void navigateToAboutFragment() {
        navController.navigate(R.id.aboutMenuFragmentS);
    }

    @Override
    void navigateToUpdateServerSettings() {
        navController.navigate(R.id.updateServerSettingsFragmentS);
    }

    @Override
    boolean isNotLoginActivity() {
        return true;
    }

    @Override
    boolean isCurrentDestinationAboutFragment() {
        if (navController.getCurrentDestination() == null)
            return false;
        return navController.getCurrentDestination().getId() == R.id.aboutMenuFragmentS;
    }

    @Override
    boolean isCurrentDestinationUpdateServerSettings() {
        if (navController.getCurrentDestination() == null)
            return false;
        return navController.getCurrentDestination().getId() == R.id.updateServerSettingsFragmentS;
    }
}
