/*
 * Copyright (C) 2017 University of Washington
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

import android.content.Context;
import android.graphics.Rect;
import android.view.MotionEvent;
import android.view.View;
import android.view.inputmethod.InputMethodManager;
import android.widget.EditText;

import androidx.lifecycle.ViewModelProvider;

import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.viewModels.LoginViewModel;

/**
 * An activity to provide a simplified login user interface.
 * <p>
 * Created by jbeorse on 5/30/17.
 */

public class LoginActivity extends AbsSyncBaseActivity {

    @Override
    void initializeViewModelAndNavController() {
        absSyncViewModel = new ViewModelProvider(LoginActivity.this).get(LoginViewModel.class);
        navController.setGraph(R.navigation.nav_graph_login);
    }

    @Override
    void navigateToHomeFragment() {
        navController.navigate(R.id.chooseSignInTypeFragment);
    }

    @Override
    void navigateToAboutFragment() {
        navController.navigate(R.id.aboutMenuFragmentL);
    }

    @Override
    void navigateToUpdateServerSettings() {
        navController.navigate(R.id.updateServerSettingsFragmentL);
    }

    @Override
    boolean isNotLoginActivity() {
        return false;
    }

    @Override
    boolean isCurrentDestinationAboutFragment() {
        if (navController.getCurrentDestination() == null)
            return false;
        return navController.getCurrentDestination().getId() == R.id.aboutMenuFragmentL;
    }

    @Override
    boolean isCurrentDestinationUpdateServerSettings() {
        if (navController.getCurrentDestination() == null)
            return false;
        return navController.getCurrentDestination().getId() == R.id.updateServerSettingsFragmentL;
    }

    @Override
    public boolean dispatchTouchEvent(MotionEvent event) {
        if (event.getAction() == MotionEvent.ACTION_DOWN) {
            View v = getCurrentFocus();
            if (v instanceof EditText) {
                Rect outRect = new Rect();
                v.getGlobalVisibleRect(outRect);
                if (!outRect.contains((int) event.getRawX(), (int) event.getRawY())) {
                    v.clearFocus();
                    InputMethodManager imm = (InputMethodManager) getSystemService(Context.INPUT_METHOD_SERVICE);
                    imm.hideSoftInputFromWindow(v.getWindowToken(), 0);
                }
            }
        }
        return super.dispatchTouchEvent(event);
    }

}
