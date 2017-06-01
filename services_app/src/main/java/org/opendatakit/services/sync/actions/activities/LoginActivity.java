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

import android.app.Fragment;
import android.app.FragmentManager;
import android.app.FragmentTransaction;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.fragments.LoginFragment;

/**
 * An activity to provide a simplified login user interface.
 *
 * Created by jbeorse on 5/30/17.
 */

public class LoginActivity extends SyncBaseActivity {

   private static final String TAG = LoginActivity.class.getSimpleName();

   @Override
   protected void onResume() {
      super.onResume();

      WebLogger.getLogger(getAppName()).i(TAG, "[onResume] getting LoginFragment");

      FragmentManager mgr = getFragmentManager();
      String newFragmentName;
      Fragment newFragment;

      // we want the list fragment
      newFragmentName = LoginFragment.NAME;
      newFragment = mgr.findFragmentByTag(newFragmentName);
      if ( newFragment == null ) {
         newFragment = new LoginFragment();
         WebLogger.getLogger(getAppName()).i(TAG, "[onResume] creating new LoginFragment");

         FragmentTransaction trans = mgr.beginTransaction();
         trans.replace(R.id.sync_activity_view, newFragment, newFragmentName);
         WebLogger.getLogger(getAppName()).i(TAG, "[onResume] replacing fragment with id " + newFragment.getId());
         trans.commit();
      }
   }

}
