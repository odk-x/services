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

package org.opendatakit.services.preferences.fragments;

import android.app.Activity;
import android.arch.lifecycle.ViewModelProviders;
import android.content.Intent;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v4.view.ViewCompat;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.PreferenceViewModel;
import org.opendatakit.utilities.ODKFileUtils;

/**
 * Challenge user with admin password; if successful, display admin-enabled
 * app properties.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class AdminPasswordChallengeFragment extends Fragment {
   private String mAppName;
   private PreferenceViewModel preferenceViewModel;

   @Override
   public void onCreate(Bundle savedInstanceState) {
      super.onCreate(savedInstanceState);

      String appName = ((IAppAwareActivity) requireActivity()).getAppName();
      if (appName == null || appName.length() == 0) {
         mAppName = ODKFileUtils.getOdkDefaultAppName();
      } else {
         mAppName = appName;
      }
//      boolean mAdminConfigured = (adminPwd != null && adminPwd.length() != 0);

//      if ( !mAdminConfigured ) {
//         Intent intent = new Intent(this, AppPropertiesActivity.class );
//         intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
//         startActivityForResult(intent, APP_PROPERTIES_RESULT_CODE);
//         return;
//      }

      preferenceViewModel = ViewModelProviders
          .of(requireActivity())
          .get(PreferenceViewModel.class);
   }

   @Nullable
   @Override
   public View onCreateView(@NonNull LayoutInflater inflater,
                            @Nullable ViewGroup container,
                            @Nullable Bundle savedInstanceState) {
      return inflater
          .inflate(R.layout.password_challenge_dialog_layout, container, false);
   }

   @Override
   public void onViewCreated(@NonNull final View view, @Nullable Bundle savedInstanceState) {
      super.onViewCreated(view, savedInstanceState);

      Button positiveButton = ViewCompat.requireViewById(view, R.id.positive_button);
      positiveButton.setOnClickListener(new View.OnClickListener() {
         @Override
         public void onClick(View v) {
            String adminPwd = CommonToolProperties
                .get(requireContext(), mAppName)
                .getProperty(CommonToolProperties.KEY_ADMIN_PW);

            EditText passwordEditText = ViewCompat.requireViewById(view, R.id.pwd_field);
            String pw = passwordEditText.getText().toString();

            if (adminPwd.equals(pw)) {
               preferenceViewModel.setAdminMode(true);
               requireFragmentManager().popBackStack();
            } else {
               Toast.makeText(requireContext(),
                   R.string.password_mismatch, Toast.LENGTH_SHORT).show();
            }
         }
      });

      ViewCompat
          .<Button>requireViewById(view, R.id.negative_button)
          .setOnClickListener(new View.OnClickListener() {
             @Override
             public void onClick(View v) {
                requireFragmentManager().popBackStack();
             }
          });
   }
}
