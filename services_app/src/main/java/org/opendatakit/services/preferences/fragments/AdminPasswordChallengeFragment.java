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

import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.core.view.ViewCompat;
import androidx.fragment.app.Fragment;
import androidx.lifecycle.ViewModelProvider;

import com.google.android.material.textfield.TextInputEditText;

import org.opendatakit.activities.IAppAwareActivity;
import org.opendatakit.properties.CommonToolProperties;
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

      preferenceViewModel = new ViewModelProvider(requireActivity())
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

            TextInputEditText passwordEditText = ViewCompat.requireViewById(view, R.id.pwd_field);
            String pw = passwordEditText.getText().toString();

            if (adminPwd.equals(pw)) {
               preferenceViewModel.setAdminMode(true);
               getParentFragmentManager().popBackStack();
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
                getParentFragmentManager().popBackStack();
             }
          });
   }
}
