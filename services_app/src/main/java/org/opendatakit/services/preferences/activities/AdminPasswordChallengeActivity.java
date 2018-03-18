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

package org.opendatakit.services.preferences.activities;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.utilities.ODKFileUtils;

/**
 * Challenge user with admin password; if successful, display admin-enabled
 * app properties.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class AdminPasswordChallengeActivity extends Activity {

   private static final int APP_PROPERTIES_RESULT_CODE = 1;

   @Override protected void onCreate(Bundle savedInstanceState) {
      super.onCreate(savedInstanceState);

      final String mAppName;
      String appName = this.getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
      if (appName == null || appName.length() == 0) {
         mAppName = ODKFileUtils.getOdkDefaultAppName();
      } else {
         mAppName = appName;
      }

      PropertiesSingleton mProps = CommonToolProperties.get(this, mAppName);

      final String adminPwd = mProps.getProperty(CommonToolProperties.KEY_ADMIN_PW);
      boolean mAdminConfigured = (adminPwd != null && adminPwd.length() != 0);

      if ( !mAdminConfigured ) {
         Intent intent = new Intent(this, AppPropertiesActivity.class );
         intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
         startActivityForResult(intent, APP_PROPERTIES_RESULT_CODE);
         return;
      }

      setContentView(R.layout.password_challenge_dialog_layout);

      Button positiveButton = findViewById(R.id.positive_button);
      positiveButton.setOnClickListener(new View.OnClickListener() {
         @Override
         public void onClick(View v) {
            EditText passwordEditText = findViewById(R.id.pwd_field);
            String pw = passwordEditText.getText().toString();

            if (adminPwd.equals(pw)) {
               Intent intent = new Intent(AdminPasswordChallengeActivity.this,
                   AppPropertiesActivity.class );
               intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
               intent.putExtra(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE, true);
               startActivityForResult(intent, APP_PROPERTIES_RESULT_CODE);
               return;
            } else {
               Toast.makeText(AdminPasswordChallengeActivity.this,
                   R.string.password_mismatch, Toast.LENGTH_SHORT).show();
            }
         }
      });

      Button negativeButton = findViewById(R.id.negative_button);
      negativeButton.setOnClickListener(new View.OnClickListener() {

         @Override
         public void onClick(View v) {
            setResult(RESULT_OK);
            finish();
         }

      });
   }

   @Override protected void onActivityResult(int requestCode, int resultCode, Intent data) {
      super.onActivityResult(requestCode, resultCode, data);
      if ( requestCode == APP_PROPERTIES_RESULT_CODE ) {
         setResult(resultCode);
         finish();
         return;
      }
   }
}
