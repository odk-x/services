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
import android.content.DialogInterface;
import android.os.Bundle;

import androidx.appcompat.app.AlertDialog;

import com.google.android.material.dialog.MaterialAlertDialogBuilder;
import com.google.android.material.dialog.MaterialDialogs;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;

/**
 * @author mitchellsundt@gmail.com
 */
public class ClearAppPropertiesActivity extends Activity {

   AlertDialog mDialog;

   @Override protected void onCreate(Bundle savedInstanceState) {
      super.onCreate(savedInstanceState);

      final String mAppName;
      String appName = this.getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
      if (appName == null || appName.length() == 0) {
         mAppName = ODKFileUtils.getOdkDefaultAppName();
      } else {
         mAppName = appName;
      }

      /**
       * New dialog styling
       * MaterialAlertDialogBuilder is standard for all ODK-X Apps
       * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
       * @params uncomment and comment the necessary codes if any error do occurs!
       */

      mDialog = new MaterialAlertDialogBuilder(ClearAppPropertiesActivity.this,R.style.OdkXAlertDialogStyle)
              .setTitle(R.string.reset_settings)
              .setMessage(R.string.confirm_reset_settings)
              .setCancelable(false)
              .setPositiveButton(R.string.ok, new DialogInterface.OnClickListener() {
                 @Override public void onClick(DialogInterface dialog, int which) {
                    // clear the device and secure properties for this appName
                    PropertiesSingleton mProps = CommonToolProperties.get(
                            ClearAppPropertiesActivity.this, mAppName);
                    mProps.clearSettings();

                    // clear the translations cache
                    LocalizationUtils.clearTranslations();

                    // clear the tables.init file that prevents re-reading and re-processing
                    // the assets/tables.init file (that preloads data from csv files)
                    File f = new File(ODKFileUtils.getTablesInitializationCompleteMarkerFile(mAppName));
                    if ( f.exists() ) {
                       f.delete();
                    }

                    // clear the initialization-complete marker files that prevent the
                    // initialization task from being executed.
                    ODKFileUtils.clearConfiguredToolFiles(mAppName);

                    setResult(RESULT_OK);
                    finish();
                    dialog.dismiss();
                 }
              })
              .setNegativeButton(R.string.cancel, new DialogInterface.OnClickListener() {
                 @Override public void onClick(DialogInterface dialog, int which) {
                    setResult(RESULT_CANCELED);
                    finish();
                    dialog.dismiss();
                 }
              }).create();
      mDialog.setCanceledOnTouchOutside(false);
      mDialog.show();
   }
}
