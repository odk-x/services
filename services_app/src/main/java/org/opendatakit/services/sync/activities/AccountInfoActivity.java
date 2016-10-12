/*
 * Copyright 2011 Google Inc. All rights reserved.
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

package org.opendatakit.services.sync.activities;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.AccountManagerCallback;
import android.accounts.AccountManagerFuture;
import android.accounts.OperationCanceledException;
import android.app.Activity;
import android.app.AlertDialog;
import android.app.Dialog;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.widget.Toast;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;

/**
 * Activity to authenticate against an account and generate a token into the
 * shared preferences.
 * 
 * @author cswenson@google.com (Christopher Swenson) (original author)
 * @author the.dylan.price@gmail.com (modified by)
 */

public class AccountInfoActivity extends Activity {
  public static final String INTENT_EXTRAS_ACCOUNT = "account";

  private static final String TAG = AccountInfoActivity.class.getSimpleName();

  private final static int WAITING_ID = 1;
  private final static int CALLBACK_DIALOG_INTENT_ID = 2;
  private final static String authString = "oauth2:https://www.googleapis.com/auth/userinfo.email";

  private String appName;
  private AccountManagerFuture<Bundle> request = null;

  /**
   * Activity startup.
   */
  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    appName = getIntent().getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if (appName == null) {
      Log.e(TAG, IntentConsts.INTENT_KEY_APP_NAME + " not supplied on intent");
      setResult(Activity.RESULT_CANCELED);
      finish();
      return;
    }
  }

  /**
   * When we resume, try to get an auth token.
   */
  @Override
  protected void onResume() {
    super.onResume();
    Intent intent = getIntent();
    if (request == null) {
      AccountManager accountManager = AccountManager.get(getApplicationContext());
      Account account = (Account) intent.getExtras().get(INTENT_EXTRAS_ACCOUNT);
      request = accountManager.getAuthToken(account, authString, null, this,
          new AuthTokenCallback(), null);
      showDialog(WAITING_ID);
    } else if (request.isCancelled() || request.isDone()) {
      failedAuthToken();
    }
  }

  /**
   * Helper class to handle getting the auth token.
   */
  private class AuthTokenCallback implements AccountManagerCallback<Bundle> {
    @Override
    public void run(AccountManagerFuture<Bundle> result) {
      Bundle bundle;
      try {
        bundle = result.getResult();
        Intent intent = (Intent) bundle.get(AccountManager.KEY_INTENT);

        if (intent != null) {
          // We need to call the intent to get the token.
          startActivityForResult(intent, CALLBACK_DIALOG_INTENT_ID);
        } else {
          gotAuthToken(bundle);
        }
      } catch (final OperationCanceledException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        AccountInfoActivity.this.runOnUiThread(new Runnable() {

          @Override
          public void run() {
            Toast.makeText(AccountInfoActivity.this, "Authentication Cancelled: " + e.toString(),
                Toast.LENGTH_LONG).show();
            failedAuthToken();
          }
        });
      } catch (final Exception e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        AccountInfoActivity.this.runOnUiThread(new Runnable() {

          @Override
          public void run() {
            Toast.makeText(AccountInfoActivity.this, "Authentication Failed: " + e.toString(),
                Toast.LENGTH_LONG).show();
            failedAuthToken();
          }
        });
      }
    }
  }

  /**
   * If we failed to get an auth token.
   */
  protected void failedAuthToken() {
    dismissDialog(WAITING_ID);
    setResult(Activity.RESULT_CANCELED);
    finish();
  }

  /**
   * If we got one, store it in shared preferences.
   * 
   * @param bundle
   */
  protected void gotAuthToken(Bundle bundle) {
    // Set the authentication token and dismiss the dialog.
    String auth_token = bundle.getString(AccountManager.KEY_AUTHTOKEN);
    PropertiesSingleton props = CommonToolProperties.get(this, appName);
    props.setProperty(CommonToolProperties.KEY_AUTH, auth_token);
    props.writeProperties();
    WebLogger.getLogger(appName).i(TAG, "TOKEN" + auth_token);

    dismissDialog(WAITING_ID);
    setResult(Activity.RESULT_OK);
    finish();
  }

  /**
   * Let the user know we are waiting on the server to authenticate.
   */
  @Override
  protected Dialog onCreateDialog(int id) {
    Dialog dialog;
    switch (id) {
    case WAITING_ID:
      AlertDialog.Builder builder = new AlertDialog.Builder(this);
      builder.setMessage(getString(R.string.sync_waiting_auth)).setCancelable(false);
      AlertDialog alert = builder.create();
      dialog = alert;
      break;
    default:
      dialog = null;
    }
    return dialog;
  }

  @Override
  protected void onActivityResult(int requestCode, int resultCode, Intent data) {
    super.onActivityResult(requestCode, resultCode, data);
  }

}
