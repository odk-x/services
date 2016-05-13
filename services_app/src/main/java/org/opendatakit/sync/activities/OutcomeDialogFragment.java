/*
 * Copyright (C) 2012-2013 University of Washington
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

package org.opendatakit.sync.activities;

import android.app.*;
import android.content.DialogInterface;
import android.os.Bundle;
import android.view.View;
import org.opendatakit.androidlibrary.R;

/**
 * Fragment-version of Progress dialog
 *
 * @author mitchellsundt@gmail.com
 */
public class OutcomeDialogFragment extends DialogFragment implements ICancelOutcomeDialog {

  public static OutcomeDialogFragment newInstance(String title, String message, boolean isOk) {
    OutcomeDialogFragment frag = new OutcomeDialogFragment();
    Bundle args = new Bundle();
    args.putString("title", title);
    args.putString("message", message);
    args.putBoolean("isOk", isOk);
    frag.setArguments(args);
    return frag;
  }

  public void setMessage(String message) {
    ((ProgressDialog) this.getDialog()).setMessage(message);
  }

  @Override
  public Dialog onCreateDialog(Bundle savedInstanceState) {
    String title = getArguments().getString("title");
    String message = getArguments().getString("message");

    DialogInterface.OnClickListener loadingButtonListener = new DialogInterface.OnClickListener() {
      @Override
      public void onClick(DialogInterface dialog, int which) {
        Fragment f = OutcomeDialogFragment.this;

        if (f != null && f instanceof ICancelOutcomeDialog) {
          // user code should dismiss the dialog
          // since this is a cancellation action...
          // dialog.dismiss();
          ((ICancelOutcomeDialog) f).cancelOutcomeDialog();
        }
      }
    };
    DialogInterface.OnShowListener showButtonListener = new DialogInterface.OnShowListener() {
      @Override
      public void onShow(DialogInterface dialog) {
        Fragment f = OutcomeDialogFragment.this;

        if (f != null && f instanceof ICancelOutcomeDialog) {
          ((ProgressDialog) dialog).getButton(DialogInterface.BUTTON_POSITIVE)
              .setVisibility(View.VISIBLE);
        } else {
          ((ProgressDialog) dialog).getButton(DialogInterface.BUTTON_POSITIVE)
              .setVisibility(View.GONE);
        }

      }
    };

    ProgressDialog mProgressDialog = new ProgressDialog(getActivity());
    mProgressDialog.setTitle(title);
    mProgressDialog.setMessage(message);
    mProgressDialog.setIcon(android.R.drawable.ic_dialog_info);
    mProgressDialog.setIndeterminate(true);
    mProgressDialog.setCancelable(false);
    mProgressDialog.setCanceledOnTouchOutside(false);
    mProgressDialog.setButton(DialogInterface.BUTTON_POSITIVE, getString(R.string.cancel),
        loadingButtonListener);
    mProgressDialog.setOnShowListener(showButtonListener);

    return mProgressDialog;
  }

  @Override public void cancelOutcomeDialog() {
    ((ProgressDialog) this.getDialog()).dismiss();
    if ( this.getArguments().getBoolean("isOK") ) {
      this.getActivity().setResult(Activity.RESULT_OK);
    } else {
      this.getActivity().setResult(Activity.RESULT_CANCELED);
    }
    this.getActivity().finish();
  }
}
