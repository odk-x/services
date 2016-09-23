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

package org.opendatakit.services.sync.activities;

import android.app.*;
import android.content.DialogInterface;
import android.os.Bundle;

/**
 * Fragment-version of Progress dialog
 * This is closable by hitting the back button
 *
 * @author mitchellsundt@gmail.com
 */
public class DismissableProgressDialogFragment extends DialogFragment {

  public static DismissableProgressDialogFragment newInstance(String title, String message) {
    DismissableProgressDialogFragment frag = new DismissableProgressDialogFragment();
    Bundle args = new Bundle();
    args.putString("title", title);
    args.putString("message", message);
    frag.setArguments(args);
    return frag;
  }

  public void setMessage(String message, int progress, int max) {
    ProgressDialog dlg = (ProgressDialog) this.getDialog();
    dlg.setMessage(message);
    if ( progress == -1 ) {
      dlg.setIndeterminate(true);
    } else {
      dlg.setIndeterminate(false);
      dlg.setMax(max);
      dlg.setProgress(progress);
    }
  }

  @Override
  public Dialog onCreateDialog(Bundle savedInstanceState) {
    String title = getArguments().getString("title");
    String message = getArguments().getString("message");

    ProgressDialog mProgressDialog = new ProgressDialog(getActivity());
    mProgressDialog.setTitle(title);
    mProgressDialog.setMessage(message);
    mProgressDialog.setIcon(android.R.drawable.ic_dialog_info);
    mProgressDialog.setIndeterminate(true);
    mProgressDialog.setCancelable(false);
    mProgressDialog.setCanceledOnTouchOutside(false);

    return mProgressDialog;
  }

  @Override public void onDismiss(DialogInterface dialog) {
    super.onDismiss(dialog);
    Activity a = getActivity();
    if ( a != null ) {
      a.setResult(Activity.RESULT_CANCELED);
      a.finish();
    }
  }

}
