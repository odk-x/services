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
import org.opendatakit.androidlibrary.R;

/**
 * Fragment-version of AlertDialog
 *
 * @author mitchellsundt@gmail.com
 */
public class DismissableOutcomeDialogFragment extends DialogFragment {
  private static final String OK_INVOKED = "ok_invoked";

  public static DismissableOutcomeDialogFragment newInstance(String title, String message, boolean isOk, String handlerTag) {
    DismissableOutcomeDialogFragment frag = new DismissableOutcomeDialogFragment();
    Bundle args = new Bundle();
    args.putString("title", title);
    args.putString("message", message);
    args.putBoolean("isOk", isOk);
    args.putString("handlerTag", handlerTag);
    frag.setArguments(args);
    return frag;
  }

  private boolean ok_invoked = false;

  public void setMessage(String message) {
    ((AlertDialog) this.getDialog()).setMessage(message);
  }

  @Override
  public Dialog onCreateDialog(Bundle savedInstanceState) {
    String title = getArguments().getString("title");
    String message = getArguments().getString("message");

    ok_invoked = savedInstanceState != null && savedInstanceState.getBoolean(OK_INVOKED);

    DialogInterface.OnClickListener okButtonListener = new DialogInterface.OnClickListener() {
      @Override public void onClick(DialogInterface dialog, int which) {
        ok_invoked = true;
        cancelOutcomeDialog();
      }
    };

    AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
    builder.setTitle(title).setMessage(message).setIcon(R.drawable.ic_info_outline_black_24dp)
        .setCancelable(false).setPositiveButton(R.string.ok, okButtonListener);

    AlertDialog dialog = builder.create();
    dialog.setCanceledOnTouchOutside(false);

    return dialog;
  }

  @Override public void onSaveInstanceState(Bundle outState) {
    super.onSaveInstanceState(outState);
    outState.putBoolean(OK_INVOKED, ok_invoked);
  }

  @Override public void onDismiss(DialogInterface dialog) {
    super.onDismiss(dialog);
    if ( !ok_invoked ) {
      Activity a = getActivity();
      if ( a != null ) {
        a.setResult(Activity.RESULT_CANCELED);
        a.finish();
      }
    }
  }

  private void cancelOutcomeDialog() {
    ok_invoked = true;
    this.getDialog().dismiss();
    if ( this.getArguments().getBoolean("isOK") ) {
      this.getActivity().setResult(Activity.RESULT_OK);
    } else {
      this.getActivity().setResult(Activity.RESULT_CANCELED);
    }

    String handlerTag = getArguments().getString("handlerTag");
    // Notify the syncFragment that this sync has completed
    // so as to release the details of the sync within the service.
    Fragment fragment = getFragmentManager().findFragmentByTag(handlerTag);
    if (fragment != null) {
      ISyncOutcomeHandler syncHandler = (ISyncOutcomeHandler) fragment;
      syncHandler.onSyncCompleted();
    }
  }
}
