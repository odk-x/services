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

package org.opendatakit.common.android.fragment;

import android.app.*;
import android.content.DialogInterface;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import org.opendatakit.androidlibrary.R;

/**
 * Fragment-version of Progress dialog
 *
 * @author mitchellsundt@gmail.com
 */
public class ProgressDialogFragment extends DialogFragment {

  public static interface CancelProgressDialog {
    public void cancelProgressDialog();
  }

  ;

  public static ProgressDialogFragment newInstance(int fragmentId, String title, String message) {
    ProgressDialogFragment frag = new ProgressDialogFragment();
    Bundle args = new Bundle();
    args.putInt("fragmentId", fragmentId);
    args.putString("title", title);
    args.putString("message", message);
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

    final Integer fragmentId = getArguments().getInt("fragmentId");
    FragmentManager mgr = getFragmentManager();
    Fragment f = mgr.findFragmentById(fragmentId);

    DialogInterface.OnClickListener loadingButtonListener = new DialogInterface.OnClickListener() {
      @Override
      public void onClick(DialogInterface dialog, int which) {
        FragmentManager mgr = getFragmentManager();
        Fragment f = mgr.findFragmentById(fragmentId);

        if (f != null && f instanceof CancelProgressDialog) {
          // user code should dismiss the dialog
          // since this is a cancellation action...
          // dialog.dismiss();
          ((CancelProgressDialog) f).cancelProgressDialog();
        }
      }
    };
    DialogInterface.OnShowListener showButtonListener = new DialogInterface.OnShowListener() {
      @Override
      public void onShow(DialogInterface dialog) {
        FragmentManager mgr = getFragmentManager();
        Fragment f = mgr.findFragmentById(fragmentId);

        if (f != null && f instanceof CancelProgressDialog) {
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
}
