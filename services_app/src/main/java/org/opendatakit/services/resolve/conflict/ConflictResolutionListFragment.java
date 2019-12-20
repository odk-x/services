/*
 * Copyright (C) 2015 University of Washington
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
package org.opendatakit.services.resolve.conflict;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.os.Handler;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ListView;
import android.widget.Toast;

import androidx.fragment.app.ListFragment;
import androidx.loader.app.LoaderManager;
import androidx.loader.content.Loader;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.fragment.ProgressDialogFragment;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.services.resolve.listener.ResolutionListener;
import org.opendatakit.services.resolve.task.ConflictResolutionListTask;
import org.opendatakit.services.resolve.views.components.ResolveRowEntry;

import java.util.ArrayList;

/**
 * @author mitchellsundt@gmail.com
 */
public class ConflictResolutionListFragment extends ListFragment implements LoaderManager
    .LoaderCallbacks<ArrayList<ResolveRowEntry>>, ResolutionListener {

  private static final String TAG = "ConflictResolutionListFragment";
  private static final int RESOLVE_ROW_LOADER = 0x02;

  public static final String NAME = "ConflictResolutionListFragment";
  public static final int ID = R.layout.conflict_resolver_chooser_list;

  private static final String PROGRESS_DIALOG_TAG = "progressConflict";

  private static final String HAVE_RESOLVED_METADATA_CONFLICTS = "haveResolvedMetadataConflicts";

  private enum DialogState {
    Progress, Alert, None
  }

  private static ConflictResolutionListTask conflictResolutionListTask = null;

  private String mAppName;
  private String mTableId;
  private boolean mHaveResolvedMetadataConflicts = false;
  private ArrayAdapter<ResolveRowEntry> mAdapter;

  private Handler handler = new Handler();
  private ProgressDialogFragment progressDialog = null;

  private Button buttonTakeAllServer;
  private Button buttonTakeAllLocal;

  @Override
  public void onSaveInstanceState(Bundle outState) {
    super.onSaveInstanceState(outState);

    outState.putBoolean(HAVE_RESOLVED_METADATA_CONFLICTS, mHaveResolvedMetadataConflicts);
  }


  @Override
  public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
    super.onCreateView(inflater, container, savedInstanceState);

    View view = inflater.inflate(ID, container, false);
    buttonTakeAllServer = view.findViewById(R.id.take_all_server);
    buttonTakeAllLocal = view.findViewById(R.id.take_all_local);

    if(buttonTakeAllServer == null || buttonTakeAllLocal == null) {
      throw new RuntimeException("Android failed to locate references to buttons");
    }

    return view;
  }

  @Override
  public void onActivityCreated(Bundle savedInstanceState) {
    super.onActivityCreated(savedInstanceState);

    Intent incomingIntent = getActivity().getIntent();
    mAppName = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if (mAppName == null || mAppName.length() == 0) {
      getActivity().setResult(Activity.RESULT_CANCELED);
      getActivity().finish();
      return;
    }

    mTableId = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_TABLE_ID);
    if (mTableId == null || mTableId.length() == 0) {
      getActivity().setResult(Activity.RESULT_CANCELED);
      getActivity().finish();
      return;
    }

    mHaveResolvedMetadataConflicts = savedInstanceState != null &&
        (savedInstanceState.containsKey(HAVE_RESOLVED_METADATA_CONFLICTS) ?
            savedInstanceState.getBoolean(HAVE_RESOLVED_METADATA_CONFLICTS) :
            false);

    // render total instance view
    mAdapter = new ArrayAdapter<ResolveRowEntry>(getActivity(), android.R.layout.simple_list_item_1);
    setListAdapter(mAdapter);

    LoaderManager.getInstance(this).initLoader(RESOLVE_ROW_LOADER, null, this);
  }

  @Override
  public void onResume() {
    super.onResume();

    buttonTakeAllLocal.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        takeAllLocal();
      }
    });
    buttonTakeAllServer.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        takeAllServer();
      }
    });

    showProgressDialog();
  }

  private void takeAllLocal() {
    if ( mAdapter == null ) return;
    resolveConflictList(true);
  }

  private void takeAllServer() {
    if ( mAdapter == null ) return;
    resolveConflictList(false);
  }

  @Override
  public void onListItemClick(ListView l, View v, int position, long id) {
    super.onListItemClick(l, v, position, id);

    ResolveRowEntry e = mAdapter.getItem(position);
    WebLogger.getLogger(mAppName).e(TAG,
        "[onListItemClick] clicked position: " + position + " rowId: " + e.rowId);
    if ( conflictResolutionListTask == null ) {
      launchRowResolution(e);
    } else {
      Toast.makeText(getActivity(), R.string.resolver_already_active, Toast.LENGTH_LONG).show();
    }
  }

  private void launchRowResolution(ResolveRowEntry e) {
    Intent i = new Intent(getActivity(), ConflictResolutionActivity.class);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
    i.putExtra(IntentConsts.INTENT_KEY_TABLE_ID, mTableId);
    i.putExtra(IntentConsts.INTENT_KEY_INSTANCE_ID, e.rowId);
    this.startActivityForResult(i, ConflictResolutionActivity.RESOLVE_ROW);
  }

  @Override
  public Loader<ArrayList<ResolveRowEntry>> onCreateLoader(int id, Bundle args) {
    // Now create and return a OdkResolveCheckpointRowLoader that will take care of
    // creating an ArrayList<ResolveRowEntry> for the data being displayed.
    return new OdkResolveConflictRowLoader(getActivity(), mAppName, mTableId,
        mHaveResolvedMetadataConflicts);
  }

  @Override
  public void onLoadFinished(Loader<ArrayList<ResolveRowEntry>> loader,
      ArrayList<ResolveRowEntry> resolveRowEntryArrayList) {
    // we have resolved the metadata conflicts -- no need to try this again
    mHaveResolvedMetadataConflicts = true;

    // this toast may be silently swallowed if there is only one remaining checkpoint in the table.
    int silentlyResolvedConflicts =
        ((OdkResolveConflictRowLoader) loader).getNumberRowsSilentlyResolved();

    if ( silentlyResolvedConflicts != 0 ) {
      if ( silentlyResolvedConflicts == 1 ) {
        Toast.makeText(getActivity(), getActivity().getString(R.string
            .silently_resolved_single_conflict), Toast.LENGTH_LONG).show();
      } else {
        Toast.makeText(getActivity(), getActivity().getString(R.string.silently_resolved_conflicts,
            silentlyResolvedConflicts), Toast.LENGTH_LONG).show();
      }
    }

    // Swap the new cursor in. (The framework will take care of closing the
    // old cursor once we return.)
    mAdapter.clear();
    if ( resolveRowEntryArrayList.size() == 1 ) {
      launchRowResolution(resolveRowEntryArrayList.get(0));
      return;
    } else if ( resolveRowEntryArrayList.isEmpty() ){
      Toast.makeText(getActivity(), R.string.conflict_auto_apply_all, Toast.LENGTH_SHORT).show();
      getActivity().setResult(Activity.RESULT_OK);
      getActivity().finish();
      return;
    }
    mAdapter.addAll(resolveRowEntryArrayList);

    if ( getView() == null ) {
      throw new IllegalStateException("Unexpectedly found no view!");
    }


    // TODO: is this needed, or does it trigger an unnecessary refresh?
    mAdapter.notifyDataSetChanged();
  }

  @Override
  public void onLoaderReset(Loader<ArrayList<ResolveRowEntry>> loader) {
    // This is called when the last ArrayList<ResolveRowEntry> provided to onLoadFinished()
    // above is about to be released. We need to make sure we are no
    // longer using it.
    mAdapter.clear();
  }

  @Override public void onDestroy() {
    if ( conflictResolutionListTask != null ) {
      conflictResolutionListTask.clearResolutionListener(null);
    }
    super.onDestroy();
  }

  private void resolveConflictList(boolean takeLocal) {
    if (mAdapter.getCount() > 0) {
      if (conflictResolutionListTask == null) {
        conflictResolutionListTask = new ConflictResolutionListTask(getActivity(), takeLocal, mAppName);
        conflictResolutionListTask.setTableId(mTableId);
        conflictResolutionListTask.setResolveRowEntryAdapter(mAdapter);
        conflictResolutionListTask.setResolutionListener(this);
        conflictResolutionListTask.execute();
      } else {
        conflictResolutionListTask.setResolutionListener(this);
        Toast.makeText(getActivity(), R.string.resolver_already_active, Toast.LENGTH_LONG)
            .show();
      }

      // show dialog box
      showProgressDialog();
    }
  }

  private void showProgressDialog() {
    if ( conflictResolutionListTask != null ) {
      conflictResolutionListTask.setResolutionListener(this);
      String progress = conflictResolutionListTask.getProgress();

      if ( conflictResolutionListTask.getResult() != null ) {
        resolutionComplete(conflictResolutionListTask.getResult());
        return;
      }

      buttonTakeAllLocal.setEnabled(false);
      buttonTakeAllServer.setEnabled(false);

      String title = getString(R.string.conflict_resolving_all);

      // try to retrieve the active dialog
      progressDialog = ProgressDialogFragment.eitherReuseOrCreateNew(
          PROGRESS_DIALOG_TAG, progressDialog,getParentFragmentManager(), title, progress, false);

    }
  }

  @Override public void resolutionProgress(String progress) {
    if ( progressDialog != null ) {
      progressDialog.setMessage(progress);
    }
  }

  @Override public void resolutionComplete(String result) {
    conflictResolutionListTask = null;

    buttonTakeAllLocal.setEnabled(true);
    buttonTakeAllServer.setEnabled(true);

    ProgressDialogFragment.dismissDialogs(PROGRESS_DIALOG_TAG, progressDialog,
            getParentFragmentManager());
    progressDialog = null;
    LoaderManager.getInstance(this).restartLoader(RESOLVE_ROW_LOADER, null, this);

    if ( result != null && result.length() != 0 ) {
      Toast.makeText(getActivity(), result, Toast.LENGTH_LONG).show();
    }
  }
}
