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
package org.opendatakit.services.resolve.checkpoint;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
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
import org.opendatakit.services.resolve.task.CheckpointResolutionListTask;
import org.opendatakit.services.resolve.views.components.ResolveRowEntry;

import java.util.ArrayList;

/**
 * @author mitchellsundt@gmail.com
 */
public class CheckpointResolutionListFragment extends ListFragment implements LoaderManager
    .LoaderCallbacks<ArrayList<ResolveRowEntry>>, ResolutionListener {

  private static final String TAG = "CheckpointResolutionListFragment";
  private static final int RESOLVE_ROW_LOADER = 0x02;

  public static final String NAME = "CheckpointResolutionListFragment";
  public static final int ID = R.layout.checkpoint_resolver_chooser_list;

  private static final String PROGRESS_DIALOG_TAG = "progressCheckpoint";

  private static final String HAVE_RESOLVED_METADATA_CONFLICTS = "haveResolvedMetadataConflicts";

  private static CheckpointResolutionListTask checkpointResolutionListTask = null;

  private String mAppName;
  private String mTableId;
  private boolean mHaveResolvedMetadataConflicts = false;
  private ArrayAdapter<ResolveRowEntry> mAdapter;

  private ProgressDialogFragment progressDialog = null;

  private Button buttonTakeAllOldest;
  private Button buttonTakeAllNewest;

  @Override
  public void onSaveInstanceState(Bundle outState) {
    super.onSaveInstanceState(outState);

    outState.putBoolean(HAVE_RESOLVED_METADATA_CONFLICTS, mHaveResolvedMetadataConflicts);
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
  public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
    super.onCreateView(inflater, container, savedInstanceState);

    View view = inflater.inflate(ID, container, false);

    buttonTakeAllNewest = view.findViewById(R.id.take_all_newest);
    buttonTakeAllOldest = view.findViewById(R.id.take_all_oldest);

    if(buttonTakeAllNewest == null || buttonTakeAllOldest == null) {
      throw new RuntimeException("Android failed to locate references to buttons");
    }

    return view;
  }

  @Override
  public void onResume() {
    super.onResume();

    buttonTakeAllNewest.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        takeAllNewest();
      }
    });
    buttonTakeAllOldest.setOnClickListener(new View.OnClickListener() {
      @Override public void onClick(View v) {
        takeAllOldest();
      }
    });

    showProgressDialog();
  }

  private void takeAllNewest() {
    if ( mAdapter == null ) return;
    resolveConflictList(true);
  }

  private void takeAllOldest() {
    if ( mAdapter == null ) return;
    this.resolveConflictList(false);
  }

  @Override
  public void onListItemClick(ListView l, View v, int position, long id) {
    super.onListItemClick(l, v, position, id);

    ResolveRowEntry e = mAdapter.getItem(position);
    WebLogger.getLogger(mAppName).e(TAG,
        "[onListItemClick] clicked position: " + position + " rowId: " + e.rowId);
    if ( checkpointResolutionListTask == null ) {
      launchRowResolution(e);
    } else {
      Toast.makeText(getActivity(), R.string.resolver_already_active, Toast.LENGTH_LONG).show();
    }
  }

  private void launchRowResolution(ResolveRowEntry e) {
    Intent i = new Intent(getActivity(), CheckpointResolutionActivity.class);
    i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, mAppName);
    i.putExtra(IntentConsts.INTENT_KEY_TABLE_ID, mTableId);
    i.putExtra(IntentConsts.INTENT_KEY_INSTANCE_ID, e.rowId);
    this.startActivityForResult(i, CheckpointResolutionActivity.RESOLVE_ROW);
  }

  @Override
  public Loader<ArrayList<ResolveRowEntry>> onCreateLoader(int id, Bundle args) {
    // Now create and return a OdkResolveCheckpointRowLoader that will take care of
    // creating an ArrayList<ResolveRowEntry> for the data being displayed.
    return new OdkResolveCheckpointRowLoader(getActivity(), mAppName, mTableId,
        mHaveResolvedMetadataConflicts);
  }

  @Override
  public void onLoadFinished(Loader<ArrayList<ResolveRowEntry>> loader,
      ArrayList<ResolveRowEntry> resolveRowEntryArrayList) {
    // we have resolved the metadata conflicts -- no need to try this again
    mHaveResolvedMetadataConflicts = true;

    // this toast may be silently swallowed if there is only one remaining checkpoint in the table.
    int silentlyResolvedCheckpoints =
        ((OdkResolveCheckpointRowLoader) loader).getNumberRowsSilentlyReverted();

    if ( silentlyResolvedCheckpoints != 0 ) {
      if ( silentlyResolvedCheckpoints == 1 ) {
        Toast.makeText(getActivity(), getActivity().getString(R.string
                .silently_resolved_single_checkpoint), Toast.LENGTH_LONG).show();
      } else {
        Toast.makeText(getActivity(), getActivity().getString(R.string.silently_resolved_checkpoints,
            silentlyResolvedCheckpoints), Toast.LENGTH_LONG).show();
      }
    }

    // Swap the new cursor in. (The framework will take care of closing the
    // old cursor once we return.)
    mAdapter.clear();
    if ( resolveRowEntryArrayList.size() == 1 ) {
      launchRowResolution(resolveRowEntryArrayList.get(0));
      return;
    } else if ( resolveRowEntryArrayList.isEmpty() ){
      Toast.makeText(getActivity(), R.string.checkpoint_auto_apply_all, Toast.LENGTH_SHORT).show();
      getActivity().setResult(Activity.RESULT_OK);
      getActivity().finish();
      return;
    }
    mAdapter.addAll(resolveRowEntryArrayList);

    if ( getView() == null ) {
      throw new IllegalStateException("Unexpectedly found no view!");
    }

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
    if ( checkpointResolutionListTask != null ) {
      checkpointResolutionListTask.clearResolutionListener(null);
    }
    super.onDestroy();
  }

  private void resolveConflictList(boolean takeNewest) {
    if (mAdapter.getCount() > 0) {
      if (checkpointResolutionListTask == null) {
        checkpointResolutionListTask = new CheckpointResolutionListTask(getActivity(), takeNewest, mAppName);
        checkpointResolutionListTask.setTableId(mTableId);
        checkpointResolutionListTask.setResolveRowEntryAdapter(mAdapter);
        checkpointResolutionListTask.setResolutionListener(this);
        checkpointResolutionListTask.execute();
      } else {
        checkpointResolutionListTask.setResolutionListener(this);
        Toast.makeText(getActivity(), R.string.resolver_already_active, Toast.LENGTH_LONG)
            .show();
      }

      // show dialog box
      showProgressDialog();
    }
  }

  private void showProgressDialog() {
    if ( checkpointResolutionListTask != null ) {
      checkpointResolutionListTask.setResolutionListener(this);
      String progress = checkpointResolutionListTask.getProgress();

      if ( checkpointResolutionListTask.getResult() != null ) {
        resolutionComplete(checkpointResolutionListTask.getResult());
        return;
      }

      buttonTakeAllOldest.setEnabled(false);
      buttonTakeAllNewest.setEnabled(false);

      String title = getString(R.string.conflict_resolving_all);

      // try to retrieve the active dialog
      progressDialog = ProgressDialogFragment.eitherReuseOrCreateNew(
          PROGRESS_DIALOG_TAG, progressDialog, getParentFragmentManager(), title, progress, false);
    }
  }

  @Override public void resolutionProgress(String progress) {
    if ( progressDialog != null ) {
      progressDialog.setMessage(progress);
    }
  }

  @Override public void resolutionComplete(String result) {
    checkpointResolutionListTask = null;

    buttonTakeAllOldest.setEnabled(true);
    buttonTakeAllNewest.setEnabled(true);

    ProgressDialogFragment.dismissDialogs(PROGRESS_DIALOG_TAG, progressDialog,
            getParentFragmentManager());
    progressDialog = null;
    LoaderManager.getInstance(this).restartLoader(RESOLVE_ROW_LOADER, null, this);

    if ( result != null && result.length() != 0 ) {
      Toast.makeText(getActivity(), result, Toast.LENGTH_LONG).show();
    }
  }
}
