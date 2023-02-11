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
import android.app.AlertDialog;
import android.content.ContentValues;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.fragment.app.ListFragment;
import androidx.loader.app.LoaderManager;
import androidx.loader.content.Loader;

import com.google.android.material.dialog.MaterialAlertDialogBuilder;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.services.resolve.views.components.ConflictResolutionColumnListAdapter;
import org.opendatakit.services.resolve.views.components.Resolution;
import org.opendatakit.services.resolve.views.components.ResolveActionList;
import org.opendatakit.services.resolve.views.components.ResolveActionType;
import org.opendatakit.services.utilities.ActiveUserAndLocale;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author mitchellsundt@gmail.com
 */
public class CheckpointResolutionRowFragment extends ListFragment implements
    ConflictResolutionColumnListAdapter.UICallbacks,  LoaderManager
    .LoaderCallbacks<ResolveActionList>  {

  private static final String TAG = "CheckpointResolutionRowFragment";
  private static final int RESOLVE_FIELD_LOADER = 0x03;

  public static final String NAME = "CheckpointResolutionRowFragment";
  public static final int ID = R.layout.checkpoint_resolver_field_list;

  private static final String BUNDLE_KEY_SHOWING_LOCAL_WITH_DELTAS_DIALOG =
      "showingLocalWithDeltasDialog";
  private static final String BUNDLE_KEY_SHOWING_LOCAL_DIALOG = "showingLocalDialog";
  private static final String BUNDLE_KEY_SHOWING_SERVER_DIALOG = "showingServerDialog";
  private static final String BUNDLE_KEY_VALUE_KEYS = "valueValueKeys";
  private static final String BUNDLE_KEY_CHOSEN_VALUES = "chosenValues";
  private static final String BUNDLE_KEY_RESOLUTION_KEYS = "resolutionKeys";
  private static final String BUNDLE_KEY_RESOLUTION_VALUES = "resolutionValues";

  private ConflictResolutionColumnListAdapter mAdapter;

  private String mAppName;
  private String mTableId;
  private String mRowId;

  private Button mButtonTakeOldest;
  private Button mButtonTakeNewest;
  private Button mButtonTakeNewestWithDeltas;

  /**
   * The message to the user as to why they're getting extra options. Will be
   * either thing to the effect of "someone has deleted something you've
   * changed", or "you've deleted something someone has changed". They'll then
   * have to choose either to delete or to go ahead and actually restore and
   * then resolve it.
   */
  private TextView mTextViewCheckpointOverviewMessage;

  private boolean mIsShowingTakeNewestWithDeltasDialog;
  private boolean mIsShowingTakeNewestDialog;
  private boolean mIsShowingTakeOldestDialog;

  private Map<String, String> mChosenValuesMap = new TreeMap<String, String>();
  private Map<String, Resolution> mUserResolutions = new TreeMap<String, Resolution>();

  private class DiscardOlderValuesAndMarkNewestAsIncompleteRowClickListener implements
      View.OnClickListener {

    @Override
    public void onClick(View v) {
      AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());
      builder.setMessage(getString(R.string.checkpoint_take_newest_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          mIsShowingTakeNewestDialog = false;
          dialog.dismiss();
          OdkConnectionInterface db = null;

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            ODKDatabaseImplUtils.get().saveAsIncompleteMostRecentCheckpointRowWithId(db, mTableId,
                mRowId);
            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("OdkResolveCheckpointRowLoader",
                mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
            WebLogger.getLogger(mAppName).printStackTrace(e);
            Toast.makeText(getActivity(), "database access failure", Toast.LENGTH_LONG).show();
            getActivity().setResult(Activity.RESULT_CANCELED);
          } finally {
            if (db != null) {
              // release the reference...
              // this does not necessarily close the db handle
              // or terminate any pending transaction
              db.releaseReference();
            }
          }
          getActivity().finish();
          WebLogger.getLogger(mAppName).d(TAG, "update to checkpointed version");
        }
      });
      builder.setCancelable(true);
      builder.setNegativeButton(getString(R.string.cancel), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          dialog.cancel();
        }
      });
      builder.setOnCancelListener(new DialogInterface.OnCancelListener() {

        @Override
        public void onCancel(DialogInterface dialog) {
          mIsShowingTakeNewestDialog = false;
        }
      });
      mIsShowingTakeNewestDialog = true;
      builder.create().show();
    }

  }

  private class ApplyDeltasAndMarkNewestAsIncompleteRowClickListener implements
      View.OnClickListener {

    @Override
    public void onClick(View v) {

      /**
       * New dialog styling
       * MaterialAlertDialogBuilder is standard for all ODK-X Apps
       * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
       * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
       */

      MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(getActivity(),R.style.OdkXAlertDialogStyle);
      builder.setMessage(getString(R.string.checkpoint_take_newest_with_deltas_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          mIsShowingTakeNewestWithDeltasDialog = false;
          dialog.dismiss();
          OdkConnectionInterface db = null;

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          ContentValues values = new ContentValues();
          for (Map.Entry<String, Resolution> entry : mUserResolutions.entrySet() ) {
            if ( entry.getValue() == Resolution.SERVER ) {
              values.put( entry.getKey(), mChosenValuesMap.get(entry.getKey()));
            }
          }

          ActiveUserAndLocale aul =
              ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            // create a new checkpoint with the revisions
            OrderedColumns orderedColumns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db,
                mTableId);
            ODKDatabaseImplUtils.get().insertCheckpointRowWithId(db, mTableId, orderedColumns,
                values, mRowId, aul.activeUser, aul.rolesList, aul.locale);

            // and save that checkpoint as incomplete
            ODKDatabaseImplUtils.get().saveAsIncompleteMostRecentCheckpointRowWithId(db, mTableId,
                mRowId);
            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("OdkResolveCheckpointRowLoader",
                mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
            WebLogger.getLogger(mAppName).printStackTrace(e);
            Toast.makeText(getActivity(), "database access failure", Toast.LENGTH_LONG).show();
            getActivity().setResult(Activity.RESULT_CANCELED);
          } finally {
            if (db != null) {
              // release the reference...
              // this does not necessarily close the db handle
              // or terminate any pending transaction
              db.releaseReference();
            }
          }
          getActivity().finish();
          WebLogger.getLogger(mAppName).d(TAG, "update to checkpointed version with deltas");
        }
      });
      builder.setCancelable(true);
      builder.setNegativeButton(getString(R.string.cancel), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          dialog.cancel();
        }
      });
      builder.setOnCancelListener(new DialogInterface.OnCancelListener() {

        @Override
        public void onCancel(DialogInterface dialog) {
          mIsShowingTakeNewestWithDeltasDialog = false;
        }
      });
      mIsShowingTakeNewestWithDeltasDialog = true;
      builder.create().show();
    }

  }

  private class DiscardAllValuesAndDeleteRowClickListener implements View.OnClickListener {

    @Override
    public void onClick(View v) {
      // We should do a popup.

      /**
       * New dialog styling
       * MaterialAlertDialogBuilder is standard for all ODK-X Apps
       * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
       * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
       */

      MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(getActivity(),R.style.OdkXAlertDialogStyle);
      builder.setMessage(getString(R.string.checkpoint_delete_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          mIsShowingTakeOldestDialog = false;
          dialog.dismiss();
          OdkConnectionInterface db = null;

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          ActiveUserAndLocale aul =
              ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, mTableId,
                mRowId, aul.activeUser, aul.rolesList );

            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("OdkResolveCheckpointRowLoader",
                mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
            WebLogger.getLogger(mAppName).printStackTrace(e);
            Toast.makeText(getActivity(), "database access failure", Toast.LENGTH_LONG).show();
            getActivity().setResult(Activity.RESULT_CANCELED);
          } finally {
            if (db != null) {
              // release the reference...
              // this does not necessarily close the db handle
              // or terminate any pending transaction
              db.releaseReference();
            }
          }
          getActivity().finish();
          WebLogger.getLogger(mAppName).d(TAG,
              "delete row (no prior save-as incomplete or complete)");
        }
      });
      builder.setCancelable(true);
      builder.setNegativeButton(getString(R.string.cancel), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          dialog.cancel();
        }
      });
      builder.setOnCancelListener(new DialogInterface.OnCancelListener() {

        @Override
        public void onCancel(DialogInterface dialog) {
          mIsShowingTakeOldestDialog = false;
          dialog.dismiss();
        }
      });
      mIsShowingTakeOldestDialog = true;
      builder.create().show();
    }
  }

  private class DiscardNewerValuesAndRetainOldestInOriginalStateRowClickListener implements
      View.OnClickListener {

    @Override
    public void onClick(View v) {

      /**
       * New dialog styling
       * MaterialAlertDialogBuilder is standard for all ODK-X Apps
       * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
       * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
       */

      MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(getActivity());
      builder.setMessage(getString(R.string.checkpoint_take_oldest_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          mIsShowingTakeOldestDialog = false;
          dialog.dismiss();
          OdkConnectionInterface db = null;

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          ActiveUserAndLocale aul =
              ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, mTableId,
                mRowId, aul.activeUser, aul.rolesList );
            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("OdkResolveCheckpointRowLoader",
                mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
            WebLogger.getLogger(mAppName).printStackTrace(e);
            Toast.makeText(getActivity(), "database access failure", Toast.LENGTH_LONG).show();
            getActivity().setResult(Activity.RESULT_CANCELED);
          } finally {
            if (db != null) {
              // release the reference...
              // this does not necessarily close the db handle
              // or terminate any pending transaction
              db.releaseReference();
            }
          }
          getActivity().finish();
          WebLogger.getLogger(mAppName).d(TAG,
              "delete the checkpoint and restore to older version");
        }
      });
      builder.setCancelable(true);
      builder.setNegativeButton(getString(R.string.cancel), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          dialog.cancel();
        }
      });
      builder.setOnCancelListener(new DialogInterface.OnCancelListener() {

        @Override
        public void onCancel(DialogInterface dialog) {
          mIsShowingTakeOldestDialog = false;
          dialog.dismiss();
        }
      });
      mIsShowingTakeOldestDialog = true;
      builder.create().show();
    }
  }

  /**
   * Sets the Activity result to OK (if successful) or CANCELLED (if an error)
   */
  private void discardAllCheckpointChanges() {
    OdkConnectionInterface db = null;

    DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

    ActiveUserAndLocale aul =
        ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(mAppName, dbHandleName);

      ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, mTableId, mRowId,
          aul.activeUser, aul.rolesList);
      getActivity().setResult(Activity.RESULT_OK);
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(mAppName).e("OdkResolveCheckpointRowLoader",
          mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(mAppName).printStackTrace(e);
      Toast.makeText(getActivity(), "database access failure", Toast.LENGTH_LONG).show();
      getActivity().setResult(Activity.RESULT_CANCELED);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
    Toast.makeText(getActivity(), R.string.checkpoint_auto_apply, Toast.LENGTH_LONG).show();
  }

  @Override
  public void onSaveInstanceState(Bundle outState) {
    super.onSaveInstanceState(outState);
    outState.putBoolean(BUNDLE_KEY_SHOWING_LOCAL_WITH_DELTAS_DIALOG, mIsShowingTakeNewestWithDeltasDialog);
    outState.putBoolean(BUNDLE_KEY_SHOWING_LOCAL_DIALOG, mIsShowingTakeNewestDialog);
    outState.putBoolean(BUNDLE_KEY_SHOWING_SERVER_DIALOG, mIsShowingTakeOldestDialog);
    // We also need to save the chosen values and decisions so
    // that we don't lose information if they rotate the screen.
    if (mChosenValuesMap.size() != mUserResolutions.size()) {
      WebLogger.getLogger(mAppName).e(
          TAG,
          "[onSaveInstanceState] chosen values and user resolutions"
              + " are not the same size. This should be impossible, so not " + "saving state.");
      return;
    }
    String[] valueKeys = new String[mChosenValuesMap.size()];
    String[] chosenValues = new String[mChosenValuesMap.size()];
    String[] resolutionKeys = new String[mUserResolutions.size()];
    String[] resolutionValues = new String[mUserResolutions.size()];
    int i = 0;
    for (Map.Entry<String, String> valueEntry : mChosenValuesMap.entrySet()) {
      valueKeys[i] = valueEntry.getKey();
      chosenValues[i] = valueEntry.getValue();
      ++i;
    }
    i = 0;
    for (Map.Entry<String, Resolution> resolutionEntry : mUserResolutions.entrySet()) {
      resolutionKeys[i] = resolutionEntry.getKey();
      resolutionValues[i] = resolutionEntry.getValue().name();
      ++i;
    }
    outState.putStringArray(BUNDLE_KEY_VALUE_KEYS, valueKeys);
    outState.putStringArray(BUNDLE_KEY_CHOSEN_VALUES, chosenValues);
    outState.putStringArray(BUNDLE_KEY_RESOLUTION_KEYS, resolutionKeys);
    outState.putStringArray(BUNDLE_KEY_RESOLUTION_VALUES, resolutionValues);
  }

  private void restoreFromInstanceState(Bundle savedInstanceState) {
    WebLogger.getLogger(mAppName).i(TAG, "restoreFromInstanceState");
    if ( savedInstanceState == null ) {
      return;
    }

    if (savedInstanceState.containsKey(BUNDLE_KEY_SHOWING_LOCAL_WITH_DELTAS_DIALOG)) {
      mIsShowingTakeNewestWithDeltasDialog = savedInstanceState.getBoolean
          (BUNDLE_KEY_SHOWING_LOCAL_WITH_DELTAS_DIALOG);
    }
    if (savedInstanceState.containsKey(BUNDLE_KEY_SHOWING_LOCAL_DIALOG)) {
      mIsShowingTakeNewestDialog = savedInstanceState.getBoolean(BUNDLE_KEY_SHOWING_LOCAL_DIALOG);
    }
    if (savedInstanceState.containsKey(BUNDLE_KEY_SHOWING_SERVER_DIALOG)) {
      mIsShowingTakeOldestDialog = savedInstanceState.getBoolean(BUNDLE_KEY_SHOWING_SERVER_DIALOG);
    }
    String[] valueKeys = savedInstanceState.getStringArray(BUNDLE_KEY_VALUE_KEYS);
    String[] chosenValues = savedInstanceState.getStringArray(BUNDLE_KEY_CHOSEN_VALUES);
    String[] resolutionKeys = savedInstanceState.getStringArray(BUNDLE_KEY_RESOLUTION_KEYS);
    String[] resolutionValues = savedInstanceState.getStringArray(BUNDLE_KEY_RESOLUTION_VALUES);
    if (valueKeys != null) {
      // Then we know that we should have the chosenValues as well, or else
      // there is trouble. We're not doing a null check here, but if we didn't
      // get it then we know there is an error. We'll throw a null pointer
      // exception, but that is better than restoring bad state.
      // Same thing goes for the resolution keys. Those and the map should
      // always go together.
      Map<String, String> chosenValuesMap = new HashMap<String, String>();
      for (int i = 0; i < valueKeys.length; i++) {
        chosenValuesMap.put(valueKeys[i], chosenValues[i]);
      }
      Map<String, Resolution> userResolutions = new HashMap<String, Resolution>();
      for (int i = 0; i < resolutionKeys.length; i++) {
        userResolutions.put(resolutionKeys[i], Resolution.valueOf(resolutionValues[i]));
      }
      mChosenValuesMap.clear();
      mUserResolutions.clear();
      mChosenValuesMap.putAll(chosenValuesMap);
      mUserResolutions.putAll(userResolutions);
    }
    // And finally, call this to make sure we update the button as appropriate.
    WebLogger.getLogger(mAppName).i(TAG, "restoreFromInstanceState - done");
  }

  @Override
  public void onActivityCreated(Bundle savedInstanceState) {
    super.onActivityCreated(savedInstanceState);

    Intent incomingIntent = getActivity().getIntent();
    mAppName = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_APP_NAME);
    if ( mAppName == null || mAppName.length() == 0 ) {
      getActivity().setResult(Activity.RESULT_CANCELED);
      getActivity().finish();
      return;
    }

    mTableId = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_TABLE_ID);
    if ( mTableId == null || mTableId.length() == 0 ) {
      getActivity().setResult(Activity.RESULT_CANCELED);
      getActivity().finish();
      return;
    }

    mRowId = incomingIntent.getStringExtra(IntentConsts.INTENT_KEY_INSTANCE_ID);
    if ( mRowId == null || mRowId.length() == 0 ) {
      getActivity().setResult(Activity.RESULT_CANCELED);
      getActivity().finish();
      return;
    }

    restoreFromInstanceState(savedInstanceState);

    // render total instance view
    mAdapter = new ConflictResolutionColumnListAdapter(getActivity(), mAppName,
        R.string.checkpoint_radio_local, R.string.checkpoint_radio_server, this);

    setListAdapter(mAdapter);

    LoaderManager.getInstance(this).initLoader(RESOLVE_FIELD_LOADER, null, this);
  }

  @Override
  public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
    View view = inflater.inflate(ID, container, false);

    this.mTextViewCheckpointOverviewMessage =
        view.findViewById(R.id.checkpoint_overview_message);
    this.mButtonTakeOldest = view.findViewById(R.id.take_oldest);
    this.mButtonTakeNewest = view.findViewById(R.id.take_newest);
    this.mButtonTakeNewestWithDeltas = view.findViewById(R.id.take_newest_with_deltas);

    return view;
  }

  @Override public void onConflictResolutionDecision(String elementKey,
      Resolution resolution, String value) {
    if ( resolution == null ) {
      mChosenValuesMap.remove(elementKey);
      mUserResolutions.remove(elementKey);
    } else {
      mChosenValuesMap.put(elementKey, value);
      mUserResolutions.put(elementKey, resolution);
    }
  }

  @Override public Resolution getConflictResolutionDecision(String elementKey) {
    return mUserResolutions.get(elementKey);
  }

  @Override public void onDecisionMade() {
    if ( mUserResolutions.size() != mAdapter.getConflictCount() ) {
      mIsShowingTakeNewestWithDeltasDialog = false;
      mButtonTakeNewestWithDeltas.setEnabled(false);
    } else {
      mButtonTakeNewestWithDeltas.setEnabled(true);
    }

    // set the listview enabled in case it'd been down due to deletion resolution.
    mAdapter.notifyDataSetChanged();
  }


  @Override
  public Loader<ResolveActionList> onCreateLoader(int id, Bundle args) {
    // Now create and return a OdkResolveCheckpointRowLoader that will take care of
    // creating an ArrayList<ResolveRowEntry> for the data being displayed.
    return new OdkResolveCheckpointFieldLoader(getActivity(), mAppName, mTableId, mRowId);
  }

  @Override
  public void onLoadFinished(Loader<ResolveActionList> loader,
      ResolveActionList resolveFieldEntryArrayList) {
    // Swap the new cursor in. (The framework will take care of closing the
    // old cursor once we return.)
    mAdapter.clear();

    if ( resolveFieldEntryArrayList == null ) {
      // something went wrong in the loader...
      getActivity().setResult(Activity.RESULT_CANCELED);
      getActivity().finish();
      return;
    }

    if ( resolveFieldEntryArrayList.noChangesInUserDefinedFieldValues() ) {
      // don't even prompt -- just remove the checkpoint
      discardAllCheckpointChanges();
      getActivity().finish();
      return;
    }

    mAdapter.addAll(resolveFieldEntryArrayList);
    // Here we'll handle the cases of whether or not rows were deleted. There
    // are three cases to consider:
    // 1) both rows were updated, neither is deleted. This is the normal case
    // 2) the server row was deleted, the local was updated (thus a conflict)
    // 3) the local was deleted, the server was updated (thus a conflict)
    // To Figure this out we'll first need the state of each version.
    // Note that these calls should never return nulls, as whenever a row is in
    // conflict, there should be a conflict type. Therefore if we throw an
    // error that is fine, as we've violated an invariant.

    if ( getView() == null ) {
      throw new IllegalStateException("Unexpectedly found no view!");
    }

    this.mButtonTakeNewest
        .setOnClickListener(new DiscardOlderValuesAndMarkNewestAsIncompleteRowClickListener());

    if (resolveFieldEntryArrayList.actionType != ResolveActionType.DELETE) {
      this.mButtonTakeOldest
          .setOnClickListener(new DiscardNewerValuesAndRetainOldestInOriginalStateRowClickListener());
      if (resolveFieldEntryArrayList.actionType == ResolveActionType.RESTORE_TO_COMPLETE) {
        this.mTextViewCheckpointOverviewMessage
            .setText(getString(R.string.checkpoint_restore_complete_or_take_newest));
        this.mButtonTakeOldest.setText(getString(R.string.checkpoint_take_oldest_finalized));
      } else {
        this.mTextViewCheckpointOverviewMessage
            .setText(getString(R.string.checkpoint_restore_incomplete_or_take_newest));
        this.mButtonTakeOldest.setText(getString(R.string.checkpoint_take_oldest_incomplete));
      }
    } else {
      this.mButtonTakeOldest.setOnClickListener(new DiscardAllValuesAndDeleteRowClickListener());
      this.mTextViewCheckpointOverviewMessage
          .setText(getString(R.string.checkpoint_remove_or_take_newest));
      this.mButtonTakeOldest.setText(getString(R.string.checkpoint_take_oldest_remove));
    }

    // hide the deltas button if it doesn't make sense
    if ( resolveFieldEntryArrayList.hideDeltasButton() ) {
      mButtonTakeNewestWithDeltas.setVisibility(View.GONE);
    } else {
      mButtonTakeNewestWithDeltas.setVisibility(View.VISIBLE);
      mButtonTakeNewestWithDeltas
          .setOnClickListener(new ApplyDeltasAndMarkNewestAsIncompleteRowClickListener());
    }

    // enable or disable the deltas button based upon whether the user has
    // made all the necessary discrepancy choices.
    if ( mUserResolutions.size() != resolveFieldEntryArrayList.conflictColumns.size() ) {
      mIsShowingTakeNewestWithDeltasDialog = false;
      mButtonTakeNewestWithDeltas.setEnabled(false);
    } else {
      mButtonTakeNewestWithDeltas.setEnabled(true);
    }

    // restore whatever dialog is visible
    if ( mIsShowingTakeNewestWithDeltasDialog ) {
      mButtonTakeNewestWithDeltas.performClick();
    }
    if ( mIsShowingTakeNewestDialog ) {
      mButtonTakeNewest.performClick();
    }

    if ( mIsShowingTakeOldestDialog ) {
      mButtonTakeOldest.performClick();
    }
    // TODO: is this needed, or does it trigger an unnecessary refresh?
    mAdapter.notifyDataSetChanged();
  }

  @Override
  public void onLoaderReset(Loader<ResolveActionList> loader) {
    // This is called when the last ArrayList<ResolveRowEntry> provided to onLoadFinished()
    // above is about to be released. We need to make sure we are no
    // longer using it.
    mAdapter.clear();
  }

}
