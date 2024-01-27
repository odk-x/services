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

import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.services.resolve.views.components.ConflictResolutionColumnListAdapter;
import org.opendatakit.services.resolve.views.components.Resolution;
import org.opendatakit.services.resolve.views.components.ResolveActionList;
import org.opendatakit.services.utilities.ActiveUserAndLocale;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author mitchellsundt@gmail.com
 */
public class ConflictResolutionRowFragment extends ListFragment implements
    ConflictResolutionColumnListAdapter.UICallbacks,  LoaderManager
    .LoaderCallbacks<ResolveActionList>  {

  private static final String TAG = "ConflictResolutionRowFragment";
  private static final int RESOLVE_FIELD_LOADER = 0x03;

  public static final String NAME = "ConflictResolutionRowFragment";
  public static final int ID = R.layout.conflict_resolver_field_list;

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

  private Button mButtonTakeServer;
  private Button mButtonTakeLocal;
  private Button mButtonTakeLocalWithDeltas;

  /**
   * The message to the user as to why they're getting extra options. Will be
   * either thing to the effect of "someone has deleted something you've
   * changed", or "you've deleted something someone has changed". They'll then
   * have to choose either to delete or to go ahead and actually restore and
   * then resolve it.
   */
  private TextView mTextViewConflictOverviewMessage;

  private boolean mIsShowingTakeLocalWithDeltasDialog = false;
  private boolean mIsShowingTakeLocalDialog = false;
  private boolean mIsShowingTakeServerDialog = false;

  private Map<String, String> mChosenValuesMap = new TreeMap<String, String>();
  private Map<String, Resolution> mUserResolutions = new TreeMap<String, Resolution>();

  private class DiscardChangesAndDeleteLocalListener implements View.OnClickListener {

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
      builder.setMessage(getString(R.string.conflict_delete_local_confirmation_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          // delete all data (since it was deleted on the server and we accepted that)
          mIsShowingTakeServerDialog = false;
          dialog.dismiss();
          OdkConnectionInterface db = null;

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          ActiveUserAndLocale aul = ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            // This is elevated privileges since we are applying the server's change locally
            ODKDatabaseImplUtils.get().resolveServerConflictWithDeleteRowWithId(db,
                mTableId, mRowId, aul.activeUser);

            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("ConflictResolveListener",
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
          WebLogger.getLogger(mAppName).d(TAG, "delete local row (apply server delete)");
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
          mIsShowingTakeServerDialog = false;
          dialog.dismiss();
        }
      });
      mIsShowingTakeServerDialog = true;
      builder.create().show();
    }
  }

  private class SetRowToDeleteOnServerListener implements View.OnClickListener {

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
      builder.setMessage(getString(R.string.conflict_delete_on_server_confirmation_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          // We're going to discard the local changes by acting as if
          // takeServer was pressed. Then we're going to flag row as
          // deleted.
          mIsShowingTakeLocalDialog = false;
          dialog.dismiss();

          OdkConnectionInterface db = null;

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          ActiveUserAndLocale aul =
              ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            // run this with user permissions, since we are taking local changes over server values
            ODKDatabaseImplUtils.get().resolveServerConflictTakeLocalRowWithId(db,
                mTableId, mRowId, aul.activeUser, aul.rolesList, aul.locale);

            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("ConflictResolveListener",
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
              "mark local row as deleted (removal on next sync to server)");
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
          mIsShowingTakeLocalDialog = false;
          dialog.dismiss();
        }
      });
      mIsShowingTakeLocalDialog = true;
      builder.create().show();
    }
  }

  private class ApplyDeltasAndTakeLocalClickListener implements View.OnClickListener {

    @Override
    public void onClick(View v) {

      /**
       * New dialog styling
       * MaterialAlertDialogBuilder is standard for all ODK-X Apps
       * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
       * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
       */

      MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(getActivity());
      builder.setMessage(getString(R.string.conflict_take_local_with_deltas_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          mIsShowingTakeLocalDialog = false;
          dialog.dismiss();
          OdkConnectionInterface db = null;

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          ActiveUserAndLocale aul =
              ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            // this is overkill, as we only need to pull the server values that were selected.
            ContentValues updateValues = new ContentValues();
            for (Map.Entry<String, Resolution> entry : mUserResolutions.entrySet() ) {
              updateValues.put( entry.getKey(), mChosenValuesMap.get(entry.getKey()));
            }

            // Use local user's role when accepting local changes over server changes.
            ODKDatabaseImplUtils.get().resolveServerConflictTakeLocalRowPlusServerDeltasWithId(db,
                mTableId, updateValues, mRowId,
                aul.activeUser, aul.rolesList, aul.locale);

            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("ConflictResolveListener",
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
          WebLogger.getLogger(mAppName).d(TAG, "update to local with deltas version");
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
          mIsShowingTakeLocalDialog = false;
        }
      });
      mIsShowingTakeLocalDialog = true;
      builder.create().show();
    }

  }

  private class TakeLocalClickListener implements View.OnClickListener {

    @Override
    public void onClick(View v) {

      /**
       * New dialog styling
       * MaterialAlertDialogBuilder is standard for all ODK-X Apps
       * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
       * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
       */

      MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(getActivity(),R.style.OdkXAlertDialogStyle);
      builder.setMessage(getString(R.string.conflict_take_local_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          mIsShowingTakeLocalDialog = false;
          dialog.dismiss();
          OdkConnectionInterface db = null;

          ActiveUserAndLocale aul =
              ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            // use local user's rolesList
            ODKDatabaseImplUtils.get().resolveServerConflictTakeLocalRowWithId(db,
                mTableId, mRowId, aul.activeUser, aul.rolesList, aul.locale);

            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("ConflictResolveListener",
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
          WebLogger.getLogger(mAppName).d(TAG, "update to local version");
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
          mIsShowingTakeLocalDialog = false;
        }
      });
      mIsShowingTakeLocalDialog = true;
      builder.create().show();
    }

  }

  private class TakeServerClickListener implements View.OnClickListener {

    @Override
    public void onClick(View v) {
      /**
       * New dialog styling
       * MaterialAlertDialogBuilder is standard for all ODK-X Apps
       * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
       * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
       */

      MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(getActivity(),R.style.OdkXAlertDialogStyle);
      builder.setMessage(getString(R.string.conflict_take_server_warning));
      builder.setPositiveButton(getString(R.string.yes), new DialogInterface.OnClickListener() {

        @Override
        public void onClick(DialogInterface dialog, int which) {
          mIsShowingTakeServerDialog = false;
          dialog.dismiss();
          OdkConnectionInterface db = null;

          ActiveUserAndLocale aul =
              ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

          DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

          try {
            // +1 referenceCount if db is returned (non-null)
            db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .getConnection(mAppName, dbHandleName);

            // use privileged user roles since we are taking server's values
            ODKDatabaseImplUtils.get().resolveServerConflictTakeServerRowWithId(db,
                mTableId, mRowId, aul.activeUser, aul.locale);

            getActivity().setResult(Activity.RESULT_OK);
          } catch (Exception e) {
            String msg = e.getLocalizedMessage();
            if (msg == null)
              msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            msg = "Exception: " + msg;
            WebLogger.getLogger(mAppName).e("ConflictResolveListener",
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
          WebLogger.getLogger(mAppName).d(TAG, "update to server version");
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
          mIsShowingTakeServerDialog = false;
        }
      });
      mIsShowingTakeServerDialog = true;
      builder.create().show();
    }

  }

  private void discardAllLocalChanges() {
    OdkConnectionInterface db = null;

    ActiveUserAndLocale aul =
        ActiveUserAndLocale.getActiveUserAndLocale(getActivity(), mAppName);

    DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(mAppName, dbHandleName);

      // use privileged user roles since we are taking server's values
      ODKDatabaseImplUtils.get().resolveServerConflictTakeServerRowWithId(db, mTableId,
          mRowId, aul.activeUser, aul.locale);

    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(mAppName).e("OdkResolveConflictRowLoader",
          mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(mAppName).printStackTrace(e);
      Toast.makeText(getActivity(), "database access failure",
          Toast.LENGTH_LONG).show();
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
    Toast.makeText(getActivity(), R.string.conflict_auto_apply, Toast.LENGTH_LONG).show();
  }

  @Override
  public void onSaveInstanceState(Bundle outState) {
    super.onSaveInstanceState(outState);

    outState.putBoolean(BUNDLE_KEY_SHOWING_LOCAL_WITH_DELTAS_DIALOG,
        mIsShowingTakeLocalWithDeltasDialog);
    outState.putBoolean(BUNDLE_KEY_SHOWING_LOCAL_DIALOG, mIsShowingTakeLocalDialog);
    outState.putBoolean(BUNDLE_KEY_SHOWING_SERVER_DIALOG, mIsShowingTakeServerDialog);
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

    // TODO: we have no way in the resolve conflicts screen to choose which filter scope
    // TODO: to take. Need to allow super-user and above to choose the local filter scope
    // TODO: vs just taking what the server has.

    if ( savedInstanceState != null ) {

      WebLogger.getLogger(mAppName).i(TAG, "onActivityCreated - restoreFromInstanceState");

      if (savedInstanceState.containsKey(BUNDLE_KEY_SHOWING_LOCAL_WITH_DELTAS_DIALOG)) {
        mIsShowingTakeLocalWithDeltasDialog = savedInstanceState.getBoolean
            (BUNDLE_KEY_SHOWING_LOCAL_WITH_DELTAS_DIALOG);
      }
      if (savedInstanceState.containsKey(BUNDLE_KEY_SHOWING_LOCAL_DIALOG)) {
        mIsShowingTakeLocalDialog = savedInstanceState.getBoolean(BUNDLE_KEY_SHOWING_LOCAL_DIALOG);
      }
      if (savedInstanceState.containsKey(BUNDLE_KEY_SHOWING_SERVER_DIALOG)) {
        mIsShowingTakeServerDialog = savedInstanceState.getBoolean(BUNDLE_KEY_SHOWING_SERVER_DIALOG);
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
      WebLogger.getLogger(mAppName).i(TAG, "onActivityCreated - restoreFromInstanceState - done");
    }

    // TODO: need to show or hide the rowFilterScope in the list of columns to reconcile based
    // TODO: upon whether the current user has RoleConsts.ROLE_SUPER_USER or
    // TODO: RoleConsts.ROLE_ADMINISTRATOR

    // render total instance view
    mAdapter = new ConflictResolutionColumnListAdapter(getActivity(), mAppName,
        R.string.conflict_radio_local, R.string.conflict_radio_server, this);

    setListAdapter(mAdapter);

    LoaderManager.getInstance(this).initLoader(RESOLVE_FIELD_LOADER, null, this);

  }

  @Override
  public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
    View view = inflater.inflate(ID, container, false);

    this.mTextViewConflictOverviewMessage =
            view.findViewById(R.id.conflict_overview_message);
    this.mButtonTakeServer = view.findViewById(R.id.take_server);
    this.mButtonTakeLocal = view.findViewById(R.id.take_local);
    this.mButtonTakeLocalWithDeltas = view.findViewById(R.id.take_local_with_deltas);

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
      mIsShowingTakeLocalWithDeltasDialog = false;
      mButtonTakeLocalWithDeltas.setEnabled(false);
    } else {
      mButtonTakeLocalWithDeltas.setEnabled(true);
    }

    // set the listview enabled in case it'd been down due to deletion resolution.
    mAdapter.notifyDataSetChanged();
  }


  @Override
  public Loader<ResolveActionList> onCreateLoader(int id, Bundle args) {
    // Now create and return a OdkResolveConflictFieldLoader that will take care of
    // creating an ResolveActionList for the data being displayed.
    return new OdkResolveConflictFieldLoader(getActivity(), mAppName, mTableId, mRowId);
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

    if ( resolveFieldEntryArrayList.noChangesInUserDefinedFieldValues() ){
      // don't even prompt -- just remove the conflict
      discardAllLocalChanges();
      getActivity().setResult(Activity.RESULT_OK);
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

    if (resolveFieldEntryArrayList.localConflictType == ConflictType.LOCAL_UPDATED_UPDATED_VALUES &&
        resolveFieldEntryArrayList.serverConflictType == ConflictType.SERVER_UPDATED_UPDATED_VALUES) {
      // Then it's a normal conflict. Hide the elements of the view relevant
      // to deletion restoration.
      this.mTextViewConflictOverviewMessage.setText(getString(R.string.conflict_resolve_or_choose));

      this.mButtonTakeLocal.setOnClickListener(new TakeLocalClickListener());
      this.mButtonTakeLocal.setText(getString(R.string.conflict_take_local_updates));
      this.mButtonTakeServer.setOnClickListener(new TakeServerClickListener());
      this.mButtonTakeServer.setText(getString(R.string.conflict_take_server_updates));
    } else if (
        resolveFieldEntryArrayList.localConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES &&
        resolveFieldEntryArrayList.serverConflictType == ConflictType.SERVER_UPDATED_UPDATED_VALUES) {
      // Then the local row was deleted, but someone had inserted a newer
      // updated version on the server.
      this.mTextViewConflictOverviewMessage
          .setText(getString(R.string.conflict_local_was_deleted_explanation));
      this.mButtonTakeServer.setOnClickListener(new TakeServerClickListener());
      this.mButtonTakeServer.setText(getString(R.string.conflict_restore_with_server_changes));
      this.mButtonTakeLocal.setOnClickListener(new SetRowToDeleteOnServerListener());
      this.mButtonTakeLocal.setText(getString(R.string.conflict_enforce_local_delete));
    } else if (
        resolveFieldEntryArrayList.localConflictType == ConflictType.LOCAL_UPDATED_UPDATED_VALUES &&
        resolveFieldEntryArrayList.serverConflictType == ConflictType.SERVER_DELETED_OLD_VALUES) {
      // Then the row was updated locally but someone had deleted it on the
      // server.
      this.mTextViewConflictOverviewMessage
          .setText(getString(R.string.conflict_server_was_deleted_explanation));
      this.mButtonTakeLocal.setOnClickListener(new TakeLocalClickListener());
      this.mButtonTakeLocal.setText(getString(R.string.conflict_restore_with_local_changes));
      this.mButtonTakeServer.setText(getString(R.string.conflict_apply_delete_from_server));
      this.mButtonTakeServer.setOnClickListener(new DiscardChangesAndDeleteLocalListener());
    }

    // hide the deltas button if it doesn't make sense
    if ( resolveFieldEntryArrayList.hideDeltasButton() ) {
      mButtonTakeLocalWithDeltas.setVisibility(View.GONE);
    } else {
      mButtonTakeLocalWithDeltas.setVisibility(View.VISIBLE);
      mButtonTakeLocalWithDeltas
          .setOnClickListener(new ApplyDeltasAndTakeLocalClickListener());
    }

    // enable or disable the deltas button based upon whether the user has
    // made all the necessary discrepancy choices.
    if ( mUserResolutions.size() != resolveFieldEntryArrayList.conflictColumns.size() ) {
      mIsShowingTakeLocalWithDeltasDialog = false;
      mButtonTakeLocalWithDeltas.setEnabled(false);
    } else {
      mButtonTakeLocalWithDeltas.setEnabled(true);
    }

    // restore whatever dialog is visible
    if (mIsShowingTakeLocalWithDeltasDialog) {
      mButtonTakeLocalWithDeltas.performClick();
    }
    if (mIsShowingTakeLocalDialog) {
      mButtonTakeLocal.performClick();
    }

    if (mIsShowingTakeServerDialog) {
      mButtonTakeServer.performClick();
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
