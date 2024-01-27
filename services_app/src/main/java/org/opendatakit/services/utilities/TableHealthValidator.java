package org.opendatakit.services.utilities;

import android.app.Activity;
import android.content.ComponentName;
import android.content.DialogInterface;
import android.content.Intent;


import androidx.appcompat.app.AlertDialog;

import com.google.android.material.dialog.MaterialAlertDialogBuilder;

import org.opendatakit.consts.IntentConsts;
import org.opendatakit.consts.RequestCodeConsts;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.services.sync.actions.activities.SyncActivity;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Check all tables in the database for changes, conflicts, or checkpoint rows. If a problem is
 * found, prompt the user to resolve or ignore it.
 */
public class TableHealthValidator {

  private static final String TAG = "TableHealthValidator";

  private String appName;
  private Activity displayActivity;

  private List<String> checkpointTables;
  private List<String> conflictTables;

  public TableHealthValidator(String appName, Activity displayActivity) {
    this.appName = appName;
    this.displayActivity = displayActivity;
  }

  /**
   * The control flow of this class is as follows:
   *  1. User calls verifyTableHealth(). We check for all health issues.
   *  2. If there are conflicts or checkpoints:
   *     * promptToResolveCheckpointsAndConflicts and then
   *     * resolveCheckpointsAndConflicts
   *     * At this point, the user MUST override onActivityResult and call verifyTableHealth again.
   *       This is because conflicts and checkpoints dirty the change data, so we need to resolve
   *       them before we can check for unsynced changes.
   *  3. If there are unsynced changes: promptToResolveChanges
   */

  public void verifyTableHealth() {
    WebLogger.getLogger(appName).i(TAG, "[verifyTableHealth]");

    OdkConnectionInterface db = null;
    DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

    checkpointTables = new LinkedList<>();
    conflictTables = new LinkedList<>();
    boolean hasChanges = false;

    try {
      // +1 referenceCount if db is returned (non-null)
      AndroidConnectFactory.configure();
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

      for (String tableId : tableIds) {
        int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);

        if (CursorUtils.getTableHealthIsClean(health)) {
          continue;
        }

        if (!hasChanges && CursorUtils.getTableHealthHasChanges(health)) {
          hasChanges = true;
        }

        if (CursorUtils.getTableHealthHasConflicts(health)) {
          conflictTables.add(tableId);
        }

        if (CursorUtils.getTableHealthHasCheckpoints(health)) {
          checkpointTables.add(tableId);
        }
      }
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    WebLogger.getLogger(appName).i(TAG,
        "[verifyTableHealth] " + "summary:\n\tUnsynced changes present: "
            + hasChanges + "\n\tNumber of conflict rows present: " + conflictTables.size()
            + "\n\tNumber of checkpoint rows present: " + checkpointTables.size());


    if ((checkpointTables != null && !checkpointTables.isEmpty()) ||
        (conflictTables != null && !conflictTables.isEmpty())) {
      promptToResolveCheckpointsAndConflicts();
    } else if (hasChanges) {
      promptToResolveChanges();
    }
  }

  private void promptToResolveChanges() {
    /**
     * New dialog styling
     * MaterialAlertDialogBuilder is standard for all ODK-X Apps
     * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
     * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
     */

    MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(displayActivity,R.style.OdkXAlertDialogStyle);
    builder.setTitle(R.string.sync_pending_changes);
    builder.setMessage(R.string.resolve_pending_changes);
    builder.setPositiveButton(R.string.ignore_changes, new DialogInterface.OnClickListener() {
      public void onClick(DialogInterface dialog, int id) {
        dialog.dismiss();
        // Proceed to change authentications unhindered. We have warned the user and they chose to
        // take a risk.
      }
    });
    builder.setNegativeButton(R.string.resolve_with_sync, new DialogInterface.OnClickListener() {
      public void onClick(DialogInterface dialog, int id) {
        dialog.dismiss();

        // Launch the Sync activity to sync your changes.
        // TODO: Can we check if we just came from sync and if so just return to that?
        Intent i = new Intent(displayActivity, SyncActivity.class);
        i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
        displayActivity.startActivity(i);
      }
    });
    AlertDialog dialog = builder.create();
    dialog.show();
  }

  private void promptToResolveCheckpointsAndConflicts() {
    /**
     * New dialog styling
     * MaterialAlertDialogBuilder is standard for all ODK-X Apps
     * OdkAlertDialogStyle present in AndroidLibrary is used to style this dialog
     * @params change MaterialAlertDialogBuilder to AlertDialog.Builder in case of any error and remove R.style... param!
     */

    MaterialAlertDialogBuilder builder = new MaterialAlertDialogBuilder(displayActivity,R.style.OdkXAlertDialogStyle);
    builder.setTitle(R.string.resolve_checkpoints_and_conflicts);
    builder.setMessage(R.string.resolve_pending_checkpoints_and_conflicts);
    builder.setPositiveButton(R.string.ok, new DialogInterface.OnClickListener() {
      public void onClick(DialogInterface dialog, int id) {
        dialog.dismiss();
        resolveConflictsAndCheckpoints();
      }
    });
    AlertDialog dialog = builder.create();
    dialog.show();
  }

  private void resolveConflictsAndCheckpoints() {

    if (checkpointTables != null && !checkpointTables.isEmpty()) {
      Iterator<String> iterator = checkpointTables.iterator();
      String tableId = iterator.next();
      checkpointTables.remove(tableId);

      Intent i;
      i = new Intent();
      i.setComponent(new ComponentName(IntentConsts.ResolveCheckpoint.APPLICATION_NAME,
          IntentConsts.ResolveCheckpoint.ACTIVITY_NAME));
      i.setAction(Intent.ACTION_EDIT);
      i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
      i.putExtra(IntentConsts.INTENT_KEY_TABLE_ID, tableId);
      displayActivity.startActivityForResult(i, RequestCodeConsts.RequestCodes.LAUNCH_CHECKPOINT_RESOLVER);
    }
    if (conflictTables != null && !conflictTables.isEmpty()) {
      Iterator<String> iterator = conflictTables.iterator();
      String tableId = iterator.next();
      conflictTables.remove(tableId);

      Intent i;
      i = new Intent();
      i.setComponent(new ComponentName(IntentConsts.ResolveConflict.APPLICATION_NAME,
          IntentConsts.ResolveConflict.ACTIVITY_NAME));
      i.setAction(Intent.ACTION_EDIT);
      i.putExtra(IntentConsts.INTENT_KEY_APP_NAME, appName);
      i.putExtra(IntentConsts.INTENT_KEY_TABLE_ID, tableId);
      displayActivity.startActivityForResult(i, RequestCodeConsts.RequestCodes.LAUNCH_CONFLICT_RESOLVER);
    }
  }
}
