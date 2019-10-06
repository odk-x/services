package org.opendatakit.services.database.utilities;

import android.database.Cursor;

import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.forms.FormInfo;
import org.opendatakit.utilities.ODKFileUtils;
import org.sqlite.database.SQLException;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Update the form definitions table based upon the content of the sdcard.
 *
 * This is extensively cleaned up from the first check-in.
 *
 * Moves all the mutable FormsProvider apis and the intialization logic into
 * ODKDatabaseImplUtils accessed via:
 * rescanTableFormDefs(String appName, DbHandle dbHandleName, String tableId)
 */

public class FormTableUtils {
  private static final String TAG = "FormTableUtils";

  private static void patchUpValues(String appName, HashMap<String, Object>
      values) {

    // require a tableId and formId...
    if (!values.containsKey(FormsColumns.TABLE_ID)) {
      throw new IllegalArgumentException(FormsColumns.TABLE_ID + " is not specified");
    }
    String tableId = (String) values.get(FormsColumns.TABLE_ID);

    if (!values.containsKey(FormsColumns.FORM_ID)) {
      throw new IllegalArgumentException(FormsColumns.FORM_ID + " is not specified");
    }
    String formId = (String) values.get(FormsColumns.FORM_ID);

    String formFolder = ODKFileUtils.getFormFolder(appName, tableId, formId);

    File formDefFolder = new File(formFolder);

    // require that it contain a formDef file
    File formDefFile = new File(formDefFolder, ODKFileUtils.FORMDEF_JSON_FILENAME);
    if (!formDefFile.exists()) {
      throw new IllegalArgumentException(ODKFileUtils.FORMDEF_JSON_FILENAME
          + " does not exist in: " + formFolder);
    }

    // parse the formDef.json
    FormInfo fiFound = new FormInfo(appName, formDefFile);

    values.put(FormsColumns.SETTINGS, fiFound.settings);
    values.put(FormsColumns.FORM_VERSION, fiFound.formVersion);
    values.put(FormsColumns.DISPLAY_NAME, fiFound.formTitle);
    values.put(FormsColumns.DEFAULT_FORM_LOCALE, fiFound.defaultLocale);
    values.put(FormsColumns.INSTANCE_NAME, fiFound.instanceName);

    String md5 = ODKFileUtils.getMd5Hash(appName, formDefFile);
    values.put(FormsColumns.JSON_MD5_HASH, md5);
    values.put(FormsColumns.DATE, fiFound.lastModificationDate);
    values.put(FormsColumns.FILE_LENGTH, fiFound.fileLength);

    return;
  }

  private static synchronized void insert(String appName, String tableId, String formId) {

    HashMap<String, Object> values = new HashMap<String, Object>();
    values.put(FormsColumns.TABLE_ID, tableId);
    values.put(FormsColumns.FORM_ID, formId);

    // force a scan from disk
    patchUpValues(appName, values);

    // first try to see if a record with this filename already exists...
    String[] projection = { FormsColumns.TABLE_ID, FormsColumns.FORM_ID };
    String selection = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
    String[] selectionArgs = { tableId, formId };
    Cursor c = null;

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      db.beginTransactionNonExclusive();
      try {
        c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, selection, selectionArgs,
            null, null, null, null);
        if (c == null) {
          throw new SQLException("FAILED Insert into " + tableId + " form " + formId
              + " -- unable to query for existing records. tableId=" + tableId + " formId=" + formId);
        }
        c.moveToFirst();
        if (c.getCount() > 0) {
          // already exists
          throw new SQLException("FAILED Insert into " + tableId + " form " + formId
              + " -- row already exists!");
        }
      } finally {
        if (c != null) {
          c.close();
        }
      }

      db.insertOrThrow(DatabaseConstants.FORMS_TABLE_NAME, null, values);
      db.setTransactionSuccessful();
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      String msg = e.getMessage();
      if ( msg == null ) {
        msg = e.toString();
      }
      throw new SQLException("FAILED Insert into " + tableId + " form " + formId + " -- " + msg, e);
    } finally {
      if (db != null) {
        try {
          db.endTransaction();
        } finally {
          try {
            db.releaseReference();
          } finally {
            // this closes the connection
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(
                appName, dbHandleName);
          }
        }
      }
    }
  }

  /**
   * This method removes the entry from the content provider, and also removes
   * any associated files. files: form.xml, [formmd5].formdef, formname
   * {directory}
   */
  private static synchronized void delete(String appName, String tableId, String formId) {

    String selection = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
    String[] selectionArgs = { tableId, formId };

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;

    try {
      // Get the database and run the query
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      db.beginTransactionNonExclusive();
      db.delete(DatabaseConstants.FORMS_TABLE_NAME, selection, selectionArgs);
      // commit the transaction...
      db.setTransactionSuccessful();
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      String msg = e.getMessage();
      if ( msg == null ) {
        msg = e.toString();
      }
      throw new SQLException("FAILED Delete from " + tableId + " form " + formId + " -- " + msg, e);
    } finally {
      if (db != null) {
        try {
          db.endTransaction();
        } finally {
          try {
            db.releaseReference();
          } finally {
            // this closes the connection
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .removeConnection(appName, dbHandleName);
          }
        }
      }
    }
  }

  private static synchronized void update(String appName, String tableId, String formId) {

    HashMap<String, Object> values = new HashMap<String, Object>();
    values.put(FormsColumns.TABLE_ID, tableId);
    values.put(FormsColumns.FORM_ID, formId);

    // force a scan from disk
    patchUpValues(appName, values);

    String[] projection = { FormsColumns.TABLE_ID, FormsColumns.FORM_ID };
    String selection = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
    String[] selectionArgs = { tableId, formId };
    /*
     * First, find out what records match this query. Replicate the
     * ContentValues if there are multiple tableIds/formIds involved
     * and the contentValues do not have formId and tableId specified.
     *
     * Otherwise, it is an error to specify the tableId or formId in
     * the ContentValues and have those not match the where results.
     *
     */

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);
      db.beginTransactionNonExclusive();

      Cursor c = null;
      try {
        c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, selection, selectionArgs,
            null, null, null, null);

        if (c == null) {
          throw new SQLException("FAILED Update of " + tableId + " form " + formId
              + " -- query for existing row did not return a cursor");
        }
        c.moveToFirst();
        if ( c.getCount() != 1 ) {
          throw new SQLException("FAILED Update to " + tableId + " form " + formId
              + " -- not exactly one row for this formId!");
        }
      } finally {
        if (c != null && !c.isClosed()) {
          c.close();
        }
      }

      // update the database with these patched-up values...
      db.update(DatabaseConstants.FORMS_TABLE_NAME, values, selection, selectionArgs);
      db.setTransactionSuccessful();

    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      String msg = e.getMessage();
      if ( msg == null ) {
        msg = e.toString();
      }
      throw new SQLException("FAILED Update on " + tableId + " form " + formId + " -- " + msg, e);
    } finally {
      if (db != null) {
        try {
          if (db.inTransaction()) {
            db.endTransaction();
          }
        } finally {
          try {
            db.releaseReference();
          } finally {
            // this closes the connection
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(
                appName, dbHandleName);
          }
        }
      }
    }
  }

  /**
   * This throws an exception on nearly all failures. I.e., the return code will
   * always be true.
   *
   * @param appName
   * @param tableIdFilter
   * @return true on success
   */
  public static boolean updateFormDir(String appName, String tableIdFilter) {

    WebLoggerIf log = WebLogger.getLogger(appName);
    log.i(TAG, "updateFormDir: " + appName + " tableId: " + tableIdFilter + " begin");

    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // collect list of all forms on the sdcard under this tableId...
    List<File> formDirs = new ArrayList<>();
    {
      File formDir = new File(ODKFileUtils.getFormsFolder(appName, tableIdFilter));
      File[] formIdDirs = formDir.listFiles(new FileFilter() {

        @Override
        public boolean accept(File pathname) {
          File formDef = new File(pathname, ODKFileUtils.FORMDEF_JSON_FILENAME);
          return pathname.isDirectory() && formDef.exists() && formDef.isFile();
        }
      });

      if (formIdDirs != null) {
        formDirs.addAll(Arrays.asList(formIdDirs));
      }
    }

    // /////////////////////////////////////////
    // look at list of forms recorded in forms table.
    Set<String> badFormIds = new HashSet<>();
    Set<String> changedFormIds = new HashSet<>();
    Set<String> duplicateFormIds = new HashSet<>();
    // 1. If a form is in the database but does not exist in the formDirs list (on the sdcard)
    //    then add it to the badFormIds set -- this set needs to be deleted.
    // 2. If the form exists, check if it has already been processed, as evidenced by being in the
    //    changedFormIds, processedFormIds, or duplicateFormIds lists. If it has, remove it
    //    from these and add it to the duplicateFormIds list.
    // 2. If the form exists in both, if the md5 hash of the form is unchanged, then remove
    //    it from the formDirs list, since the matching hash indicates that the file is unchanged
    //    and the information in the forms table is valid. Add to processedFormIds.
    // 3. If the form exists in both, but the md5 hash has changed, add it to the changedFormIds
    //    and processedFormIds sets and remove it from the formDirs list. These forms need to be
    //    updated.
    // 4. Finally, at the end, the formDirs will contain only the forms to be inserted.
    {
      Set<String> processedIds = new HashSet<String>();

      // Get the database and run the query
      DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
      OdkConnectionInterface db = null;
      try {
        // +1 referenceCount if db is returned (non-null)
        db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(appName, dbHandleName);

        String selection = FormsColumns.TABLE_ID + "=?";
        String[] selectionArgs = { tableIdFilter };

        Cursor c = db.query(DatabaseConstants.FORMS_TABLE_NAME, null, selection, selectionArgs,
            null, null, null, null);

        if (c == null) {
          WebLogger.getLogger(appName)
              .w(TAG, "updateFormDir " + appName + " null cursor returned from query.");
        } else {
          try {
            if (c.moveToFirst()) {
              do {
                String formId = CursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.FORM_ID));

                String formDir = ODKFileUtils.getFormFolder(appName, tableIdFilter, formId);
                File f = new File(formDir);
                File formDefJson = new File(f, ODKFileUtils.FORMDEF_JSON_FILENAME);
                if (!f.exists() || !f.isDirectory() || !formDefJson.exists() || !formDefJson.isFile()) {
                  // the form definition does not exist
                  badFormIds.add(formId);
                } else if ( processedIds.contains(formId) ) {
                  // formdef.json exists. But...
                  // there are two database records for this formId.
                  // remove it from the changedFormIds set and add it to the
                  // duplicateFormIds set.
                  changedFormIds.remove(formId);
                  duplicateFormIds.add(formId);
                } else {
                  // formdef.json exists. See if it is unchanged...
                  String json_md5 = CursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.JSON_MD5_HASH));
                  String fileMd5 = ODKFileUtils.getMd5Hash(appName, formDefJson);
                  if (json_md5 == null || !json_md5.equals(fileMd5)) {
                    // it HAS changed -- add it to the changed list
                    changedFormIds.add(formId);
                  }
                  // remove it from the formDirs list
                  formDirs.remove(f);
                  // and add it to the processed formId list.
                  processedIds.add(formId);
                }
              } while (c.moveToNext());
            }
          } finally {
            c.close();
          }
        }
      } finally {
        if (db != null) {
          try {
            db.releaseReference();
          } finally {
            // this closes the connection
            // if it was successful, then the InvalidateMonitor will close the connection
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(
                appName, dbHandleName);
          }
        }
      }
    }

    ///////////////////////////
    ///////////////////////////
    // we now have a:
    // 1. set of forms (duplicateFormIds) that had two or more entries in the form definitions
    // table (internal error).
    // 2. a set of forms (badFormIds) that no longer exist on the sdcard and  which need to be
    // removed from the form definitions table.
    // 3. a set of forms (changedFormIds) whose formDef.json have changed and therefore need
    // their form definition record updated.
    // 4. a list of formDirs who have formDef.json files that need to be scanned and added to the
    // form definitions table.

    ////////////////////////////
    // The duplicateFormIds should be deleted from the form definitions table.
    // They should then be processed to insert new records into the table.
    for (String formId : duplicateFormIds) {
      delete(appName, tableIdFilter, formId);
      insert(appName, tableIdFilter, formId);
    }

    //////////////////////////
    // delete the bad forms from the database -- these have no formDef.json files.
    // Leave the other files that might be under these formDir locations unchanged.
    // i.e., Assume that the files are as intended by the application architect.
    for (String formId : badFormIds) {
      delete(appName, tableIdFilter, formId);
    }

    //////////////////////////
    // Update the changed forms
    for ( String formId : changedFormIds ) {
      update(appName, tableIdFilter, formId);
    }

    ///////////////////////////
    // insert the forms that are new
    for (int i = 0; i < formDirs.size(); ++i) {
      File formDir = formDirs.get(i);

      String formId = formDir.getName();
      insert(appName, tableIdFilter, formId);
    }

    log.i(TAG, "updateFormDir: " + appName + " tableId: " + tableIdFilter + " end");
    return true;
  }
}
