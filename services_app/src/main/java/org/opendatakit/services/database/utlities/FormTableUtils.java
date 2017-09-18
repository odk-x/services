package org.opendatakit.services.database.utlities;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import android.net.Uri;
import android.support.annotation.NonNull;
import android.text.TextUtils;
import org.apache.commons.lang3.StringUtils;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.LocalKeyValueStoreConstants;
import org.opendatakit.database.data.TableMetaDataEntries;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.provider.FormsProviderAPI;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.forms.FormInfo;
import org.opendatakit.utilities.NameUtil;
import org.opendatakit.utilities.ODKFileUtils;
import org.sqlite.database.sqlite.SQLiteException;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;

/**
 * Move all the mutable FormsProvider apis and the intialization logic into
 * ODKDatabaseImplUtils accessed via
 * rescanTableFormDefs(String appName, DbHandle dbHandleName, String tableId)
 */

public class FormTableUtils {
  private static final String TAG = "FormTableUtils";

  private static FormSpec patchUpValues(String appName, HashMap<String, Object>
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

    FormSpec formSpec = new FormSpec();
    formSpec.tableId = tableId;
    formSpec.formId = formId;

    String formFolder = ODKFileUtils.getFormFolder(appName, tableId, formId);

    File formDefFolder = new File(formFolder);

    if (!values.containsKey(FormsColumns.DISPLAY_NAME)) {
      values.put(FormsColumns.DISPLAY_NAME,
          NameUtil.normalizeDisplayName(NameUtil.constructSimpleDisplayName(formId)));
    }

    // require that it contain a formDef file
    File formDefFile = new File(formFolder, ODKFileUtils.FORMDEF_JSON_FILENAME);
    if (!formDefFile.exists()) {
      throw new IllegalArgumentException(ODKFileUtils.FORMDEF_JSON_FILENAME
          + " does not exist in: " + formFolder);
    }

    // get the supplied date and hash
    // if these match, skip parsing for other fields.

    if (values.containsKey(FormsColumns.DATE) &&
        values.containsKey(FormsColumns.FILE_LENGTH)) {
      // we can avoid file I/O if these values match those of the formDefFile.
      Long existingModificationDate = (Long) values.get(FormsColumns.DATE);
      Long existingFileLength = (Long) values.get(FormsColumns.FILE_LENGTH);

      // date is the last modification date of the formDef file
      Long now = formDefFile.lastModified();
      Long length = formDefFile.length();

      if (now.equals(existingModificationDate) && length.equals(existingFileLength)) {
        // assume everything is unchanged...
        return formSpec;
      }
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

    return formSpec;
  }

  private static synchronized Uri insert(@NonNull Uri uri, ContentValues
      initialValues) {

    List<String> segments = uri.getPathSegments();

    if (segments.size() != 1) {
      throw new IllegalArgumentException("Unknown URI (too many segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLoggerIf log = WebLogger.getLogger(appName);

    HashMap<String, Object> values = new HashMap<String, Object>();
    if (initialValues != null) {
      for (String key : initialValues.keySet()) {
        values.put(key, initialValues.get(key));
      }
    }

    // force a scan from disk
    values.remove(FormsColumns.DATE);
    values.remove(FormsColumns.JSON_MD5_HASH);
    FormSpec formSpec = patchUpValues(appName, values);

    // first try to see if a record with this filename already exists...
    String[] projection = { FormsColumns.TABLE_ID, FormsColumns.FORM_ID };
    String selection = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
    String[] selectionArgs = { formSpec.tableId, formSpec.formId };
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
          throw new SQLException("FAILED Insert into " + uri
              + " -- unable to query for existing records. tableId=" + formSpec.tableId + " formId=" + formSpec.formId);
        }
        c.moveToFirst();
        if (c.getCount() > 0) {
          // already exists
          throw new SQLException("FAILED Insert into " + uri
              + " -- row already exists for  tableId=" + formSpec.tableId + " formId=" + formSpec.formId);
        }
      } catch (Exception e) {
        log.w(TAG, "FAILED Insert into " + uri + " -- query for existing row failed: " + e);

        if (e instanceof SQLException) {
          throw (SQLException) e;
        } else {
          throw new SQLException("FAILED Insert into " + uri + " -- query for existing row failed: "
              + e);
        }
      } finally {
        if (c != null) {
          c.close();
        }
      }

      try {
        db.insertOrThrow(DatabaseConstants.FORMS_TABLE_NAME, null, values);
        db.setTransactionSuccessful();
        // and notify listeners of the new row...
        Uri formUri = Uri.withAppendedPath(
            Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName),
            (String) values.get(FormsColumns.FORM_ID));
        return formUri;
      } catch (Exception e) {
        log.w(TAG, "FAILED Insert into " + uri + " -- insert of row failed: " + e);

        if (e instanceof SQLException) {
          throw (SQLException) e;
        } else {
          throw new SQLException("FAILED Insert into " + uri + " -- insert of row failed: "
              + e);
        }
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("FAILED Insert into " + uri + " -- insert of row failed: "
          + e);
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
   * Parse the URI for the form. This is either of the form:
   * <p>
   * /appName/_ID/
   * use the _ID column to retrieve the referenced form
   * this is a numeric row id.
   * <p>
   * or
   * <p>
   * /appName/tableId/
   * this will return all forms for a given tableId.
   * <p>
   * or
   * <p>
   * /appName/tableId//
   * this requires a call to updatePatchedFilter(...) to
   * retrieve and use the default formId for this tableId.
   * If there is no configured default formId, use the
   * tableId as the formId.
   * <p>
   * or
   * <p>
   * /appName/tableId/formId/
   *
   * @param uri
   * @param segments
   * @param where
   * @param whereArgs
   * @return
   */
  private static PatchedFilter extractUriFeatures(Uri uri, List<String> segments, String where,
                                            String[] whereArgs) {

    PatchedFilter pf = new PatchedFilter();

    if (segments.size() < 1 || segments.size() > 3) {
      throw new IllegalArgumentException("Unknown URI (incorrect number of segments!) " + uri);
    }

    pf.appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(pf.appName);

    pf.tableId = null;
    pf.formId = null;
    pf.numericFormId = null;
    // assume that we are not dealing with _ID values...
    pf.tableId = segments.size() >= 2 ? segments.get(1) : null;
    pf.isNumericFormId = StringUtils.isNumeric(pf.tableId);
    if (pf.isNumericFormId) {
      pf.numericFormId = pf.tableId;
      pf.tableId = null;
      if (segments.size() == 3) {
        // user is trying to mix a /_ID uri with a /tableId/formId uri.
        throw new IllegalArgumentException("Unknown URI ( _ID cannot be combined with other segments!) " + uri);
      }
    }
    // and handle formId
    pf.formId = segments.size() == 3 ? segments.get(2) : null;

    // Modify the where clause to account for the presence of any additional segments
    if (segments.size() == 1) {
      // no segments -- directly use whatever filter the user specified
      pf.whereId = where;
      pf.whereIdArgs = whereArgs;
    } else if (segments.size() == 2) {
      // either a tableId or a numericFormId is specified.
      // combine this filter with the where clause the user supplied.
      if (TextUtils.isEmpty(where)) {
        pf.whereId = (pf.isNumericFormId ? FormsColumns._ID : FormsColumns.TABLE_ID) + "=?";
        pf.whereIdArgs = new String[1];
        pf.whereIdArgs[0] = pf.isNumericFormId ? pf.numericFormId : pf.tableId;
      } else {
        pf.whereId = (pf.isNumericFormId ? FormsColumns._ID : FormsColumns.TABLE_ID) + "=? AND (" + where
            + ")";
        pf.whereIdArgs = new String[whereArgs.length + 1];
        pf.whereIdArgs[0] = pf.isNumericFormId ? pf.numericFormId : pf.tableId;
        System.arraycopy(whereArgs, 0, pf.whereIdArgs, 1, whereArgs.length);
      }
    } else {
      // we have both a tableId and a formId.
      pf.requiresBlankFormIdPatch = pf.formId == null || pf.formId.equals("_");

      // combine with the filter clause the user supplied.
      if (TextUtils.isEmpty(where)) {
        pf.whereId = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
        pf.whereIdArgs = new String[2];
        pf.whereIdArgs[0] = pf.tableId;
        pf.whereIdArgs[1] = pf.formId;
      } else {
        pf.whereId = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=? AND (" + where
            + ")";
        pf.whereIdArgs = new String[whereArgs.length + 2];
        pf.whereIdArgs[0] = pf.tableId;
        pf.whereIdArgs[1] = pf.formId;
        System.arraycopy(whereArgs, 0, pf.whereIdArgs, 2, whereArgs.length);
      }
    }

    return pf;
  }

  /**
   * If the URL did not include a formId, update the patched filter to use the
   * default formId for this tableId. If no default formId is specified in the
   * table's properties, use the tableId form as the default form.
   *
   * @param db
   * @param pf
   */
  private static void updatePatchedFilter(OdkConnectionInterface db, PatchedFilter pf) {
    if (pf.requiresBlankFormIdPatch) {
      TableMetaDataEntries values = ODKDatabaseImplUtils.get()
          .getTableMetadata(db, pf.tableId,
              LocalKeyValueStoreConstants.DefaultSurveyForm.PARTITION,
              LocalKeyValueStoreConstants.DefaultSurveyForm.ASPECT,
              LocalKeyValueStoreConstants.DefaultSurveyForm.KEY_FORM_ID);
      if (values.getEntries() == null || values.getEntries().size() != 1) {
        // use the tableId as the default formId
        pf.formId = pf.tableId;
      } else {
        pf.formId = values.getEntries().get(0).value;
      }
      pf.whereIdArgs[1] = pf.formId;
    }
  }

  private static Cursor query(@NonNull Uri uri, String[] projection, String where,
                              String[] whereArgs, String sortOrder) {

    List<String> segments = uri.getPathSegments();

    PatchedFilter pf = extractUriFeatures(uri, segments, where, whereArgs);
    WebLoggerIf log = WebLogger.getLogger(pf.appName);


    // Get the database and run the query
    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    boolean success = false;
    Cursor c = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(pf.appName, dbHandleName);
      updatePatchedFilter(db, pf);

      c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, pf.whereId, pf.whereIdArgs,
          null, null, sortOrder, null);

      if (c == null) {
        log.w(TAG, "Unable to query database");
        return null;
      }
      success = true;
      return c;
    } catch (Exception e) {
      log.w(TAG, "Exception while querying database");
      log.printStackTrace(e);
      return null;
    } finally {
      if (db != null) {
        try {
          db.releaseReference();
        } finally {
          if (!success) {
            // this closes the connection
            // if it was successful, then the InvalidateMonitor will close the connection
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(
                pf.appName, dbHandleName);
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
  private static synchronized int delete(@NonNull Uri uri, String where, String[]
      whereArgs) {

    List<String> segments = uri.getPathSegments();

    PatchedFilter pf = extractUriFeatures(uri, segments, where, whereArgs);
    WebLoggerIf logger = WebLogger.getLogger(pf.appName);

    String[] projection = { FormsColumns._ID, FormsColumns.TABLE_ID, FormsColumns.FORM_ID };

    HashMap<String, FormSpec> directories = new HashMap<String, FormSpec>();

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    Cursor c = null;

    Integer idValue = null;
    String tableIdValue = null;
    String formIdValue = null;
    try {
      // Get the database and run the query
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(pf.appName, dbHandleName);
      db.beginTransactionNonExclusive();
      updatePatchedFilter(db, pf);

      c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, pf.whereId, pf.whereIdArgs,
          null, null, null, null);

      if (c == null) {
        throw new SQLException("FAILED Delete into " + uri
            + " -- unable to query for existing records");
      }

      int idxId = c.getColumnIndex(FormsColumns._ID);
      int idxTableId = c.getColumnIndex(FormsColumns.TABLE_ID);
      int idxFormId = c.getColumnIndex(FormsColumns.FORM_ID);

      if (c.moveToFirst()) {
        do {
          idValue = CursorUtils.getIndexAsType(c, Integer.class, idxId);
          tableIdValue = CursorUtils.getIndexAsString(c, idxTableId);
          formIdValue = CursorUtils.getIndexAsString(c, idxFormId);
          FormSpec formSpec = new FormSpec();
          formSpec.tableId = tableIdValue;
          formSpec.formId = formIdValue;
          formSpec.success = false;
          directories.put(idValue.toString(), formSpec);
        } while (c.moveToNext());
      }
      c.close();
      c = null;

      // and now go through this list moving the directories
      // into the pending-deletion location and deleting them.
      for (Map.Entry<String, FormSpec> de : directories.entrySet()) {
        String id = de.getKey();
        FormSpec fs = de.getValue();

        File srcDir = new File(ODKFileUtils.getFormFolder(pf.appName, fs.tableId, fs.formId));
        File destDir = new File(ODKFileUtils.getPendingDeletionTablesFolder(pf.appName),
            fs.tableId + "." + fs.formId + "." + System.currentTimeMillis());

        try {
          if (db.delete(DatabaseConstants.FORMS_TABLE_NAME, FormsColumns._ID + "=?", new String[]{ id }) > 0) {
            fs.success = true;
          }
          ODKFileUtils.moveDirectory(srcDir, destDir);
        } catch (IOException e) {
          logger.e(TAG, "Unable to move directory prior to deleting it: " + e);
          logger.printStackTrace(e);
        }
      }

      // commit the transaction...
      db.setTransactionSuccessful();

    } catch (Exception e) {
      logger.w(TAG, "FAILED Delete from " + uri + " -- query for existing row failed: " + e);

      if (e instanceof SQLException) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Delete from " + uri + " -- query for existing row failed: "
            + e);
      }
    } finally {
      if (db != null) {
        try {
          try {
            if (c != null && !c.isClosed()) {
              c.close();
            }
          } finally {
            if (db.inTransaction()) {
              db.endTransaction();
            }
          }
        } finally {
          try {
            db.releaseReference();
          } finally {
            // this closes the connection
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(
                pf.appName, dbHandleName);
          }
        }
      }
    }

    // and now, go through all the files in the pending-deletion
    // directory and try to release them.

    File destFolder = new File(ODKFileUtils.getPendingDeletionTablesFolder(pf.appName));

    File[] delDirs = destFolder.listFiles();
    for (File formIdDir : delDirs) {
      try {
        ODKFileUtils.deleteDirectory(formIdDir);
      } catch (IOException e) {
        logger.e(TAG, "Unable to remove directory " + e);
        logger.printStackTrace(e);
      }
    }

    int failureCount = 0;
    for (Map.Entry<String, FormSpec> e : directories.entrySet()) {
      String id = e.getKey();
      FormSpec fs = e.getValue();
      if (fs.success) {
        Uri formUri =
            Uri.withAppendedPath(
                Uri.withAppendedPath(
                    Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, pf.appName),
                    fs.tableId), fs.formId);
        Uri idUri = Uri.withAppendedPath(
            Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, pf.appName),
            id);
      } else {
        ++failureCount;
      }
    }

    int count = directories.size();
    if (failureCount != 0) {
      throw new SQLiteException("Unable to delete all forms (" + (count - failureCount) + " of " + count + " deleted)");
    }
    return count;
  }

  private static synchronized int update(@NonNull Uri uri, ContentValues values,
                                         String where,
                                      String[] whereArgs) {

    List<String> segments = uri.getPathSegments();

    PatchedFilter pf = extractUriFeatures(uri, segments, where, whereArgs);
    WebLoggerIf logger = WebLogger.getLogger(pf.appName);

    /*
     * First, find out what records match this query. Replicate the
     * ContentValues if there are multiple tableIds/formIds involved
     * and the contentValues do not have formId and tableId specified.
     *
     * Otherwise, it is an error to specify the tableId or formId in
     * the ContentValues and have those not match the where results.
     *
     */
    String contentTableId = values != null && values.containsKey(FormsColumns.TABLE_ID) ?
        values.getAsString(FormsColumns.TABLE_ID) : null;
    String contentFormId = values != null && values.containsKey(FormsColumns.FORM_ID) ?
        values.getAsString(FormsColumns.FORM_ID) : null;

    HashMap<FormSpec, HashMap<String, Object>> matchedValues = new HashMap<FormSpec, HashMap<String, Object>>();

    DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();
    OdkConnectionInterface db = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(pf.appName, dbHandleName);
      db.beginTransactionNonExclusive();
      updatePatchedFilter(db, pf);

      Cursor c = null;
      try {
        c = db.query(DatabaseConstants.FORMS_TABLE_NAME, null, pf.whereId, pf.whereIdArgs,
            null, null, null, null);

        if (c == null) {
          throw new SQLException("FAILED Update of " + uri
              + " -- query for existing row did not return a cursor");
        }
        if (c.moveToFirst()) {
          int idxId = c.getColumnIndex(FormsColumns._ID);
          int idxTableId = c.getColumnIndex(FormsColumns.TABLE_ID);
          int idxFormId = c.getColumnIndex(FormsColumns.FORM_ID);

          Integer idValue = null;
          String tableIdValue = null;
          String formIdValue = null;

          do {
            idValue = CursorUtils.getIndexAsType(c, Integer.class, idxId);
            tableIdValue = CursorUtils.getIndexAsString(c, idxTableId);
            formIdValue = CursorUtils.getIndexAsString(c, idxFormId);

            if (contentTableId != null && !contentTableId.equals(tableIdValue)) {
              throw new SQLException("Modification of tableId for an existing form is prohibited");
            }
            if (contentFormId != null && !contentFormId.equals(formIdValue)) {
              throw new SQLException("Modification of formId for an existing form is prohibited");
            }

            HashMap<String, Object> cv = new HashMap<String, Object>();
            if (values != null) {
              for (String key : values.keySet()) {
                cv.put(key, values.get(key));
              }
            }
            cv.put(FormsColumns.TABLE_ID, tableIdValue);
            cv.put(FormsColumns.FORM_ID, formIdValue);
            for (int idx = 0; idx < c.getColumnCount(); ++idx) {
              String colName = c.getColumnName(idx);
              if (colName.equals(FormsColumns._ID)) {
                // don't insert the PK
                continue;
              }

              if (c.isNull(idx)) {
                cv.put(colName, null);
              } else {
                // everything else, we control...
                Class<?> dataType = CursorUtils.getIndexDataType(c, idx);
                if (dataType == String.class) {
                  cv.put(colName, CursorUtils.getIndexAsString(c, idx));
                } else if (dataType == Long.class) {
                  cv.put(colName, CursorUtils.getIndexAsType(c, Long.class, idx));
                } else if (dataType == Double.class) {
                  cv.put(colName, CursorUtils.getIndexAsType(c, Double.class, idx));
                }
              }
            }

            FormSpec formSpec = patchUpValues(pf.appName, cv);
            formSpec._id = idValue.toString();
            formSpec.success = false;
            matchedValues.put(formSpec, cv);

          } while (c.moveToNext());
        } else {
          // no match on where clause...
          return 0;
        }
      } finally {
        if (c != null && !c.isClosed()) {
          c.close();
        }
      }

      // go through the entries and update the database with these patched-up values...

      for (Map.Entry<FormSpec, HashMap<String, Object>> e : matchedValues.entrySet()) {
        FormSpec fs = e.getKey();
        HashMap<String, Object> cv = e.getValue();

        if (db.update(DatabaseConstants.FORMS_TABLE_NAME, cv,
            FormsColumns._ID + "=?", new String[]{ fs._id }) > 0) {
          fs.success = true;
        }
      }
      db.setTransactionSuccessful();

    } catch (Exception e) {
      logger.w(TAG, "FAILED Update of " + uri + " -- query for existing row failed: " + e);

      if (e instanceof SQLException) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Update of " + uri + " -- query for existing row failed: "
            + e);
      }
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
                pf.appName, dbHandleName);
          }
        }
      }
    }

    int failureCount = 0;
    for (FormSpec fs : matchedValues.keySet()) {
      if (fs.success) {
        Uri formUri =
            Uri.withAppendedPath(
                Uri.withAppendedPath(
                    Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, pf.appName),
                    fs.tableId), fs.formId);
        Uri idUri = Uri.withAppendedPath(
            Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, pf.appName),
            fs._id);
      } else {
        ++failureCount;
      }
    }

    int count = matchedValues.size();
    if (failureCount != 0) {
      throw new SQLiteException("Unable to update all forms (" + (count - failureCount) + " of " + count + " updated)");
    }
    return count;
  }

  private static class FormSpec {
    String tableId;
    String formId;
    boolean success = true;
    String _id = null;
  }

  private static class PatchedFilter {
    String appName;
    String tableId;
    String formId;
    String numericFormId;
    boolean isNumericFormId;
    boolean requiresBlankFormIdPatch;
    String whereId;
    String[] whereIdArgs;
  }

  public static boolean updateFormDir(String appName, String tableIdFilter) {

    boolean success = true;
    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // /////////////////////////////////////////
    // scan for new forms...

    File tablesDir = new File(ODKFileUtils.getTablesFolder(appName));

    File[] tableIdDirs = new File[1];
    tableIdDirs[0] = new File(ODKFileUtils.getTablesFolder(appName, tableIdFilter));

    List<File> formDirs = new ArrayList<>();
    for (File tableIdDir : tableIdDirs) {
      String tableId = tableIdDir.getName();

      File formDir = new File(ODKFileUtils.getFormsFolder(appName, tableId));
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
    // remove forms that no longer exist
    // remove the forms that haven't changed
    // from the discovered list
    removeStaleFormInfo(appName, tableIdFilter, formDirs);

    // this is the complete list of forms we need to scan and possibly add
    // to the FormsProvider
    for (int i = 0; i < formDirs.size(); ++i) {
      File formDir = formDirs.get(i);

      String formId = formDir.getName();
      String tableId = formDir.getParentFile().getParentFile().getName();

      // specifically target this form...
      WebLogger.getLogger(appName).i(TAG, "updateFormInfo: form: " + formDir.getAbsolutePath());

      boolean formSuccess = updateFormDir(appName, tableId, formId, formDir,
          ODKFileUtils.getPendingDeletionTablesFolder(appName) + File.separator);
      success = success && formSuccess;
    }

    return success;
  }

  /**
   * Remove definitions from the Forms database that are no longer present on
   * disk.
   */
  private static void removeStaleFormInfo(String appName, String tableIdFilter,
                                          List<File> discoveredFormDefDirs) {

    WebLogger.getLogger(appName).i(TAG, "removeStaleFormInfo " + appName + " begin");
    ArrayList<Uri> badEntries = new ArrayList<>();
    Cursor c = null;
    try {
      c = query(Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName), null, null, null, null);

      if (c == null) {
        WebLogger.getLogger(appName)
            .w(TAG, "removeStaleFormInfo " + appName + " null cursor returned from query.");
        return;
      }

      if (c.moveToFirst()) {
        do {
          String tableId = CursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.TABLE_ID));
          if ( !tableId.equals(tableIdFilter) ) {
            continue;
          }
          String formId = CursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.FORM_ID));
          Uri otherUri = Uri.withAppendedPath(
              Uri.withAppendedPath(Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName), tableId),
              formId);

          String formDir = ODKFileUtils.getFormFolder(appName, tableId, formId);
          File f = new File(formDir);
          File formDefJson = new File(f, ODKFileUtils.FORMDEF_JSON_FILENAME);
          if (!f.exists() || !f.isDirectory() || !formDefJson.exists() || !formDefJson.isFile()) {
            // the form definition does not exist
            badEntries.add(otherUri);
          } else {
            // ////////////////////////////////
            // formdef.json exists. See if it is
            // unchanged...
            String json_md5 = CursorUtils
                .getIndexAsString(c, c.getColumnIndex(FormsColumns.JSON_MD5_HASH));
            String fileMd5 = ODKFileUtils.getMd5Hash(appName, formDefJson);
            if (json_md5 != null && json_md5.equals(fileMd5)) {
              // it is unchanged -- no need to rescan it
              discoveredFormDefDirs.remove(f);
            }
          }
        } while (c.moveToNext());
      }
    } catch (Exception e) {
      WebLogger.getLogger(appName)
          .e(TAG, "removeStaleFormInfo " + appName + " exception: " + e.toString());
      WebLogger.getLogger(appName).printStackTrace(e);
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }

    // delete the other entries (and directories)
    for (Uri badUri : badEntries) {
      WebLogger.getLogger(appName)
          .i(TAG, "removeStaleFormInfo: " + appName + " deleting: " + badUri.toString());
      try {
        delete(badUri, null, null);
      } catch (Exception e) {
        WebLogger.getLogger(appName)
            .e(TAG, "removeStaleFormInfo " + appName + " exception: " + e.toString());
        WebLogger.getLogger(appName).printStackTrace(e);
        // and continue -- don't throw an error
      }
    }
    WebLogger.getLogger(appName).i(TAG, "removeStaleFormInfo " + appName + " end");
  }

  /**
   * Scan the given formDir and update the Forms database. If it is the
   * formsFolder, then any 'framework' forms should be forbidden. If it is not
   * the formsFolder, only 'framework' forms should be allowed
   *
   * @param tableId
   * @param formId
   * @param formDir
   * @param baseStaleMediaPath -- path prefix to the stale forms/framework directory.
   */
  private static boolean updateFormDir(String appName, String tableId, String formId,
                                    File formDir,
                                  String baseStaleMediaPath) {
    String formDirectoryPath = formDir.getAbsolutePath();
    WebLogger.getLogger(appName).i(TAG, "updateFormDir: " + formDirectoryPath);

    Cursor c = null;
    try {
      String selection = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
      String[] selectionArgs = { tableId, formId };
      c = query(Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName), null,
              selection,
              selectionArgs, null);

      if (c == null) {
        WebLogger.getLogger(appName)
            .w(TAG, "updateFormDir: " + formDirectoryPath + " null cursor -- cannot update!");
        return false;
      }

      if (c.getCount() > 1) {
        c.close();
        WebLogger.getLogger(appName).w(TAG, "updateFormDir: " + formDirectoryPath
            + " multiple records from cursor -- delete all and restore!");
        // we have multiple records for this one directory.
        // Rename the directory. Delete the records, and move the
        // directory back.
        File tempMediaPath = moveToStaleDirectory(formDir, baseStaleMediaPath);

        delete(Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName), selection,
                selectionArgs);

        ODKFileUtils.moveDirectory(tempMediaPath, formDir);

        ContentValues cv = new ContentValues();
        cv.put(FormsColumns.TABLE_ID, tableId);
        cv.put(FormsColumns.FORM_ID, formId);
        insert(Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName), cv);
      } else if (c.getCount() == 1) {
        c.close();
        ContentValues cv = new ContentValues();
        cv.put(FormsColumns.TABLE_ID, tableId);
        cv.put(FormsColumns.FORM_ID, formId);
        update(Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName), cv, null, null);
      } else if (c.getCount() == 0) {
        c.close();
        ContentValues cv = new ContentValues();
        cv.put(FormsColumns.TABLE_ID, tableId);
        cv.put(FormsColumns.FORM_ID, formId);
        insert(Uri.withAppendedPath(FormsProviderAPI.CONTENT_URI, appName), cv);
      }
    } catch (IOException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName)
          .e(TAG, "updateFormDir: " + formDirectoryPath + " exception: " + e.toString());
      return false;
    } catch (IllegalArgumentException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName)
          .e(TAG, "updateFormDir: " + formDirectoryPath + " exception: " + e.toString());
      try {
        ODKFileUtils.deleteDirectory(formDir);
        WebLogger.getLogger(appName).i(TAG,
            "updateFormDir: " + formDirectoryPath + " Removing -- unable to parse formDef file: "
                + e.toString());
      } catch (IOException e1) {
        WebLogger.getLogger(appName).printStackTrace(e1);
        WebLogger.getLogger(appName).i(TAG,
            "updateFormDir: " + formDirectoryPath + " Removing -- unable to delete form directory: "
                + formDir.getName() + " error: " + e.toString());
      }
      return false;
    } catch (Exception e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName)
          .e(TAG, "updateFormDir: " + formDirectoryPath + " exception: " + e.toString());
      return false;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return true;
  }

  /**
   * Construct a directory name that is unused in the stale path and move
   * mediaPath there.
   *
   * @param mediaPath
   * @param baseStaleMediaPath -- the stale directory corresponding to the mediaPath container
   * @return the directory within the stale directory that the mediaPath was
   * renamed to.
   * @throws IOException
   */
  private static File moveToStaleDirectory(File mediaPath, String baseStaleMediaPath) throws
      IOException {
    // we have a 'framework' form in the forms directory.
    // Move it to the stale directory.
    // Delete all records referring to this directory.
    int i = 0;
    File tempMediaPath = new File(
        baseStaleMediaPath + mediaPath.getName() + "_" + Integer.toString(i));
    while (tempMediaPath.exists()) {
      ++i;
      tempMediaPath = new File(
          baseStaleMediaPath + mediaPath.getName() + "_" + Integer.toString(i));
    }
    ODKFileUtils.moveDirectory(mediaPath, tempMediaPath);
    return tempMediaPath;
  }

}
