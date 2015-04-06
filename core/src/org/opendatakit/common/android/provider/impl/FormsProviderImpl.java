/*
 * Copyright (C) 2007 The Android Open Source Project
 * Copyright (C) 2011-2013 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.common.android.provider.impl;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opendatakit.androidlibrary.R;
import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.database.OdkDatabase;
import org.opendatakit.common.android.logic.FormInfo;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.utilities.ODKCursorUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.core.application.Core;
import org.opendatakit.database.service.OdkDbHandle;
import org.sqlite.database.sqlite.SQLiteException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DataSetObserver;
import android.database.SQLException;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

/**
 * This provider supports two types of path references to identify a given row:
 * 
 * getFormsAuthority() / appName / _ID
 * getFormsAuthority() / appName / tableId / formId
 * 
 * The former uses the internal PK that Android generates for tables.
 * The later uses the user-specified tableId and formId.
 * 
 */
public abstract class FormsProviderImpl extends ContentProvider {
  static final String t = "FormsProvider";

  public abstract String getFormsAuthority();

  private class InvalidateMonitor extends DataSetObserver {
    String appName;
    OdkDbHandle dbHandleName;
    
    InvalidateMonitor(String appName, OdkDbHandle dbHandleName) {
      this.appName = appName;
      this.dbHandleName = dbHandleName;
    }
    
    @Override
    public void onInvalidated() {
      super.onInvalidated();
      DatabaseFactory.get().releaseDatabase(getContext(), appName, dbHandleName);
    }
  }

  class FormSpec {
    String tableId;
    String formId;
    boolean success = true;
    String _id = null;
  }

  @Override
  public boolean onCreate() {
    
    // IMPORTANT NOTE: the Application object is not yet created!

    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      File f = new File(ODKFileUtils.getOdkFolder());
      if (!f.exists()) {
        f.mkdir();
      } else if (!f.isDirectory()) {
        Log.e(t, f.getAbsolutePath() + " is not a directory!");
        return false;
      }
    } catch (Exception e) {
      Log.e(t, "External storage not available");
      return false;
    }

    return true;
  }
  
  private FormSpec patchUpValues(String appName, ContentValues values) {

    // require a tableId and formId...
    if ( !values.containsKey(FormsColumns.TABLE_ID)) {
      throw new IllegalArgumentException(FormsColumns.TABLE_ID + " is not specified");
    }
    String tableId = values.getAsString(FormsColumns.TABLE_ID);
    
    if ( !values.containsKey(FormsColumns.FORM_ID)) {
      throw new IllegalArgumentException(FormsColumns.FORM_ID + " is not specified");
    }
    String formId = values.getAsString(FormsColumns.FORM_ID);

    FormSpec formSpec = new FormSpec();
    formSpec.tableId = tableId;
    formSpec.formId = formId;
    
    String formFolder = ODKFileUtils.getFormFolder(appName, tableId, formId);

    File formDefFolder = new File(formFolder);
    
    if (values.containsKey(FormsColumns.DISPLAY_NAME) == false) {
      values.put(FormsColumns.DISPLAY_NAME, formDefFolder.getName());
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
      Long existingModificationDate = values.getAsLong(FormsColumns.DATE);
      Long existingFileLength = values.getAsLong(FormsColumns.FILE_LENGTH);
      
      // date is the last modification date of the formDef file
      Long now = formDefFile.lastModified();
      Long length = formDefFile.length();
      
      if ( now.equals(existingModificationDate) && length.equals(existingFileLength) ) {
        // assume everything is unchanged...
        return formSpec;
      }
    }
    
    // parse the formDef.json
    FormInfo fiFound = new FormInfo(getContext(), appName, formDefFile);

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

  @Override
  public synchronized Uri insert(Uri uri, ContentValues initialValues) {
    if ( Core.getInstance().shouldWaitForDebugger() ) {
      android.os.Debug.waitForDebugger();
    }

    List<String> segments = uri.getPathSegments();

    if (segments.size() != 1) {
      throw new IllegalArgumentException("Unknown URI (too many segments!) " + uri);
    }

    String appName = segments.get(0);
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(appName);
    WebLogger log = WebLogger.getLogger(appName);

    ContentValues values;
    if (initialValues != null) {
      values = new ContentValues(initialValues);
    } else {
      values = new ContentValues();
    }

    // force a scan from disk
    values.remove(FormsColumns.DATE);
    values.remove(FormsColumns.JSON_MD5_HASH);
    FormSpec formSpec = patchUpValues(appName, values);

    if (values.containsKey(FormsColumns.DISPLAY_SUBTEXT) == false) {
      Date today = new Date();
      String ts = new SimpleDateFormat(getContext().getString(R.string.added_on_date_at_time),
                                       Locale.getDefault()).format(today);
      values.put(FormsColumns.DISPLAY_SUBTEXT, ts);
    }

    // first try to see if a record with this filename already exists...
    String[] projection = { FormsColumns.TABLE_ID, FormsColumns.FORM_ID };
    String selection = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
    String[] selectionArgs = { formSpec.tableId, formSpec.formId };
    Cursor c = null;
    
    OdkDbHandle dbHandleName = DatabaseFactory.get().generateInternalUseDbHandle();
    OdkDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(getContext(), appName, dbHandleName);
      ODKDatabaseImplUtils.get().beginTransactionNonExclusive(db);
      try {
        c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, selection, selectionArgs,
            null, null, null, null);
        if (c == null) {
          throw new SQLException("FAILED Insert into " + uri
              + " -- unable to query for existing records. tableId=" + formSpec.tableId + " formId=" + formSpec.formId);
        }
        if (c.getCount() > 0) {
          // already exists
          throw new SQLException("FAILED Insert into " + uri
              + " -- row already exists for  tableId=" + formSpec.tableId + " formId=" + formSpec.formId);
        }
      } catch (Exception e) {
        log.w(t, "FAILED Insert into " + uri + " -- query for existing row failed: " + e.toString());

        if (e instanceof SQLException) {
          throw (SQLException) e;
        } else {
          throw new SQLException("FAILED Insert into " + uri + " -- query for existing row failed: "
              + e.toString());
        }
      } finally {
        if (c != null) {
          c.close();
        }
      }

      try {
        long rowId = db.insertOrThrow(DatabaseConstants.FORMS_TABLE_NAME, null, values);
        db.setTransactionSuccessful();
        // and notify listeners of the new row...
        Uri formUri = Uri.withAppendedPath(
            Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), appName),
            values.getAsString(FormsColumns.FORM_ID));
        getContext().getContentResolver().notifyChange(formUri, null);
        Uri idUri = Uri.withAppendedPath(
            Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), appName),
            Long.toString(rowId));
        getContext().getContentResolver().notifyChange(idUri, null);

        return formUri;
      } catch (Exception e) {
        log.w(t, "FAILED Insert into " + uri + " -- insert of row failed: " + e.toString());

        if (e instanceof SQLException) {
          throw (SQLException) e;
        } else {
          throw new SQLException("FAILED Insert into " + uri + " -- insert of row failed: "
              + e.toString());
        }
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("FAILED Insert into " + uri + " -- insert of row failed: "
          + e.toString());
    } finally {
      if ( db != null ) {
        db.endTransaction();
        db.close();
        DatabaseFactory.get().releaseDatabase(getContext(), appName, dbHandleName);
      }
    }
  }

  class PatchedFilter {
    String appName;
    String tableId;
    String formId;
    String numericFormId;
    boolean isNumericFormId;
    String whereId;
    String[] whereIdArgs;
  }
  
  private PatchedFilter extractUriFeatures( Uri uri, List<String> segments, String where, String[] whereArgs ) {

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
    pf.tableId = ((segments.size() >= 2) ? segments.get(1) : null);
    pf.isNumericFormId = StringUtils.isNumeric(pf.tableId);
    if ( pf.isNumericFormId ) {
      pf.numericFormId = pf.tableId;
      pf.tableId = null;
      if ( segments.size() == 3 ) {
        // user is trying to mix a /_ID uri with a /tableId/formId uri.
        throw new IllegalArgumentException("Unknown URI ( _ID cannot be combined with other segments!) " + uri);
      }
    }
    // and handle formId
    pf.formId = ((segments.size() == 3) ? segments.get(2) : null);

    // Modify the where clause to account for the presence of any additional segments
    if (segments.size() == 1) {
      // no segments -- directly use whatever filter the user specified
      pf.whereId = where;
      pf.whereIdArgs = whereArgs;
    } else if ( segments.size() == 2) {
      // either a tableId or a numericFormId is specified.
      // combine this filter with the where clause the user supplied.
      if (TextUtils.isEmpty(where)) {
        pf.whereId = (pf.isNumericFormId ? FormsColumns._ID : FormsColumns.TABLE_ID) + "=?";
        pf.whereIdArgs = new String[1];
        pf.whereIdArgs[0] = (pf.isNumericFormId ? pf.numericFormId : pf.tableId);
      } else {
        pf.whereId = (pf.isNumericFormId ? FormsColumns._ID : FormsColumns.TABLE_ID) + "=? AND (" + where
            + ")";
        pf.whereIdArgs = new String[whereArgs.length + 1];
        pf.whereIdArgs[0] =  (pf.isNumericFormId ? pf.numericFormId : pf.tableId);
        for (int i = 0; i < whereArgs.length; ++i) {
          pf.whereIdArgs[i + 1] = whereArgs[i];
        }
      }
    } else {
      // we have both a tableId and a formId.
      // combine with the filter clause the user supplied.
      if (TextUtils.isEmpty(where)) {
        pf.whereId = FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=?";
        pf.whereIdArgs = new String[2];
        pf.whereIdArgs[0] = pf.tableId;
        pf.whereIdArgs[1] = pf.formId;
      } else {
        pf.whereId =  FormsColumns.TABLE_ID + "=? AND " + FormsColumns.FORM_ID + "=? AND (" + where
            + ")";
        pf.whereIdArgs = new String[whereArgs.length + 2];
        pf.whereIdArgs[0] = pf.tableId;
        pf.whereIdArgs[1] = pf.formId;
        for (int i = 0; i < whereArgs.length; ++i) {
          pf.whereIdArgs[i + 2] = whereArgs[i];
        }
      }
    }
    
    return pf;
  }

  @Override
  public String getType(Uri uri) {
    List<String> segments = uri.getPathSegments();
    
    PatchedFilter pf = extractUriFeatures( uri, segments, null, null );

    if ( pf.isNumericFormId || segments.size() == 3) {
      return FormsColumns.CONTENT_ITEM_TYPE;
    } else {
      return FormsColumns.CONTENT_TYPE;
    }
  }
  
  @Override
  public Cursor query(Uri uri, String[] projection, String where, String[] whereArgs,
                      String sortOrder) {
    if ( Core.getInstance().shouldWaitForDebugger() ) {
      android.os.Debug.waitForDebugger();
    }

    List<String> segments = uri.getPathSegments();
    
    PatchedFilter pf = extractUriFeatures( uri, segments, where, whereArgs );
    WebLogger log = WebLogger.getLogger(pf.appName);


    // Get the database and run the query
    OdkDbHandle dbHandleName = DatabaseFactory.get().generateInternalUseDbHandle();
    OdkDatabase db = null;
    boolean success = false;
    Cursor c = null;
    try {
      db = DatabaseFactory.get().getDatabase(getContext(), pf.appName, dbHandleName);
      c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, pf.whereId, pf.whereIdArgs,
          null, null, sortOrder, null);
      success = true;
    } catch (Exception e) {
      log.w(t, "Unable to query database");
      return null;
    } finally {
      if ( !success && db != null ) {
        db.close();
        DatabaseFactory.get().releaseDatabase(getContext(), pf.appName, dbHandleName);
      }
    }

    if (c == null) {
      log.w(t, "Unable to query database");
      return null;
    }
    // Tell the cursor what uri to watch, so it knows when its source data
    // changes
    c.setNotificationUri(getContext().getContentResolver(), uri);
    c.registerDataSetObserver(new InvalidateMonitor(pf.appName, dbHandleName));
    return c;
  }

  /**
   * This method removes the entry from the content provider, and also removes
   * any associated files. files: form.xml, [formmd5].formdef, formname
   * {directory}
   */
  @Override
  public synchronized int delete(Uri uri, String where, String[] whereArgs) {
    if ( Core.getInstance().shouldWaitForDebugger() ) {
      android.os.Debug.waitForDebugger();
    }

    List<String> segments = uri.getPathSegments();
    
    PatchedFilter pf = extractUriFeatures( uri, segments, where, whereArgs );
    WebLogger log = WebLogger.getLogger(pf.appName);

    String[] projection = { FormsColumns._ID, FormsColumns.TABLE_ID, FormsColumns.FORM_ID };

    HashMap<String, FormSpec> directories = new HashMap<String, FormSpec>();

    OdkDbHandle dbHandleName = DatabaseFactory.get().generateInternalUseDbHandle();
    OdkDatabase db = null;
    Cursor c = null;
    
    Integer idValue = null;
    String tableIdValue = null;
    String formIdValue = null;
    try {
      // Get the database and run the query
      db = DatabaseFactory.get().getDatabase(getContext(), pf.appName, dbHandleName);
      ODKDatabaseImplUtils.get().beginTransactionNonExclusive(db);
      c = db.query(DatabaseConstants.FORMS_TABLE_NAME, projection, pf.whereId, pf.whereIdArgs,
          null, null, null, null);

      if (c == null) {
        throw new SQLException("FAILED Delete into " + uri
            + " -- unable to query for existing records");
      }
      
      int idxId = c.getColumnIndex(FormsColumns._ID);
      int idxTableId = c.getColumnIndex(FormsColumns.TABLE_ID);
      int idxFormId = c.getColumnIndex(FormsColumns.FORM_ID);
      
      if ( c.moveToFirst() ) {
        do {
          idValue = ODKCursorUtils.getIndexAsType(c, Integer.class, idxId);
          tableIdValue = ODKCursorUtils.getIndexAsString(c, idxTableId);
          formIdValue = ODKCursorUtils.getIndexAsString(c, idxFormId);
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
      for ( Entry<String, FormSpec> de : directories.entrySet() ) {
        String id = de.getKey();
        FormSpec fs = de.getValue();
        
        File srcDir = new File( ODKFileUtils.getFormFolder(pf.appName, fs.tableId, fs.formId));
        File destDir = new File( ODKFileUtils.getPendingDeletionTablesFolder(pf.appName),
                  fs.tableId + "." + fs.formId + "." + System.currentTimeMillis());
        
        try {
          FileUtils.moveDirectory(srcDir, destDir);
          if ( db.delete(DatabaseConstants.FORMS_TABLE_NAME, FormsColumns._ID + "=?", new String[] { id }) > 0 ) {
            fs.success = true;
          };
        } catch (IOException e) {
          e.printStackTrace();
          log.e(t, "Unable to move directory prior to deleting it: " + e.toString());
        }
      }
      
      // commit the transaction...
      db.setTransactionSuccessful();
      
    } catch (Exception e) {
      log.w(t, "FAILED Delete from " + uri + " -- query for existing row failed: " + e.toString());

      if (e instanceof SQLException) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Delete from " + uri + " -- query for existing row failed: "
            + e.toString());
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
      if ( db != null ) {
        db.endTransaction();
        db.close();
        DatabaseFactory.get().releaseDatabase(getContext(), pf.appName, dbHandleName);
      }
    }

    // and now, go through all the files in the pending-deletion 
    // directory and try to release them.

    File destFolder = new File( ODKFileUtils.getPendingDeletionTablesFolder(pf.appName));
    
    File[] delDirs = destFolder.listFiles();
    for (File formIdDir : delDirs) {
      try {
        FileUtils.deleteDirectory(formIdDir);
      } catch (IOException e) {
        e.printStackTrace();
        log.e(t, "Unable to remove directory " + e.toString());
      }
    }

    int failureCount = 0;
    for ( Entry<String, FormSpec> e : directories.entrySet() ) {
      String id = e.getKey();
      FormSpec fs = e.getValue();
      if ( fs.success ) {
        Uri formUri = 
          Uri.withAppendedPath(
            Uri.withAppendedPath(
              Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), pf.appName),
                fs.tableId), fs.formId);
        getContext().getContentResolver().notifyChange(formUri, null);
        Uri idUri = Uri.withAppendedPath(
            Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), pf.appName),
            id);
        getContext().getContentResolver().notifyChange(idUri, null);
      } else {
        ++failureCount;
      }
    }
    getContext().getContentResolver().notifyChange(uri, null);

    int count = directories.size();
    if (failureCount != 0) {
      throw new SQLiteException("Unable to delete all forms (" + (count-failureCount) + " of " + count + " deleted)");
    }
    return count;
  }

  @Override
  public synchronized int update(Uri uri, ContentValues values, String where, String[] whereArgs) {
    if ( Core.getInstance().shouldWaitForDebugger() ) {
      android.os.Debug.waitForDebugger();
    }

    List<String> segments = uri.getPathSegments();
    
    PatchedFilter pf = extractUriFeatures( uri, segments, where, whereArgs );
    WebLogger log = WebLogger.getLogger(pf.appName);

    /*
     * First, find out what records match this query. Replicate the 
     * ContentValues if there are multiple tableIds/formIds involved
     * and the contentValues do not have formId and tableId specified.
     * 
     * Otherwise, it is an error to specify the tableId or formId in
     * the ContentValues and have those not match the where results.
     * 
     */
    String contentTableId = (values != null && values.containsKey(FormsColumns.TABLE_ID)) ?
        values.getAsString(FormsColumns.TABLE_ID) : null;
    String contentFormId = (values != null && values.containsKey(FormsColumns.FORM_ID)) ?
        values.getAsString(FormsColumns.FORM_ID) : null;
        
    HashMap<FormSpec, ContentValues> matchedValues = new HashMap<FormSpec, ContentValues>();
    
    OdkDbHandle dbHandleName = DatabaseFactory.get().generateInternalUseDbHandle();
    OdkDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(getContext(), pf.appName, dbHandleName);
      ODKDatabaseImplUtils.get().beginTransactionNonExclusive(db);
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
            idValue = ODKCursorUtils.getIndexAsType(c, Integer.class, idxId);
            tableIdValue = ODKCursorUtils.getIndexAsString(c, idxTableId);
            formIdValue = ODKCursorUtils.getIndexAsString(c, idxFormId);
            
            if ( contentTableId != null && !contentTableId.equals(tableIdValue) ) {
              throw new SQLException("Modification of tableId for an existing form is prohibited");
            }
            if ( contentFormId != null && !contentFormId.equals(formIdValue) ) {
              throw new SQLException("Modification of formId for an existing form is prohibited");
            }
            
            ContentValues cv = (values == null) ? new ContentValues() : new ContentValues(values);
            cv.put(FormsColumns.TABLE_ID, tableIdValue);
            cv.put(FormsColumns.FORM_ID, formIdValue);
            for ( int idx = 0 ; idx < c.getColumnCount() ; ++idx ) {
              String colName = c.getColumnName(idx);
              if ( colName.equals(FormsColumns._ID)) {
                // don't insert the PK
                continue;
              }
              if ( colName.equals(FormsColumns.DISPLAY_SUBTEXT) && 
                   cv.containsKey(FormsColumns.DISPLAY_SUBTEXT) ) {
                // allow user to specify their own values
                continue;
              }
              
              // everything else, we control...
              Class<?> dataType = ODKCursorUtils.getIndexDataType(c, idx);
              if ( dataType == String.class ) {
                cv.put(colName, ODKCursorUtils.getIndexAsString(c, idx));
              } else if ( dataType == Long.class ) {
                cv.put(colName, ODKCursorUtils.getIndexAsType(c, Long.class, idx));
              } else if ( dataType == Double.class ) {
                cv.put(colName, ODKCursorUtils.getIndexAsType(c, Double.class, idx));
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
      
      for ( Entry<FormSpec, ContentValues> e : matchedValues.entrySet() ) {
        FormSpec fs = e.getKey();
        ContentValues cv = e.getValue();
        
        // Make sure that the necessary fields are all set
        if (cv.containsKey(FormsColumns.DATE) == true) {
          Date today = new Date();
          String ts = new SimpleDateFormat(getContext().getString(R.string.added_on_date_at_time),
                                           Locale.getDefault()).format(today);
          cv.put(FormsColumns.DISPLAY_SUBTEXT, ts);
        }
  
        if ( db.update(DatabaseConstants.FORMS_TABLE_NAME, cv, 
            FormsColumns._ID + "=?", new String[] { fs._id }) > 0 ) {
          fs.success = true;
        }
      }
      db.setTransactionSuccessful();
      
    } catch (Exception e) {
      log.w(t, "FAILED Update of " + uri + " -- query for existing row failed: " + e.toString());

      if (e instanceof SQLException) {
        throw (SQLException) e;
      } else {
        throw new SQLException("FAILED Update of " + uri + " -- query for existing row failed: "
            + e.toString());
      }
    } finally {
      if ( db != null ) {
        db.endTransaction();
        db.close();
        DatabaseFactory.get().releaseDatabase(getContext(), pf.appName, dbHandleName);
      }
    }


    int failureCount = 0;
    for ( FormSpec fs : matchedValues.keySet() ) {
      if ( fs.success ) {
        Uri formUri = 
          Uri.withAppendedPath(
            Uri.withAppendedPath(
              Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), pf.appName),
                fs.tableId), fs.formId);
        getContext().getContentResolver().notifyChange(formUri, null);
        Uri idUri = Uri.withAppendedPath(
            Uri.withAppendedPath(Uri.parse("content://" + getFormsAuthority()), pf.appName),
            fs._id);
        getContext().getContentResolver().notifyChange(idUri, null);
      } else {
        ++failureCount;
      }
    }
    getContext().getContentResolver().notifyChange(uri, null);

    int count = matchedValues.size();
    if (failureCount != 0) {
      throw new SQLiteException("Unable to update all forms (" + (count-failureCount) + " of " + count + " updated)");
    }
    return count;
  }
}
