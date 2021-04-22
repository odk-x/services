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

package org.opendatakit.services.forms.provider;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.DataSetObserver;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;

import androidx.annotation.NonNull;

import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.LocalKeyValueStoreConstants;
import org.opendatakit.database.data.TableMetaDataEntries;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.util.List;

/**
 * This provider supports two types of path references to identify a given row:
 * <p>
 * getFormsAuthority() / appName / _ID
 * getFormsAuthority() / appName / tableId / formId
 * <p>
 * The former uses the internal PK that Android generates for tables.
 * The later uses the user-specified tableId and formId.
 * /**
 * This class provides a read-only view onto the set of
 * forms within the ODK toolsuite.
 */
public class FormsProvider extends ContentProvider {
  static final String t = "FormsProvider";

  /**
   * change to true expression if you want to debug this content provider
   */
  private static void possiblyWaitForContentProviderDebugger() {
    if (false) {
      android.os.Debug.waitForDebugger();
      int len = "for setting breakpoint".length();
    }
  }

  @Override
  public boolean onCreate() {

    // IMPORTANT NOTE: the Application object is not yet created!

    // Used to ensure that the singleton has been initialized properly
    AndroidConnectFactory.configure();

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

  private boolean isNumeric(final CharSequence charSequence) {
    if (charSequence == null) {
      return false;
    }

    int size = charSequence.length();
    if (size == 0) {
      return false;
    }

    for (int i = 0; i < size; i++) {
      if (!Character.isDigit(charSequence.charAt(i))) {
        return false;
      }
    }
    return true;
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
  private PatchedFilter extractUriFeatures(Uri uri, List<String> segments, String where, String[] whereArgs) {

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

    pf.isNumericFormId = isNumeric(pf.tableId);
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
  private void updatePatchedFilter(OdkConnectionInterface db, PatchedFilter pf) {
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

  @Override
  public String getType(@NonNull Uri uri) {
    List<String> segments = uri.getPathSegments();

    PatchedFilter pf = extractUriFeatures(uri, segments, null, null);

    if (pf.isNumericFormId || segments.size() == 3) {
      return FormsColumns.CONTENT_ITEM_TYPE;
    } else {
      return FormsColumns.CONTENT_TYPE;
    }
  }

  @Override
  public Cursor query(@NonNull Uri uri, String[] projection, String where, String[] whereArgs,
                      String sortOrder) {
    possiblyWaitForContentProviderDebugger();

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
        log.w(t, "Unable to query database");
        return null;
      }
      // Tell the cursor what uri to watch, so it knows when its source data changes
      if (getContext() != null) {
        c.setNotificationUri(getContext().getContentResolver(), uri);
      }
      c.registerDataSetObserver(new InvalidateMonitor(pf.appName, dbHandleName));
      success = true;
      return c;
    } catch (Exception e) {
      log.w(t, "Exception while querying database");
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

  @Override
  public synchronized int delete(@NonNull Uri uri, String where, String[] whereArgs) {
    throw new UnsupportedOperationException("delete is not supported");
  }

  @Override
  public synchronized int update(@NonNull Uri uri, ContentValues values, String where, String[] whereArgs) {
    throw new UnsupportedOperationException("update is not supported");
  }

  @Override
  public synchronized Uri insert(@NonNull Uri uri, ContentValues initialValues) {
    throw new UnsupportedOperationException("insert is not supported");
  }

  private static class InvalidateMonitor extends DataSetObserver {
    String appName;
    DbHandle dbHandleName;

    InvalidateMonitor(String appName, DbHandle dbHandleName) {
      this.appName = appName;
      this.dbHandleName = dbHandleName;
    }

    @Override
    public void onInvalidated() {
      super.onInvalidated();

      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(appName,
          dbHandleName);
    }
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
}
