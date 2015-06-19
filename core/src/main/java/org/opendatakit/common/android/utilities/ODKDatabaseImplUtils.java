/*
 * Copyright (C) 2014 University of Washington
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
package org.opendatakit.common.android.utilities;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.ElementType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColorRule;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.Row;
import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.database.OdkDatabase;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.InstanceColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;
import org.opendatakit.database.service.KeyValueStoreEntry;
import org.sqlite.database.sqlite.SQLiteException;

import android.content.ContentValues;
import android.database.Cursor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class ODKDatabaseImplUtils {

  private static final String t = "ODKDatabaseImplUtils";

  /*
   * These are the columns that are present in any row in the database. Each row
   * should have these in addition to the user-defined columns. If you add a
   * column here you have to be sure to also add it in the create table
   * statement, which can't be programmatically created easily.
   */
  private static final List<String> ADMIN_COLUMNS;

  /**
   * These are the columns that should be exported
   */
  private static final List<String> EXPORT_COLUMNS;

  static {
    ArrayList<String> adminColumns = new ArrayList<String>();
    adminColumns.add(DataTableColumns.ID);
    adminColumns.add(DataTableColumns.ROW_ETAG);
    adminColumns.add(DataTableColumns.SYNC_STATE); // not exportable
    adminColumns.add(DataTableColumns.CONFLICT_TYPE); // not exportable
    adminColumns.add(DataTableColumns.FILTER_TYPE);
    adminColumns.add(DataTableColumns.FILTER_VALUE);
    adminColumns.add(DataTableColumns.FORM_ID);
    adminColumns.add(DataTableColumns.LOCALE);
    adminColumns.add(DataTableColumns.SAVEPOINT_TYPE);
    adminColumns.add(DataTableColumns.SAVEPOINT_TIMESTAMP);
    adminColumns.add(DataTableColumns.SAVEPOINT_CREATOR);
    Collections.sort(adminColumns);
    ADMIN_COLUMNS = Collections.unmodifiableList(adminColumns);

    ArrayList<String> exportColumns = new ArrayList<String>();
    exportColumns.add(DataTableColumns.ID);
    exportColumns.add(DataTableColumns.ROW_ETAG);
    exportColumns.add(DataTableColumns.FILTER_TYPE);
    exportColumns.add(DataTableColumns.FILTER_VALUE);
    exportColumns.add(DataTableColumns.FORM_ID);
    exportColumns.add(DataTableColumns.LOCALE);
    exportColumns.add(DataTableColumns.SAVEPOINT_TYPE);
    exportColumns.add(DataTableColumns.SAVEPOINT_TIMESTAMP);
    exportColumns.add(DataTableColumns.SAVEPOINT_CREATOR);
    Collections.sort(exportColumns);
    EXPORT_COLUMNS = Collections.unmodifiableList(exportColumns);
  }

  private static ODKDatabaseImplUtils databaseUtil = new ODKDatabaseImplUtils();

  static {
    // register a state-reset manipulator for 'databaseUtil' field.
    StaticStateManipulator.get().register(50, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        databaseUtil = new ODKDatabaseImplUtils();
      }

    });
  }

  public static ODKDatabaseImplUtils get() {
    return databaseUtil;
  }

  /**
   * For mocking -- supply a mocked object.
   * 
   * @param util
   */
  public static void set(ODKDatabaseImplUtils util) {
    databaseUtil = util;
  }

  protected ODKDatabaseImplUtils() {
  }

  /**
   * Return an unmodifiable list of the admin columns that must be present in
   * every database table.
   * 
   * @return
   */
  public List<String> getAdminColumns() {
    return ADMIN_COLUMNS;
  }

  /**
   * Return an unmodifiable list of the admin columns that should be exported to
   * a CSV file. This list excludes the SYNC_STATE and CONFLICT_TYPE columns.
   * 
   * @return
   */
  public List<String> getExportColumns() {
    return EXPORT_COLUMNS;
  }

  public void beginTransactionNonExclusive(OdkDatabase db) {
    boolean success = false;
    try {
      db.beginTransactionNonExclusive();
      success = true;
    } finally {
      if (!success) {
        DatabaseFactory.get().dumpInfo();
      }
    }
  }

  /**
   * Perform a raw query with bind parameters.
   * 
   * @param db
   * @param sql
   * @param selectionArgs
   * @return
   */
  public Cursor rawQuery(OdkDatabase db, String sql, String[] selectionArgs) {
    Cursor c = db.rawQuery(sql, selectionArgs);
    return c;
  }

  /**
   * Perform a query with the given parameters.
   * 
   * @param db
   * @param distinct
   *          true if each returned row should be distinct (collapse duplicates)
   * @param table
   * @param columns
   * @param selection
   * @param selectionArgs
   * @param groupBy
   * @param having
   * @param orderBy
   * @param limit
   * @return
   */
  public Cursor queryDistinct(OdkDatabase db, String table, String[] columns, String selection,
      String[] selectionArgs, String groupBy, String having, String orderBy, String limit)
      throws SQLiteException {
    Cursor c = db.queryDistinct(table, columns, selection, selectionArgs, groupBy, having, orderBy,
        limit);
    return c;
  }

  public Cursor query(OdkDatabase db, String table, String[] columns, String selection,
      String[] selectionArgs, String groupBy, String having, String orderBy, String limit)
      throws SQLiteException {
    Cursor c = db.query(table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
    return c;
  }

  /**
   * Get a {@link UserTable} for this table based on the given where clause. All
   * columns from the table are returned.
   * <p>
   * SELECT * FROM table WHERE whereClause GROUP BY groupBy[]s HAVING
   * havingClause ORDER BY orderbyElement orderByDirection
   * <p>
   * If any of the clause parts are omitted (null), then the appropriate
   * simplified SQL statement is constructed.
   * 
   * @param db
   * @param appName
   * @param tableId
   * @param columnDefns
   * @param whereClause
   *          the whereClause for the selection, beginning with "WHERE". Must
   *          include "?" instead of actual values, which are instead passed in
   *          the selectionArgs.
   * @param selectionArgs
   *          an array of string values for bind parameters
   * @param groupBy
   *          an array of elementKeys
   * @param having
   * @param orderByElementKey
   *          elementKey to order the results by
   * @param orderByDirection
   *          either "ASC" or "DESC"
   * @return
   */
  public UserTable rawSqlQuery(OdkDatabase db, String appName, String tableId,
      OrderedColumns columnDefns, String whereClause, String[] selectionArgs, String[] groupBy,
      String having, String orderByElementKey, String orderByDirection) {
    Cursor c = null;
    try {
      StringBuilder s = new StringBuilder();
      s.append("SELECT * FROM \"").append(tableId).append("\" ");
      if (whereClause != null && whereClause.length() != 0) {
        s.append(" WHERE ").append(whereClause);
      }
      if (groupBy != null && groupBy.length != 0) {
        s.append(" GROUP BY ");
        boolean first = true;
        for (String elementKey : groupBy) {
          if (!first) {
            s.append(", ");
          }
          first = false;
          s.append(elementKey);
        }
        if (having != null && having.length() != 0) {
          s.append(" HAVING ").append(having);
        }
      }
      if (orderByElementKey != null && orderByElementKey.length() != 0) {
        s.append(" ORDER BY ").append(orderByElementKey);
        if (orderByDirection != null && orderByDirection.length() != 0) {
          s.append(" ").append(orderByDirection);
        } else {
          s.append(" ASC");
        }
      }
      String sqlQuery = s.toString();
      c = db.rawQuery(sqlQuery, selectionArgs);
      UserTable table = buildUserTable(c, columnDefns, whereClause, selectionArgs, groupBy, having,
          orderByElementKey, orderByDirection);
      return table;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
  }

  private UserTable buildUserTable(Cursor c, OrderedColumns columnDefns, String whereClause,
      String[] selectionArgs, String[] groupBy, String having, String orderByElementKey,
      String orderByDirection) {

    UserTable userTable = null;

    int rowIdIndex = c.getColumnIndexOrThrow(DataTableColumns.ID);
    // These maps will map the element key to the corresponding index in
    // either data or metadata. If the user has defined a column with the
    // element key _my_data, and this column is at index 5 in the data
    // array, dataKeyToIndex would then have a mapping of _my_data:5.
    // The sync_state column, if present at index 7, would have a mapping
    // in metadataKeyToIndex of sync_state:7.
    List<String> adminCols = ODKDatabaseImplUtils.get().getAdminColumns();
    String[] mAdminColumnOrder = adminCols.toArray(new String[adminCols.size()]);
    HashMap<String, Integer> mElementKeyToIndex = new HashMap<String, Integer>();
    List<String> userColumnOrder = columnDefns.getRetentionColumnNames();
    String[] mElementKeyForIndex = new String[userColumnOrder.size() + mAdminColumnOrder.length];
    int[] cursorIndex = new int[userColumnOrder.size() + mAdminColumnOrder.length];
    int i = 0;
    for (i = 0; i < userColumnOrder.size(); i++) {
      String elementKey = userColumnOrder.get(i);
      mElementKeyForIndex[i] = elementKey;
      mElementKeyToIndex.put(elementKey, i);
      cursorIndex[i] = c.getColumnIndex(elementKey);
    }

    for (int j = 0; j < mAdminColumnOrder.length; j++) {
      // TODO: problem is here. unclear how to best get just the
      // metadata in here. hmm.
      String elementKey = mAdminColumnOrder[j];
      mElementKeyForIndex[i + j] = elementKey;
      mElementKeyToIndex.put(elementKey, i + j);
      cursorIndex[i + j] = c.getColumnIndex(elementKey);
    }

    c.moveToFirst();
    int rowCount = c.getCount();

    userTable = new UserTable(columnDefns, whereClause, selectionArgs, groupBy, having,
        orderByElementKey, orderByDirection, mAdminColumnOrder, mElementKeyToIndex,
        mElementKeyForIndex, rowCount);

    String[] rowData = new String[userColumnOrder.size() + mAdminColumnOrder.length];
    if (c.moveToFirst()) {
      do {
        if (c.isNull(rowIdIndex)) {
          throw new IllegalStateException("Unexpected null value for rowId");
        }
        String rowId = ODKCursorUtils.getIndexAsString(c, rowIdIndex);
        // First get the user-defined data for this row.
        for (i = 0; i < cursorIndex.length; i++) {
          String value = ODKCursorUtils.getIndexAsString(c, cursorIndex[i]);
          rowData[i] = value;
        }
        Row nextRow = new Row(userTable, rowId, rowData.clone());
        userTable.addRow(nextRow);
      } while (c.moveToNext());
    }
    c.close();
    return userTable;
  }

  /**
   * Return the row(s) for the given tableId and rowId. If the row has
   * checkpoints or conflicts, the returned UserTable will have more than one
   * Row returned. Otherwise, it will contain a single row.
   * 
   * @param db
   * @param appName
   * @param tableId
   * @param orderedDefns
   * @param rowId
   * @return
   */
  public UserTable getDataInExistingDBTableWithId(OdkDatabase db, String appName, String tableId,
      OrderedColumns orderedDefns, String rowId) {

    UserTable table = rawSqlQuery(db, appName, tableId, orderedDefns, DataTableColumns.ID + "=?",
        new String[] { rowId }, null, null, DataTableColumns.SAVEPOINT_TIMESTAMP, "DESC");

    return table;
  }

  /**
   * Return all the columns in the given table, including any metadata columns.
   * This does a direct query against the database and is suitable for accessing
   * non-managed tables. It does not access any metadata and therefore will not
   * report non-unit-of-retention (grouping) columns.
   * 
   * @param db
   * @param tableId
   * @return
   */
  public String[] getAllColumnNames(OdkDatabase db, String tableId) {
    Cursor cursor = db.rawQuery("SELECT * FROM " + tableId + " LIMIT 1", null);
    String[] colNames = cursor.getColumnNames();

    return colNames;
  }

  /**
   * Retrieve the list of user-defined columns for a tableId using the metadata
   * for that table. Returns the unit-of-retention and non-unit-of-retention
   * (grouping) columns.
   * 
   * @param db
   * @param appName
   * @param tableId
   * @return
   */
  public OrderedColumns getUserDefinedColumns(OdkDatabase db, String appName, String tableId) {
    ArrayList<Column> userDefinedColumns = new ArrayList<Column>();
    String selection = ColumnDefinitionsColumns.TABLE_ID + "=?";
    String[] selectionArgs = { tableId };
    //@formatter:off
    String[] cols = { 
        ColumnDefinitionsColumns.ELEMENT_KEY, 
        ColumnDefinitionsColumns.ELEMENT_NAME,
        ColumnDefinitionsColumns.ELEMENT_TYPE, 
        ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS
      };
    //@formatter:on
    Cursor c = null;
    try {
      c = db.query(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME, cols, selection, selectionArgs,
          null, null, ColumnDefinitionsColumns.ELEMENT_KEY + " ASC", null);

      int elemKeyIndex = c.getColumnIndexOrThrow(ColumnDefinitionsColumns.ELEMENT_KEY);
      int elemNameIndex = c.getColumnIndexOrThrow(ColumnDefinitionsColumns.ELEMENT_NAME);
      int elemTypeIndex = c.getColumnIndexOrThrow(ColumnDefinitionsColumns.ELEMENT_TYPE);
      int listChildrenIndex = c
          .getColumnIndexOrThrow(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS);
      c.moveToFirst();
      while (!c.isAfterLast()) {
        String elementKey = ODKCursorUtils.getIndexAsString(c, elemKeyIndex);
        String elementName = ODKCursorUtils.getIndexAsString(c, elemNameIndex);
        String elementType = ODKCursorUtils.getIndexAsString(c, elemTypeIndex);
        String listOfChildren = ODKCursorUtils.getIndexAsString(c, listChildrenIndex);
        userDefinedColumns.add(new Column(elementKey, elementName, elementType, listOfChildren));
        c.moveToNext();
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return new OrderedColumns(appName, tableId, userDefinedColumns);
  }

  /**
   * Verifies that the tableId exists in the database.
   *
   * @param db
   * @param tableId
   * @return true if table is listed in table definitions.
   */
  public boolean hasTableId(OdkDatabase db, String tableId) {
    Cursor c = null;
    try {
      //@formatter:off
      c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME, null, 
          TableDefinitionsColumns.TABLE_ID + "=?", 
          new String[] { tableId }, null, null, null, null);
      //@formatter:on

      if (c.moveToFirst()) {
        // we know about the table...
        // tableId is the database table name...
        return true;
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return false;
  }

  /**
   * Return the health of a data table. The health can be one of
   * <ul>
   * <li>TABLE_HEALTH_IS_CLEAN = 0</li>
   * <li>TABLE_HEALTH_HAS_CONFLICTS = 1</li>
   * <li>TABLE_HEALTH_HAS_CHECKPOINTS = 2</li>
   * <li>TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS = 3</li>
   * <ul>
   * 
   * @param db
   * @param tableId
   * @return
   */
  public int getTableHealth(OdkDatabase db, String tableId) {
    StringBuilder b = new StringBuilder();
    b.append("SELECT SUM(case when _savepoint_type is null then 1 else 0 end) as checkpoints,")
        .append("SUM(case when _conflict_type is not null then 1 else 0 end) as conflicts from \"")
        .append(tableId).append("\"");

    Cursor c = null;
    try {
      c = db.rawQuery(b.toString(), null);
      int idxCheckpoints = c.getColumnIndex("checkpoints");
      int idxConflicts = c.getColumnIndex("conflicts");
      c.moveToFirst();
      Integer checkpoints = ODKCursorUtils.getIndexAsType(c, Integer.class, idxCheckpoints);
      Integer conflicts = ODKCursorUtils.getIndexAsType(c, Integer.class, idxConflicts);
      c.close();

      int outcome = ODKCursorUtils.TABLE_HEALTH_IS_CLEAN;
      if (checkpoints != null && checkpoints != 0) {
        outcome += ODKCursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS;
      }
      if (conflicts != null && conflicts != 0) {
        outcome += ODKCursorUtils.TABLE_HEALTH_HAS_CONFLICTS;
      }
      return outcome;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
  }

  /**
   * Return all the tableIds in the database.
   * 
   * @param db
   * @return an ArrayList<String> of tableIds
   */
  public ArrayList<String> getAllTableIds(OdkDatabase db) {
    ArrayList<String> tableIds = new ArrayList<String>();
    Cursor c = null;
    try {
      c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME,
          new String[] { TableDefinitionsColumns.TABLE_ID }, null, null, null, null,
          TableDefinitionsColumns.TABLE_ID + " ASC", null);

      if (c.moveToFirst()) {
        int idxId = c.getColumnIndex(TableDefinitionsColumns.TABLE_ID);
        do {
          String tableId = c.getString(idxId);
          if (tableId == null || tableId.length() == 0) {
            c.close();
            throw new IllegalStateException("getAllTableIds: Unexpected tableId found!");
          }
          tableIds.add(tableId);
        } while (c.moveToNext());
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return tableIds;
  }

  /**
   * Drop the given tableId and remove all the files (both configuration and
   * data attachments) associated with that table.
   * 
   * @param db
   * @param appName
   * @param tableId
   */
  public void deleteDBTableAndAllData(OdkDatabase db, final String appName, final String tableId) {

    SyncETagsUtils seu = new SyncETagsUtils();
    boolean dbWithinTransaction = db.inTransaction();
    try {
      String whereClause = TableDefinitionsColumns.TABLE_ID + " = ?";
      String[] whereArgs = { tableId };

      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      // Drop the table used for the formId
      db.execSQL("DROP TABLE IF EXISTS \"" + tableId + "\";");

      // Delete the server sync ETags associated with this table
      seu.deleteAllSyncETagsForTableId(db, tableId);

      // Delete the table definition for the tableId
      int count = db.delete(DatabaseConstants.TABLE_DEFS_TABLE_NAME, whereClause, whereArgs);

      // Delete the column definitions for this tableId
      db.delete(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME, whereClause, whereArgs);

      // Delete the uploads for the tableId
      String uploadWhereClause = InstanceColumns.DATA_TABLE_TABLE_ID + " = ?";
      db.delete(DatabaseConstants.UPLOADS_TABLE_NAME, uploadWhereClause, whereArgs);

      // Delete the values from the 4 key value stores
      db.delete(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, whereClause, whereArgs);
      db.delete(DatabaseConstants.KEY_VALULE_STORE_SYNC_TABLE_NAME, whereClause, whereArgs);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }

    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }

    // And delete the files from the SDCard...
    String tableDir = ODKFileUtils.getTablesFolder(appName, tableId);
    try {
      FileUtils.deleteDirectory(new File(tableDir));
    } catch (IOException e1) {
      e1.printStackTrace();
      throw new IllegalStateException("Unable to delete the " + tableDir + " directory", e1);
    }

    String assetsCsvDir = ODKFileUtils.getAssetsCsvFolder(appName);
    try {
      Collection<File> files = FileUtils.listFiles(new File(assetsCsvDir), new IOFileFilter() {

        @Override
        public boolean accept(File file) {
          String[] parts = file.getName().split("\\.");
          return (parts[0].equals(tableId) && parts[parts.length - 1].equals("csv") && (parts.length == 2
              || parts.length == 3 || (parts.length == 4 && parts[parts.length - 2]
              .equals("properties"))));
        }

        @Override
        public boolean accept(File dir, String name) {
          String[] parts = name.split("\\.");
          return (parts[0].equals(tableId) && parts[parts.length - 1].equals("csv") && (parts.length == 2
              || parts.length == 3 || (parts.length == 4 && parts[parts.length - 2]
              .equals("properties"))));
        }
      }, new IOFileFilter() {

        // don't traverse into directories
        @Override
        public boolean accept(File arg0) {
          return false;
        }

        // don't traverse into directories
        @Override
        public boolean accept(File arg0, String arg1) {
          return false;
        }
      });

      FileUtils.deleteDirectory(new File(tableDir));
      for (File f : files) {
        FileUtils.deleteQuietly(f);
      }
    } catch (IOException e1) {
      e1.printStackTrace();
      throw new IllegalStateException("Unable to delete the " + tableDir + " directory", e1);
    }
  }

  /**
   * Update the schema and data-modification ETags of a given tableId.
   * 
   * @param db
   * @param tableId
   * @param schemaETag
   * @param lastDataETag
   */
  public void updateDBTableETags(OdkDatabase db, String tableId, String schemaETag,
      String lastDataETag) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.SCHEMA_ETAG, schemaETag);
    cvTableDef.put(TableDefinitionsColumns.LAST_DATA_ETAG, lastDataETag);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }
      db.update(DatabaseConstants.TABLE_DEFS_TABLE_NAME, cvTableDef,
          TableDefinitionsColumns.TABLE_ID + "=?", new String[] { tableId });

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }

    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Update the timestamp of the last entirely-successful synchronization
   * attempt of this table.
   * 
   * @param db
   * @param tableId
   */
  public void updateDBTableLastSyncTime(OdkDatabase db, String tableId) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME,
        TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }
      db.update(DatabaseConstants.TABLE_DEFS_TABLE_NAME, cvTableDef,
          TableDefinitionsColumns.TABLE_ID + "=?", new String[] { tableId });

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Get the table definition entry for a tableId. This specifies the schema
   * ETag, the data-modification ETag, and the date-time of the last successful
   * sync of the table to the server.
   * 
   * @param db
   * @param tableId
   * @return
   */
  public TableDefinitionEntry getTableDefinitionEntry(OdkDatabase db, String tableId) {

    TableDefinitionEntry e = null;
    Cursor c = null;
    try {
      StringBuilder b = new StringBuilder();
      ArrayList<String> selArgs = new ArrayList<String>();
      b.append(KeyValueStoreColumns.TABLE_ID).append("=?");
      selArgs.add(tableId);

      c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME, null, b.toString(),
          selArgs.toArray(new String[selArgs.size()]), null, null, null, null);
      if (c.moveToFirst()) {
        int idxSchemaETag = c.getColumnIndex(TableDefinitionsColumns.SCHEMA_ETAG);
        int idxLastDataETag = c.getColumnIndex(TableDefinitionsColumns.LAST_DATA_ETAG);
        int idxLastSyncTime = c.getColumnIndex(TableDefinitionsColumns.LAST_SYNC_TIME);

        if (c.getCount() != 1) {
          throw new IllegalStateException(
              "Two or more TableDefinitionEntry records found for tableId " + tableId);
        }

        e = new TableDefinitionEntry(tableId);
        e.setSchemaETag(c.getString(idxSchemaETag));
        e.setLastDataETag(c.getString(idxLastDataETag));
        e.setLastSyncTime(c.getString(idxLastSyncTime));
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return e;
  }

  /**
   * Insert or update a single table-level metadata KVS entry.
   * 
   * @param db
   * @param entry
   */
  public void replaceDBTableMetadata(OdkDatabase db, KeyValueStoreEntry entry) {
    ContentValues values = new ContentValues();
    values.put(KeyValueStoreColumns.TABLE_ID, entry.tableId);
    values.put(KeyValueStoreColumns.PARTITION, entry.partition);
    values.put(KeyValueStoreColumns.ASPECT, entry.aspect);
    values.put(KeyValueStoreColumns.VALUE_TYPE, entry.type);
    values.put(KeyValueStoreColumns.VALUE, entry.value);
    values.put(KeyValueStoreColumns.KEY, entry.key);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }
      db.replaceOrThrow(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, values);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Insert or update a list of table-level metadata KVS entries. If clear is
   * true, then delete the existing set of values for this tableId before
   * inserting the new values.
   * 
   * @param db
   * @param tableId
   * @param metadata
   *          a List<KeyValueStoreEntry>
   * @param clear
   *          if true then delete the existing set of values for this tableId
   *          before inserting the new ones.
   */
  public void replaceDBTableMetadata(OdkDatabase db, String tableId,
      List<KeyValueStoreEntry> metadata, boolean clear) {

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      if (clear) {
        db.delete(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME,
            KeyValueStoreColumns.TABLE_ID + "=?", new String[] { tableId });
      }
      for (KeyValueStoreEntry e : metadata) {
        ContentValues values = new ContentValues();
        if (!tableId.equals(e.tableId)) {
          throw new IllegalArgumentException(
              "updateDBTableMetadata: expected all kvs entries to share the same tableId");
        }
        if (e.value == null || e.value.trim().length() == 0) {
          deleteDBTableMetadata(db, e.tableId, e.partition, e.aspect, e.key);
        } else {
          values.put(KeyValueStoreColumns.TABLE_ID, e.tableId);
          values.put(KeyValueStoreColumns.PARTITION, e.partition);
          values.put(KeyValueStoreColumns.ASPECT, e.aspect);
          values.put(KeyValueStoreColumns.KEY, e.key);
          values.put(KeyValueStoreColumns.VALUE_TYPE, e.type);
          values.put(KeyValueStoreColumns.VALUE, e.value);
          db.replaceOrThrow(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, values);
        }
      }

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * The deletion filter includes all non-null arguments. If all arguments
   * (except the db) are null, then all properties are removed.
   * 
   * @param db
   * @param tableId
   * @param partition
   * @param aspect
   * @param key
   */
  public void deleteDBTableMetadata(OdkDatabase db, String tableId, String partition,
      String aspect, String key) {

    StringBuilder b = new StringBuilder();
    ArrayList<String> selArgs = new ArrayList<String>();
    if (tableId != null) {
      b.append(KeyValueStoreColumns.TABLE_ID).append("=?");
      selArgs.add(tableId);
    }
    if (partition != null) {
      if (b.length() != 0) {
        b.append(" AND ");
      }
      b.append(KeyValueStoreColumns.PARTITION).append("=?");
      selArgs.add(partition);
    }
    if (aspect != null) {
      if (b.length() != 0) {
        b.append(" AND ");
      }
      b.append(KeyValueStoreColumns.ASPECT).append("=?");
      selArgs.add(aspect);
    }
    if (key != null) {
      if (b.length() != 0) {
        b.append(" AND ");
      }
      b.append(KeyValueStoreColumns.KEY).append("=?");
      selArgs.add(key);
    }

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      db.delete(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, b.toString(),
          selArgs.toArray(new String[selArgs.size()]));

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Filters results by all non-null field values.
   * 
   * @param db
   * @param tableId
   * @param partition
   * @param aspect
   * @param key
   * @return
   */
  public ArrayList<KeyValueStoreEntry> getDBTableMetadata(OdkDatabase db, String tableId,
      String partition, String aspect, String key) {

    ArrayList<KeyValueStoreEntry> entries = new ArrayList<KeyValueStoreEntry>();

    Cursor c = null;
    try {
      StringBuilder b = new StringBuilder();
      ArrayList<String> selArgs = new ArrayList<String>();
      if (tableId != null) {
        b.append(KeyValueStoreColumns.TABLE_ID).append("=?");
        selArgs.add(tableId);
      }
      if (partition != null) {
        if (b.length() != 0) {
          b.append(" AND ");
        }
        b.append(KeyValueStoreColumns.PARTITION).append("=?");
        selArgs.add(partition);
      }
      if (aspect != null) {
        if (b.length() != 0) {
          b.append(" AND ");
        }
        b.append(KeyValueStoreColumns.ASPECT).append("=?");
        selArgs.add(aspect);
      }
      if (key != null) {
        if (b.length() != 0) {
          b.append(" AND ");
        }
        b.append(KeyValueStoreColumns.KEY).append("=?");
        selArgs.add(key);
      }

      c = db.query(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, b.toString(),
          selArgs.toArray(new String[selArgs.size()]), null, null, null, null);
      if (c.moveToFirst()) {
        int idxTableId = c.getColumnIndex(KeyValueStoreColumns.TABLE_ID);
        int idxPartition = c.getColumnIndex(KeyValueStoreColumns.PARTITION);
        int idxAspect = c.getColumnIndex(KeyValueStoreColumns.ASPECT);
        int idxKey = c.getColumnIndex(KeyValueStoreColumns.KEY);
        int idxType = c.getColumnIndex(KeyValueStoreColumns.VALUE_TYPE);
        int idxValue = c.getColumnIndex(KeyValueStoreColumns.VALUE);

        do {
          KeyValueStoreEntry e = new KeyValueStoreEntry();
          e.tableId = c.getString(idxTableId);
          e.partition = c.getString(idxPartition);
          e.aspect = c.getString(idxAspect);
          e.key = c.getString(idxKey);
          e.type = c.getString(idxType);
          e.value = c.getString(idxValue);
          entries.add(e);
        } while (c.moveToNext());
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return entries;
  }

  /**
   * Clean up the KVS row data types. This simplifies the migration process by
   * enforcing the proper data types regardless of what the values are in the
   * imported CSV files.
   * 
   * @param db
   * @param tableId
   */
  public void enforceTypesDBTableMetadata(OdkDatabase db, String tableId) {

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      StringBuilder b = new StringBuilder();
      b.setLength(0);
      //@formatter:off
      b.append("UPDATE \"").append(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME)
          .append("\" SET ").append(KeyValueStoreColumns.VALUE_TYPE).append("=? WHERE ")
          .append(KeyValueStoreColumns.PARTITION).append("=? AND ")
          .append(KeyValueStoreColumns.KEY).append("=?");
      //@formatter:on

      String sql = b.toString();
      String[] fields = new String[3];

      // for columns

      fields[0] = ElementDataType.array.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.string.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_FORMAT;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.object.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.bool.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_VISIBLE;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.array.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_JOINS;
      db.execSQL(sql, fields);

      // and for the table...

      fields[0] = ElementDataType.array.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_COL_ORDER;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.object.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_DISPLAY_NAME;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.array.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_GROUP_BY_COLS;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.string.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_INDEX_COL;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.object.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_SORT_COL;
      db.execSQL(sql, fields);

      fields[0] = ElementDataType.object.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_SORT_ORDER;
      db.execSQL(sql, fields);

      // TODO: color rule groups

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /*
   * Create a user defined database table metadata - table definiton and KVS
   * values
   */
  private void createDBTableMetadata(OdkDatabase db, String tableId) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    // Add the table id into table definitions
    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.TABLE_ID, tableId);
    cvTableDef.putNull(TableDefinitionsColumns.SCHEMA_ETAG);
    cvTableDef.putNull(TableDefinitionsColumns.LAST_DATA_ETAG);
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME, -1);

    db.replaceOrThrow(DatabaseConstants.TABLE_DEFS_TABLE_NAME, null, cvTableDef);

    // Add the tables values into KVS
    ArrayList<ContentValues> cvTableValKVS = new ArrayList<ContentValues>();

    ContentValues cvTableVal = null;

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_COL_ORDER);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "array");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, "defaultViewType");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "SPREADSHEET");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_DISPLAY_NAME);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "\"" + tableId + "\"");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_GROUP_BY_COLS);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "array");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_INDEX_COL);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_SORT_COL);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_SORT_ORDER);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "string");
    cvTableVal.put(KeyValueStoreColumns.VALUE, "");
    cvTableValKVS.add(cvTableVal);

    cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, "TableColorRuleGroup");
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, "StatusColumn.ruleList");
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "object");
    try {
      List<ColorRule> rules = ColorRuleUtil.getDefaultSyncStateColorRules();
      List<TreeMap<String, Object>> jsonableList = new ArrayList<TreeMap<String, Object>>();
      for (ColorRule rule : rules) {
        jsonableList.add(rule.getJsonRepresentation());
      }
      String value = ODKFileUtils.mapper.writeValueAsString(jsonableList);
      cvTableVal.put(KeyValueStoreColumns.VALUE, value);
      cvTableValKVS.add(cvTableVal);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }

    // Now add Tables values into KVS
    for (int i = 0; i < cvTableValKVS.size(); i++) {
      db.replaceOrThrow(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null,
          cvTableValKVS.get(i));
    }
  }

  /*
   * Build the start of a create table statement -- specifies all the metadata
   * columns. Caller must then add all the user-defined column definitions and
   * closing parentheses.
   */
  private String getUserDefinedTableCreationStatement(String tableId) {
    /*
     * Resulting string should be the following String createTableCmd =
     * "CREATE TABLE IF NOT EXISTS " + tableId + " (" + DataTableColumns.ID +
     * " TEXT NOT NULL, " + DataTableColumns.ROW_ETAG + " TEXT NULL, " +
     * DataTableColumns.SYNC_STATE + " TEXT NOT NULL, " +
     * DataTableColumns.CONFLICT_TYPE + " INTEGER NULL," +
     * DataTableColumns.FILTER_TYPE + " TEXT NULL," +
     * DataTableColumns.FILTER_VALUE + " TEXT NULL," + DataTableColumns.FORM_ID
     * + " TEXT NULL," + DataTableColumns.LOCALE + " TEXT NULL," +
     * DataTableColumns.SAVEPOINT_TYPE + " TEXT NULL," +
     * DataTableColumns.SAVEPOINT_TIMESTAMP + " TEXT NOT NULL," +
     * DataTableColumns.SAVEPOINT_CREATOR + " TEXT NULL";
     */

    String createTableCmd = "CREATE TABLE IF NOT EXISTS " + tableId + " (";

    List<String> cols = getAdminColumns();

    String endSeq = ", ";
    for (int i = 0; i < cols.size(); ++i) {
      if (i == cols.size() - 1) {
        endSeq = "";
      }
      String colName = cols.get(i);
      //@formatter:off
      if (colName.equals(DataTableColumns.ID) 
          || colName.equals(DataTableColumns.SYNC_STATE)
          || colName.equals(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
        createTableCmd = createTableCmd + colName + " TEXT NOT NULL" + endSeq;
      } else if (colName.equals(DataTableColumns.ROW_ETAG)
          || colName.equals(DataTableColumns.FILTER_TYPE)
          || colName.equals(DataTableColumns.FILTER_VALUE)
          || colName.equals(DataTableColumns.FORM_ID) 
          || colName.equals(DataTableColumns.LOCALE)
          || colName.equals(DataTableColumns.SAVEPOINT_TYPE)
          || colName.equals(DataTableColumns.SAVEPOINT_CREATOR)) {
        createTableCmd = createTableCmd + colName + " TEXT NULL" + endSeq;
      } else if (colName.equals(DataTableColumns.CONFLICT_TYPE)) {
        createTableCmd = createTableCmd + colName + " INTEGER NULL" + endSeq;
      }
      //@formatter:on
    }

    return createTableCmd;
  }

  /*
   * Create a user defined database table metadata - table definiton and KVS
   * values
   */
  private void createDBTableWithColumns(OdkDatabase db, String appName, String tableId,
      OrderedColumns orderedDefs) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    String createTableCmd = getUserDefinedTableCreationStatement(tableId);

    StringBuilder createTableCmdWithCols = new StringBuilder();
    createTableCmdWithCols.append(createTableCmd);

    for (ColumnDefinition column : orderedDefs.getColumnDefinitions()) {
      if (!column.isUnitOfRetention()) {
        continue;
      }
      ElementType elementType = column.getType();

      ElementDataType dataType = elementType.getDataType();
      String dbType;
      if (dataType == ElementDataType.array) {
        dbType = "TEXT";
      } else if (dataType == ElementDataType.bool) {
        dbType = "INTEGER";
      } else if (dataType == ElementDataType.configpath) {
        dbType = "TEXT";
      } else if (dataType == ElementDataType.integer) {
        dbType = "INTEGER";
      } else if (dataType == ElementDataType.number) {
        dbType = "REAL";
      } else if (dataType == ElementDataType.object) {
        dbType = "TEXT";
      } else if (dataType == ElementDataType.rowpath) {
        dbType = "TEXT";
      } else if (dataType == ElementDataType.string) {
        dbType = "TEXT";
      } else {
        throw new IllegalStateException("unexpected ElementDataType: " + dataType.name());
      }
      //@formatter:off
      createTableCmdWithCols.append(", ").append(column.getElementKey())
        .append(" ").append(dbType).append(" NULL");
      //@formatter:on
    }

    createTableCmdWithCols.append(");");

    db.execSQL(createTableCmdWithCols.toString());

    // Create the metadata for the table - table def and KVS
    createDBTableMetadata(db, tableId);

    // Now need to call the function to write out all the column values
    for (ColumnDefinition column : orderedDefs.getColumnDefinitions()) {
      createNewColumnMetadata(db, tableId, column);
    }

    // Need to address column order
    ContentValues cvTableVal = new ContentValues();
    cvTableVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvTableVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_TABLE);
    cvTableVal.put(KeyValueStoreColumns.ASPECT, KeyValueStoreConstants.ASPECT_DEFAULT);
    cvTableVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.TABLE_COL_ORDER);
    cvTableVal.put(KeyValueStoreColumns.VALUE_TYPE, "array");

    StringBuilder tableDefCol = new StringBuilder();

    boolean needsComma = false;
    for (ColumnDefinition def : orderedDefs.getColumnDefinitions()) {
      if (!def.isUnitOfRetention()) {
        continue;
      }
      if (needsComma) {
        tableDefCol.append(",");
      }
      needsComma = true;
      tableDefCol.append("\"").append(def.getElementKey()).append("\"");
    }

    WebLogger.getLogger(appName).i(t,
        "Column order for table " + tableId + " is " + tableDefCol.toString());
    String colOrderVal = "[" + tableDefCol.toString() + "]";
    cvTableVal.put(KeyValueStoreColumns.VALUE, colOrderVal);

    // Now add Tables values into KVS
    db.replaceOrThrow(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, cvTableVal);
  }

  /*
   * Create a new column metadata in the database - add column values to KVS and
   * column definitions
   */
  private void createNewColumnMetadata(OdkDatabase db, String tableId, ColumnDefinition column) {
    String colName = column.getElementKey();
    ArrayList<ContentValues> cvColValKVS = new ArrayList<ContentValues>();

    ContentValues cvColVal;

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.array.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, "[]");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_FORMAT);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.string.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, "");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_NAME);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.object.name());
    String colDisplayName = "\"" + colName + "\"";
    cvColVal.put(KeyValueStoreColumns.VALUE, colDisplayName);
    cvColValKVS.add(cvColVal);

    // TODO: change bool to be integer valued in the KVS?
    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_DISPLAY_VISIBLE);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.bool.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, column.isUnitOfRetention() ? "true" : "false");
    cvColValKVS.add(cvColVal);

    cvColVal = new ContentValues();
    cvColVal.put(KeyValueStoreColumns.TABLE_ID, tableId);
    cvColVal.put(KeyValueStoreColumns.PARTITION, KeyValueStoreConstants.PARTITION_COLUMN);
    cvColVal.put(KeyValueStoreColumns.ASPECT, colName);
    cvColVal.put(KeyValueStoreColumns.KEY, KeyValueStoreConstants.COLUMN_JOINS);
    cvColVal.put(KeyValueStoreColumns.VALUE_TYPE, ElementDataType.object.name());
    cvColVal.put(KeyValueStoreColumns.VALUE, "");
    cvColValKVS.add(cvColVal);

    // Now add all this data into the database
    for (int i = 0; i < cvColValKVS.size(); i++) {
      db.replaceOrThrow(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null,
          cvColValKVS.get(i));
    }

    // Create column definition
    ContentValues cvColDefVal = null;

    cvColDefVal = new ContentValues();
    cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableId);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, column.getElementName());
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, column.getElementType());
    cvColDefVal.put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS,
        column.getListChildElementKeys());

    // Now add this data into the database
    db.replaceOrThrow(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME, null, cvColDefVal);
  }

  /**
   * If the tableId is not recorded in the TableDefinition metadata table, then
   * create the tableId with the indicated columns. This will synthesize
   * reasonable metadata KVS entries for table.
   * 
   * If the tableId is present, then this is a no-op.
   * 
   * @param db
   * @param appName
   * @param tableId
   * @param columns
   * @return the ArrayList<ColumnDefinition> of the user columns in the table.
   */
  public OrderedColumns createOrOpenDBTableWithColumns(OdkDatabase db, String appName,
      String tableId, List<Column> columns) {
    boolean dbWithinTransaction = db.inTransaction();
    boolean success = false;
    OrderedColumns orderedDefs = new OrderedColumns(appName, tableId, columns);
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }
      if (!hasTableId(db, tableId)) {
        createDBTableWithColumns(db, appName, tableId, orderedDefs);
      }

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
      success = true;
      return orderedDefs;
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
      if (success == false) {

        // Get the names of the columns
        StringBuilder colNames = new StringBuilder();
        if (columns != null) {
          for (Column column : columns) {
            colNames.append(" ").append(column.getElementKey()).append(",");
          }
          if (colNames != null && colNames.length() > 0) {
            colNames.deleteCharAt(colNames.length() - 1);
            WebLogger.getLogger(appName).e(
                t,
                "createOrOpenDBTableWithColumns: Error while adding table " + tableId
                    + " with columns:" + colNames.toString());
          }
        } else {
          WebLogger.getLogger(appName).e(
              t,
              "createOrOpenDBTableWithColumns: Error while adding table " + tableId
                  + " with columns: null");
        }
      }
    }
  }

  /**
   * Call this when the schema on the server has changed w.r.t. the schema on
   * the device. In this case, we do not know whether the rows on the device
   * match those on the server.
   *
   * <ul>
   * <li>Reset all 'in_conflict' rows to their original local state (changed or
   * deleted).</li>
   * <li>Leave all 'deleted' rows in 'deleted' state.</li>
   * <li>Leave all 'changed' rows in 'changed' state.</li>
   * <li>Reset all 'synced' rows to 'new_row' to ensure they are sync'd to the
   * server.</li>
   * <li>Reset all 'synced_pending_files' rows to 'new_row' to ensure they are
   * sync'd to the server.</li>
   * </ul>
   * 
   * @param db
   * @param tableId
   */
  public void changeDataRowsToNewRowState(OdkDatabase db, String tableId) {

    StringBuilder b = new StringBuilder();

    // remove server conflicting rows
    b.setLength(0);
    b.append("DELETE FROM \"").append(tableId).append("\" WHERE ")
        .append(DataTableColumns.SYNC_STATE).append(" =? AND ")
        .append(DataTableColumns.CONFLICT_TYPE).append(" IN (?, ?)");

    String sqlConflictingServer = b.toString();
    //@formatter:off
    String argsConflictingServer[] = { 
        SyncState.in_conflict.name(),
        Integer.toString(ConflictType.SERVER_DELETED_OLD_VALUES),
        Integer.toString(ConflictType.SERVER_UPDATED_UPDATED_VALUES) 
      };
    //@formatter:on

    // update local delete conflicts to deletes
    b.setLength(0);
    //@formatter:off
    b.append("UPDATE \"").append(tableId).append("\" SET ")
      .append(DataTableColumns.SYNC_STATE).append(" =?, ")
      .append(DataTableColumns.CONFLICT_TYPE).append(" = null WHERE ")
      .append(DataTableColumns.CONFLICT_TYPE).append(" = ?");
    //@formatter:on

    String sqlConflictingLocalDeleting = b.toString();
    //@formatter:off
    String argsConflictingLocalDeleting[] = {
        SyncState.deleted.name(),
        Integer.toString(ConflictType.LOCAL_DELETED_OLD_VALUES) 
      };
    //@formatter:on

    // update local update conflicts to updates
    String sqlConflictingLocalUpdating = sqlConflictingLocalDeleting;
    //@formatter:off
    String argsConflictingLocalUpdating[] = { 
        SyncState.changed.name(),
        Integer.toString(ConflictType.LOCAL_UPDATED_UPDATED_VALUES) 
      };
    //@formatter:on

    // reset all 'rest' rows to 'insert'
    b.setLength(0);
    //@formatter:off
    b.append("UPDATE \"").append(tableId).append("\" SET ")
      .append(DataTableColumns.SYNC_STATE).append(" =? WHERE ")
      .append(DataTableColumns.SYNC_STATE).append(" =?");
    //@formatter:on

    String sqlRest = b.toString();
    //@formatter:off
    String argsRest[] = {
        SyncState.new_row.name(), 
        SyncState.synced.name()
      };
    //@formatter:on

    String sqlRestPendingFiles = sqlRest;
    //@formatter:off
    String argsRestPendingFiles[] = {
        SyncState.new_row.name(),
        SyncState.synced_pending_files.name() 
      };
    //@formatter:on

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      db.execSQL(sqlConflictingServer, argsConflictingServer);
      db.execSQL(sqlConflictingLocalDeleting, argsConflictingLocalDeleting);
      db.execSQL(sqlConflictingLocalUpdating, argsConflictingLocalUpdating);
      db.execSQL(sqlRest, argsRest);
      db.execSQL(sqlRestPendingFiles, argsRestPendingFiles);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Deletes the server conflict row (if any) for this rowId in this tableId.
   * 
   * @param db
   * @param tableId
   * @param rowId
   */
  public void deleteServerConflictRowWithId(OdkDatabase db, String tableId, String rowId) {
    // delete the old server-values in_conflict row if it exists
    String whereClause = String.format("%s = ? AND %s = ? AND %s IN " + "( ?, ? )",
        DataTableColumns.ID, DataTableColumns.SYNC_STATE, DataTableColumns.CONFLICT_TYPE);
    String[] whereArgs = { rowId, SyncState.in_conflict.name(),
        String.valueOf(ConflictType.SERVER_DELETED_OLD_VALUES),
        String.valueOf(ConflictType.SERVER_UPDATED_UPDATED_VALUES) };

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      db.delete(tableId, whereClause, whereArgs);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Change the conflictType for the given row from null (not in conflict) to
   * the specified one.
   * 
   * @param db
   * @param tableId
   * @param rowId
   * @param conflictType
   *          expected to be one of ConflictType.LOCAL_DELETED_OLD_VALUES (0) or
   *          ConflictType.LOCAL_UPDATED_UPDATED_VALUES (1)
   */
  public void placeRowIntoConflict(OdkDatabase db, String tableId, String rowId, int conflictType) {

    String whereClause = String.format("%s = ? AND %s IS NULL", DataTableColumns.ID,
        DataTableColumns.CONFLICT_TYPE);
    String[] whereArgs = { rowId };

    ContentValues cv = new ContentValues();
    cv.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());
    cv.put(DataTableColumns.CONFLICT_TYPE, conflictType);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      db.update(tableId, cv, whereClause, whereArgs);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Changes the conflictType for the given row from the specified one to null
   * and set the sync state of this row to the indicated value. In general, you
   * should first update the local conflict record with its new values, then
   * call deleteServerConflictRowWithId(...) and then call this method.
   * 
   * @param db
   * @param tableId
   * @param rowId
   * @param syncState
   * @param conflictType
   */
  public void restoreRowFromConflict(OdkDatabase db, String tableId, String rowId,
      SyncState syncState, int conflictType) {

    String whereClause = String.format("%s = ? AND %s = ?", DataTableColumns.ID,
        DataTableColumns.CONFLICT_TYPE);
    String[] whereArgs = { rowId, String.valueOf(conflictType) };

    ContentValues cv = new ContentValues();
    cv.putNull(DataTableColumns.CONFLICT_TYPE);
    cv.put(DataTableColumns.SYNC_STATE, syncState.name());
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      db.update(tableId, cv, whereClause, whereArgs);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * 
   * @param db
   * @param appName
   * @param tableId
   * @param rowId
   * @return the sync state of the row (see {@link SyncState}), or null if the
   *         row does not exist.
   */
  public SyncState getSyncState(OdkDatabase db, String appName, String tableId, String rowId) {
    Cursor c = null;
    try {
      c = db.query(tableId, new String[] { DataTableColumns.SYNC_STATE }, DataTableColumns.ID
          + " = ?", new String[] { rowId }, null, null, null, null);
      if (c.moveToFirst()) {
        int syncStateIndex = c.getColumnIndex(DataTableColumns.SYNC_STATE);
        if (!c.isNull(syncStateIndex)) {
          String val = ODKCursorUtils.getIndexAsString(c, syncStateIndex);
          return SyncState.valueOf(val);
        }
      }
      return null;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
  }

  /**
   * Delete the specified rowId in this tableId. Deletion respects sync
   * semantics. If the row is in the SyncState.new_row state, then the row and
   * its associated file attachments are immediately deleted. Otherwise, the row
   * is placed into the SyncState.deleted state and will be retained until the
   * device can delete the record on the server.
   * <p>
   * If you need to immediately delete a record that would otherwise sync to the
   * server, call updateRowETagAndSyncState(...) to set the row to
   * SyncState.new_row, and then call this method and it will be immediately
   * deleted (in this case, unless the record on the server was already deleted,
   * it will remain and not be deleted during any subsequent synchronizations).
   * 
   * @param db
   * @param appName
   * @param tableId
   * @param rowId
   */
  public void deleteDataInExistingDBTableWithId(OdkDatabase db, String appName, String tableId,
      String rowId) {
    SyncState syncState = getSyncState(db, appName, tableId, rowId);

    boolean dbWithinTransaction = db.inTransaction();
    if (syncState == SyncState.new_row) {
      String[] whereArgs = { rowId };
      String whereClause = DataTableColumns.ID + " = ?";

      try {
        if (!dbWithinTransaction) {
          beginTransactionNonExclusive(db);
        }

        db.delete(tableId, whereClause, whereArgs);

        if (!dbWithinTransaction) {
          db.setTransactionSuccessful();
        }
      } finally {
        if (!dbWithinTransaction) {
          db.endTransaction();
        }
      }

      File instanceFolder = new File(ODKFileUtils.getInstanceFolder(appName, tableId, rowId));
      try {
        FileUtils.deleteDirectory(instanceFolder);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        WebLogger.getLogger(appName).e(t,
            "Unable to delete this directory: " + instanceFolder.getAbsolutePath());
        WebLogger.getLogger(appName).printStackTrace(e);
      }
    } else if (syncState == SyncState.synced || syncState == SyncState.changed) {
      String[] whereArgs = { rowId };
      ContentValues values = new ContentValues();
      values.put(DataTableColumns.SYNC_STATE, SyncState.deleted.name());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
          TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));
      try {
        if (!dbWithinTransaction) {
          beginTransactionNonExclusive(db);
        }

        db.update(tableId, values, DataTableColumns.ID + " = ?", whereArgs);

        if (!dbWithinTransaction) {
          db.setTransactionSuccessful();
        }
      } finally {
        if (!dbWithinTransaction) {
          db.endTransaction();
        }
      }
    }
  }

  /*
   * Internal method to execute a delete statement with the given where clause
   */
  private void rawDeleteDataInDBTable(OdkDatabase db, String tableId, String whereClause,
      String[] whereArgs) {
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      db.delete(tableId, whereClause, whereArgs);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Delete any checkpoint rows for the given rowId in the tableId. Checkpoint
   * rows are created by ODK Survey to hold intermediate values during the
   * filling-in of the form. They act as restore points in the Survey, should
   * the application die.
   * 
   * @param db
   * @param appName
   * @param tableId
   * @param rowId
   */
  public void deleteCheckpointRowsWithId(OdkDatabase db, String appName, String tableId,
      String rowId) {
    rawDeleteDataInDBTable(db, tableId, DataTableColumns.ID + "=? AND "
        + DataTableColumns.SAVEPOINT_TYPE + " IS NULL", new String[] { rowId });
  }

  /**
   * Update all rows for the given rowId to SavepointType 'INCOMPLETE' and
   * remove all but the most recent row. When used with a rowId that has
   * checkpoints, this updates to the most recent checkpoint and removes any
   * earlier checkpoints, incomplete or complete savepoints. Otherwise, it has
   * the general effect of resetting the rowId to an INCOMPLETE state.
   * 
   * @param db
   * @param tableId
   * @param rowId
   */
  public void saveAsIncompleteMostRecentCheckpointDataInDBTableWithId(OdkDatabase db,
      String tableId, String rowId) {
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      db.execSQL("UPDATE \"" + tableId + "\" SET " + DataTableColumns.SAVEPOINT_TYPE + "= ? WHERE "
          + DataTableColumns.ID + "=?",
          new String[] { SavepointTypeManipulator.incomplete(), rowId });
      db.delete(tableId, DataTableColumns.ID + "=? AND " + DataTableColumns.SAVEPOINT_TIMESTAMP
          + " NOT IN (SELECT MAX(" + DataTableColumns.SAVEPOINT_TIMESTAMP + ") FROM \"" + tableId
          + "\" WHERE " + DataTableColumns.ID + "=?)", new String[] { rowId, rowId });

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  /**
   * Update the given rowId with the values in the cvValues. If certain metadata
   * values are not specified in the cvValues, then suitable default values may
   * be supplied for them. Furthermore, if the cvValues do not specify certain
   * metadata fields, then an exception may be thrown if there are more than one
   * row matching this rowId.
   * 
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   */
  public void updateDataInExistingDBTableWithId(OdkDatabase db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId) {

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, rowId);
    cvDataTableVal.putAll(cvValues);

    upsertDataIntoExistingDBTable(db, tableId, orderedColumns, cvDataTableVal, true);
  }

  /**
   * Insert the given rowId with the values in the cvValues. If certain metadata
   * values are not specified in the cvValues, then suitable default values may
   * be supplied for them.
   * 
   * If a row with this rowId and certain matching metadata fields is present,
   * then an exception is thrown.
   * 
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   */
  public void insertDataIntoExistingDBTableWithId(OdkDatabase db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId) {

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, rowId);
    cvDataTableVal.putAll(cvValues);

    upsertDataIntoExistingDBTable(db, tableId, orderedColumns, cvDataTableVal, false);
  }

  /*
   * Write data into a user defined database table
   * 
   * TODO: This is broken w.r.t. updates of partial fields
   */
  private void upsertDataIntoExistingDBTable(OdkDatabase db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, boolean shouldUpdate) {
    String rowId = null;
    String whereClause = null;
    boolean specifiesConflictType = cvValues.containsKey(DataTableColumns.CONFLICT_TYPE);
    boolean nullConflictType = specifiesConflictType
        && (cvValues.get(DataTableColumns.CONFLICT_TYPE) == null);
    String[] whereArgs = new String[specifiesConflictType ? (1 + (nullConflictType ? 0 : 1)) : 1];
    boolean update = false;

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.putAll(cvValues);

    if (cvDataTableVal.containsKey(DataTableColumns.ID)) {
      // The user specified a row id; we need to determine whether to
      // insert or update the record, or to reject the action because
      // there are either checkpoint records for this row id, or, if
      // a server conflict is associated with this row, that the
      // _conflict_type to update was not specified.
      //
      // i.e., the tuple (_id, _conflict_type) should be unique. If
      // we find that there are more than 0 or 1 records matching this
      // tuple, then we should reject the update request.
      //
      // TODO: perhaps we want to allow updates to the local conflict
      // row if there are no checkpoints on it? I.e., change the
      // tri-state conflict type to a pair of states (local / remote).
      // and all local changes are flagged local. Remote only exists
      // if the server is in conflict.

      rowId = cvDataTableVal.getAsString(DataTableColumns.ID);
      if (rowId == null) {
        throw new IllegalArgumentException(DataTableColumns.ID + ", if specified, cannot be null");
      }

      if (specifiesConflictType) {
        if (nullConflictType) {
          whereClause = DataTableColumns.ID + " = ?" + " AND " + DataTableColumns.CONFLICT_TYPE
              + " IS NULL";
          whereArgs[0] = rowId;
        } else {
          whereClause = DataTableColumns.ID + " = ?" + " AND " + DataTableColumns.CONFLICT_TYPE
              + " = ?";
          whereArgs[0] = rowId;
          whereArgs[1] = cvValues.getAsString(DataTableColumns.CONFLICT_TYPE);
        }
      } else {
        whereClause = DataTableColumns.ID + " = ?";
        whereArgs[0] = rowId;
      }

      String sel = "SELECT * FROM " + tableId + " WHERE " + whereClause;
      String[] selArgs = whereArgs;
      Cursor cursor = rawQuery(db, sel, selArgs);

      // There must be only one row in the db for the update to work
      if (shouldUpdate) {
        if (cursor.getCount() == 1) {
          update = true;
        } else if (cursor.getCount() > 1) {
          throw new IllegalArgumentException(t + ": row id " + rowId
              + " has more than 1 row in table " + tableId);
        }
      } else {
        if (cursor.getCount() > 0) {
          throw new IllegalArgumentException(t + ": id " + rowId + " is already present in table "
              + tableId);
        }
      }

    } else {
      rowId = "uuid:" + UUID.randomUUID().toString();
    }

    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields
    // TODO: This is broken w.r.t. updates of partial fields

    if (!cvDataTableVal.containsKey(DataTableColumns.ID)) {
      cvDataTableVal.put(DataTableColumns.ID, rowId);
    }

    if (update) {
      if (!cvDataTableVal.containsKey(DataTableColumns.SYNC_STATE)
          || (cvDataTableVal.get(DataTableColumns.SYNC_STATE) == null)) {
        cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.changed.name());
      }

      if (cvDataTableVal.containsKey(DataTableColumns.LOCALE)
          && (cvDataTableVal.get(DataTableColumns.LOCALE) == null)) {
        cvDataTableVal.put(DataTableColumns.LOCALE, DataTableColumns.DEFAULT_LOCALE);
      }

      if (cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE)
          && (cvDataTableVal.get(DataTableColumns.SAVEPOINT_TYPE) == null)) {
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)
          || cvDataTableVal.get(DataTableColumns.SAVEPOINT_TIMESTAMP) == null) {
        String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR)
          || (cvDataTableVal.get(DataTableColumns.SAVEPOINT_CREATOR) == null)) {
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR,
            DataTableColumns.DEFAULT_SAVEPOINT_CREATOR);
      }
    } else {

      if (!cvDataTableVal.containsKey(DataTableColumns.ROW_ETAG)
          || cvDataTableVal.get(DataTableColumns.ROW_ETAG) == null) {
        cvDataTableVal.put(DataTableColumns.ROW_ETAG, DataTableColumns.DEFAULT_ROW_ETAG);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.SYNC_STATE)
          || (cvDataTableVal.get(DataTableColumns.SYNC_STATE) == null)) {
        cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.CONFLICT_TYPE)) {
        cvDataTableVal.putNull(DataTableColumns.CONFLICT_TYPE);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.FILTER_TYPE)
          || (cvDataTableVal.get(DataTableColumns.FILTER_TYPE) == null)) {
        cvDataTableVal.put(DataTableColumns.FILTER_TYPE, DataTableColumns.DEFAULT_FILTER_TYPE);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.FILTER_VALUE)
          || (cvDataTableVal.get(DataTableColumns.FILTER_VALUE) == null)) {
        cvDataTableVal.put(DataTableColumns.FILTER_VALUE, DataTableColumns.DEFAULT_FILTER_VALUE);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.FORM_ID)) {
        cvDataTableVal.putNull(DataTableColumns.FORM_ID);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.LOCALE)
          || (cvDataTableVal.get(DataTableColumns.LOCALE) == null)) {
        cvDataTableVal.put(DataTableColumns.LOCALE, DataTableColumns.DEFAULT_LOCALE);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE)
          || (cvDataTableVal.get(DataTableColumns.SAVEPOINT_TYPE) == null)) {
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)
          || cvDataTableVal.get(DataTableColumns.SAVEPOINT_TIMESTAMP) == null) {
        String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
      }

      if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR)
          || (cvDataTableVal.get(DataTableColumns.SAVEPOINT_CREATOR) == null)) {
        cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR,
            DataTableColumns.DEFAULT_SAVEPOINT_CREATOR);
      }
    }

    cleanUpValuesMap(orderedColumns, cvDataTableVal);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        beginTransactionNonExclusive(db);
      }

      if (update) {
        db.update(tableId, cvDataTableVal, whereClause, whereArgs);
      } else {
        db.insertOrThrow(tableId, null, cvDataTableVal);
      }

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }

  }

  /**
   * Update the ETag and SyncState of a given rowId. There should be exactly one
   * record for this rowId in thed database (i.e., no conflicts or checkpoints).
   * 
   * @param db
   * @param tableId
   * @param rowId
   * @param rowETag
   * @param state
   */
  public void updateRowETagAndSyncState(OdkDatabase db, String tableId, String rowId,
      String rowETag, SyncState state) {

    String whereClause = DataTableColumns.ID + " = ?";
    String[] whereArgs = { rowId };

    ContentValues cvDataTableVal = new ContentValues();

    String sel = "SELECT * FROM " + tableId + " WHERE " + whereClause;
    String[] selArgs = whereArgs;
    Cursor cursor = rawQuery(db, sel, selArgs);

    // There must be only one row in the db
    if (cursor.getCount() != 1) {
      throw new IllegalArgumentException(t + ": row id " + rowId
          + " does not have exactly 1 row in table " + tableId);
    }

    cvDataTableVal.put(DataTableColumns.ROW_ETAG, rowETag);
    cvDataTableVal.put(DataTableColumns.SYNC_STATE, state.name());

    db.update(tableId, cvDataTableVal, whereClause, whereArgs);
  }

  /**
   * If the caller specified a complex json value for a structured type, flush
   * the value through to the individual columns.
   * 
   * @param orderedColumns
   * @param values
   */
  private void cleanUpValuesMap(OrderedColumns orderedColumns, ContentValues values) {

    Map<String, String> toBeResolved = new HashMap<String, String>();

    for (String key : values.keySet()) {
      if (DataTableColumns.CONFLICT_TYPE.equals(key)) {
        continue;
      } else if (DataTableColumns.FILTER_TYPE.equals(key)) {
        continue;
      } else if (DataTableColumns.FILTER_TYPE.equals(key)) {
        continue;
      } else if (DataTableColumns.FILTER_VALUE.equals(key)) {
        continue;
      } else if (DataTableColumns.FORM_ID.equals(key)) {
        continue;
      } else if (DataTableColumns.ID.equals(key)) {
        continue;
      } else if (DataTableColumns.LOCALE.equals(key)) {
        continue;
      } else if (DataTableColumns.ROW_ETAG.equals(key)) {
        continue;
      } else if (DataTableColumns.SAVEPOINT_CREATOR.equals(key)) {
        continue;
      } else if (DataTableColumns.SAVEPOINT_TIMESTAMP.equals(key)) {
        continue;
      } else if (DataTableColumns.SAVEPOINT_TYPE.equals(key)) {
        continue;
      } else if (DataTableColumns.SYNC_STATE.equals(key)) {
        continue;
      } else if (DataTableColumns._ID.equals(key)) {
        continue;
      }
      // OK it is one of the data columns
      ColumnDefinition cp = orderedColumns.find(key);
      if (!cp.isUnitOfRetention()) {
        toBeResolved.put(key, values.getAsString(key));
      }
    }

    // remove these non-retained values from the values set...
    for (String key : toBeResolved.keySet()) {
      values.remove(key);
    }

    while (!toBeResolved.isEmpty()) {

      Map<String, String> moreToResolve = new HashMap<String, String>();

      for (Map.Entry<String, String> entry : toBeResolved.entrySet()) {
        String key = entry.getKey();
        String json = entry.getValue();
        if (json == null) {
          // don't need to do anything
          // since the value is null
          continue;
        }
        ColumnDefinition cp = orderedColumns.find(key);
        try {
          Map<String, Object> struct = ODKFileUtils.mapper.readValue(json, Map.class);
          for (ColumnDefinition child : cp.getChildren()) {
            String subkey = child.getElementKey();
            ColumnDefinition subcp = orderedColumns.find(subkey);
            if (subcp.isUnitOfRetention()) {
              ElementType subtype = subcp.getType();
              ElementDataType type = subtype.getDataType();
              if (type == ElementDataType.integer) {
                values.put(subkey, (Integer) struct.get(subcp.getElementName()));
              } else if (type == ElementDataType.number) {
                values.put(subkey, (Double) struct.get(subcp.getElementName()));
              } else if (type == ElementDataType.bool) {
                values.put(subkey, ((Boolean) struct.get(subcp.getElementName())) ? 1 : 0);
              } else {
                values.put(subkey, (String) struct.get(subcp.getElementName()));
              }
            } else {
              // this must be a javascript structure... re-JSON it and save (for
              // next round).
              moreToResolve.put(subkey,
                  ODKFileUtils.mapper.writeValueAsString(struct.get(subcp.getElementName())));
            }
          }
        } catch (JsonParseException e) {
          e.printStackTrace();
          throw new IllegalStateException("should not be happening");
        } catch (JsonMappingException e) {
          e.printStackTrace();
          throw new IllegalStateException("should not be happening");
        } catch (IOException e) {
          e.printStackTrace();
          throw new IllegalStateException("should not be happening");
        }
      }

      toBeResolved = moreToResolve;
    }
  }
}
