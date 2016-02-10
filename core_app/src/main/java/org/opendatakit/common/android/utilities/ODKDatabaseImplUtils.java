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

import android.content.ContentValues;
import android.database.Cursor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

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
import org.opendatakit.common.android.data.*;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.provider.*;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;
import org.opendatakit.database.service.KeyValueStoreEntry;
import org.sqlite.database.sqlite.SQLiteException;

import java.io.File;
import java.io.IOException;
import java.util.*;

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

   /**
    * When a KVS change is made, enforce in the database layer that the
    * value_type of some KVS entries is a specific type.  Log an error
    * if the user attempts to do something differently, but correct
    * the error. This is largely for migration / forward compatibility.
    */
  private static ArrayList<String[]> knownKVSValueTypeRestrictions = new ArrayList<String[]>();

   /**
    * Same as above, but quick access via the key.
    * For now, we know that the keys are all unique.
    * Eventually this might need to be a MultiMap.
    */
   private static TreeMap<String,ArrayList<String[]>> keyToKnownKVSValueTypeRestrictions = new
       TreeMap<String,ArrayList<String[]>>();

   private static void updateKeyToKnownKVSValueTypeRestrictions(String[] field) {
      ArrayList<String[]> fields = keyToKnownKVSValueTypeRestrictions.get(field[2]);
      if ( fields == null ) {
         fields = new ArrayList<String[]>();
         keyToKnownKVSValueTypeRestrictions.put(field[2], fields);
      }
      fields.add(field);
   }

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

     // declare the KVS value_type restrictions we know about...
     // This is a list of triples: ( required value type, partition_label, key_label )
     {
        String[] fields;

        // for columns
        fields = new String[3];
        fields[0] = ElementDataType.string.name();
        fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
        fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.string.name();
        fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
        fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_FORMAT;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.object.name();
        fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
        fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.bool.name();
        fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
        fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_VISIBLE;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.array.name();
        fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
        fields[2] = KeyValueStoreConstants.COLUMN_JOINS;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        // and for the table...
        fields = new String[3];
        fields[0] = ElementDataType.array.name();
        fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
        fields[2] = KeyValueStoreConstants.TABLE_COL_ORDER;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.object.name();
        fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
        fields[2] = KeyValueStoreConstants.TABLE_DISPLAY_NAME;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.array.name();
        fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
        fields[2] = KeyValueStoreConstants.TABLE_GROUP_BY_COLS;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.string.name();
        fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
        fields[2] = KeyValueStoreConstants.TABLE_INDEX_COL;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.object.name();
        fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
        fields[2] = KeyValueStoreConstants.TABLE_SORT_COL;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        fields = new String[3];
        fields[0] = ElementDataType.object.name();
        fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
        fields[2] = KeyValueStoreConstants.TABLE_SORT_ORDER;
        knownKVSValueTypeRestrictions.add(fields);
        updateKeyToKnownKVSValueTypeRestrictions(fields);

        // TODO: color rule groups
     }

    // Used to ensure that the singleton has been initialized properly
    AndroidConnectFactory.configure();
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

  /**
   * Perform a raw query with bind parameters.
   *
   * @param db
   * @param sql
   * @param selectionArgs
   * @return
   */
  public Cursor rawQuery(OdkConnectionInterface db, String sql, String[] selectionArgs) {
    Cursor c = db.rawQuery(sql, selectionArgs);
    return c;
  }

  /**
   * Perform a query with the given parameters.
   *
   * @param db
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
  public Cursor queryDistinct(OdkConnectionInterface db, String table, String[] columns, String selection,
      String[] selectionArgs, String groupBy, String having, String orderBy, String limit)
      throws SQLiteException {
    Cursor c = db.queryDistinct(table, columns, selection, selectionArgs, groupBy, having, orderBy,
        limit);
    return c;
  }

  public Cursor query(OdkConnectionInterface db, String table, String[] columns, String selection,
      String[] selectionArgs, String groupBy, String having, String orderBy, String limit)
      throws SQLiteException {
    Cursor c = db.query(table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
    return c;
  }

  /**
   * Get a {@link UserTable} for this table based on the given where clause. All
   * columns from the table are returned.
   * <p/>
   * SELECT * FROM table WHERE whereClause GROUP BY groupBy[]s HAVING
   * havingClause ORDER BY orderbyElement orderByDirection
   * <p/>
   * If any of the clause parts are omitted (null), then the appropriate
   * simplified SQL statement is constructed.
   *
   * @param db
   * @param appName
   * @param tableId
   * @param columnDefns
   * @param whereClause       the whereClause for the selection, beginning with "WHERE". Must
   *                          include "?" instead of actual values, which are instead passed in
   *                          the selectionArgs.
   * @param selectionArgs     an array of string values for bind parameters
   * @param groupBy           an array of elementKeys
   * @param having
   * @param orderByElementKey elementKey to order the results by
   * @param orderByDirection  either "ASC" or "DESC"
   * @return
   */
  public UserTable rawSqlQuery(OdkConnectionInterface db, String appName, String tableId,
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

    c.moveToFirst();
    int rowCount = c.getCount();

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

  public RawUserTable arbitraryQuery(OdkConnectionInterface db, String appName,
      String sqlCommand, String[] sqlBindArgs) {
    Cursor c = null;
    try {
      c = db.rawQuery(sqlCommand, sqlBindArgs);
      RawUserTable table = buildRawUserTable(c, sqlCommand, sqlBindArgs);
      return table;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
  }

  private RawUserTable buildRawUserTable(Cursor c,
      String sqlCommand, String[] sqlBindArgs) {

    if ( !c.moveToFirst() ) {
      String[] mElementKeyForIndex = null;

      // Attempt to retrieve the columns from the cursor.
      // These may not be available if there were no rows returned.
      // It depends upon the cursor implementation.
      try {
        int columnCount = c.getColumnCount();
        mElementKeyForIndex = new String[columnCount];
        int i;

        for (i = 0; i < columnCount; ++i) {
          String columnName = c.getColumnName(i);
          mElementKeyForIndex[i] = columnName;
        }
      } catch ( Exception e ) {
        // ignore.
      }

      // if they were not available, declare an empty array.
      if ( mElementKeyForIndex == null ) {
        mElementKeyForIndex = new String[0];
      }
      c.close();

      // we have no idea what the table should contain because it has no rows...
      return new RawUserTable(sqlCommand, sqlBindArgs, mElementKeyForIndex, 0);
    }

    int rowCount = c.getCount();
    int columnCount = c.getColumnCount();

    RawUserTable userTable = null;

    // may be -1 if there is no _id column in the result set.
    int rowIdIndex = c.getColumnIndex(DataTableColumns.ID);

    // These maps will map the element key to the corresponding index in
    // either data or metadata. If the user has defined a column with the
    // element key _my_data, and this column is at index 5 in the data
    // array, dataKeyToIndex would then have a mapping of _my_data:5.
    // The sync_state column, if present at index 7, would have a mapping
    // in metadataKeyToIndex of sync_state:7.
    String[] mElementKeyForIndex = new String[columnCount];
    int i;

    for ( i = 0 ; i < columnCount ; ++i ) {
      String columnName = c.getColumnName(i);
      mElementKeyForIndex[i] = columnName;
    }

    userTable = new RawUserTable(sqlCommand, sqlBindArgs, mElementKeyForIndex, rowCount);

    rowCount = 0;
    String[] rowData = new String[columnCount];
    do {
      String rowId;
      if ( rowIdIndex == -1 ) {
        rowId = Integer.toString(rowCount);
      } else {
        if (c.isNull(rowIdIndex)) {
          throw new IllegalStateException("Unexpected null value for rowId");
        }
        rowId = ODKCursorUtils.getIndexAsString(c, rowIdIndex);
      }
      ++rowCount;
      // First get the user-defined data for this row.
      for (i = 0; i < columnCount; i++) {
        String value = ODKCursorUtils.getIndexAsString(c, i);
        rowData[i] = value;
      }
      RawRow nextRow = new RawRow(userTable, rowId, rowData.clone());
      userTable.addRow(nextRow);
    } while (c.moveToNext());
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
  public UserTable getRowsWithId(OdkConnectionInterface db, String appName, String tableId,
      OrderedColumns orderedDefns, String rowId) {

    UserTable table = rawSqlQuery(db, appName, tableId, orderedDefns, DataTableColumns.ID + "=?",
            new String[]{rowId}, null, null, DataTableColumns.SAVEPOINT_TIMESTAMP, "DESC");

    return table;
  }

  /**
   * Return the row with the most recent changes for the given tableId and rowId.
   * If the rowId does not exist, it returns an empty UserTable for this tableId.
   * If the row has conflicts, it throws an exception. Otherwise, it returns the
   * most recent checkpoint or non-checkpoint value; it will contain a single row.
   *
   * @param db
   * @param appName
   * @param tableId
   * @param orderedDefns
   * @param rowId
   * @return
   */
   public UserTable getMostRecentRowWithId(OdkConnectionInterface db, String appName,
       String tableId, OrderedColumns orderedDefns, String rowId) {

      StringBuilder b = new StringBuilder();
      UserTable table = rawSqlQuery(db, appName, tableId, orderedDefns, DataTableColumns.ID + "=?",
          new String[]{rowId}, null, null, DataTableColumns.SAVEPOINT_TIMESTAMP, "DESC");

      if ( table.getNumberOfRows() == 0 ) {
         return table;
      }

      // most recent savepoint timestamp...
      UserTable t = new UserTable(table, Collections.singletonList(Integer.valueOf(0)));
      if ( t.hasConflictRows() ) {
         throw new IllegalStateException("row is in conflict");
      }
      return t;
   }

  public UserTable getConflictingRowsInExistingDBTableWithId(OdkConnectionInterface db, String
      appName, String tableId,
      OrderedColumns orderedDefns, String rowId) {

    StringBuilder b = new StringBuilder();
    UserTable table = rawSqlQuery(db, appName, tableId, orderedDefns, DataTableColumns.ID + "=?",
        new String[]{rowId}, null, null, DataTableColumns.SAVEPOINT_TIMESTAMP, "DESC");

    if ( table.getNumberOfRows() == 0 ) {
      return table;
    }

    for ( int i = 0 ; i < table.getNumberOfRows() ; ++i ) {
      if ( table.getRowAtIndex(i).getRawDataOrMetadataByElementKey(DataTableColumns
          .CONFLICT_TYPE) == null ) {
        throw new IllegalStateException("row is not in conflict");
      }
    }

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
  public String[] getAllColumnNames(OdkConnectionInterface db, String tableId) {
    Cursor cursor = null;
    try {
      cursor = db.rawQuery("SELECT * FROM " + tableId + " LIMIT 1", null);
      // If this query has been executed before, the cursor is created using the
      // previously-constructed PreparedStatement for the query. There is no actual
      // interaction with the database itself at the time the Cursor is constructed.
      // The first database interaction is when the content of the cursor is fetched.
      //
      // This can be triggered by a call to getCount().
      // At that time, if the table does not exist, it will throw an exception.
      cursor.moveToFirst();
      cursor.getCount();
      // Otherwise, when cached, getting the column names doesn't call into the database
      // and will not, itself, detect that the table has been dropped.
      String[] colNames = cursor.getColumnNames();
      return colNames;
    } finally {
      if ( cursor != null && !cursor.isClosed() ) {
        cursor.close();
      }
    }
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
  public OrderedColumns getUserDefinedColumns(OdkConnectionInterface db, String appName, String tableId) {
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
  public boolean hasTableId(OdkConnectionInterface db, String tableId) {
    Cursor c = null;
    try {
      //@formatter:off
      c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME, null, 
          TableDefinitionsColumns.TABLE_ID + "=?", 
          new String[] { tableId }, null, null, null, null);
      //@formatter:on
      // we know about the table...
      // tableId is the database table name...
      return (c != null) && c.moveToFirst() && (c.getCount() != 0);
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
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
  public int getTableHealth(OdkConnectionInterface db, String tableId) {
    StringBuilder b = new StringBuilder();
    b.append("SELECT SUM(case when _savepoint_type is null then 1 else 0 end) as checkpoints,")
        .append("SUM(case when _conflict_type is not null then 1 else 0 end) as conflicts from \"")
        .append(tableId).append("\"");

    Cursor c = null;
    try {
      c = db.rawQuery(b.toString(), null);
      Integer checkpoints = null;
      Integer conflicts = null;
      if ( c != null ) {
        if (c.moveToFirst()) {
          int idxCheckpoints = c.getColumnIndex("checkpoints");
          int idxConflicts = c.getColumnIndex("conflicts");
          checkpoints = ODKCursorUtils.getIndexAsType(c, Integer.class, idxCheckpoints);
          conflicts = ODKCursorUtils.getIndexAsType(c, Integer.class, idxConflicts);
        }
        c.close();
      }

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
  public ArrayList<String> getAllTableIds(OdkConnectionInterface db) {
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
  public void deleteDBTableAndAllData(OdkConnectionInterface db, final String appName, final String tableId) {

    SyncETagsUtils seu = new SyncETagsUtils();
    boolean dbWithinTransaction = db.inTransaction();

    String[] whereArgs = { tableId };

    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      // Drop the table used for the formId
      db.execSQL("DROP TABLE IF EXISTS \"" + tableId + "\";", null);

      // Delete the server sync ETags associated with this table
      seu.deleteAllSyncETagsForTableId(db, tableId);

      // Delete the table definition for the tableId
       int count;
       {
          String whereClause = TableDefinitionsColumns.TABLE_ID + " = ?";

          count = db.delete(DatabaseConstants.TABLE_DEFS_TABLE_NAME, whereClause, whereArgs);
       }

       // Delete the column definitions for this tableId
       {
          String whereClause = ColumnDefinitionsColumns.TABLE_ID + " = ?";

          db.delete(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME, whereClause, whereArgs);
       }

       // Delete the uploads for the tableId
       {
          String uploadWhereClause = InstanceColumns.DATA_TABLE_TABLE_ID + " = ?";
          db.delete(DatabaseConstants.UPLOADS_TABLE_NAME, uploadWhereClause, whereArgs);
       }

       // Delete the values from the 4 key value stores
       {
          String whereClause = KeyValueStoreColumns.TABLE_ID + " = ?";

          db.delete(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, whereClause, whereArgs);
       }

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
      File file = new File(assetsCsvDir);
      if(file.exists()) {
        Collection<File> files = FileUtils.listFiles(file, new IOFileFilter() {

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
  public void updateDBTableETags(OdkConnectionInterface db, String tableId, String schemaETag,
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
        db.beginTransactionNonExclusive();
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
  public void updateDBTableLastSyncTime(OdkConnectionInterface db, String tableId) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    ContentValues cvTableDef = new ContentValues();
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME,
        TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
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
  public TableDefinitionEntry getTableDefinitionEntry(OdkConnectionInterface db, String tableId) {

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

   /**
    * Ensure that the kvs entry is valid.
    *
    * @param appName
    * @param tableId
    * @param kvs
    * @throws IllegalArgumentException
    */
   private void validateKVSEntry(String appName, String tableId, KeyValueStoreEntry kvs) throws
       IllegalArgumentException {

      if ( kvs.tableId == null || kvs.tableId.trim().length() == 0 ) {
         throw new IllegalArgumentException("KVS entry has a null or empty tableId");
      }

      if ( !kvs.tableId.equals(tableId) ) {
         throw new IllegalArgumentException("KVS entry has a mismatched tableId");
      }

      if ( kvs.partition == null || kvs.partition.trim().length() == 0 ) {
         throw new IllegalArgumentException("KVS entry has a null or empty partition");
      }

      if ( kvs.aspect == null || kvs.aspect.trim().length() == 0 ) {
         throw new IllegalArgumentException("KVS entry has a null or empty aspect");
      }

      if ( kvs.key == null || kvs.key.trim().length() == 0 ) {
         throw new IllegalArgumentException("KVS entry has a null or empty key");
      }

      // a null value will remove the entry from the KVS
      if ( kvs.value != null && kvs.value.trim().length() != 0 ) {
         // validate the type....
         if ( kvs.type == null || kvs.type.trim().length() == 0 ) {
            throw new IllegalArgumentException("KVS entry has a null or empty type");
         }

         // find subset matching the key...
         ArrayList<String[]> kvsValueTypeRestrictions =
             keyToKnownKVSValueTypeRestrictions.get(kvs.key);

         if ( kvsValueTypeRestrictions != null ) {
            for (String[] restriction : kvsValueTypeRestrictions) {
               if (kvs.partition.equals(restriction[1]) && kvs.key.equals(restriction[2])) {
                  // see if the client specified an incorrect type
                  if (!kvs.type.equals(restriction[0])) {
                     String type = kvs.type;
                     kvs.type = restriction[0];

                     // TODO: detect whether the value conforms to the specified type.
                     enforceKVSValueType(kvs, ElementDataType.valueOf(restriction[0]));

                     WebLogger.getLogger(appName)
                         .w("validateKVSEntry", "Client Error: KVS value type reset from " + type +
                             " to " + restriction[0] +
                             " table: " + kvs.tableId +
                             " partition: " + restriction[1] +
                             " key: " + restriction[2]);
                  }
               }
            }
         }
      } else {
         // makes later tests easier...
         kvs.value = null;
      }
   }

   private void enforceKVSValueType(KeyValueStoreEntry e, ElementDataType type ) {
      e.type = type.name();
      if (e.value != null) {
         if ( type == ElementDataType.integer ) {
            // TODO: can add matcher if we want to
         } else if ( type == ElementDataType.number ) {
            // TODO: can add matcher if we want to
         } else if ( type == ElementDataType.bool ) {
            // TODO: can add matcher if we want to
         } else if ( type == ElementDataType.string ||
             type == ElementDataType.rowpath || type == ElementDataType.configpath ) {
            // anything goes here...
         } else if (type == ElementDataType.array) {
            // minimal test for valid representation
            if (!e.value.startsWith("[") || !e.value.endsWith("]")) {
               throw new IllegalArgumentException("array value type is not an array! " +
                   "TableId: " + e.tableId + " Partition: " + e.partition + " Aspect: " + e.aspect +
                   " Key: " + e.key);
            }
         } else if ( type == ElementDataType.object ) {
            // this could be any value type
            // TODO: test for any of the above values...
            if (e.value.startsWith("\"") && !e.value.endsWith("\"")) {
               throw new IllegalArgumentException("object value type is a malformed string! " +
                   "TableId: " + e.tableId + " Partition: " + e.partition + " Aspect: " + e.aspect +
                   " Key: " + e.key);
            }
            if (e.value.startsWith("[") && !e.value.endsWith("]")) {
               throw new IllegalArgumentException("object value type is a malformed array! " +
                   "TableId: " + e.tableId + " Partition: " + e.partition + " Aspect: " + e.aspect +
                   " Key: " + e.key);
            }
            if (e.value.startsWith("{") && !e.value.endsWith("}")) {
               throw new IllegalArgumentException("object value type is a malformed object! " +
                   "TableId: " + e.tableId + " Partition: " + e.partition + " Aspect: " + e.aspect +
                   " Key: " + e.key);
            }
         } else {
            // and who knows what goes here...
         }
      }
   }

   /**
    * REVISIT THESE TO ENFORCE SAFE UPDATES OF KVS database
    * *********************************************************************************************
    * *********************************************************************************************
    * *********************************************************************************************
    * *********************************************************************************************
    */


   /**
    * Insert or update a single table-level metadata KVS entry.
    * The tableId, partition, aspect and key cannot be null or empty strings.
    * If e.value is null or an empty string, the entry is deleted.
    *
    * @param db
    * @param e  a KeyValueStoreEntry. If e.value is null or an empty string, the entry is deleted.
    */
  public void replaceDBTableMetadata(OdkConnectionInterface db, KeyValueStoreEntry e) {
    validateKVSEntry( db.getAppName(), e.tableId, e);

    ContentValues values = new ContentValues();
    values.put(KeyValueStoreColumns.TABLE_ID, e.tableId);
    values.put(KeyValueStoreColumns.PARTITION, e.partition);
    values.put(KeyValueStoreColumns.ASPECT, e.aspect);
    values.put(KeyValueStoreColumns.KEY, e.key);
    values.put(KeyValueStoreColumns.VALUE_TYPE, e.type);
    values.put(KeyValueStoreColumns.VALUE, e.value);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }
      if (e.value == null || e.value.trim().length() == 0) {
         deleteDBTableMetadata(db, e.tableId, e.partition, e.aspect, e.key);
      } else {
         db.replaceOrThrow(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, values);
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
   * Insert or update a list of table-level metadata KVS entries. If clear is
   * true, then delete the existing set of values for this tableId before
   * inserting the new values.
   *
   * @param db
   * @param tableId
   * @param metadata a List<KeyValueStoreEntry>
   * @param clear    if true then delete the existing set of values for this tableId
   *                 before inserting the new ones.
   */
  public void replaceDBTableMetadata(OdkConnectionInterface db, String tableId,
      List<KeyValueStoreEntry> metadata, boolean clear) {

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      if (clear) {
        db.delete(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME,
            KeyValueStoreColumns.TABLE_ID + "=?", new String[] { tableId });
      }

      for (KeyValueStoreEntry e : metadata) {
        replaceDBTableMetadata(db, e);
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

   public void replaceDBTableMetadataSubList(OdkConnectionInterface db, String tableId,
       String partition, String aspect,
       List<KeyValueStoreEntry> metadata) {

      StringBuilder b = new StringBuilder();
      ArrayList<String> whereArgsList = new ArrayList<String>();

      if ( tableId == null || tableId.trim().length() == 0 ) {
         throw new IllegalArgumentException("tableId cannot be null or an empty string");
      }
      b.append(KeyValueStoreColumns.TABLE_ID).append("=?");
      whereArgsList.add(tableId);
      if ( partition != null ) {
         b.append(" AND ").append(KeyValueStoreColumns.PARTITION).append("=?");
         whereArgsList.add(partition);
      }
      if ( aspect != null ) {
         b.append(" AND ").append(KeyValueStoreColumns.ASPECT).append("=?");
         whereArgsList.add(aspect);
      }
      String whereClause = b.toString();
      String[] whereArgs = whereArgsList.toArray(new String[whereArgsList.size()]);

      boolean dbWithinTransaction = db.inTransaction();
      try {
         if (!dbWithinTransaction) {
            db.beginTransactionNonExclusive();
         }

         db.delete(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME,
             whereClause, whereArgs);

         for (KeyValueStoreEntry e : metadata) {
            replaceDBTableMetadata(db, e);
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
  public void deleteDBTableMetadata(OdkConnectionInterface db, String tableId, String partition,
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
        db.beginTransactionNonExclusive();
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
  public ArrayList<KeyValueStoreEntry> getDBTableMetadata(OdkConnectionInterface db, String tableId,
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
  public void enforceTypesDBTableMetadata(OdkConnectionInterface db, String tableId) {

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
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

       for ( String[] fields : knownKVSValueTypeRestrictions ){
          db.execSQL(sql, fields);
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

  /*
   * Create a user defined database table metadata - table definiton and KVS
   * values
   */
  private void createDBTableMetadata(OdkConnectionInterface db, String tableId) {
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
  }

  /*
   * Create a user defined database table metadata - table definiton and KVS
   * values
   */
  private void createDBTableWithColumns(OdkConnectionInterface db, String appName, String tableId,
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

    db.execSQL(createTableCmdWithCols.toString(), null);

    // Create the metadata for the table - table def and KVS
    createDBTableMetadata(db, tableId);

    // Now need to call the function to write out all the column values
    for (ColumnDefinition column : orderedDefs.getColumnDefinitions()) {
      createNewColumnMetadata(db, tableId, column);
    }
  }

  /*
   * Create a new column metadata in the database - add column values to KVS and
   * column definitions
   */
  private void createNewColumnMetadata(OdkConnectionInterface db, String tableId, ColumnDefinition column) {
    String colName = column.getElementKey();

    // Create column definition
    ContentValues cvColDefVal = new ContentValues();
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
    * Verifies that the schema the client has matches that of the given tableId.
    *
    * @param db
    * @param appName
    * @param tableId
    * @param orderedDefs
    */
   private void verifyTableSchema(OdkConnectionInterface db, String appName, String tableId,
       OrderedColumns orderedDefs ) {
      // confirm that the column definitions are unchanged...
      OrderedColumns existingDefns = getUserDefinedColumns(db, appName, tableId);
      if (existingDefns.getColumnDefinitions().size() != orderedDefs.getColumnDefinitions()
          .size()) {
         throw new IllegalStateException(
             "Unexpectedly found tableId with different column definitions that already exists!");
      }
      for (ColumnDefinition ci : orderedDefs.getColumnDefinitions()) {
         ColumnDefinition existingDefn;
         try {
            existingDefn = existingDefns.find(ci.getElementKey());
         } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unexpectedly failed to match elementKey: " + ci.getElementKey());
         }
         if (!existingDefn.getElementName().equals(ci.getElementName())) {
            throw new IllegalStateException(
                "Unexpected mis-match of elementName for elementKey: " + ci.getElementKey());
         }
         List<ColumnDefinition> refList = existingDefn.getChildren();
         List<ColumnDefinition> ciList = ci.getChildren();
         if (refList.size() != ciList.size()) {
            throw new IllegalStateException(
                "Unexpected mis-match of listOfStringElementKeys for elementKey: " + ci
                    .getElementKey());
         }
         for (int i = 0; i < ciList.size(); ++i) {
            if (!refList.contains(ciList.get(i))) {
               throw new IllegalStateException(
                   "Unexpected mis-match of listOfStringElementKeys[" + i + "] for elementKey: " + ci.getElementKey());
            }
         }
         ElementType type = ci.getType();
         ElementType existingType = existingDefn.getType();
         if (!existingType.equals(type)) {
            throw new IllegalStateException(
                "Unexpected mis-match of elementType for elementKey: " + ci.getElementKey());
         }
      }
   }

   /**
    * Compute the app-global choiceListId for this choiceListJSON
    * and register the tuple of (choiceListId, choiceListJSON).
    * Return choiceListId.
    *
    * @param db
    * @param appName
    * @param choiceListJSON -- the actual JSON choice list text.
    * @return choiceListId -- the unique code mapping to the choiceListJSON
    */
   public String setChoiceList(OdkConnectionInterface db, String appName,String choiceListJSON ) {
      ChoiceListUtils utils = new ChoiceListUtils();
      boolean dbWithinTransaction = db.inTransaction();
      boolean success = false;
      try {
         if (!dbWithinTransaction) {
            db.beginTransactionNonExclusive();
         }
         if ( choiceListJSON == null || choiceListJSON.trim().length() == 0 ) {
            return null;
         }

         String choiceListId = ODKFileUtils.getNakedMd5Hash(appName, choiceListJSON);

         utils.setChoiceList(db, choiceListId, choiceListJSON);

         if (!dbWithinTransaction) {
            db.setTransactionSuccessful();
         }
         success = true;
         return choiceListId;
      } finally {
         if (!dbWithinTransaction) {
            db.endTransaction();
         }
         if (success == false) {

            WebLogger.getLogger(appName)
                .e(t, "setChoiceList: Error while updating choiceList entry " + choiceListJSON);
         }
      }
   }

   /**
    * Return the choice list JSON corresponding to the choiceListId
    *
    * @param db
    * @param appName
    * @param choiceListId -- the md5 hash of the choiceListJSON
    * @return choiceListJSON -- the actual JSON choice list text.
    */
   public String getChoiceList(OdkConnectionInterface db, String appName, String choiceListId ) {
      ChoiceListUtils utils = new ChoiceListUtils();

      if ( choiceListId == null || choiceListId.trim().length() == 0 ) {
         return null;
      }
      return utils.getChoiceList(db, choiceListId);
   }

  /**
   * If the tableId is not recorded in the TableDefinition metadata table, then
   * create the tableId with the indicated columns. This will synthesize
   * reasonable metadata KVS entries for table.
   * <p/>
   * If the tableId is present, then this is a no-op.
   *
   * @param db
   * @param appName
   * @param tableId
   * @param columns
   * @return the ArrayList<ColumnDefinition> of the user columns in the table.
   */
  public OrderedColumns createOrOpenDBTableWithColumns(OdkConnectionInterface db, String appName,
      String tableId, List<Column> columns) {
    boolean dbWithinTransaction = db.inTransaction();
    boolean success = false;
    OrderedColumns orderedDefs = new OrderedColumns(appName, tableId, columns);
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }
      if (!hasTableId(db, tableId)) {
         createDBTableWithColumns(db, appName, tableId, orderedDefs);
      } else {
         verifyTableSchema(db, appName, tableId, orderedDefs);
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
    * If the tableId is not recorded in the TableDefinition metadata table, then
    * create the tableId with the indicated columns. This will synthesize
    * reasonable metadata KVS entries for table.
    * <p/>
    * If the tableId is present, then this is a no-op.
    *
    * @param db
    * @param appName
    * @param tableId
    * @param columns
    * @return the ArrayList<ColumnDefinition> of the user columns in the table.
    */
   public OrderedColumns createOrOpenDBTableWithColumnsAndProperties(OdkConnectionInterface db, String appName,
       String tableId, List<Column> columns, List<KeyValueStoreEntry> metaData, boolean clear)
       throws JsonProcessingException {
      boolean dbWithinTransaction = db.inTransaction();
      boolean success = false;

      OrderedColumns orderedDefs = new OrderedColumns(appName, tableId, columns);

      try {
         if (!dbWithinTransaction) {
            db.beginTransactionNonExclusive();
         }
         boolean created = false;
         if (!hasTableId(db, tableId)) {
            createDBTableWithColumns(db, appName, tableId, orderedDefs);
            created = true;
         } else {
            // confirm that the column definitions are unchanged...
            verifyTableSchema(db, appName, tableId, orderedDefs);
         }

         replaceDBTableMetadata(db, tableId, metaData, (clear || created));
         enforceTypesDBTableMetadata(db, tableId);

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
                      "createOrOpenDBTableWithColumnsAndProperties: Error while adding table " + tableId
                          + " with columns:" + colNames.toString());
               }
            } else {
               WebLogger.getLogger(appName).e(
                   t,
                   "createOrOpenDBTableWithColumnsAndProperties: Error while adding table " + tableId
                       + " with columns: null");
            }
         }
      }
   }

   /***********************************************************************************************
    * REVISIT THESE TO ENFORCE SAFE UPDATES OF KVS database
    * *********************************************************************************************
    * *********************************************************************************************
    * *********************************************************************************************
    * *********************************************************************************************
    */


   /**
    * Call this when the schema on the server has changed w.r.t. the schema on
    * the device. In this case, we do not know whether the rows on the device
    * match those on the server.
    * <p/>
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
  public void changeDataRowsToNewRowState(OdkConnectionInterface db, String tableId) {

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
        db.beginTransactionNonExclusive();
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
    *
    * Clean up this table and set the dataETag to null.
    *
    * changeDataRowsToNewRowState(sc.getAppName(), db, tableId);
    *
    * we need to clear out the dataETag so
    * that we will pull all server changes and sync our properties.
    *
    * updateDBTableETags(sc.getAppName(), db, tableId, null, null);
    *
    * Although the server does not recognize this tableId, we can
    * keep our record of the ETags for the table-level files and
    * manifest. These may enable us to short-circuit the restoration
    * of the table-level files should another client be simultaneously
    * trying to restore those files to the server.
    *
    * However, we do need to delete all the instance-level files,
    * as these are tied to the schemaETag we hold, and that is now
    * invalid.
    *
    * if the local table ever had any server sync information for this
    * host then clear it. If the user changed the server URL, we have
    * already cleared this information.
    *
    * Clearing it here handles the case where an admin deleted the
    * table on the server and we are now re-pushing that table to
    * the server.
    *
    * We do not know whether the rows on the device match those on the server.
    * We will find out later, in the course of the sync.
    *
    * if (tableInstanceFilesUri != null) {
    *   deleteAllSyncETagsUnderServer(sc.getAppName(), db, tableInstanceFilesUri);
    * }
    *
    * @param db
    * @param tableId
    * @parma schemaETag
    * @param tableInstanceFilesUri
    */
   public void serverTableSchemaETagChanged(OdkConnectionInterface db,
       String tableId, String schemaETag, String tableInstanceFilesUri) {

      boolean dbWithinTransaction = db.inTransaction();
      try {
         if (!dbWithinTransaction) {
            db.beginTransactionNonExclusive();
         }

         changeDataRowsToNewRowState(db, tableId);

         updateDBTableETags(db, tableId, schemaETag, null);

         if (tableInstanceFilesUri != null) {
            SyncETagsUtils seu = new SyncETagsUtils();
            seu.deleteAllSyncETagsUnderServer(db, tableInstanceFilesUri);
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
   * Deletes the server conflict row (if any) for this rowId in this tableId.
   * 
   * @param db
   * @param tableId
   * @param rowId
   */
  private void deleteServerConflictRowWithId(OdkConnectionInterface db, String tableId,
      String rowId) {
    // delete the old server-values in_conflict row if it exists
    String whereClause = String.format("%s = ? AND %s = ? AND %s IN ( ?, ? )",
        DataTableColumns.ID, DataTableColumns.SYNC_STATE, DataTableColumns.CONFLICT_TYPE);
    String[] whereArgs = { rowId, SyncState.in_conflict.name(),
        String.valueOf(ConflictType.SERVER_DELETED_OLD_VALUES),
        String.valueOf(ConflictType.SERVER_UPDATED_UPDATED_VALUES) };

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
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
   * Delete any prior server conflict row.
   * Move the local row into the indicated local conflict state.
   * Insert a server row with the values specified in the cvValues array.
   *
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @param localRowConflictType
   */
  public void placeRowIntoServerConflictWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId, int localRowConflictType) {


    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      this.deleteServerConflictRowWithId(db, tableId, rowId);
      this.placeRowIntoConflict(db, tableId, rowId, localRowConflictType);
      this.insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);

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
  private void placeRowIntoConflict(OdkConnectionInterface db, String tableId, String rowId, int
      conflictType) {

    String whereClause = String.format("%s = ? AND %s IS NULL", DataTableColumns.ID,
        DataTableColumns.CONFLICT_TYPE);
    String[] whereArgs = { rowId };

    ContentValues cv = new ContentValues();
    cv.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());
    cv.put(DataTableColumns.CONFLICT_TYPE, conflictType);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
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
  public void restoreRowFromConflict(OdkConnectionInterface db, String tableId, String rowId,
      SyncState syncState, Integer conflictType) {

    String whereClause;
    String[] whereArgs;

    if ( conflictType == null ) {
      whereClause = String.format("%s = ? AND %s IS NULL", DataTableColumns.ID,
          DataTableColumns.CONFLICT_TYPE);
      whereArgs = new String[]{ rowId };
    } else {
      whereClause = String.format("%s = ? AND %s = ?", DataTableColumns.ID,
          DataTableColumns.CONFLICT_TYPE);
      whereArgs = new String[]{ rowId, String.valueOf(conflictType) };
    }

    ContentValues cv = new ContentValues();
    cv.putNull(DataTableColumns.CONFLICT_TYPE);
    cv.put(DataTableColumns.SYNC_STATE, syncState.name());
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
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
   * @throws IllegalStateException if the row has conflicts or checkpoints
   */
  public SyncState getSyncState(OdkConnectionInterface db, String appName, String tableId, String rowId)
      throws IllegalStateException {
    Cursor c = null;
    try {
      c = db.query(tableId, new String[] { DataTableColumns.SYNC_STATE }, DataTableColumns.ID
          + " = ?", new String[] { rowId }, null, null, null, null);
      c.moveToFirst();
      if (c.getCount() > 1) {
         throw new IllegalStateException(t + ": row has conflicts or checkpoints");
      }
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
  public void deleteRowWithId(OdkConnectionInterface db, String appName, String tableId,
      String rowId) {

    boolean shouldPhysicallyDelete = false;

    boolean dbWithinTransaction = db.inTransaction();
    try {
       if (!dbWithinTransaction) {
          db.beginTransactionNonExclusive();
       }
       SyncState syncState = getSyncState(db, appName, tableId, rowId);

       if (syncState == SyncState.new_row) {
         // we can safely remove this record from the database
         String[] whereArgs = { rowId };
         String whereClause = DataTableColumns.ID + " = ?";

         db.delete(tableId, whereClause, whereArgs);
         shouldPhysicallyDelete = true;

       } else if (syncState != SyncState.in_conflict) {

         String[] whereArgs = { rowId };
         ContentValues values = new ContentValues();
         values.put(DataTableColumns.SYNC_STATE, SyncState.deleted.name());
         values.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
             TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));

         db.update(tableId, values, DataTableColumns.ID + " = ?", whereArgs);
       }
       // TODO: throw exception if in the SyncState.in_conflict state?

       if (!dbWithinTransaction) {
         db.setTransactionSuccessful();
       }
    } finally {
       if (!dbWithinTransaction) {
          db.endTransaction();
       }
    }

    if ( shouldPhysicallyDelete ) {
       File instanceFolder = new File(ODKFileUtils.getInstanceFolder(appName, tableId, rowId));
       try {
          FileUtils.deleteDirectory(instanceFolder);
       } catch (IOException e) {
          // TODO Auto-generated catch block
          WebLogger.getLogger(appName)
               .e(t, "Unable to delete this directory: " + instanceFolder.getAbsolutePath());
          WebLogger.getLogger(appName).printStackTrace(e);
       }
    }
  }

  /*
   * Internal method to execute a delete statement with the given where clause
   */
  private void rawDeleteDataInDBTable(OdkConnectionInterface db, String tableId, String whereClause,
      String[] whereArgs) {
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
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
  public void deleteAllCheckpointRowsWithId(OdkConnectionInterface db, String appName,
      String tableId, String rowId) {
    rawDeleteDataInDBTable(db, tableId, DataTableColumns.ID + "=? AND "
            + DataTableColumns.SAVEPOINT_TYPE + " IS NULL", new String[] { rowId });
  }

  /**
   * Delete any checkpoint rows for the given rowId in the tableId. Checkpoint
   * rows are created by ODK Survey to hold intermediate values during the
   * filling-in of the form. They act as restore points in the Survey, should
   * the application die.
   *  @param db
   * @param tableId
   * @param rowId
   */
  public void deleteLastCheckpointRowWithId(OdkConnectionInterface db, String tableId,
                                            String rowId) {
    rawDeleteDataInDBTable(db, tableId, DataTableColumns.ID + "=? AND "
            + DataTableColumns.SAVEPOINT_TYPE + " IS NULL "
            + " AND " + DataTableColumns.SAVEPOINT_TIMESTAMP
            + " IN (SELECT MAX(" + DataTableColumns.SAVEPOINT_TIMESTAMP + ") FROM \"" + tableId
            + "\" WHERE " + DataTableColumns.ID + "=?)", new String[]{rowId, rowId});
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
  public void saveAsIncompleteMostRecentCheckpointRowWithId(OdkConnectionInterface db,
      String tableId, String rowId) {
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
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
   * Update all rows for the given rowId to SavepointType 'COMPLETE' and
   * remove all but the most recent row. When used with a rowId that has
   * checkpoints, this updates to the most recent checkpoint and removes any
   * earlier checkpoints, incomplete or complete savepoints. Otherwise, it has
   * the general effect of resetting the rowId to an COMPLETE state.
   *
   * @param db
   * @param tableId
   * @param rowId
   */
  public void saveAsCompleteMostRecentCheckpointRowWithId(OdkConnectionInterface db, String tableId,
      String rowId) {
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.execSQL("UPDATE \"" + tableId + "\" SET " + DataTableColumns.SAVEPOINT_TYPE + "= ? WHERE "
                      + DataTableColumns.ID + "=?",
              new String[] { SavepointTypeManipulator.complete(), rowId });
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
  public void updateRowWithId(OdkConnectionInterface db, String tableId,
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
    * Delete the local and server conflict records to resolve a server conflict
    *
    * A combination of primitive actions, all performed in one transaction:
    *
    * // delete the record of the server row
    * deleteServerConflictRowWithId(appName, dbHandleName, tableId, rowId);
    *
    * // move the local record into the 'new_row' sync state
    * // so it can be physically deleted.
    * updateRowETagAndSyncState(appName, dbHandleName, tableId, rowId, null,
    *                           SyncState.new_row.name());
    * // move the local conflict back into the normal (null) state
    * deleteRowWithId(appName, dbHandleName, tableId, rowId);
    *
    * @param db
    * @param appName
    * @param tableId
    * @param rowId
    */
   public void resolveServerConflictWithDeleteRowWithId(OdkConnectionInterface db, String appName,
       String tableId, String rowId) {

      boolean inTransaction = false;
      try {

         inTransaction = db.inTransaction();
         if ( !inTransaction ) {
            db.beginTransactionNonExclusive();
         }

         // delete the record of the server row
         ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);

         // move the local record into the 'new_row' sync state
         // so it can be physically deleted.
         ODKDatabaseImplUtils.get().updateRowETagAndSyncState(db, tableId, rowId, null,
             SyncState.new_row);

         // move the local conflict back into the normal (null) state
         ODKDatabaseImplUtils.get().deleteRowWithId(db, appName, tableId, rowId);

         if ( !inTransaction ) {
            db.setTransactionSuccessful();
         }
      } finally {
         if ( db != null ) {
            if ( !inTransaction ) {
               db.endTransaction();
            }
         }
      }
   }

  /**
   * Resolve the server conflict by taking the local changes.
   * If the local changes are to delete this record, the record will be deleted
   * upon the next successful sync.
   *
   * @param db
   * @param appName
   * @param tableId
   * @param rowId
   */
  public void resolveServerConflictTakeLocalRowWithId(OdkConnectionInterface db, String appName,
      String tableId, String rowId) {

    boolean inTransaction = false;
    try {

      inTransaction = db.inTransaction();
      if ( !inTransaction ) {
        db.beginTransactionNonExclusive();
      }

      OrderedColumns orderedColumns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db,
          appName, tableId);

      // get both conflict records for this row.
      // the local record is always before the server record (due to conflict_type values)
      UserTable table = ODKDatabaseImplUtils.get().rawSqlQuery(db, appName, tableId, orderedColumns,
          DataTableColumns.ID + "=?" +
              " AND " + DataTableColumns.CONFLICT_TYPE + " IS NOT NULL", new String[] { rowId },
          null, null, DataTableColumns.CONFLICT_TYPE, "ASC");

      if ( table.getNumberOfRows() != 2) {
        throw new IllegalStateException(
            "Did not find a server and local row when resolving conflicts for rowId: " + rowId);
      }
      Row localRow = table.getRowAtIndex(0);
      Row serverRow = table.getRowAtIndex(1);

      int localConflictType = Integer.parseInt(
          localRow.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE));

      int serverConflictType = Integer.parseInt(
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE));

      if ( localConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES &&
           localConflictType != ConflictType.LOCAL_DELETED_OLD_VALUES ) {
        throw new IllegalStateException(
            "Did not find local conflict row when resolving conflicts for rowId: " + rowId);
      }

      if ( serverConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES &&
           serverConflictType != ConflictType.SERVER_DELETED_OLD_VALUES ) {
        throw new IllegalStateException(
            "Did not find server conflict row when resolving conflicts for rowId: " + rowId);
      }

      // update what was the local conflict record with the local's changes
      // by the time we apply the update, the local conflict record will be
      // restored to the proper (conflict_type, sync_state) values.
      //
      // No need to specify them here.
      ContentValues updateValues = new ContentValues();
      updateValues.put(DataTableColumns.ROW_ETAG,
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.ROW_ETAG));

      // take the server's filter metadata values ...
      updateValues.put(DataTableColumns.FILTER_TYPE,
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.FILTER_TYPE));
      updateValues.put(DataTableColumns.FILTER_VALUE,
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.FILTER_VALUE));

      // Figure out whether to take the server or local metadata fields.
      // and whether to take the server or local data fields.

      SyncState finalSyncState = SyncState.changed;

      if ( localConflictType == ConflictType.LOCAL_UPDATED_UPDATED_VALUES ) {
        // We are updating -- preserve the local metadata and column values
        // this is a no-op, as we are updating the local record, so we don't
        // need to do anything special.
      } else {
        finalSyncState = SyncState.deleted;

        // Deletion is really a "TakeServerChanges" action, but ending with 'deleted' as
        // the final sync state.

        // copy everything over from the server row
        updateValues.put(DataTableColumns.FORM_ID,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.FORM_ID));
        updateValues.put(DataTableColumns.LOCALE,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.LOCALE));
        updateValues.put(DataTableColumns.SAVEPOINT_TYPE,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TYPE));
        updateValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TIMESTAMP));
        updateValues.put(DataTableColumns.SAVEPOINT_CREATOR,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_CREATOR));

        // including the values of the user fields on the server
        for ( String elementKey : orderedColumns.getRetentionColumnNames()) {
          updateValues.put(elementKey, serverRow.getRawDataOrMetadataByElementKey(elementKey));
        }
      }

      // delete the record of the server row
      ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);

      // move the local conflict back into the normal non-conflict (null) state
      // and set the final sync state.
      ODKDatabaseImplUtils.get().restoreRowFromConflict(db, tableId, rowId, finalSyncState,
          localConflictType);

      // update local with the changes
      ODKDatabaseImplUtils.get().updateRowWithId(db, tableId, orderedColumns, updateValues, rowId);

      // and reset the sync state to whatever it should be (update will make it changed)
      ODKDatabaseImplUtils.get().restoreRowFromConflict(db, tableId, rowId, finalSyncState, null);

      if ( !inTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( db != null ) {
        if ( !inTransaction ) {
          db.endTransaction();
        }
      }
    }
  }

  /**
   * Resolve the server conflict by taking the local changes plus a value map
   * of select server field values.  This map should not update any metadata
   * fields -- it should just contain user data fields.
   *
   * It is an error to call this if the local change is to delete the row.
   *
   * @param db
   * @param appName
   * @param tableId
   * @param cvValues  key-value pairs from the server record that we should incorporate.
   * @param rowId
   */
  public void resolveServerConflictTakeLocalRowPlusServerDeltasWithId(OdkConnectionInterface db,
      String appName, String tableId, ContentValues cvValues, String rowId) {

    boolean inTransaction = false;
    try {

      inTransaction = db.inTransaction();
      if ( !inTransaction ) {
        db.beginTransactionNonExclusive();
      }

      OrderedColumns orderedColumns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db,
          appName, tableId);

      // get both conflict records for this row.
      // the local record is always before the server record (due to conflict_type values)
      UserTable table = ODKDatabaseImplUtils.get().rawSqlQuery(db, appName, tableId, orderedColumns,
          DataTableColumns.ID + "=?" +
              " AND " + DataTableColumns.CONFLICT_TYPE + " IS NOT NULL", new String[] { rowId },
          null, null, DataTableColumns.CONFLICT_TYPE, "ASC");

      if ( table.getNumberOfRows() != 2) {
        throw new IllegalStateException(
            "Did not find a server and local row when resolving conflicts for rowId: " + rowId);
      }
      Row localRow = table.getRowAtIndex(0);
      Row serverRow = table.getRowAtIndex(1);

      int localConflictType = Integer.parseInt(
          localRow.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE));

      int serverConflictType = Integer.parseInt(
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE));

      if ( localConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES &&
          localConflictType != ConflictType.LOCAL_DELETED_OLD_VALUES ) {
        throw new IllegalStateException(
            "Did not find local conflict row when resolving conflicts for rowId: " + rowId);
      }

      if ( serverConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES &&
          serverConflictType != ConflictType.SERVER_DELETED_OLD_VALUES ) {
        throw new IllegalStateException(
            "Did not find server conflict row when resolving conflicts for rowId: " + rowId);
      }

      if ( localConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES ) {
        throw new IllegalStateException(
            "Local row is marked for deletion -- blending does not make sense for rowId: " +
                rowId);
      }

      // clean up the incoming map of server values to retain
      cleanUpValuesMap(orderedColumns, cvValues);
      // update the local conflict record with the local's changes
      ContentValues updateValues = cvValues;
      updateValues.put(DataTableColumns.ROW_ETAG,
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.ROW_ETAG));

      // update what was the local conflict record with the local's changes
      // by the time we apply the update, the local conflict record will be
      // restored to the proper (conflict_type, sync_state) values.
      //
      // No need to specify them here.

      // take the server's filter metadata values ...
      updateValues.put(DataTableColumns.FILTER_TYPE,
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.FILTER_TYPE));
      updateValues.put(DataTableColumns.FILTER_VALUE,
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.FILTER_VALUE));

      // but take the local's metadata values (i.e., do not change these
      // during the update) ...
      updateValues.put(DataTableColumns.FORM_ID,
          localRow.getRawDataOrMetadataByElementKey(DataTableColumns.FORM_ID));
      updateValues.put(DataTableColumns.LOCALE,
          localRow.getRawDataOrMetadataByElementKey(DataTableColumns.LOCALE));
      updateValues.put(DataTableColumns.SAVEPOINT_TYPE,
          localRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TYPE));
      updateValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
          localRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TIMESTAMP));
      updateValues.put(DataTableColumns.SAVEPOINT_CREATOR,
          localRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_CREATOR));

      // delete the record of the server row
      ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);

      // move the local conflict back into the normal (null) state
      ODKDatabaseImplUtils.get().restoreRowFromConflict(db, tableId, rowId, SyncState.changed,
          localConflictType);

      // update local with server's changes
      ODKDatabaseImplUtils.get().updateRowWithId(db, tableId, orderedColumns, updateValues, rowId);

      if ( !inTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( db != null ) {
        if ( !inTransaction ) {
          db.endTransaction();
        }
      }
    }
  }

  /**
   * Resolve the server conflict by taking the server changes.  This may delete the local row.
   *
   * @param db
   * @param appName
   * @param tableId
   * @param rowId
   */
  public void resolveServerConflictTakeServerRowWithId(OdkConnectionInterface db, String appName,
      String tableId, String rowId) {

    boolean inTransaction = false;
    try {

      inTransaction = db.inTransaction();
      if ( !inTransaction ) {
        db.beginTransactionNonExclusive();
      }

      OrderedColumns orderedColumns = ODKDatabaseImplUtils.get().getUserDefinedColumns(db,
          appName, tableId);

      // get both conflict records for this row.
      // the local record is always before the server record (due to conflict_type values)
      UserTable table = ODKDatabaseImplUtils.get().rawSqlQuery(db, appName, tableId, orderedColumns,
          DataTableColumns.ID + "=?" +
              " AND " + DataTableColumns.CONFLICT_TYPE + " IS NOT NULL", new String[] { rowId },
          null, null, DataTableColumns.CONFLICT_TYPE, "ASC");

      if ( table.getNumberOfRows() != 2) {
        throw new IllegalStateException(
            "Did not find a server and local row when resolving conflicts for rowId: " + rowId);
      }
      Row localRow = table.getRowAtIndex(0);
      Row serverRow = table.getRowAtIndex(1);

      int localConflictType = Integer.parseInt(
          localRow.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE));

      int serverConflictType = Integer.parseInt(
          serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE));

      if ( localConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES &&
          localConflictType != ConflictType.LOCAL_DELETED_OLD_VALUES ) {
        throw new IllegalStateException(
            "Did not find local conflict row when resolving conflicts for rowId: " + rowId);
      }

      if ( serverConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES &&
          serverConflictType != ConflictType.SERVER_DELETED_OLD_VALUES ) {
        throw new IllegalStateException(
            "Did not find server conflict row when resolving conflicts for rowId: " + rowId);
      }

      if ( serverConflictType == ConflictType.SERVER_DELETED_OLD_VALUES ) {

        // delete the record of the server row
        ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);

        // move the local record into the 'new_row' sync state
        // so it can be physically deleted.
        ODKDatabaseImplUtils.get().updateRowETagAndSyncState(db, tableId, rowId, null,
            SyncState.new_row);

        // and delete the local conflict and all of its associated attachments
        ODKDatabaseImplUtils.get().deleteRowWithId(db, appName, tableId, rowId);

      } else {
        // update the local conflict record with the server's changes
        ContentValues updateValues = new ContentValues();
        updateValues.put(DataTableColumns.ROW_ETAG,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.ROW_ETAG));

        // update what was the local conflict record with the server's changes
        // by the time we apply the update, the local conflict record will be
        // restored to the proper (conflict_type, sync_state) values.
        //
        // No need to specify them here.

        // take the server's metadata values too...
        updateValues.put(DataTableColumns.FILTER_TYPE,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.FILTER_TYPE));
        updateValues.put(DataTableColumns.FILTER_VALUE,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.FILTER_VALUE));
        updateValues.put(DataTableColumns.FORM_ID,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.FORM_ID));
        updateValues.put(DataTableColumns.LOCALE,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.LOCALE));
        updateValues.put(DataTableColumns.SAVEPOINT_TYPE,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TYPE));
        updateValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TIMESTAMP));
        updateValues.put(DataTableColumns.SAVEPOINT_CREATOR,
            serverRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_CREATOR));

        // take all the data values from the server...
        for (String elementKey : orderedColumns.getRetentionColumnNames()) {
          updateValues.put(elementKey, serverRow.getRawDataOrMetadataByElementKey(elementKey));
        }

        // determine whether we should flag this as pending files
        // or whether it can transition directly to sync'd.
        SyncState newState;
        {
          boolean hasUriFragments = false;
          // we are collapsing to the server state. Examine the
          // server row. Look at all the columns that may contain file
          // attachments. If they do (non-null, non-empty), then
          // set the hasUriFragments flag to true and break out of the loop.
          //
          // Set the resolved row to synced_pending_files if there are
          // non-null, non-empty file attachments in the row. This
          // ensures that we will pull down those attachments at the next
          // sync.
          for ( ColumnDefinition cd : orderedColumns.getColumnDefinitions() ) {
            if (cd.getType().getDataType() != ElementDataType.rowpath) {
              // not a file attachment
              continue;
            }
            String v = serverRow.getRawDataOrMetadataByElementKey(cd.getElementKey());
            if ( v != null && v.length() != 0 ) {
              // non-null file attachment specified on server row
              hasUriFragments = true;
              break;
            }
          }
          newState = hasUriFragments ? SyncState.synced_pending_files :
              SyncState.synced;
        }

        // delete the record of the server row
        ODKDatabaseImplUtils.get().deleteServerConflictRowWithId(db, tableId, rowId);

        // move the local conflict back into either the synced or synced_pending_files
        // state
        ODKDatabaseImplUtils.get().restoreRowFromConflict(db, tableId, rowId, newState,
            localConflictType);

        // update local with server's changes
        ODKDatabaseImplUtils.get().updateRowWithId(db, tableId, orderedColumns, updateValues, rowId);

        // and reset the sync state to whatever it should be (update will make it changed)
        ODKDatabaseImplUtils.get().restoreRowFromConflict(db, tableId, rowId, newState, null);
      }

      if ( !inTransaction ) {
        db.setTransactionSuccessful();
      }
    } finally {
      if ( db != null ) {
        if ( !inTransaction ) {
          db.endTransaction();
        }
      }
    }
  }

  /**
   * Inserts a checkpoint row for the given rowId in the tableId. Checkpoint
   * rows are created by ODK Survey to hold intermediate values during the
   * filling-in of the form. They act as restore points in the Survey, should
   * the application die.
   *
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   */
  public void insertCheckpointRowWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId) {

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table for checkpoint" + tableId);
    }

    if (cvValues.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
      throw new IllegalArgumentException(t + ": No user supplied savepoint timestamp can be included for a checkpoint");
    }

    if (cvValues.containsKey(DataTableColumns.SAVEPOINT_TYPE)) {
      throw new IllegalArgumentException(t + ": No user supplied savepoint type can be included for a checkpoint");
    }

    if (cvValues.containsKey(DataTableColumns.CONFLICT_TYPE)) {
      throw new IllegalArgumentException(t + ": No user supplied conflict type can be included for a checkpoint");
    }

    // If a rowId is specified, a cursor will be needed to
    // get the current row to create a checkpoint with the relevant
    // data
    Cursor c = null;
    try {
      // Allow the user to pass in no rowId if this is the first
      // checkpoint row that the user is adding
      if (rowId == null) {
        String rowIdToUse = ODKDataUtils.genUUID();
        ContentValues currValues = new ContentValues();
        currValues.putAll(cvValues);
        currValues.put(DataTableColumns._ID, rowIdToUse);
        insertCheckpointIntoExistingDBTable(db, tableId, orderedColumns, currValues);
        return;
      }

      c = db.query(tableId, null,
          DataTableColumns.ID + "=?" + " AND " + DataTableColumns.SAVEPOINT_TIMESTAMP + " IN (SELECT MAX(" + DataTableColumns.SAVEPOINT_TIMESTAMP + ") FROM \"" + tableId
              + "\" WHERE " + DataTableColumns.ID + "=?)", new String[] { rowId, rowId }, null,
          null, null, null);
      c.moveToFirst();

      if (c.getCount() > 1) {
        throw new IllegalStateException(t + ": More than one checkpoint at a timestamp");
      }

      // Inserting a checkpoint for the first time
      if (c.getCount() <= 0) {
        cvValues.put(DataTableColumns._ID, rowId);
        insertCheckpointIntoExistingDBTable(db, tableId, orderedColumns, cvValues);
        return;
      } else {
        // Make sure that the conflict_type of any existing row
        // is null, otherwise throw an exception
        int conflictIndex = c.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
        if (!c.isNull(conflictIndex)) {
          throw new IllegalStateException(t + ":  A checkpoint cannot be added for a row that is in conflict");
        }

        ContentValues currValues = new ContentValues();

        currValues.putAll(cvValues);

        // This is unnecessary
        // We should only have one row at this point
        //c.moveToFirst();

        // Get the number of columns to iterate over and add
        // those values to the content values
        for (int i = 0; i < c.getColumnCount(); i++) {
          String name = c.getColumnName(i);

          if (currValues.containsKey(name)) {
            continue;
          }

          if (c.isNull(i) || name.equals(DataTableColumns.SAVEPOINT_TIMESTAMP) ||
              name.equals(DataTableColumns.SAVEPOINT_TYPE)) {
            currValues.putNull(name);
            continue;
          }

          Class<?> theClass = ODKCursorUtils.getIndexDataType(c, i);
          Object object = ODKCursorUtils.getIndexAsType(c, theClass, i);
          insertValueIntoContentValues(currValues, theClass, name, object);
        }

        insertCheckpointIntoExistingDBTable(db, tableId, orderedColumns, currValues);
      }
    } finally {
      if ( c != null && !c.isClosed() ) {
        c.close();
      }
    }
  }

  private void insertValueIntoContentValues(ContentValues cv, Class<?> theClass, String name, Object obj) {

    if (obj == null) {
      cv.putNull(name);
      return;
    }

    // Couldn't use the ODKCursorUtils.getIndexAsType
    // because assigning the result to Object v
    // would not work for the currValues.put function
    if (theClass == Long.class) {
      cv.put(name, (Long) obj);
    } else if (theClass == Integer.class) {
      cv.put(name, (Integer)obj);
    } else if (theClass == Double.class) {
      cv.put(name, (Double)obj);
    } else if (theClass == String.class) {
      cv.put(name, (String)obj);
    } else if (theClass == Boolean.class) {
      // stored as integers
      Integer v = (Integer)obj;
      cv.put(name, Boolean.valueOf(v != 0));
    } else if (theClass == ArrayList.class) {
      cv.put(name, (String)obj);
    } else if (theClass == HashMap.class) {
      // json deserialization of an object
      cv.put(name, (String)obj);
    } else {
      throw new IllegalStateException("Unexpected data type in SQLite table " + theClass.toString());
    }
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
  public void insertRowWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId) {

    if (cvValues == null || cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.put(DataTableColumns.ID, rowId);
    cvDataTableVal.putAll(cvValues);

    upsertDataIntoExistingDBTable(db, tableId, orderedColumns, cvDataTableVal, false);
  }

  /**
   * Write checkpoint into the database
   *
   */
  private void insertCheckpointIntoExistingDBTable(OdkConnectionInterface db, String tableId,
                                             OrderedColumns orderedColumns, ContentValues cvValues) {
    String whereClause = null;
    String[] whereArgs = new String[1];
    String rowId = null;

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.putAll(cvValues);

    if (cvDataTableVal.containsKey(DataTableColumns.ID)) {

      rowId = cvDataTableVal.getAsString(DataTableColumns.ID);
      if (rowId == null) {
        throw new IllegalArgumentException(DataTableColumns.ID + ", if specified, cannot be null");
      }
    } else {
      throw new IllegalArgumentException(t + ": rowId should not be null in insertCheckpointIntoExistingDBTable in the ContentValues");
    }

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
      cvDataTableVal.putNull(DataTableColumns.SAVEPOINT_TYPE);
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


    cleanUpValuesMap(orderedColumns, cvDataTableVal);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.insertOrThrow(tableId, null, cvDataTableVal);

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
   * Write data into a user defined database table
   * 
   * TODO: This is broken w.r.t. updates of partial fields
   */
  private void upsertDataIntoExistingDBTable(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, boolean shouldUpdate) {
    String rowId = null;
    String whereClause = null;
    boolean specifiesConflictType = cvValues.containsKey(DataTableColumns.CONFLICT_TYPE);
    boolean nullConflictType = specifiesConflictType
        && (cvValues.get(DataTableColumns.CONFLICT_TYPE) == null);
    String[] whereArgs = new String[specifiesConflictType ? (1 + (nullConflictType ? 0 : 1)) : 1];
    boolean update = false;
    String updatedSyncState = SyncState.new_row.name();

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    ContentValues cvDataTableVal = new ContentValues();
    cvDataTableVal.putAll(cvValues);


     boolean dbWithinTransaction = db.inTransaction();
     try {
       if (!dbWithinTransaction) {
         db.beginTransactionNonExclusive();
       }

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
         Cursor cursor = null;
          try {
            cursor = rawQuery(db, sel, selArgs);
            cursor.moveToFirst();

            // There must be only one row in the db for the update to work
            if (shouldUpdate) {
              if (cursor.getCount() == 1) {
             if (cursor.moveToFirst()) {
               int syncStateCursorIndex = cursor.getColumnIndex(DataTableColumns.SYNC_STATE);
               updatedSyncState = cursor.getString(syncStateCursorIndex);

               if (updatedSyncState.equals(SyncState.deleted.name()) ||
                   updatedSyncState.equals(SyncState.in_conflict.name())) {
                 throw new IllegalStateException(t + ": Cannot update a deleted or in-conflict row");
               } else if (updatedSyncState.equals(SyncState.synced.name()) ||
                   updatedSyncState.equals(SyncState.synced_pending_files.name())) {
                 updatedSyncState = SyncState.changed.name();
               }
             }
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
          } finally {
             if ( cursor != null ) {
                cursor.close();
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
           cvDataTableVal.put(DataTableColumns.SYNC_STATE, updatedSyncState);
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
   * record for this rowId in the database (i.e., no conflicts or checkpoints).
   * 
   * @param db
   * @param tableId
   * @param rowId
   * @param rowETag
   * @param state
   */
  public void updateRowETagAndSyncState(OdkConnectionInterface db, String tableId, String rowId,
      String rowETag, SyncState state) {

    String whereClause = DataTableColumns.ID + " = ?";
    String[] whereArgs = { rowId };

    ContentValues cvDataTableVal = new ContentValues();

    cvDataTableVal.put(DataTableColumns.ROW_ETAG, rowETag);
    cvDataTableVal.put(DataTableColumns.SYNC_STATE, state.name());

    Cursor cursor = null;
     boolean dbWithinTransaction = db.inTransaction();
     try {
        if (!dbWithinTransaction) {
           db.beginTransactionNonExclusive();
        }

       String sel = "SELECT * FROM " + tableId + " WHERE " + whereClause;
       String[] selArgs = whereArgs;

       try {
         cursor = rawQuery(db, sel, selArgs);

         cursor.moveToFirst();

         // There must be only one row in the db
         if (cursor.getCount() != 1) {
           throw new IllegalArgumentException(
               t + ": row id " + rowId + " does not have exactly 1 row in table " + tableId);
         }
       } finally {
         if ( cursor != null && !cursor.isClosed()) {
           cursor.close();
         }
       }

        db.update(tableId, cvDataTableVal, whereClause, whereArgs);

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

  public static void initializeDatabase(OdkConnectionInterface db) {
    commonTableDefn(db);
  }

  private static void commonTableDefn(OdkConnectionInterface db) {
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", "starting");
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", DatabaseConstants.UPLOADS_TABLE_NAME);
    db.execSQL(InstanceColumns.getTableCreateSql(DatabaseConstants.UPLOADS_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", DatabaseConstants.FORMS_TABLE_NAME);
    db.execSQL(FormsColumns.getTableCreateSql(DatabaseConstants.FORMS_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);
    db.execSQL(ColumnDefinitionsColumns.getTableCreateSql(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);
    db.execSQL(KeyValueStoreColumns.getTableCreateSql(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", DatabaseConstants.TABLE_DEFS_TABLE_NAME);
    db.execSQL(TableDefinitionsColumns.getTableCreateSql(DatabaseConstants.TABLE_DEFS_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", DatabaseConstants.SYNC_ETAGS_TABLE_NAME);
    db.execSQL(SyncETagColumns.getTableCreateSql(DatabaseConstants.SYNC_ETAGS_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", DatabaseConstants.CHOICE_LIST_TABLE_NAME);
    db.execSQL(ChoiceListColumns.getTableCreateSql(DatabaseConstants.CHOICE_LIST_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", "done");
  }
}
