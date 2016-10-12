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
package org.opendatakit.services.database.utlities;

import android.content.ContentValues;
import android.database.Cursor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.LocalKeyValueStoreConstants;
import org.opendatakit.database.data.*;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.database.utilities.KeyValueStoreUtils;
import org.opendatakit.database.utilities.QueryUtil;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.utilities.DataHelper;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.StaticStateManipulator;
import org.opendatakit.utilities.StaticStateManipulator.IStaticFieldManipulator;
import org.opendatakit.database.queries.QueryBounds;
import org.opendatakit.database.RoleConsts;
import org.opendatakit.provider.*;
import org.sqlite.database.sqlite.SQLiteException;

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

public class ODKDatabaseImplUtils {

  private static final String t = "ODKDatabaseImplUtils";

  /**
   * Constants to minimize creation of String objects on the stack.
   *
   * Used with StringBuilder to reduce GC overhead
   */
  private static final String K_SELECT_FROM = "SELECT * FROM ";
  private static final String S_AND = " AND ";
  private static final String S_EQUALS_PARAM = " =?";
  private static final String S_IS_NULL = " IS NULL";
  private static final String S_IS_NOT_NULL = " IS NOT NULL";
  private static final String K_WHERE = " WHERE ";
  private static final String K_LIMIT = " LIMIT ";
  private static final String K_OFFSET = " OFFSET ";

  private static final String K_TABLE_DEFS_TABLE_ID_EQUALS_PARAM = TableDefinitionsColumns.TABLE_ID + S_EQUALS_PARAM;

  private static final String K_COLUMN_DEFS_TABLE_ID_EQUALS_PARAM = ColumnDefinitionsColumns.TABLE_ID + S_EQUALS_PARAM;

  private static final String K_KVS_TABLE_ID_EQUALS_PARAM = KeyValueStoreColumns.TABLE_ID + S_EQUALS_PARAM;
  private static final String K_KVS_PARTITION_EQUALS_PARAM = KeyValueStoreColumns.PARTITION + S_EQUALS_PARAM;
  private static final String K_KVS_ASPECT_EQUALS_PARAM = KeyValueStoreColumns.ASPECT + S_EQUALS_PARAM;
  private static final String K_KVS_KEY_EQUALS_PARAM = KeyValueStoreColumns.KEY + S_EQUALS_PARAM;

  private static final String K_DATATABLE_ID_EQUALS_PARAM = DataTableColumns.ID + S_EQUALS_PARAM;

  /**
   * The rolesList expansion is very time consuming.
   * Implement a simple 1-deep cache and a
   * special expansion of the privileged user roles list.
   */
  private static String cachedRolesList;
  private static List<String> cachedRolesArray;
  private static final List<String> cachedAdminRolesArray;
  private static final TypeReference<ArrayList<String>> arrayListTypeReference;

  static {
    arrayListTypeReference = new TypeReference<ArrayList<String>>() {};

    ArrayList<String> rolesArray = null;
    {
      try {
        rolesArray = ODKFileUtils.mapper.readValue(RoleConsts.ADMIN_ROLES_LIST,
            arrayListTypeReference);
      } catch (IOException e) {
        throw new IllegalStateException("this should never happen");
      }
    }
    cachedAdminRolesArray = Collections.unmodifiableList(rolesArray);
  }

  private static List<String> getRolesArray(String rolesList) {

    if ( rolesList == null || rolesList.length() == 0 ) {
      return null;
    } else if ( RoleConsts.ADMIN_ROLES_LIST.equals(rolesList) ) {
      return cachedAdminRolesArray;
    } else if ( cachedRolesList != null && cachedRolesList.equals(rolesList) ) {
      return cachedRolesArray;
    }
    // figure out whether we have a privileged user or not
    ArrayList<String> rolesArray = null;
    {
      try {
        rolesArray = ODKFileUtils.mapper.readValue(rolesList, arrayListTypeReference);
      } catch (IOException e) {
        throw new IllegalStateException("this should never happen");
      }
    }
    cachedRolesArray = Collections.unmodifiableList(rolesArray);
    cachedRolesList = rolesList;
    return cachedRolesArray;
  }


  public enum AccessColumnType {
    NO_EFFECTIVE_ACCESS_COLUMN,
    LOCKED_EFFECTIVE_ACCESS_COLUMN,
    UNLOCKED_EFFECTIVE_ACCESS_COLUMN }

  public static class AccessContext {
    public final AccessColumnType accessColumnType;
    public final boolean canCreateRow;
    public final String activeUser;
    // true if user is a super-user or administrator
    public final boolean isPrivilegedUser;
    public final boolean isUnverifiedUser;
    private final List<String> rolesArray;

    AccessContext(AccessColumnType accessColumnType, boolean canCreateRow, String activeUser,
        List<String> rolesArray) {
      if ( activeUser == null ) {
        throw new IllegalStateException("activeUser cannot be null!");
      }
      this.accessColumnType = accessColumnType;
      this.canCreateRow = canCreateRow;
      this.activeUser = activeUser;
      this.rolesArray = rolesArray;
      if ( rolesArray == null ) {
        this.isPrivilegedUser = false;
        this.isUnverifiedUser = true;
      } else {
        this.isPrivilegedUser = rolesArray.contains(RoleConsts.ROLE_SUPER_USER) ||
            rolesArray.contains(RoleConsts.ROLE_ADMINISTRATOR);
        this.isUnverifiedUser = false;
      }
    }

    public boolean hasRole(String role) {
      return (rolesArray == null) ? false : rolesArray.contains(role);
    }

    public AccessContext cloneAsPrivilegedUser() {


      // figure out whether we have a privileged user or not
      List<String> rolesArray = getRolesArray(RoleConsts.ADMIN_ROLES_LIST);

      AccessContext that = new AccessContext(accessColumnType, true, activeUser, rolesArray);
      return that;
    }
  }

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
  private static final ArrayList<Object[]> knownKVSValueTypeRestrictions = new ArrayList<Object[]>();

  /**
   * Same as above, but quick access via the key.
   * For now, we know that the keys are all unique.
   * Eventually this might need to be a MultiMap.
   */
  private static final TreeMap<String, ArrayList<Object[]>> keyToKnownKVSValueTypeRestrictions = new TreeMap<String, ArrayList<Object[]>>();

  private static void updateKeyToKnownKVSValueTypeRestrictions(Object[] field) {
    ArrayList<Object[]> fields = keyToKnownKVSValueTypeRestrictions.get(field[2]);
    if (fields == null) {
      fields = new ArrayList<Object[]>();
      keyToKnownKVSValueTypeRestrictions.put((String) field[2], fields);
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
      Object[] fields;

      // for columns
      fields = new Object[3];
      fields[0] = ElementDataType.string.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
      fields[0] = ElementDataType.string.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_FORMAT;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
      fields[0] = ElementDataType.object.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_NAME;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
      fields[0] = ElementDataType.bool.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_DISPLAY_VISIBLE;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
      fields[0] = ElementDataType.array.name();
      fields[1] = KeyValueStoreConstants.PARTITION_COLUMN;
      fields[2] = KeyValueStoreConstants.COLUMN_JOINS;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      // and for the table...
      fields = new Object[3];
      fields[0] = ElementDataType.array.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_COL_ORDER;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
      fields[0] = ElementDataType.object.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_DISPLAY_NAME;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
      fields[0] = ElementDataType.array.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_GROUP_BY_COLS;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
      fields[0] = ElementDataType.string.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_INDEX_COL;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
      fields[0] = ElementDataType.object.name();
      fields[1] = KeyValueStoreConstants.PARTITION_TABLE;
      fields[2] = KeyValueStoreConstants.TABLE_SORT_COL;
      knownKVSValueTypeRestrictions.add(fields);
      updateKeyToKnownKVSValueTypeRestrictions(fields);

      fields = new Object[3];
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

      @Override public void reset() {
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

  private ODKDatabaseImplUtils() {
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
  
  private String applyQueryBounds(String sqlCommand, QueryBounds sqlQueryBounds) {
    if (sqlCommand == null || sqlQueryBounds == null) {
      return sqlCommand;
    }

    StringBuilder b = new StringBuilder();
    b.append(sqlCommand).append(K_LIMIT).append(sqlQueryBounds.mLimit)
        .append(K_OFFSET).append(sqlQueryBounds.mOffset);
    return b.toString();
  }

  public AccessContext getAccessContext(OdkConnectionInterface db, String tableId,
                                        String activeUser, String rolesList ) {

    // figure out whether we have a privileged user or not
    List<String> rolesArray = getRolesArray(rolesList);

    if ( tableId == null ) {
      return new AccessContext(AccessColumnType.NO_EFFECTIVE_ACCESS_COLUMN, false, activeUser, rolesArray);
    }
    if ( tableId.trim().length() == 0 ) {
      throw new IllegalArgumentException("tableId can be null but cannot be blank");
    }

    Boolean isLocked = false;
    {
      ArrayList<KeyValueStoreEntry> lockedList = this
          .getTableMetadata(db, tableId, KeyValueStoreConstants.PARTITION_TABLE,
              LocalKeyValueStoreConstants.TableSecurity.ASPECT,
              LocalKeyValueStoreConstants.TableSecurity.KEY_LOCKED).getEntries();

      if (lockedList.size() != 0) {
        if (lockedList.size() != 1) {
          throw new IllegalStateException("should be impossible");
        }

        isLocked = KeyValueStoreUtils.getBoolean(db.getAppName(), lockedList.get(0));
      }
    }

    AccessColumnType accessColumnType = (isLocked ?
        AccessColumnType.LOCKED_EFFECTIVE_ACCESS_COLUMN :
        AccessColumnType.UNLOCKED_EFFECTIVE_ACCESS_COLUMN);
    boolean canCreateRow = false;
    if ( isLocked ) {
      // only super-user or tables administrator can create rows in locked tables.
      canCreateRow = rolesList.contains(RoleConsts.ROLE_SUPER_USER) ||
                     rolesList.contains(RoleConsts.ROLE_ADMINISTRATOR);
    } else if ( rolesList == null ) {
      // this is the unverified user case. By default, they can create rows.
      // Administrator can use table properties to manage that capability.
      canCreateRow = true;
      ArrayList<KeyValueStoreEntry> canUnverifiedCreateList = this.getTableMetadata(db, tableId,
          KeyValueStoreConstants.PARTITION_TABLE,
          LocalKeyValueStoreConstants.TableSecurity.ASPECT,
          LocalKeyValueStoreConstants.TableSecurity.KEY_UNVERIFIED_USER_CAN_CREATE).getEntries();

      if ( canUnverifiedCreateList.size() != 0 ) {
        if ( canUnverifiedCreateList.size() != 1 ) {
          throw new IllegalStateException("should be impossible");
        }

        canCreateRow = KeyValueStoreUtils.getBoolean(db.getAppName(), canUnverifiedCreateList.get(0));
      }
    } else {
      canCreateRow = true;
    }

    return new AccessContext (accessColumnType, canCreateRow, activeUser, rolesArray);
  }

  /**
   * Optionally add the _effective_access column to the SELECT statement.
   *
   * @param b
   * @param wrappedSqlArgs
   * @param accessContext
   */
  private void buildAccessRights(StringBuilder b, ArrayList<Object> wrappedSqlArgs,
                  AccessContext accessContext ) {

    if ( accessContext.accessColumnType == AccessColumnType.NO_EFFECTIVE_ACCESS_COLUMN ) {
      return;
    }

    b.append(", ");
    if ( accessContext.isPrivilegedUser ) {
      // privileged user
      b.append("\"rwd\" as ").append(DataTableColumns.EFFECTIVE_ACCESS);
    } else if ( accessContext.isUnverifiedUser ) {
      // un-verified user or anonymous user
      if ( accessContext.accessColumnType == AccessColumnType.UNLOCKED_EFFECTIVE_ACCESS_COLUMN ) {
        // unlocked tables have r, rw (modify) and rwd (default or new_row) options
        b.append("case when T.").append(DataTableColumns.SYNC_STATE)
            .append("= \"").append(SyncState.new_row.name()).append("\" then \"rwd\" ")
            .append(" when T.").append(DataTableColumns.FILTER_TYPE)
            .append("= \"").append(RowFilterScope.Type.DEFAULT.name()).append("\" then \"rwd\" ")
            .append(" when T.").append(DataTableColumns.FILTER_TYPE)
            .append("= \"").append(RowFilterScope.Type.MODIFY.name()).append("\" then \"rw\" ")
            .append(" else \"r\" end as ").append(DataTableColumns.EFFECTIVE_ACCESS);
      } else {
        // locked tables have just rwd (new_row) and r options
        b.append("case when T.").append(DataTableColumns.SYNC_STATE)
            .append("= \"").append(SyncState.new_row.name()).append("\" then \"rwd\" ")
            .append(" else \"r\" end as ").append(DataTableColumns.EFFECTIVE_ACCESS);
      }
    } else {
      // ordinary user
      if ( accessContext.accessColumnType == AccessColumnType.UNLOCKED_EFFECTIVE_ACCESS_COLUMN ) {
        // unlocked tables have r, rw (modify) and rwd (default or new_row) options
        b.append("case when T.").append(DataTableColumns.SYNC_STATE).append("= \"")
            .append(SyncState.new_row.name()).append("\" then \"rwd\" ")
            .append(" when T.").append(DataTableColumns.FILTER_VALUE).append("= ?")
            .append(" then \"rwd\" ")
            .append(" when T.").append(DataTableColumns.FILTER_TYPE).append("= \"")
            .append(RowFilterScope.Type.DEFAULT.name()).append("\" then \"rwd\" ")
            .append(" when T.").append(DataTableColumns.FILTER_TYPE).append("= \"")
            .append(RowFilterScope.Type.MODIFY.name()).append("\" then \"rw\" ")
            .append(" else \"r\" end as ").append(DataTableColumns.EFFECTIVE_ACCESS);
        wrappedSqlArgs.add(accessContext.activeUser);
      } else {
        // locked tables have just rwd (new_row) and r options
        b.append("case when T.").append(DataTableColumns.SYNC_STATE).append("= \"")
            .append(SyncState.new_row.name()).append("\" then \"rwd\" ")
            .append(" when T.").append(DataTableColumns.FILTER_VALUE).append("= ?")
            .append(" then \"rw\" ")
            .append(" else \"r\" end as ").append(DataTableColumns.EFFECTIVE_ACCESS);
        wrappedSqlArgs.add(accessContext.activeUser);
      }
    }
  }

  /**
   * Perform a raw query with bind parameters.
   *
   * @param db
   * @param sqlCommand
   * @param selectionArgs
   * @param sqlQueryBounds offset and max number of rows to return (zero is infinite)
   * @param accessContext  for managing what effective accesses to return
   * @return
   */
  public Cursor rawQuery(OdkConnectionInterface db, String sqlCommand, Object[] selectionArgs,
      QueryBounds sqlQueryBounds, AccessContext accessContext) {

    Cursor c = db.rawQuery(sqlCommand + " LIMIT 1", selectionArgs);
    if (c.moveToFirst() ) {
      // see if we have the columns needed to apply row-level filtering
      boolean hasFilterType = c.getColumnIndex(DataTableColumns.FILTER_TYPE) != -1;
      boolean hasFilterValue = c.getColumnIndex(DataTableColumns.FILTER_VALUE) != -1;
      boolean hasSyncState = c.getColumnIndex(DataTableColumns.SYNC_STATE) != -1;
      c.close();

      if ( !(hasFilterType && hasFilterValue && hasSyncState) ) {
        // nope. we require all 3 to apply row-level filtering

        // no need to filter this resultset
        String sql = applyQueryBounds(sqlCommand, sqlQueryBounds);
        c = db.rawQuery(sql, selectionArgs);
        return c;
      }

      // augment query result list with the effective access controls for the row ("r", "rw", or "rwd")
      StringBuilder b = new StringBuilder();
      ArrayList<Object> wrappedSqlArgs = new ArrayList<Object>();

      b.append("SELECT *");
      buildAccessRights(b, wrappedSqlArgs, accessContext);
      b.append(" FROM (").append(sqlCommand).append(") AS T");
      if ( selectionArgs != null ) {
        Collections.addAll(wrappedSqlArgs, selectionArgs);
      }
      // apply row-level visibility filter only if we are not privileged
      // privileged users see everything.
      if ( !accessContext.isPrivilegedUser ) {
        b.append(" WHERE T.")
            .append(DataTableColumns.FILTER_TYPE)
            .append(" != \"").append(RowFilterScope.Type.HIDDEN.name()).append("\" OR T.")
            .append(DataTableColumns.SYNC_STATE)
            .append(" = \"").append(SyncState.new_row.name()).append("\"");
        if (!accessContext.isUnverifiedUser && accessContext.activeUser != null &&
            accessContext.hasRole(RoleConsts.ROLE_USER)) {
          // visible if activeUser matches the filter value
          b.append(" OR T.").append(DataTableColumns.FILTER_VALUE).append(" = ?");
          wrappedSqlArgs.add(accessContext.activeUser);
        }
      }
      String wrappedSql = b.toString();
      String limitAppliedSql = applyQueryBounds(wrappedSql, sqlQueryBounds);
      c = db.rawQuery(limitAppliedSql, wrappedSqlArgs.toArray());
      return c;
    } else {
      // cursor is empty!
      return c;
    }
  }

  /**
   * TESTING ONLY
   * <p/>
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
  public Cursor queryDistinctForTest(OdkConnectionInterface db, String table, String[] columns,
      String selection, Object[] selectionArgs, String groupBy, String having, String orderBy,
      String limit) throws SQLiteException {
    Cursor c = db
        .queryDistinct(table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
    return c;
  }

  /**
   * TESTING ONLY
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
   * @throws SQLiteException
   */
  public Cursor queryForTest(OdkConnectionInterface db, String table, String[] columns, String
      selection,
      Object[] selectionArgs, String groupBy, String having, String orderBy, String limit)
      throws SQLiteException {
    Cursor c = db.query(table, columns, selection, selectionArgs, groupBy, having, orderBy, limit);
    return c;
  }

  /**
   * Get a {@link BaseTable} for this table based on the given sql query. All
   * columns from the table are returned.  Up to sqlLimit rows are returned
   * (zero is infinite).
   * <p/>
   * The result set is filtered according to the supplied rolesList if there
   * is a FILTER_TYPE column present in the result set.
   *
   * @param db
   * @param sqlCommand     the query to run
   * @param sqlBindArgs    the selection parameters
   * @param sqlCommand
   * @param sqlQueryBounds offset and max number of rows to return (zero is infinite)
   * @param accessContext  for managing what effective accesses to return
   * @return
   */
  public BaseTable query(OdkConnectionInterface db, String tableId, String sqlCommand,
      Object[] sqlBindArgs, QueryBounds sqlQueryBounds, AccessContext accessContext) {

    Cursor c = null;
    try {
      c = rawQuery(db, sqlCommand, sqlBindArgs, sqlQueryBounds, accessContext);
      BaseTable table = buildBaseTable(db, c, tableId, accessContext.canCreateRow);
      return table;
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
  }

  /**
   * Get a {@link BaseTable} for this table based on the given sql query. All
   * columns from the table are returned.
   * <p/>
   * The number of rows returned are limited to no greater than the sqlLimit (zero is infinite).
   *
   * @param db
   * @param sqlCommand     the query to run
   * @param sqlBindArgs    the selection parameters
   * @param sqlQueryBounds the number of rows to return (zero is infinite)
   * @param accessContext  for managing what effective accesses to return
   * @return
   */
  public BaseTable privilegedQuery(OdkConnectionInterface db, String tableId, String sqlCommand,
      Object[] sqlBindArgs, QueryBounds sqlQueryBounds, AccessContext accessContext) {

    if (!accessContext.isPrivilegedUser) {
      accessContext = accessContext.cloneAsPrivilegedUser();
    }
    return query(db, tableId, sqlCommand, sqlBindArgs, sqlQueryBounds, accessContext);
  }

  /**
   * Privileged execute of an arbitrary SQL command.
   * For obvious reasons, this is very dangerous!
   *
   * The sql command can be any valid SQL command that does not return a result set.
   * No data is returned (e.g., insert into table ... or similar).
   *
   * @param db
   * @param sqlCommand
   * @param sqlBindArgs
   */
  public void privilegedExecute(OdkConnectionInterface db, String sqlCommand, Object[]
      sqlBindArgs) {
    db.execSQL(sqlCommand, sqlBindArgs);
  }

  private BaseTable buildBaseTable(OdkConnectionInterface db, Cursor c, String tableId,
      boolean canCreateRow) {

    HashMap<String, Integer> mElementKeyToIndex = null;
    String[] mElementKeyForIndex = null;

    if (!c.moveToFirst()) {

      // Attempt to retrieve the columns from the cursor.
      // These may not be available if there were no rows returned.
      // It depends upon the cursor implementation.
      try {
        int columnCount = c.getColumnCount();
        mElementKeyForIndex = new String[columnCount];
        mElementKeyToIndex = new HashMap<>(columnCount);
        int i;

        for (i = 0; i < columnCount; ++i) {
          String columnName = c.getColumnName(i);
          mElementKeyForIndex[i] = columnName;
          mElementKeyToIndex.put(columnName, i);
        }
      } catch (Exception e) {
        // ignore.
      }

      // if they were not available, declare an empty array.
      if (mElementKeyForIndex == null) {
        mElementKeyForIndex = new String[0];
      }
      c.close();

      // we have no idea what the table should contain because it has no rows...
      BaseTable table = new BaseTable(null, mElementKeyForIndex, mElementKeyToIndex, 0);
      table.setEffectiveAccessCreateRow(canCreateRow);
      return table;
    }

    int rowCount = c.getCount();
    int columnCount = c.getColumnCount();

    BaseTable table = null;

    // These maps will map the element key to the corresponding index in
    // either data or metadata. If the user has defined a column with the
    // element key _my_data, and this column is at index 5 in the data
    // array, dataKeyToIndex would then have a mapping of _my_data:5.
    // The sync_state column, if present at index 7, would have a mapping
    // in metadataKeyToIndex of sync_state:7.
    mElementKeyForIndex = new String[columnCount];
    mElementKeyToIndex = new HashMap<>(columnCount);

    int i;

    for (i = 0; i < columnCount; ++i) {
      String columnName = c.getColumnName(i);
      mElementKeyForIndex[i] = columnName;
      mElementKeyToIndex.put(columnName, i);
    }

    table = new BaseTable(null, mElementKeyForIndex, mElementKeyToIndex, rowCount);

    String[] rowData = new String[columnCount];
    do {
      // First get the user-defined data for this row.
      for (i = 0; i < columnCount; i++) {
        String value = CursorUtils.getIndexAsString(c, i);
        rowData[i] = value;
      }

      Row nextRow = new Row(rowData.clone(), table);
      table.addRow(nextRow);
    } while (c.moveToNext());
    c.close();

    table.setEffectiveAccessCreateRow(canCreateRow);

    if (tableId != null) {
      table.setMetaDataRev(getTableDefinitionRevId(db, tableId));
    }
    return table;
  }

  /************** LOCAL ONLY TABLE OPERATIONS ***************/

  /**
   * Create a local only table and prepend the given id with an "L_"
   *
   * @param db
   * @param tableId
   * @param columns
   * @return
   */
  public OrderedColumns createLocalOnlyTableWithColumns(OdkConnectionInterface db,
      String tableId, List<Column> columns) {

    boolean dbWithinTransaction = db.inTransaction();
    boolean success = false;

    OrderedColumns orderedDefs = new OrderedColumns(db.getAppName(), tableId, columns);
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      createTableWithColumns(db, tableId, orderedDefs, false);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
      success = true;
      return orderedDefs;
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
      if (!success) {

        // Get the names of the columns
        StringBuilder colNames = new StringBuilder();
        if (columns != null) {
          for (Column column : columns) {
            colNames.append(" ").append(column.getElementKey()).append(",");
          }
          if (colNames != null && colNames.length() > 0) {
            colNames.deleteCharAt(colNames.length() - 1);
            WebLogger.getLogger(db.getAppName()).e(t,
                "createLocalOnlyTableWithColumns: Error while adding table " + tableId
                    + " with columns:" + colNames.toString());
          }
        } else {
          WebLogger.getLogger(db.getAppName()).e(t,
              "createLocalOnlyTableWithColumns: Error while adding table " + tableId
                  + " with columns: null");
        }
      }
    }
  }

  /**
   * Drop the given local only table
   *
   * @param db
   * @param tableId
   */
  public void deleteLocalOnlyTable(OdkConnectionInterface db,
      String tableId) {

    boolean dbWithinTransaction = db.inTransaction();

    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      // Drop the table used for the formId
      StringBuilder b = new StringBuilder();
      b.append("DROP TABLE IF EXISTS ").append(tableId).append(";");
      db.execSQL(b.toString(), null);

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
   * Insert a row into a local only table
   *
   * @param db
   * @param tableId
   * @param rowValues
   * @throws ActionNotAuthorizedException
   */
  public void insertLocalOnlyRow(OdkConnectionInterface db, String tableId,
      ContentValues rowValues) throws IllegalArgumentException {

    if (rowValues == null || rowValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    HashMap<String,Object> cvDataTableVal = new HashMap<String,Object>();
    for ( String key : rowValues.keySet() ) {
      cvDataTableVal.put(key, rowValues.get(key));
    }

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

  /**
   * Update a row in a local only table
   *
   * @param db
   * @param tableId
   * @param rowValues
   * @param whereClause
   * @param bindArgs
   * @throws ActionNotAuthorizedException
   */
  public void updateLocalOnlyRow(OdkConnectionInterface db, String tableId,
      ContentValues rowValues, String whereClause, Object[] bindArgs)
      throws IllegalArgumentException {

    if (rowValues == null || rowValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    HashMap<String,Object> cvDataTableVal = new HashMap<String,Object>();
    for ( String key : rowValues.keySet() ) {
      cvDataTableVal.put(key, rowValues.get(key));
    }

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.update(tableId, cvDataTableVal, whereClause, bindArgs);

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
   * Delete a row in a local only table
   *
   * @param db
   * @param tableId
   * @param whereClause
   * @param bindArgs
   * @throws ActionNotAuthorizedException
   */
  public void deleteLocalOnlyRow(OdkConnectionInterface db, String tableId, String whereClause,
      Object[] bindArgs) {

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.delete(tableId, whereClause, bindArgs);

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
   * Return the row(s) for the given tableId and rowId. If the row has
   * checkpoints or conflicts, the returned BaseTable will have more than one
   * Row returned. Otherwise, it will contain a single row.
   *
   * @param db
   * @param tableId
   * @param rowId
   * @param activeUser
   * @param rolesList
   * @return
   */
  public BaseTable getRowsWithId(OdkConnectionInterface db, String tableId, String rowId,
      String activeUser, String rolesList ) {

    AccessContext accessContext = getAccessContext(db, tableId, activeUser, rolesList);

    BaseTable table = query(db, tableId, QueryUtil
        .buildSqlStatement(tableId, QueryUtil.GET_ROWS_WITH_ID_WHERE,
            QueryUtil.GET_ROWS_WITH_ID_GROUP_BY, QueryUtil.GET_ROWS_WITH_ID_HAVING,
            QueryUtil.GET_ROWS_WITH_ID_ORDER_BY_KEYS,
            QueryUtil.GET_ROWS_WITH_ID_ORDER_BY_DIR), new String[] { rowId }, null,
        accessContext);

    return table;
  }

  /**
   * Return the row(s) for the given tableId and rowId. If the row has
   * checkpoints or conflicts, the returned BaseTable will have more than one
   * Row returned. Otherwise, it will contain a single row.
   *
   * @param db
   * @param tableId
   * @param rowId
   * @param activeUser
   * @return
   */
  public BaseTable privilegedGetRowsWithId(OdkConnectionInterface db, String tableId, String
      rowId, String activeUser ) {

    AccessContext accessContext = getAccessContext(db, tableId, activeUser, RoleConsts
        .ADMIN_ROLES_LIST);

    BaseTable table = privilegedQuery(db, tableId, QueryUtil
        .buildSqlStatement(tableId, QueryUtil.GET_ROWS_WITH_ID_WHERE,
            QueryUtil.GET_ROWS_WITH_ID_GROUP_BY, QueryUtil.GET_ROWS_WITH_ID_HAVING,
            QueryUtil.GET_ROWS_WITH_ID_ORDER_BY_KEYS,
            QueryUtil.GET_ROWS_WITH_ID_ORDER_BY_DIR), new String[] { rowId }, null,
        accessContext);

    return table;
  }

  /**
   * Return the row with the most recent changes for the given tableId and rowId.
   * If the rowId does not exist, it returns an empty BaseTable for this tableId.
   * If the row has conflicts, it throws an exception. Otherwise, it returns the
   * most recent checkpoint or non-checkpoint value; it will contain a single row.
   *
   * @param db
   * @param tableId
   * @param rowId
   * @return
   */
  public BaseTable getMostRecentRowWithId(OdkConnectionInterface db, String tableId,
      String rowId, String activeUser, String rolesList) {

    BaseTable table = getRowsWithId(db, tableId, rowId, activeUser, rolesList);

    if (table.getNumberOfRows() == 0) {
      return table;
    }

    // most recent savepoint timestamp...
    BaseTable t = new BaseTable(table, Collections.singletonList(0));

    if (hasConflictRows(t)) {
      throw new IllegalStateException("row is in conflict");
    }
    return t;
  }

  /**
   * Return the row with the most recent changes for the given tableId and rowId.
   * If the rowId does not exist, it returns an empty BaseTable for this tableId.
   * If the row has conflicts, it throws an exception. Otherwise, it returns the
   * most recent checkpoint or non-checkpoint value; it will contain a single row.
   *
   * @param db
   * @param tableId
   * @param rowId
   * @return
   */
  public BaseTable privilegedGetMostRecentRowWithId(OdkConnectionInterface db, String tableId,
      String rowId, String activeUser) {

    BaseTable table = privilegedGetRowsWithId(db, tableId, rowId, activeUser);

    if (table.getNumberOfRows() == 0) {
      return table;
    }

    // most recent savepoint timestamp...
    BaseTable t = new BaseTable(table, Collections.singletonList(0));

    if (hasConflictRows(t)) {
      throw new IllegalStateException("row is in conflict");
    }
    return t;
  }

  private boolean hasConflictRows(BaseTable table) {
    List<Row> rows = table.getRows();
    for (Row row : rows) {
      String conflictType = row.getDataByKey(DataTableColumns.CONFLICT_TYPE);
      if (conflictType != null && conflictType.length() != 0) {
        return true;
      }
    }
    return false;
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
      StringBuilder b = new StringBuilder();
      b.append(K_SELECT_FROM).append(tableId).append(K_LIMIT).append("1");
      cursor = db.rawQuery(b.toString(), null);
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
      if (cursor != null && !cursor.isClosed()) {
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
   * @param tableId
   * @return
   */
  public OrderedColumns getUserDefinedColumns(OdkConnectionInterface db,
      String tableId) {
    ArrayList<Column> userDefinedColumns = new ArrayList<Column>();
    String selection = K_COLUMN_DEFS_TABLE_ID_EQUALS_PARAM;
    Object[] selectionArgs = { tableId };
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
        String elementKey = CursorUtils.getIndexAsString(c, elemKeyIndex);
        String elementName = CursorUtils.getIndexAsString(c, elemNameIndex);
        String elementType = CursorUtils.getIndexAsString(c, elemTypeIndex);
        String listOfChildren = CursorUtils.getIndexAsString(c, listChildrenIndex);
        userDefinedColumns.add(new Column(elementKey, elementName, elementType, listOfChildren));
        c.moveToNext();
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return new OrderedColumns(db.getAppName(), tableId, userDefinedColumns);
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
          K_TABLE_DEFS_TABLE_ID_EQUALS_PARAM,
          new Object[] { tableId }, null, null, null, null);
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
        .append("SUM(case when _conflict_type is not null then 1 else 0 end) as conflicts from ")
        .append(tableId);

    Cursor c = null;
    try {
      c = db.rawQuery(b.toString(), null);
      Integer checkpoints = null;
      Integer conflicts = null;
      if (c != null) {
        if (c.moveToFirst()) {
          int idxCheckpoints = c.getColumnIndex("checkpoints");
          int idxConflicts = c.getColumnIndex("conflicts");
          checkpoints = CursorUtils.getIndexAsType(c, Integer.class, idxCheckpoints);
          conflicts = CursorUtils.getIndexAsType(c, Integer.class, idxConflicts);
        }
        c.close();
      }

      int outcome = CursorUtils.TABLE_HEALTH_IS_CLEAN;
      if (checkpoints != null && checkpoints != 0) {
        outcome += CursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS;
      }
      if (conflicts != null && conflicts != 0) {
        outcome += CursorUtils.TABLE_HEALTH_HAS_CONFLICTS;
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
   * @param tableId
   */
  public void deleteTableAndAllData(OdkConnectionInterface db,
      final String tableId) {

    SyncETagsUtils seu = new SyncETagsUtils();
    boolean dbWithinTransaction = db.inTransaction();

    Object[] whereArgs = { tableId };

    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      // Drop the table used for the formId
      StringBuilder b = new StringBuilder();
      b.append("DROP TABLE IF EXISTS ").append(tableId).append(";");
      db.execSQL(b.toString(), null);

      // Delete the server sync ETags associated with this table
      seu.deleteAllSyncETagsForTableId(db, tableId);

      // Delete the table definition for the tableId
      int count;
      {
        String whereClause = K_TABLE_DEFS_TABLE_ID_EQUALS_PARAM;

        count = db.delete(DatabaseConstants.TABLE_DEFS_TABLE_NAME, whereClause, whereArgs);
      }

      // Delete the column definitions for this tableId
      {
        String whereClause = K_COLUMN_DEFS_TABLE_ID_EQUALS_PARAM;

        db.delete(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME, whereClause, whereArgs);
      }

      // Delete the uploads for the tableId
      {
        String uploadWhereClause = InstanceColumns.DATA_TABLE_TABLE_ID + " = ?";
        db.delete(DatabaseConstants.UPLOADS_TABLE_NAME, uploadWhereClause, whereArgs);
      }

      // Delete the values from the 4 key value stores
      {
        String whereClause = K_KVS_TABLE_ID_EQUALS_PARAM;

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
    String tableDir = ODKFileUtils.getTablesFolder(db.getAppName(), tableId);
    try {
      FileUtils.deleteDirectory(new File(tableDir));
    } catch (IOException e1) {
      e1.printStackTrace();
      throw new IllegalStateException("Unable to delete the " + tableDir + " directory", e1);
    }

    String assetsCsvDir = ODKFileUtils.getAssetsCsvFolder(db.getAppName());
    try {
      File file = new File(assetsCsvDir);
      if (file.exists()) {
        Collection<File> files = FileUtils.listFiles(file, new IOFileFilter() {

          @Override public boolean accept(File file) {
            String[] parts = file.getName().split("\\.");
            return (parts[0].equals(tableId) && parts[parts.length - 1].equals("csv") && (
                parts.length == 2 || parts.length == 3 || (parts.length == 4 && parts[parts.length
                    - 2].equals("properties"))));
          }

          @Override public boolean accept(File dir, String name) {
            String[] parts = name.split("\\.");
            return (parts[0].equals(tableId) && parts[parts.length - 1].equals("csv") && (
                parts.length == 2 || parts.length == 3 || (parts.length == 4 && parts[parts.length
                    - 2].equals("properties"))));
          }
        }, new IOFileFilter() {

          // don't traverse into directories
          @Override public boolean accept(File arg0) {
            return false;
          }

          // don't traverse into directories
          @Override public boolean accept(File arg0, String arg1) {
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
  public void privilegedUpdateTableETags(OdkConnectionInterface db, String tableId,
      String schemaETag, String lastDataETag) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    TreeMap<String,Object> cvTableDef = new TreeMap<String,Object>();
    cvTableDef.put(TableDefinitionsColumns.SCHEMA_ETAG, schemaETag);
    cvTableDef.put(TableDefinitionsColumns.LAST_DATA_ETAG, lastDataETag);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }
      db.update(DatabaseConstants.TABLE_DEFS_TABLE_NAME, cvTableDef,
          K_TABLE_DEFS_TABLE_ID_EQUALS_PARAM, new Object[] { tableId });

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
  public void privilegedUpdateTableLastSyncTime(OdkConnectionInterface db, String tableId) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    TreeMap<String,Object> cvTableDef = new TreeMap<String,Object>();
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME,
        TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }
      db.update(DatabaseConstants.TABLE_DEFS_TABLE_NAME, cvTableDef,
          K_TABLE_DEFS_TABLE_ID_EQUALS_PARAM, new Object[] { tableId });

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
      b.append(K_KVS_TABLE_ID_EQUALS_PARAM);
      selArgs.add(tableId);

      c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME, null, b.toString(),
          selArgs.toArray(new String[selArgs.size()]), null, null, null, null);
      if (c.moveToFirst()) {
        int idxSchemaETag = c.getColumnIndex(TableDefinitionsColumns.SCHEMA_ETAG);
        int idxLastDataETag = c.getColumnIndex(TableDefinitionsColumns.LAST_DATA_ETAG);
        int idxLastSyncTime = c.getColumnIndex(TableDefinitionsColumns.LAST_SYNC_TIME);
        int idxRevId = c.getColumnIndex(TableDefinitionsColumns.REV_ID);

        if (c.getCount() != 1) {
          throw new IllegalStateException(
              "Two or more TableDefinitionEntry records found for tableId " + tableId);
        }

        e = new TableDefinitionEntry(tableId);
        e.setSchemaETag(c.getString(idxSchemaETag));
        e.setLastDataETag(c.getString(idxLastDataETag));
        e.setLastSyncTime(c.getString(idxLastSyncTime));
        e.setRevId(c.getString(idxRevId));
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return e;
  }

  public String getTableDefinitionRevId(OdkConnectionInterface db, String tableId) {
    String revId = null;
    Cursor c = null;
    try {
      StringBuilder b = new StringBuilder();
      ArrayList<String> selArgs = new ArrayList<String>();
      b.append(K_KVS_TABLE_ID_EQUALS_PARAM);
      selArgs.add(tableId);

      c = db.query(DatabaseConstants.TABLE_DEFS_TABLE_NAME, null, b.toString(),
          selArgs.toArray(new String[selArgs.size()]), null, null, null, null);
      if (c.moveToFirst()) {
        int idxRevId = c.getColumnIndex(TableDefinitionsColumns.REV_ID);

        if (c.getCount() != 1) {
          throw new IllegalStateException(
              "Two or more TableDefinitionEntry records found for tableId " + tableId);
        }

        revId = c.getString(idxRevId);
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return revId;
  }

  /*
   * Build the start of a create table statement -- specifies all the metadata
   * columns. Caller must then add all the user-defined column definitions and
   * closing parentheses.
   */
  private void addMetadataFieldsToTableCreationStatement(StringBuilder b) {
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
        b.append(colName).append(" TEXT NOT NULL").append(endSeq);
      } else if (colName.equals(DataTableColumns.ROW_ETAG)
          || colName.equals(DataTableColumns.FILTER_TYPE)
          || colName.equals(DataTableColumns.FILTER_VALUE)
          || colName.equals(DataTableColumns.FORM_ID)
          || colName.equals(DataTableColumns.LOCALE)
          || colName.equals(DataTableColumns.SAVEPOINT_TYPE)
          || colName.equals(DataTableColumns.SAVEPOINT_CREATOR)) {
        b.append(colName).append(" TEXT NULL").append(endSeq);
      } else if (colName.equals(DataTableColumns.CONFLICT_TYPE)) {
        b.append(colName).append(" INTEGER NULL").append(endSeq);
      }
      //@formatter:on
    }
  }

  /**
   * Ensure that the kvs entry is valid.
   *
   * @param appName
   * @param tableId
   * @param kvs
   * @throws IllegalArgumentException
   */
  private void validateKVSEntry(String appName, String tableId, KeyValueStoreEntry kvs)
      throws IllegalArgumentException {

    if (kvs.tableId == null || kvs.tableId.trim().length() == 0) {
      throw new IllegalArgumentException("KVS entry has a null or empty tableId");
    }

    if (!kvs.tableId.equals(tableId)) {
      throw new IllegalArgumentException("KVS entry has a mismatched tableId");
    }

    if (kvs.partition == null || kvs.partition.trim().length() == 0) {
      throw new IllegalArgumentException("KVS entry has a null or empty partition");
    }

    if (kvs.aspect == null || kvs.aspect.trim().length() == 0) {
      throw new IllegalArgumentException("KVS entry has a null or empty aspect");
    }

    if (kvs.key == null || kvs.key.trim().length() == 0) {
      throw new IllegalArgumentException("KVS entry has a null or empty key");
    }

    // a null value will remove the entry from the KVS
    if (kvs.value != null && kvs.value.trim().length() != 0) {
      // validate the type....
      if (kvs.type == null || kvs.type.trim().length() == 0) {
        throw new IllegalArgumentException("KVS entry has a null or empty type");
      }

      // find subset matching the key...
      ArrayList<Object[]> kvsValueTypeRestrictions = keyToKnownKVSValueTypeRestrictions
          .get(kvs.key);

      if (kvsValueTypeRestrictions != null) {
        for (Object[] restriction : kvsValueTypeRestrictions) {
          if (kvs.partition.equals(restriction[1]) && kvs.key.equals(restriction[2])) {
            // see if the client specified an incorrect type
            if (!kvs.type.equals(restriction[0])) {
              String type = kvs.type;
              kvs.type = (String) restriction[0];

              // TODO: detect whether the value conforms to the specified type.
              enforceKVSValueType(kvs, ElementDataType.valueOf(kvs.type));

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

  private void enforceKVSValueType(KeyValueStoreEntry e, ElementDataType type) {
    e.type = type.name();
    if (e.value != null) {
      if (type == ElementDataType.integer) {
        // TODO: can add matcher if we want to
      } else if (type == ElementDataType.number) {
        // TODO: can add matcher if we want to
      } else if (type == ElementDataType.bool) {
        // TODO: can add matcher if we want to
      } else if (type == ElementDataType.string ||
          type == ElementDataType.rowpath || type == ElementDataType.configpath) {
        // anything goes here...
      } else if (type == ElementDataType.array) {
        // minimal test for valid representation
        if (!e.value.startsWith("[") || !e.value.endsWith("]")) {
          throw new IllegalArgumentException("array value type is not an array! " +
              "TableId: " + e.tableId + " Partition: " + e.partition + " Aspect: " + e.aspect +
              " Key: " + e.key);
        }
      } else if (type == ElementDataType.object) {
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
  public void replaceTableMetadata(OdkConnectionInterface db, KeyValueStoreEntry e) {
    validateKVSEntry(db.getAppName(), e.tableId, e);

    TreeMap<String,Object> values = new TreeMap<String,Object>();
    values.put(KeyValueStoreColumns.TABLE_ID, e.tableId);
    values.put(KeyValueStoreColumns.PARTITION, e.partition);
    values.put(KeyValueStoreColumns.ASPECT, e.aspect);
    values.put(KeyValueStoreColumns.KEY, e.key);
    values.put(KeyValueStoreColumns.VALUE_TYPE, e.type);
    values.put(KeyValueStoreColumns.VALUE, e.value);

    TreeMap<String, Object> metadataRev = new TreeMap<String, Object>();
    metadataRev.put(TableDefinitionsColumns.REV_ID, UUID.randomUUID().toString());

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }
      if (e.value == null || e.value.trim().length() == 0) {
        deleteTableMetadata(db, e.tableId, e.partition, e.aspect, e.key);
      } else {
        db.replaceOrThrow(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, null, values);
      }

      // Update the table definition table with a new revision ID, essentially telling all caches
      // of this table's metadata that they are dirty.
      db.update(DatabaseConstants.TABLE_DEFS_TABLE_NAME, metadataRev,
          K_TABLE_DEFS_TABLE_ID_EQUALS_PARAM, new Object[] {e.tableId});

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
  public void replaceTableMetadata(OdkConnectionInterface db, String tableId,
      List<KeyValueStoreEntry> metadata, boolean clear) {

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      if (clear) {
        db.delete(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME,
            K_KVS_TABLE_ID_EQUALS_PARAM, new Object[] { tableId });
      }

      for (KeyValueStoreEntry e : metadata) {
        replaceTableMetadata(db, e);
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

  public void replaceTableMetadataSubList(OdkConnectionInterface db, String tableId,
      String partition, String aspect, List<KeyValueStoreEntry> metadata) {

    StringBuilder b = new StringBuilder();
    ArrayList<Object> whereArgsList = new ArrayList<Object>();

    if (tableId == null || tableId.trim().length() == 0) {
      throw new IllegalArgumentException("tableId cannot be null or an empty string");
    }
    b.append(K_KVS_TABLE_ID_EQUALS_PARAM);
    whereArgsList.add(tableId);
    if (partition != null) {
      b.append(S_AND).append(K_KVS_PARTITION_EQUALS_PARAM);
      whereArgsList.add(partition);
    }
    if (aspect != null) {
      b.append(S_AND).append(K_KVS_ASPECT_EQUALS_PARAM);
      whereArgsList.add(aspect);
    }
    String whereClause = b.toString();
    Object[] whereArgs = whereArgsList.toArray(new Object[whereArgsList.size()]);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.delete(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME, whereClause, whereArgs);

      for (KeyValueStoreEntry e : metadata) {
        replaceTableMetadata(db, e);
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
  public void deleteTableMetadata(OdkConnectionInterface db, String tableId, String partition,
      String aspect, String key) {

    StringBuilder b = new StringBuilder();
    ArrayList<String> selArgs = new ArrayList<String>();
    if (tableId != null) {
      b.append(K_KVS_TABLE_ID_EQUALS_PARAM);
      selArgs.add(tableId);
    }
    if (partition != null) {
      if (b.length() != 0) {
        b.append(S_AND);
      }
      b.append(K_KVS_PARTITION_EQUALS_PARAM);
      selArgs.add(partition);
    }
    if (aspect != null) {
      if (b.length() != 0) {
        b.append(S_AND);
      }
      b.append(K_KVS_ASPECT_EQUALS_PARAM);
      selArgs.add(aspect);
    }
    if (key != null) {
      if (b.length() != 0) {
        b.append(S_AND);
      }
      b.append(K_KVS_KEY_EQUALS_PARAM);
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
  public TableMetaDataEntries getTableMetadata(OdkConnectionInterface db, String tableId,
      String partition, String aspect, String key) {

    TableMetaDataEntries metadata = new TableMetaDataEntries(tableId,
        getTableDefinitionRevId(db, tableId));

    Cursor c = null;
    try {
      StringBuilder b = new StringBuilder();
      ArrayList<String> selArgs = new ArrayList<String>();
      if (tableId != null) {
        b.append(K_KVS_TABLE_ID_EQUALS_PARAM);
        selArgs.add(tableId);
      }
      if (partition != null) {
        if (b.length() != 0) {
          b.append(S_AND);
        }
        b.append(K_KVS_PARTITION_EQUALS_PARAM);
        selArgs.add(partition);
      }
      if (aspect != null) {
        if (b.length() != 0) {
          b.append(S_AND);
        }
        b.append(K_KVS_ASPECT_EQUALS_PARAM);
        selArgs.add(aspect);
      }
      if (key != null) {
        if (b.length() != 0) {
          b.append(S_AND);
        }
        b.append(K_KVS_KEY_EQUALS_PARAM);
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
          metadata.addEntry(e);
        } while (c.moveToNext());
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
    return metadata;
  }

  /**
   * Clean up the KVS row data types. This simplifies the migration process by
   * enforcing the proper data types regardless of what the values are in the
   * imported CSV files.
   *
   * @param db
   */
  private void enforceTypesTableMetadata(OdkConnectionInterface db) {

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      StringBuilder b = new StringBuilder();
      b.setLength(0);
      //@formatter:off
      b.append("UPDATE ").append(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME)
          .append(" SET ").append(KeyValueStoreColumns.VALUE_TYPE).append(S_EQUALS_PARAM)
          .append(K_WHERE)
          .append(K_KVS_PARTITION_EQUALS_PARAM).append(S_AND)
          .append(K_KVS_KEY_EQUALS_PARAM);
      //@formatter:on

      String sql = b.toString();

      for (Object[] fields : knownKVSValueTypeRestrictions) {
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
  private void createTableMetadata(OdkConnectionInterface db, String tableId) {
    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    // Add the table id into table definitions
    TreeMap<String,Object> cvTableDef = new TreeMap<String,Object>();
    cvTableDef.put(TableDefinitionsColumns.TABLE_ID, tableId);
    cvTableDef.put(TableDefinitionsColumns.REV_ID, UUID.randomUUID().toString());
    cvTableDef.put(TableDefinitionsColumns.SCHEMA_ETAG, null);
    cvTableDef.put(TableDefinitionsColumns.LAST_DATA_ETAG, null);
    cvTableDef.put(TableDefinitionsColumns.LAST_SYNC_TIME, -1);

    db.replaceOrThrow(DatabaseConstants.TABLE_DEFS_TABLE_NAME, null, cvTableDef);
  }

  /*
   * Create a user defined database table metadata - table definiton and KVS
   * values
   *
   * @param db
   * @param tableId
   * @param orderedDefs
   */
  private void createTableWithColumns(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedDefs, boolean isSynchronized) {

    if (tableId == null || tableId.length() <= 0) {
      throw new IllegalArgumentException(t + ": application name and table name must be specified");
    }

    StringBuilder createTableCmdWithCols = new StringBuilder();
    createTableCmdWithCols.append("CREATE TABLE IF NOT EXISTS ").append(tableId).append(" (");

    if (isSynchronized) {
      addMetadataFieldsToTableCreationStatement(createTableCmdWithCols);
    }

    boolean first = true;
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

      if (!first || isSynchronized) {
        createTableCmdWithCols.append(", ");
      }

      createTableCmdWithCols.append(column.getElementKey()).append(" ").append(dbType)
          .append(" NULL");
      first = false;
    }

    createTableCmdWithCols.append(");");

    db.execSQL(createTableCmdWithCols.toString(), null);

    if (isSynchronized) {
      // Create the metadata for the table - table def and KVS
      createTableMetadata(db, tableId);

      // Now need to call the function to write out all the column values
      for (ColumnDefinition column : orderedDefs.getColumnDefinitions()) {
        createNewColumnMetadata(db, tableId, column);
      }
    }
  }

  /*
   * Create a new column metadata in the database - add column values to KVS and
   * column definitions
   */
  private void createNewColumnMetadata(OdkConnectionInterface db, String tableId,
      ColumnDefinition column) {
    String colName = column.getElementKey();

    // Create column definition
    TreeMap<String,Object> cvColDefVal = new TreeMap<String,Object>();
    cvColDefVal.put(ColumnDefinitionsColumns.TABLE_ID, tableId);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_KEY, colName);
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_NAME, column.getElementName());
    cvColDefVal.put(ColumnDefinitionsColumns.ELEMENT_TYPE, column.getElementType());
    cvColDefVal
        .put(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, column.getListChildElementKeys());

    // Now add this data into the database
    db.replaceOrThrow(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME, null, cvColDefVal);
  }

  /**
   * Verifies that the schema the client has matches that of the given tableId.
   *
   * @param db
   * @param tableId
   * @param orderedDefs
   */
  private void verifyTableSchema(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedDefs) {
    // confirm that the column definitions are unchanged...
    OrderedColumns existingDefns = getUserDefinedColumns(db, tableId);
    if (existingDefns.getColumnDefinitions().size() != orderedDefs.getColumnDefinitions().size()) {
      throw new IllegalStateException(
          "Unexpectedly found tableId with different column definitions that already exists!");
    }
    for (ColumnDefinition ci : orderedDefs.getColumnDefinitions()) {
      ColumnDefinition existingDefn;
      try {
        existingDefn = existingDefns.find(ci.getElementKey());
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException(
            "Unexpectedly failed to match elementKey: " + ci.getElementKey());
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
              "Unexpected mis-match of listOfStringElementKeys[" + i + "] for elementKey: " + ci
                  .getElementKey());
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
   * @param choiceListJSON -- the actual JSON choice list text.
   * @return choiceListId -- the unique code mapping to the choiceListJSON
   */
  public String setChoiceList(OdkConnectionInterface db, String choiceListJSON) {
    ChoiceListUtils utils = new ChoiceListUtils();
    boolean dbWithinTransaction = db.inTransaction();
    boolean success = false;
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }
      if (choiceListJSON == null || choiceListJSON.trim().length() == 0) {
        return null;
      }

      String choiceListId = ODKFileUtils.getNakedMd5Hash(db.getAppName(), choiceListJSON);

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
      if (!success) {

        WebLogger.getLogger(db.getAppName())
            .e(t, "setChoiceList: Error while updating choiceList entry " + choiceListJSON);
      }
    }
  }

  /**
   * Return the choice list JSON corresponding to the choiceListId
   *
   * @param db
   * @param choiceListId -- the md5 hash of the choiceListJSON
   * @return choiceListJSON -- the actual JSON choice list text.
   */
  public String getChoiceList(OdkConnectionInterface db, String choiceListId) {
    ChoiceListUtils utils = new ChoiceListUtils();

    if (choiceListId == null || choiceListId.trim().length() == 0) {
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
   * @param tableId
   * @param columns
   * @return the ArrayList<ColumnDefinition> of the user columns in the table.
   */
  public OrderedColumns createOrOpenTableWithColumns(OdkConnectionInterface db,
      String tableId, List<Column> columns) {
    boolean dbWithinTransaction = db.inTransaction();
    boolean success = false;

    OrderedColumns orderedDefs = new OrderedColumns(db.getAppName(), tableId, columns);
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }
      if (!hasTableId(db, tableId)) {
        createTableWithColumns(db, tableId, orderedDefs, true);
      } else {
        verifyTableSchema(db, tableId, orderedDefs);
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
      if (!success) {

        // Get the names of the columns
        StringBuilder colNames = new StringBuilder();
        if (columns != null) {
          for (Column column : columns) {
            colNames.append(" ").append(column.getElementKey()).append(",");
          }
          if (colNames != null && colNames.length() > 0) {
            colNames.deleteCharAt(colNames.length() - 1);
            WebLogger.getLogger(db.getAppName()).e(t,
                "createOrOpenTableWithColumns: Error while adding table " + tableId
                    + " with columns:" + colNames.toString());
          }
        } else {
          WebLogger.getLogger(db.getAppName()).e(t,
              "createOrOpenTableWithColumns: Error while adding table " + tableId
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
   * @param tableId
   * @param columns
   * @return the ArrayList<ColumnDefinition> of the user columns in the table.
   */
  public OrderedColumns createOrOpenTableWithColumnsAndProperties(OdkConnectionInterface db,
      String tableId, List<Column> columns, List<KeyValueStoreEntry> metaData,
      boolean clear) throws JsonProcessingException {
    boolean dbWithinTransaction = db.inTransaction();
    boolean success = false;

    OrderedColumns orderedDefs = new OrderedColumns(db.getAppName(), tableId, columns);

    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }
      boolean created = false;
      if (!hasTableId(db, tableId)) {
        createTableWithColumns(db, tableId, orderedDefs, true);
        created = true;
      } else {
        // confirm that the column definitions are unchanged...
        verifyTableSchema(db, tableId, orderedDefs);
      }

      replaceTableMetadata(db, tableId, metaData, (clear || created));
      enforceTypesTableMetadata(db);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
      success = true;
      return orderedDefs;
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
      if (!success) {

        // Get the names of the columns
        StringBuilder colNames = new StringBuilder();
        if (columns != null) {
          for (Column column : columns) {
            colNames.append(" ").append(column.getElementKey()).append(",");
          }
          if (colNames != null && colNames.length() > 0) {
            colNames.deleteCharAt(colNames.length() - 1);
            WebLogger.getLogger(db.getAppName()).e(t,
                "createOrOpenTableWithColumnsAndProperties: Error while adding table " + tableId
                    + " with columns:" + colNames.toString());
          }
        } else {
          WebLogger.getLogger(db.getAppName()).e(t,
              "createOrOpenTableWithColumnsAndProperties: Error while adding table " + tableId
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
   * SYNC only
   * <p/>
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
  private void changeDataRowsToNewRowState(OdkConnectionInterface db, String tableId) {

    StringBuilder b = new StringBuilder();

    // remove server conflicting rows
    b.setLength(0);
    b.append("DELETE FROM ").append(tableId).append(K_WHERE)
        .append(DataTableColumns.SYNC_STATE).append(S_EQUALS_PARAM).append(S_AND)
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
    b.append("UPDATE ").append(tableId).append(" SET ")
      .append(DataTableColumns.SYNC_STATE).append(S_EQUALS_PARAM).append(", ")
      .append(DataTableColumns.CONFLICT_TYPE).append(" = null").append(K_WHERE)
      .append(DataTableColumns.CONFLICT_TYPE).append(S_EQUALS_PARAM);
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
    b.append("UPDATE ").append(tableId).append(" SET ")
      .append(DataTableColumns.SYNC_STATE).append(S_EQUALS_PARAM).append(K_WHERE)
      .append(DataTableColumns.SYNC_STATE).append(S_EQUALS_PARAM);
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
   * SYNC
   * <p/>
   * Clean up this table and set the dataETag to null.
   * <p/>
   * changeDataRowsToNewRowState(sc.getAppName(), db, tableId);
   * <p/>
   * we need to clear out the dataETag so
   * that we will pull all server changes and sync our properties.
   * <p/>
   * updateTableETags(sc.getAppName(), db, tableId, null, null);
   * <p/>
   * Although the server does not recognize this tableId, we can
   * keep our record of the ETags for the table-level files and
   * manifest. These may enable us to short-circuit the restoration
   * of the table-level files should another client be simultaneously
   * trying to restore those files to the server.
   * <p/>
   * However, we do need to delete all the instance-level files,
   * as these are tied to the schemaETag we hold, and that is now
   * invalid.
   * <p/>
   * if the local table ever had any server sync information for this
   * host then clear it. If the user changed the server URL, we have
   * already cleared this information.
   * <p/>
   * Clearing it here handles the case where an admin deleted the
   * table on the server and we are now re-pushing that table to
   * the server.
   * <p/>
   * We do not know whether the rows on the device match those on the server.
   * We will find out later, in the course of the sync.
   * <p/>
   * if (tableInstanceFilesUri != null) {
   * deleteAllSyncETagsUnderServer(sc.getAppName(), db, tableInstanceFilesUri);
   * }
   *
   * @param db
   * @param tableId
   * @param tableInstanceFilesUri
   * @parma schemaETag
   */
  public void serverTableSchemaETagChanged(OdkConnectionInterface db, String tableId,
      String schemaETag, String tableInstanceFilesUri) {

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      changeDataRowsToNewRowState(db, tableId);

      privilegedUpdateTableETags(db, tableId, schemaETag, null);

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
    StringBuilder b = new StringBuilder();
    b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
        .append(DataTableColumns.SYNC_STATE).append(S_EQUALS_PARAM).append(S_AND)
        .append(DataTableColumns.CONFLICT_TYPE).append(" IN ( ?, ? )");
    Object[] whereArgs = { rowId, SyncState.in_conflict.name(),
        String.valueOf(ConflictType.SERVER_DELETED_OLD_VALUES),
        String.valueOf(ConflictType.SERVER_UPDATED_UPDATED_VALUES) };

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.delete(tableId, b.toString(), whereArgs);

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
   * @param serverValues
   * @param rowId
   * @param localRowConflictType
   * @param activeUser
   * @param locale
   */
  public void privilegedPlaceRowIntoConflictWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues serverValues, String rowId,
      int localRowConflictType,
      String activeUser, String locale) {

    // The rolesList of the activeUser does not impact the execution of this action.
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      this.deleteServerConflictRowWithId(db, tableId, rowId);
      this.placeRowIntoConflict(db, tableId, rowId, localRowConflictType);
      this.privilegedInsertRowWithId(db, tableId, orderedColumns, serverValues,
          rowId, activeUser, locale, false);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (!dbWithinTransaction) {
        db.endTransaction();
      }
    }
  }

  public boolean identicalValue(String localValue, String serverValue, ElementDataType dt) {

    if (localValue == null && serverValue == null) {
      return true;
    } else if (localValue == null || serverValue == null) {
      return false;
    } else if (localValue.equals(serverValue)) {
      return true;
    }

    // NOT textually identical.
    //
    // Everything must be textually identical except possibly number fields
    // which may have rounding due to different database implementations,
    // data representations, and marshaling libraries.
    //
    if (dt == ElementDataType.number) {
      // !!Important!! Double.valueOf(str) handles NaN and +/-Infinity
      Double localNumber = Double.valueOf(localValue);
      Double serverNumber = Double.valueOf(serverValue);

      if (localNumber.equals(serverNumber)) {
        // simple case -- trailing zeros or string representation mix-up
        //
        return true;
      } else if (localNumber.isInfinite() && serverNumber.isInfinite()) {
        // if they are both plus or both minus infinity, we have a match
        if (Math.signum(localNumber) == Math.signum(serverNumber)) {
          return true;
        } else {
          return false;
        }
      } else if (localNumber.isNaN() || localNumber.isInfinite() || serverNumber.isNaN()
          || serverNumber.isInfinite()) {
        // one or the other is special1
        return false;
      } else {
        double localDbl = localNumber;
        double serverDbl = serverNumber;
        if (localDbl == serverDbl) {
          return true;
        }
        // OK. We have two values like 9.80 and 9.8
        // consider them equal if they are adjacent to each other.
        double localNear = localDbl;
        int idist = 0;
        int idistMax = 128;
        for (idist = 0; idist < idistMax; ++idist) {
          localNear = Math.nextAfter(localNear, serverDbl);
          if (localNear == serverDbl) {
            break;
          }
        }
        if (idist < idistMax) {
          return true;
        }
        return false;
      }
    } else {
      // textual identity is required!
      return false;
    }
  }

  /**
   * Delete any prior server conflict row.
   * Examine database and incoming server values to determine how to apply the
   * server values to the database. This might delete the existing row, update
   * it, or create a conflict row.
   *
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param serverValues  field values for this row coming from server.
   *                      All fields must have values. The SyncState field
   *                      (a local field that does not come from the server)
   *                      should be "changed" or "deleted"
   * @param rowId
   * @param activeUser
   * @param rolesList  passed in to determine if the current user is a privileged user
   * @param locale
   */
  public void privilegedPerhapsPlaceRowIntoConflictWithId(OdkConnectionInterface db,
      String tableId, OrderedColumns orderedColumns, ContentValues serverValues, String rowId,
      String activeUser, String rolesList, String locale) {

    AccessContext accessContext = getAccessContext(db, tableId, activeUser, rolesList);

    // The rolesList of the activeUser does not impact the execution of this action.
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      // delete any existing server conflict row
      this.deleteServerConflictRowWithId(db, tableId, rowId);
      // fetch the current local (possibly-in-conflict) row
      BaseTable baseTable = this.privilegedGetRowsWithId(db, tableId, rowId, activeUser);
      if ( baseTable.getNumberOfRows() == 0 ) {
        throw new IllegalArgumentException("no matching row found for server conflict");
      } else if ( baseTable.getNumberOfRows() != 1 ) {
        throw new IllegalArgumentException("row has checkpoints or database is corrupt");
      }

      Row localRow = baseTable.getRowAtIndex(0);

      if ( localRow.getDataByKey(DataTableColumns.SAVEPOINT_TYPE) == null ) {
        throw new IllegalArgumentException("row has checkpoints");
      }

      String strSyncState = localRow.getDataByKey(DataTableColumns.SYNC_STATE);
      SyncState state = SyncState.valueOf(strSyncState);

      int localRowConflictTypeBeforeSync = -1;
      if (state == SyncState.in_conflict) {
        // we need to remove the in_conflict records that refer to the
        // prior state of the server
        String localRowConflictTypeBeforeSyncStr = localRow.getDataByKey(DataTableColumns.CONFLICT_TYPE);
        if (localRowConflictTypeBeforeSyncStr == null) {
          // this row is in conflict. It MUST have a non-null conflict type.
          throw new IllegalStateException("conflict type is null on an in-conflict row");
        }

        localRowConflictTypeBeforeSync = Integer.parseInt(localRowConflictTypeBeforeSyncStr);
        if (localRowConflictTypeBeforeSync == ConflictType.SERVER_DELETED_OLD_VALUES
            || localRowConflictTypeBeforeSync == ConflictType.SERVER_UPDATED_UPDATED_VALUES) {
          // should be impossible
          throw new IllegalStateException("only the local conflict record should remain");
        }
      }

      boolean isServerRowDeleted =
          serverValues.getAsString(DataTableColumns.SYNC_STATE).equals(SyncState.deleted.name());

      boolean executeDropThrough = false;

      if ( isServerRowDeleted ) {
        if ( state == SyncState.synced ) {
          // the server's change should be applied locally.
          this.privilegedDeleteRowWithId(db, tableId, rowId, activeUser);
        } else if ( state == SyncState.synced_pending_files ) {
          // sync logic may want to UPLOAD local files up to server before calling this routine.
          // this would prevent loss of attachments that have not yet been pushed to server.

          // the server's change should be applied locally.
          this.privilegedDeleteRowWithId(db, tableId, rowId, activeUser);
        } else if ( (state == SyncState.deleted)  || ((state == SyncState.in_conflict)
            && (localRowConflictTypeBeforeSync == ConflictType.LOCAL_DELETED_OLD_VALUES))) {
          // this occurs if
          // (1) a delete request was never ACKed but it was performed
          // on the server.
          // (2) if there is an unresolved conflict held locally with the
          // local action being to delete the record, and the prior server
          // state being a value change, but the newly sync'd state now
          // reflects a deletion by another party.
          //
          // the server's change should be applied locally.
          this.privilegedDeleteRowWithId(db, tableId, rowId, activeUser);
        } else {
          // need to resolve a conflict situation...
          executeDropThrough = true;
        }
      } else if ( state == SyncState.synced || state == SyncState.synced_pending_files ) {
        // When a prior sync ends with conflicts, we will not update the table's "lastDataETag"
        // and when we next sync, we will pull the same server row updates as when the
        // conflicts were raised (elsewhere in the table).
        //
        // Therefore, we can expect many server row updates to have already been locally
        // applied (for the rows were not in conflict). Detect and ignore these already-
        // processed changes by testing for the server and device having identical field values.
        //

        boolean isDifferent = false;
        for ( int i = 0 ; i < baseTable.getWidth() ; ++i ) {
          String colName = baseTable.getElementKey(i);
          if ( DataTableColumns.ID.equals(colName) ||
              DataTableColumns.CONFLICT_TYPE.equals(colName) ||
              DataTableColumns.EFFECTIVE_ACCESS.equals(colName) ||
              DataTableColumns.SYNC_STATE.equals(colName) ) {
            // these values are ignored during comparisons
            continue;
          }
          String localValue = localRow.getDataByKey(colName);
          String serverValue = serverValues.containsKey(colName) ?
              serverValues.getAsString(colName) : null;

          ElementDataType dt = ElementDataType.string;
          try {
            ColumnDefinition cd = orderedColumns.find(colName);
            dt = cd.getType().getDataType();
          } catch ( IllegalArgumentException e ) {
            // ignore
          }
          if ( !identicalValue(localValue, serverValue, dt) ) {
            isDifferent = true;
            break;
          }
        }

        if ( isDifferent ) {
          // Local row needs to be updated with server values.
          //
          // detect and handle file attachment column changes and sync state
          // (need to change serverRow value of this)
          boolean hasNonNullAttachments = false;

          for ( ColumnDefinition cd : orderedColumns.getColumnDefinitions() ) {
            // todo: does not handle array containing (types containing) rowpath elements
            if ( cd.isUnitOfRetention() &&
                cd.getType().getDataType().equals(ElementDataType.rowpath)) {
              String uriFragment = serverValues.getAsString(cd.getElementKey());
              String localUriFragment = localRow.getDataByKey(cd.getElementKey());
              if (uriFragment != null ) {
                if (localUriFragment == null || !localUriFragment.equals(uriFragment)) {
                  hasNonNullAttachments = true;
                  break;
                }
              }
            }
          }

          // update the row from the changes on the server
          ContentValues values = new ContentValues(serverValues);

          values.put(DataTableColumns.SYNC_STATE, (hasNonNullAttachments ||
              (state == SyncState.synced_pending_files))
              ? SyncState.synced_pending_files.name() : SyncState.synced.name());
          values.putNull(DataTableColumns.CONFLICT_TYPE);

          this.privilegedUpdateRowWithId(db, tableId, orderedColumns, values, rowId, activeUser,
              locale, false);
        }

        // and don't execute the drop-through in this case.
      } else {
        executeDropThrough = true;
      }

      if ( executeDropThrough ) {
        // SyncState.deleted and server is not deleting
        // SyncState.new_row and record exists on server
        // SyncState.changed and new change (or delete) on server
        // SyncState.in_conflict and new change (or delete) on server

        // ALSO: this case can occur when our prior sync attempt pulled down changes that
        // placed local row(s) in conflict -- which we have since resolved -- and we are now
        // issuing a sync to push those changes up to the server.
        //
        // This is because we do not update our local table's "lastDataETag" until we have
        // sync'd and applied all changes from the server and have no local conflicts or
        // checkpoints on the table. Because of this, when we issue a sync after resolving
        // a conflict, we will get the set of server row changes that include the row(s)
        // that were previously in conflict. This will appear as one of:
        //    changed | changed
        //    changed | deleted
        //    deleted | changed
        //    deleted | deleted
        //
        // BUT, this time, however, the local records will have the same rowETag as the
        // server record, indicating that they are valid changes (or deletions) on top of
        // the server's current version of this same row.
        //
        // If this is the case (the rowETags match), then we should not place the row into
        // conflict, but should instead ignore the reported content from the server and push
        // the local row's change or delete up to the server in the next section.
        //
        // If not, when we reach this point in the code, the rowETag of the server row
        // should **not match** our local row -- indicating that the server has a change from
        // another source, and that we should place the row into conflict.
        //
        String localRowETag = localRow.getDataByKey(DataTableColumns.ROW_ETAG);
        String serverRowETag = serverValues.getAsString(DataTableColumns.ROW_ETAG);
        boolean isDifferentRowETag = (localRowETag == null) || !localRowETag.equals(serverRowETag);
        if (!isDifferentRowETag) {
          // ignore the server record.
          // This is an update we will push to the server.
          // todo: make sure local row is not in_conflict at this point.
          if ( state == SyncState.in_conflict ) {
            // don't think this is logically possible at this point, but we should
            // transition record to changed or deleted state.
            ContentValues values = new ContentValues();
            for ( int i = 0 ; i < baseTable.getWidth() ; ++i ) {
              String colName = baseTable.getElementKey(i);
              if ( DataTableColumns.EFFECTIVE_ACCESS.equals(colName) ) {
                continue;
              }
              if ( localRow.getDataByIndex(i) == null ) {
                values.putNull(colName);
              } else {
                values.put(colName, localRow.getDataByIndex(i));
              }
            }
            values.put(DataTableColumns.ID, rowId);
            values.putNull(DataTableColumns.CONFLICT_TYPE);
            values.put(DataTableColumns.SYNC_STATE,
                (localRowConflictTypeBeforeSync == ConflictType.LOCAL_DELETED_OLD_VALUES) ?
                  SyncState.deleted.name() : SyncState.changed.name());

            this.privilegedUpdateRowWithId(db, tableId, orderedColumns, values, rowId,
                activeUser, locale, false);
          }
        } else {
          // figure out what the localRow conflict type should be...
          int localRowConflictType;
          if (state == SyncState.changed) {
            // SyncState.changed and new change on server
            localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
          } else if (state == SyncState.new_row) {
            // SyncState.new_row and record exists on server
            // The 'new_row' case occurs if an insert is never ACKed but
            // completes successfully on the server.
            localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
          } else if (state == SyncState.deleted) {
            // SyncState.deleted and server is not deleting
            localRowConflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
          } else if (state == SyncState.in_conflict) {
            // SyncState.in_conflict and new change on server
            // leave the local conflict type unchanged (retrieve it and
            // use it).
            localRowConflictType = localRowConflictTypeBeforeSync;
          } else {
            throw new IllegalStateException("Unexpected state encountered");
          }

          boolean isDifferentPrivilegedFields = false;
          {
            String[] privilegedColumns = new String[] {
                DataTableColumns.FILTER_TYPE,
                DataTableColumns.FILTER_VALUE };
            for ( int i = 0 ; i < privilegedColumns.length ; ++i ) {
              String colName = privilegedColumns[i];
              String localValue = localRow.getDataByKey(colName);
              String serverValue = serverValues.containsKey(colName) ?
                  serverValues.getAsString(colName) : null;

              ElementDataType dt = ElementDataType.string;
              try {
                ColumnDefinition cd = orderedColumns.find(colName);
                dt = cd.getType().getDataType();
              } catch ( IllegalArgumentException e ) {
                // ignore
              }

              boolean sameValue = identicalValue(localValue, serverValue, dt);

              if (!sameValue) {
                isDifferentPrivilegedFields = true;
                break;
              }
            }
          }
          boolean isDifferentExcludingPrivilegedFields = false;
          for ( int i = 0 ; i < baseTable.getWidth() ; ++i ) {
            String colName = baseTable.getElementKey(i);
            if ( DataTableColumns.ID.equals(colName) ||
                DataTableColumns.CONFLICT_TYPE.equals(colName) ||
                DataTableColumns.EFFECTIVE_ACCESS.equals(colName) ||
                DataTableColumns.SYNC_STATE.equals(colName) ||
                DataTableColumns.ROW_ETAG.equals(colName) ||
                DataTableColumns.FILTER_TYPE.equals(colName) ||
                DataTableColumns.FILTER_VALUE.equals(colName) ) {
              // these values are ignored during this comparison
              continue;
            }
            String localValue = localRow.getDataByKey(colName);
            String serverValue = serverValues.containsKey(colName) ?
                serverValues.getAsString(colName) : null;

            ElementDataType dt = ElementDataType.string;
            try {
              ColumnDefinition cd = orderedColumns.find(colName);
              dt = cd.getType().getDataType();
            } catch ( IllegalArgumentException e ) {
              // ignore
            }

            if ( serverValue != null && dt == ElementDataType.bool ) {
              serverValue = Integer.toString(DataHelper.boolToInt(serverValues.getAsBoolean(colName)));
            }
            boolean sameValue = identicalValue(localValue, serverValue, dt);

            if ( !sameValue ) {
              isDifferentExcludingPrivilegedFields = true;
              break;
            }
          }

          boolean hasNonNullDifferingServerAttachments = false;

          if ( isDifferentExcludingPrivilegedFields || (state != SyncState.synced) ) {
            for (ColumnDefinition cd : orderedColumns.getColumnDefinitions()) {
              // todo: does not handle array containing (types containing) rowpath elements
              if (cd.isUnitOfRetention() && cd.getType().getDataType().equals(ElementDataType.rowpath)) {
                String uriFragment = serverValues.getAsString(cd.getElementKey());
                String localUriFragment = localRow.getDataByKey(cd.getElementKey());
                if (uriFragment != null ) {
                  if (localUriFragment == null || !localUriFragment.equals(uriFragment)) {
                    hasNonNullDifferingServerAttachments = true;
                  }
                }
              }
            }
          }

          ContentValues values = new ContentValues(serverValues);

          if ( isDifferentExcludingPrivilegedFields || isDifferentPrivilegedFields ||
              isDifferentRowETag || isServerRowDeleted ) {

            if ( isDifferentExcludingPrivilegedFields || isServerRowDeleted ||
                 (localRowConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES) ||
                 (accessContext.isPrivilegedUser && isDifferentPrivilegedFields) ) {

              this.placeRowIntoConflict(db, tableId, rowId, localRowConflictType);
              serverValues.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());
              serverValues.put(DataTableColumns.CONFLICT_TYPE,
                  (isServerRowDeleted ?
                    ConflictType.SERVER_DELETED_OLD_VALUES :
                    ConflictType.SERVER_UPDATED_UPDATED_VALUES ));
              this.privilegedInsertRowWithId(db, tableId, orderedColumns, serverValues,
                  rowId, activeUser, locale, false);
            } else {
              // just apply the server RowETag and filterScope to the local row
              values.put(DataTableColumns.SYNC_STATE, hasNonNullDifferingServerAttachments
                  ? SyncState.synced_pending_files.name() : SyncState.synced.name());
              values.putNull(DataTableColumns.CONFLICT_TYPE);

              // move the local conflict back into the normal non-conflict (null) state
              // set the sync state to "changed" temporarily (otherwise we can't update)

              this.restoreRowFromConflict(db, tableId, rowId, SyncState.changed,
                  localRowConflictTypeBeforeSync);

              this.privilegedUpdateRowWithId(db, tableId, orderedColumns, values, rowId, activeUser,
                  locale, false);
            }
          } else {
            // data matches -- update row to adjust sync state and conflict type
            // if needed.
            SyncState destState = (hasNonNullDifferingServerAttachments ||
                (state == SyncState.synced_pending_files))
                ? SyncState.synced_pending_files : SyncState.synced;

            if ((state != destState) || isDifferentRowETag ||
                (localRow.getDataByKey(DataTableColumns.CONFLICT_TYPE) != null) ) {
              // todo: handle case where local row was in conflict
              // server has now matched the local row's state. i.e.,
              // update rowEtag, clear conflictType and adjust syncState on row.
              values.put(DataTableColumns.SYNC_STATE, destState.name());
              values.putNull(DataTableColumns.CONFLICT_TYPE);

              // move the local conflict back into the normal non-conflict (null) state
              // set the sync state to "changed" temporarily (otherwise we can't update)

              this.restoreRowFromConflict(db, tableId, rowId, SyncState.changed,
                  localRowConflictTypeBeforeSync);

              this.privilegedUpdateRowWithId(db, tableId, orderedColumns, values, rowId, activeUser,
                  locale, false);

              WebLogger.getLogger(db.getAppName()).w(t, "identical rows returned from server -- " + "SHOULDN'T THESE NOT HAPPEN?");
            }
          }
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
   * Change the conflictType for the given row from null (not in conflict) to
   * the specified one.
   *
   * @param db
   * @param tableId
   * @param rowId
   * @param conflictType expected to be one of ConflictType.LOCAL_DELETED_OLD_VALUES (0) or
   *                     ConflictType.LOCAL_UPDATED_UPDATED_VALUES (1)
   */
  private void placeRowIntoConflict(OdkConnectionInterface db, String tableId, String rowId,
      int conflictType) {

    StringBuilder b = new StringBuilder();
    b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
        .append(DataTableColumns.CONFLICT_TYPE).append(S_IS_NULL);
    Object[] whereArgs = { rowId };

    TreeMap<String,Object> cv = new TreeMap<String,Object>();
    cv.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());
    cv.put(DataTableColumns.CONFLICT_TYPE, conflictType);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.update(tableId, cv, b.toString(), whereArgs);

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
  private void restoreRowFromConflict(OdkConnectionInterface db, String tableId, String rowId,
      SyncState syncState, Integer conflictType) {

    // TODO: is roleList applicable here?

    StringBuilder b = new StringBuilder();
    Object[] whereArgs;

    if (conflictType == null) {
      b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
          .append(DataTableColumns.CONFLICT_TYPE).append(S_IS_NULL);
      whereArgs = new Object[] { rowId };
    } else {
      b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
          .append(DataTableColumns.CONFLICT_TYPE).append(S_EQUALS_PARAM);
      whereArgs = new Object[] { rowId, conflictType };
    }

    TreeMap<String,Object> cv = new TreeMap<String,Object>();
    cv.put(DataTableColumns.CONFLICT_TYPE, null);
    cv.put(DataTableColumns.SYNC_STATE, syncState.name());
    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      db.update(tableId, cv, b.toString(), whereArgs);

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
   * @param db
   * @param tableId
   * @param rowId
   * @return the sync state of the row (see {@link SyncState}), or null if the
   * row does not exist.  Rows are required to have non-null sync states.
   * @throws IllegalStateException if the row has a null sync state or has
   *                               2+ conflicts or checkpoints and
   *                               those do not have matching sync states!
   */
  public SyncState getSyncState(OdkConnectionInterface db, String tableId,
      String rowId) throws IllegalStateException {
    Cursor c = null;
    try {
      c = db.query(tableId, new String[] { DataTableColumns.SYNC_STATE },
          K_DATATABLE_ID_EQUALS_PARAM, new Object[] { rowId }, null, null, null, null);

      if (c.moveToFirst()) {
        int syncStateIndex = c.getColumnIndex(DataTableColumns.SYNC_STATE);
        if (c.isNull(syncStateIndex)) {
          throw new IllegalStateException(t + ": row had a null sync state!");
        }
        String val = CursorUtils.getIndexAsString(c, syncStateIndex);
        while (c.moveToNext()) {
          if (c.isNull(syncStateIndex)) {
            throw new IllegalStateException(t + ": row had a null sync state!");
          }
          String otherVal = CursorUtils.getIndexAsString(c, syncStateIndex);
          if (!val.equals(otherVal)) {
            throw new IllegalStateException(t + ": row with 2+ conflicts or checkpoints does "
                + "not have matching sync states!");
          }
        }
        return SyncState.valueOf(val);
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
   * <p/>
   * If you need to immediately delete a record that would otherwise sync to the
   * server, call updateRowETagAndSyncState(...) to set the row to
   * SyncState.new_row, and then call this method and it will be immediately
   * deleted (in this case, unless the record on the server was already deleted,
   * it will remain and not be deleted during any subsequent synchronizations).
   *
   * @param db
   * @param tableId
   * @param rowId
   * @param activeUser
   * @param rolesList
   * @throws ActionNotAuthorizedException
   */
  public void deleteRowWithId(OdkConnectionInterface db, String tableId,
      String rowId, String activeUser, String rolesList) throws ActionNotAuthorizedException {

    // TODO: rolesList of user may impact whether we can delete the record.
    // Particularly with sync'd records, is there anything special to do here?
    // consider sync path vs. tools path.
    // I.e., if the user is super-user or higher, we should take local FilterScope.
    // otherwise, we should take server FilterScope. Or should we allow user to select
    // which to take?

    boolean shouldPhysicallyDelete = false;

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      Object[] whereArgs = new Object[] { rowId };
      String whereClause = K_DATATABLE_ID_EQUALS_PARAM;

      // first need to test whether we can delete all the rows under this rowId.
      // If we can't, then throw an access violation
      Cursor c = null;
      try {
        c = db.query(tableId,
            new String[] { DataTableColumns.SYNC_STATE, DataTableColumns.FILTER_TYPE,
                DataTableColumns.FILTER_VALUE }, whereClause, whereArgs, null, null,
                DataTableColumns.SAVEPOINT_TIMESTAMP + " ASC", null);
        boolean hasFirst = c.moveToFirst();

        int idxSyncState = c.getColumnIndex(DataTableColumns.SYNC_STATE);
        int idxFilterType = c.getColumnIndex(DataTableColumns.FILTER_TYPE);
        int idxFilterValue = c.getColumnIndex(DataTableColumns.FILTER_VALUE);

        List<String> rolesArray = getRolesArray(rolesList);

        TableSecuritySettings tss = getTableSecuritySettings(db, tableId);

        if ( hasFirst ) {
          do {
            // verify each row
            String priorSyncState = c.getString(idxSyncState);
            String priorFilterType = c.isNull(idxFilterType) ? null : c.getString(idxFilterType);
            String priorFilterValue = c.isNull(idxFilterValue) ? null : c.getString(idxFilterValue);

            tss.allowRowChange(activeUser, rolesArray, priorSyncState, priorFilterType,
                priorFilterValue, RowChange.DELETE_ROW);

          } while (c.moveToNext());
        }

      } finally {
        if (c != null && !c.isClosed()) {
          c.close();
        }
        c = null;
      }

      // delete any checkpoints
      whereClause = K_DATATABLE_ID_EQUALS_PARAM + S_AND + DataTableColumns.SAVEPOINT_TYPE + S_IS_NULL;
      db.delete(tableId, whereClause, whereArgs);

      // this will return null if there are no rows.
      SyncState syncState = getSyncState(db, tableId, rowId);

      if (syncState == null) {
        // the rowId no longer exists (we deleted all checkpoints)
        shouldPhysicallyDelete = true;

      } else if (syncState == SyncState.new_row) {
        // we can safely remove this record from the database
        whereClause = K_DATATABLE_ID_EQUALS_PARAM;

        db.delete(tableId, whereClause, whereArgs);
        shouldPhysicallyDelete = true;

      } else if (syncState != SyncState.in_conflict) {

        TreeMap<String,Object> values = new TreeMap<String,Object>();
        values.put(DataTableColumns.SYNC_STATE, SyncState.deleted.name());
        values.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
            TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));

        db.update(tableId, values, K_DATATABLE_ID_EQUALS_PARAM, whereArgs);
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

    if (shouldPhysicallyDelete) {
      File instanceFolder = new File(ODKFileUtils.getInstanceFolder(db.getAppName(), tableId, rowId));
      try {
        FileUtils.deleteDirectory(instanceFolder);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        WebLogger.getLogger(db.getAppName())
            .e(t, "Unable to delete this directory: " + instanceFolder.getAbsolutePath());
        WebLogger.getLogger(db.getAppName()).printStackTrace(e);
      }
    }
  }

  /*
    * Internal method to execute a delete checkpoint statement with the given where clause
    *
    * @param db
    * @param tableId
    * @param rowId
    * @param whereClause
    * @param whereArgs
    * @param activeUser
    * @param rolesList
    * @throws ActionNotAuthorizedException
   */
  private void rawCheckpointDeleteDataInTable(OdkConnectionInterface db,
      String tableId, String rowId, String whereClause, Object[] whereArgs, String activeUser,
      String rolesList) throws ActionNotAuthorizedException {

    boolean shouldPhysicallyDelete = false;

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      // first need to test whether we can delete all the rows that are selected
      // by the where clause. If we can't, then throw an access violation
      Cursor c = null;
      try {
        c = db.query(tableId,
            new String[] { DataTableColumns.SYNC_STATE, DataTableColumns.FILTER_TYPE,
                DataTableColumns.FILTER_VALUE }, whereClause, whereArgs, null, null, null, null);
        boolean hasRow = c.moveToFirst();

        int idxSyncState = c.getColumnIndex(DataTableColumns.SYNC_STATE);
        int idxFilterType = c.getColumnIndex(DataTableColumns.FILTER_TYPE);
        int idxFilterValue = c.getColumnIndex(DataTableColumns.FILTER_VALUE);

        List<String> rolesArray = getRolesArray(rolesList);

        TableSecuritySettings tss = getTableSecuritySettings(db, tableId);

        if ( hasRow ) {
          do {
            // the row is entirely removed -- delete the attachments
            String priorSyncState = c.getString(idxSyncState);
            String priorFilterType = c.isNull(idxFilterType) ? null : c.getString(idxFilterType);
            String priorFilterValue = c.isNull(idxFilterValue) ? null : c.getString(idxFilterValue);

            tss.allowRowChange(activeUser, rolesArray, priorSyncState, priorFilterType,
                priorFilterValue, RowChange.DELETE_ROW);
          } while (c.moveToNext());
        }

      } finally {
        if (c != null && !c.isClosed()) {
          c.close();
        }
        c = null;
      }

      db.delete(tableId, whereClause, whereArgs);

      // see how many rows remain.
      // If there are none, then we should delete all the attachments for this row.
      c = null;
      try {
        c = db.query(tableId, new String[] { DataTableColumns.SYNC_STATE },
            K_DATATABLE_ID_EQUALS_PARAM, new Object[] { rowId }, null, null, null, null);
        c.moveToFirst();
        // the row is entirely removed -- delete the attachments
        shouldPhysicallyDelete = (c.getCount() == 0);
      } finally {
        if (c != null && !c.isClosed()) {
          c.close();
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

    if (shouldPhysicallyDelete) {
      File instanceFolder = new File(ODKFileUtils.getInstanceFolder(db.getAppName(), tableId, rowId));
      try {
        FileUtils.deleteDirectory(instanceFolder);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        WebLogger.getLogger(db.getAppName())
            .e(t, "Unable to delete this directory: " + instanceFolder.getAbsolutePath());
        WebLogger.getLogger(db.getAppName()).printStackTrace(e);
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
   * @param tableId
   * @param rowId
   * @param activeUser
   * @param rolesList
   * @throws ActionNotAuthorizedException
   */
  public void deleteAllCheckpointRowsWithId(OdkConnectionInterface db,
      String tableId, String rowId, String activeUser, String rolesList)
      throws ActionNotAuthorizedException {
    StringBuilder b = new StringBuilder();
    b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
        .append(DataTableColumns.SAVEPOINT_TYPE).append(S_IS_NULL);

    rawCheckpointDeleteDataInTable(db, tableId, rowId,
        b.toString(),
        new Object[] { rowId }, activeUser, rolesList);
  }

  /**
   * Delete any checkpoint rows for the given rowId in the tableId. Checkpoint
   * rows are created by ODK Survey to hold intermediate values during the
   * filling-in of the form. They act as restore points in the Survey, should
   * the application die.
   *
   * @param db
   * @param tableId
   * @param rowId
   * @param activeUser
   * @param rolesList
   * @throws ActionNotAuthorizedException
   */
  public void deleteLastCheckpointRowWithId(OdkConnectionInterface db,
      String tableId, String rowId, String activeUser, String rolesList)
      throws ActionNotAuthorizedException {
    StringBuilder b = new StringBuilder();
    b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
        .append(DataTableColumns.SAVEPOINT_TYPE).append(S_IS_NULL).append(S_AND)
        .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(" IN (SELECT MAX(")
             .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(") FROM ").append(tableId)
           .append(K_WHERE).append(K_DATATABLE_ID_EQUALS_PARAM).append(")");

    rawCheckpointDeleteDataInTable(db, tableId, rowId,
        b.toString(), new Object[] { rowId, rowId }, activeUser, rolesList);
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

    // TODO: if user becomes unverified, we still allow them to save-as-incomplete ths record.
    // Is this the behavior we want?  I think it would be difficult to explain otherwise.

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      StringBuilder b = new StringBuilder();
      b.append("UPDATE ").append(tableId).append(" SET ")
          .append(DataTableColumns.SAVEPOINT_TYPE).append(S_EQUALS_PARAM).append(K_WHERE)
          .append(K_DATATABLE_ID_EQUALS_PARAM);
      db.execSQL(b.toString(), new Object[] { SavepointTypeManipulator.incomplete(), rowId });
      b.setLength(0);
      b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
          .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(" NOT IN (SELECT MAX(")
          .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(") FROM ").append(tableId)
          .append(K_WHERE).append(K_DATATABLE_ID_EQUALS_PARAM).append(")");
      db.delete(tableId, b.toString(), new Object[] { rowId, rowId });

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

    // TODO: if user becomes unverified, we still allow them to save-as-complete ths record.
    // Is this the behavior we want?  I think it would be difficult to explain otherwise.

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      StringBuilder b = new StringBuilder();
      b.append("UPDATE ").append(tableId).append(" SET ")
          .append(DataTableColumns.SAVEPOINT_TYPE).append(S_EQUALS_PARAM).append(K_WHERE)
          .append(K_DATATABLE_ID_EQUALS_PARAM);
      db.execSQL(b.toString(), new Object[] { SavepointTypeManipulator.complete(), rowId });

      b.setLength(0);
      b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
          .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(" NOT IN (SELECT MAX(")
               .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(") FROM ").append(tableId)
          .append(K_WHERE).append(K_DATATABLE_ID_EQUALS_PARAM).append(")");
      db.delete(tableId, b.toString(), new Object[] { rowId, rowId });

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
   * @param activeUser
   * @param rolesList
   * @param locale
   */
  public void updateRowWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId, String activeUser,
      String rolesList, String locale) throws ActionNotAuthorizedException {

    // TODO: make sure caller passes in the correct roleList for the use case.
    // TODO: for multi-step sync actions, we probably need an internal variant of this.

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    HashMap<String,Object> cvDataTableVal = new HashMap<String,Object>();
    cvDataTableVal.put(DataTableColumns.ID, rowId);
    for (String key: cvValues.keySet()) {
      cvDataTableVal.put(key, cvValues.get(key));
    }

    upsertDataIntoExistingTable(db, tableId, orderedColumns, cvDataTableVal, true, false,
        activeUser, rolesList, locale, false);
  }

  private void updateRowWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, Map<String,Object> cvValues, String activeUser,
      String rolesList, String locale) throws ActionNotAuthorizedException {

    // TODO: make sure caller passes in the correct roleList for the use case.
    // TODO: for multi-step sync actions, we probably need an internal variant of this.

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    if (!cvValues.containsKey(DataTableColumns.ID)) {
      throw new IllegalArgumentException(t + ": No rowId in cvValues map " + tableId);
    }

    upsertDataIntoExistingTable(db, tableId, orderedColumns, cvValues, true, false,
        activeUser, rolesList, locale, false);
  }

  /**
   * Update the given rowId with the values in the cvValues. All field
   * values are specified in the cvValues. This is a server-induced update
   * of the row to match all fields from the server. An error is thrown if
   * there isn't a row matching this rowId or if there are checkpoint or
   * conflict entries for this rowId.
   *
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @param activeUser
   * @param locale
   * @param asCsvRequestedChange
   */
  private void privilegedUpdateRowWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId, String activeUser,
      String locale, boolean asCsvRequestedChange) {

    // TODO: make sure caller passes in the correct roleList for the use case.
    // TODO: for multi-step sync actions, we probably need an internal variant of this.

    String rolesList = RoleConsts.ADMIN_ROLES_LIST;

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    HashMap<String,Object> cvDataTableVal = new HashMap<String,Object>();
    cvDataTableVal.put(DataTableColumns.ID, rowId);
    for (String key: cvValues.keySet()) {
      cvDataTableVal.put(key, cvValues.get(key));
    }

    try {
      upsertDataIntoExistingTable(db, tableId, orderedColumns, cvDataTableVal, true, true,
          activeUser, rolesList, locale, asCsvRequestedChange);
    } catch (ActionNotAuthorizedException e) {
      WebLogger.getLogger(db.getAppName()).printStackTrace(e);
      throw new IllegalStateException(e);
    }
  }

  /**
   * SYNC Only. ADMIN Privileges!
   * <p/>
   * Delete the local row.
   *
   * @param db
   * @param tableId
   * @param rowId
   * @param activeUser
   */
  public void privilegedDeleteRowWithId(OdkConnectionInterface db, String tableId,
      String rowId, String activeUser) {

    // TODO: make sure caller passes in the correct roleList for the use case.
    String rolesList = RoleConsts.ADMIN_ROLES_LIST;

    boolean inTransaction = false;
    try {

      inTransaction = db.inTransaction();
      if (!inTransaction) {
        db.beginTransactionNonExclusive();
      }

      // delete the record of the server row
      deleteServerConflictRowWithId(db, tableId, rowId);

      // move the local record into the 'new_row' sync state
      // so it can be physically deleted.

      if ( privilegedUpdateRowETagAndSyncState(db, tableId, rowId, null, SyncState.new_row,
          activeUser) ) {

        // delete what was the local conflict record
        deleteRowWithId(db, tableId, rowId, activeUser, rolesList);
      }

      if (!inTransaction) {
        db.setTransactionSuccessful();
      }
    } catch (ActionNotAuthorizedException e) {
      WebLogger.getLogger(db.getAppName()).printStackTrace(e);
      throw new IllegalStateException(e);
    } finally {
      if (db != null) {
        if (!inTransaction) {
          db.endTransaction();
        }
      }
    }
  }

  /**
   * Delete the local and server conflict records to resolve a server conflict
   * <p/>
   * A combination of primitive actions, all performed in one transaction:
   * <p/>
   * // delete the record of the server row
   * deleteServerConflictRowWithId(appName, dbHandleName, tableId, rowId);
   * <p/>
   * // move the local record into the 'new_row' sync state
   * // so it can be physically deleted.
   * updateRowETagAndSyncState(appName, dbHandleName, tableId, rowId, null,
   * SyncState.new_row.name());
   * // move the local conflict back into the normal (null) state
   * deleteRowWithId(appName, dbHandleName, tableId, rowId);
   *
   * @param db
   * @param tableId
   * @param rowId
   * @param activeUser
   * @param rolesList
   * @throws ActionNotAuthorizedException
   */
  public void resolveServerConflictWithDeleteRowWithId(OdkConnectionInterface db,
      String tableId, String rowId, String activeUser, String rolesList)
      throws ActionNotAuthorizedException {

    // TODO: make sure caller passes in the correct roleList for the use case.

    boolean inTransaction = false;
    try {

      inTransaction = db.inTransaction();
      if (!inTransaction) {
        db.beginTransactionNonExclusive();
      }

      // delete the record of the server row
      deleteServerConflictRowWithId(db, tableId, rowId);

      // move the local record into the 'new_row' sync state
      // so it can be physically deleted.

      if ( !privilegedUpdateRowETagAndSyncState(db, tableId, rowId, null, SyncState.new_row,
          activeUser) ) {
        throw new IllegalArgumentException(
            "row id " + rowId + " does not have exactly 1 row in table " + tableId);
      }

      // move the local conflict back into the normal (null) state
      deleteRowWithId(db, tableId, rowId, activeUser, rolesList);

      if (!inTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (db != null) {
        if (!inTransaction) {
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
   * @param tableId
   * @param rowId
   * @param activeUser
   * @param rolesList
   * @param locale
   */
  public void resolveServerConflictTakeLocalRowWithId(OdkConnectionInterface db,
      String tableId, String rowId, String activeUser, String rolesList, String locale)
      throws ActionNotAuthorizedException {

    // TODO: if rolesList contains RoleConsts.ROLE_ADMINISTRATOR or  RoleConsts.ROLE_SUPER_USER
    // TODO: then we should take the local rowFilterScope values. Otherwise use server values.

    // I.e., if the user is super-user or higher, we should take local FilterScope.
    // otherwise, we should take server FilterScope. Or should we allow user to select
    // which to take?

    boolean inTransaction = false;
    try {

      inTransaction = db.inTransaction();
      if (!inTransaction) {
        db.beginTransactionNonExclusive();
      }

      OrderedColumns orderedColumns = getUserDefinedColumns(db, tableId);

      AccessContext accessContext = getAccessContext(db, tableId, activeUser,
          RoleConsts.ADMIN_ROLES_LIST);

      // get both conflict records for this row.
      // the local record is always before the server record (due to conflict_type values)
      StringBuilder b = new StringBuilder();
      b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
          .append(DataTableColumns.CONFLICT_TYPE).append(S_IS_NOT_NULL);
      BaseTable table = privilegedQuery(db, tableId,
          QueryUtil.buildSqlStatement(tableId, b.toString(), null, null,
              new String[] { DataTableColumns.CONFLICT_TYPE }, new String[] { "ASC" }),
          new Object[] { rowId }, null, accessContext);

      if (table.getNumberOfRows() != 2) {
        throw new IllegalStateException(
            "Did not find a server and local row when resolving conflicts for rowId: " + rowId);
      }
      Row localRow = table.getRowAtIndex(0);
      Row serverRow = table.getRowAtIndex(1);

      int localConflictType = Integer
          .parseInt(localRow.getDataByKey(DataTableColumns.CONFLICT_TYPE));

      int serverConflictType = Integer
          .parseInt(serverRow.getDataByKey(DataTableColumns.CONFLICT_TYPE));

      if (localConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES
          && localConflictType != ConflictType.LOCAL_DELETED_OLD_VALUES) {
        throw new IllegalStateException(
            "Did not find local conflict row when resolving conflicts for rowId: " + rowId);
      }

      if (serverConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES
          && serverConflictType != ConflictType.SERVER_DELETED_OLD_VALUES) {
        throw new IllegalStateException(
            "Did not find server conflict row when resolving conflicts for rowId: " + rowId);
      }

      // update what was the local conflict record with the local's changes
      // by the time we apply the update, the local conflict record will be
      // restored to the proper (conflict_type, sync_state) values.
      //
      // No need to specify them here.
      TreeMap<String,Object> updateValues = new TreeMap<String,Object>();
      updateValues.put(DataTableColumns.ID, rowId);
      updateValues
          .put(DataTableColumns.ROW_ETAG, serverRow.getDataByKey(DataTableColumns.ROW_ETAG));

      // take the server's filter metadata values ...
      TreeMap<String,Object> privilegedUpdateValues = new TreeMap<String,Object>();
      privilegedUpdateValues.put(DataTableColumns.ID, rowId);
      privilegedUpdateValues
          .put(DataTableColumns.FILTER_TYPE, serverRow.getDataByKey(DataTableColumns.FILTER_TYPE));
      privilegedUpdateValues.put(DataTableColumns.FILTER_VALUE,
          serverRow.getDataByKey(DataTableColumns.FILTER_VALUE));
      privilegedUpdateValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
          serverRow.getDataByKey(DataTableColumns.SAVEPOINT_TIMESTAMP));
      privilegedUpdateValues.put(DataTableColumns.SAVEPOINT_CREATOR,
          serverRow.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

      // Figure out whether to take the server or local metadata fields.
      // and whether to take the server or local data fields.

      SyncState finalSyncState = SyncState.changed;

      if (localConflictType == ConflictType.LOCAL_UPDATED_UPDATED_VALUES) {
        // We are updating -- preserve the local metadata and column values
        // this is a no-op, as we are updating the local record, so we don't
        // need to do anything special.
      } else {
        finalSyncState = SyncState.deleted;

        // Deletion is really a "TakeServerChanges" action, but ending with 'deleted' as
        // the final sync state.

        // copy everything over from the server row
        updateValues
            .put(DataTableColumns.FORM_ID, serverRow.getDataByKey(DataTableColumns.FORM_ID));
        updateValues.put(DataTableColumns.LOCALE, serverRow.getDataByKey(DataTableColumns.LOCALE));
        updateValues.put(DataTableColumns.SAVEPOINT_TYPE,
            serverRow.getDataByKey(DataTableColumns.SAVEPOINT_TYPE));
        updateValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
            serverRow.getDataByKey(DataTableColumns.SAVEPOINT_TIMESTAMP));
        updateValues.put(DataTableColumns.SAVEPOINT_CREATOR,
            serverRow.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

        // including the values of the user fields on the server
        for (String elementKey : orderedColumns.getRetentionColumnNames()) {
          updateValues.put(elementKey, serverRow.getDataByKey(elementKey));
        }
      }

      // delete the record of the server row
      deleteServerConflictRowWithId(db, tableId, rowId);

      // move the local conflict back into the normal non-conflict (null) state
      // set the sync state to "changed" temporarily (otherwise we can't update)

      restoreRowFromConflict(db, tableId, rowId, SyncState.changed, localConflictType);

      // update local with the changes
      updateRowWithId(db, tableId, orderedColumns, updateValues, activeUser, rolesList,
          locale);

      // update as if user has admin privileges.
      // do this so we can update the filter type and filter value
      updateRowWithId( db, tableId, orderedColumns, privilegedUpdateValues,
          activeUser, RoleConsts.ADMIN_ROLES_LIST, locale);

      // and if we are deleting, try to delete it.
      // this may throw an ActionNotAuthorizedException
      if ( finalSyncState == SyncState.deleted ) {
        deleteRowWithId(db, tableId, rowId, activeUser, rolesList);
      }

      if (!inTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (db != null) {
        if (!inTransaction) {
          db.endTransaction();
        }
      }
    }
  }

  /**
   * Resolve the server conflict by taking the local changes plus a value map
   * of select server field values.  This map should not update any metadata
   * fields -- it should just contain user data fields.
   * <p/>
   * It is an error to call this if the local change is to delete the row.
   *
   * @param db
   * @param tableId
   * @param cvValues   key-value pairs from the server record that we should incorporate.
   * @param rowId
   * @param activeUser
   * @param rolesList
   * @param locale
   */
  public void resolveServerConflictTakeLocalRowPlusServerDeltasWithId(OdkConnectionInterface db,
      String tableId, ContentValues cvValues, String rowId, String activeUser,
      String rolesList, String locale) throws ActionNotAuthorizedException {

    // TODO: if rolesList does not contain RoleConsts.ROLE_SUPER_USER or RoleConsts.ROLE_ADMINISTRATOR
    // TODO: then take the server's rowFilterScope rather than the user's values of those.
    // TODO: and apply the update only if the user roles support that update.

    // I.e., if the user is super-user or higher, we should take local FilterScope.
    // otherwise, we should take server FilterScope. Or should we allow user to select
    // which to take?

    boolean inTransaction = false;
    try {

      inTransaction = db.inTransaction();
      if (!inTransaction) {
        db.beginTransactionNonExclusive();
      }

      OrderedColumns orderedColumns = getUserDefinedColumns(db, tableId);

      AccessContext accessContext = getAccessContext(db, tableId, activeUser,
          RoleConsts.ADMIN_ROLES_LIST);

      // get both conflict records for this row.
      // the local record is always before the server record (due to conflict_type values)
      BaseTable table = privilegedQuery(db, tableId,
          QueryUtil.buildSqlStatement(tableId, K_DATATABLE_ID_EQUALS_PARAM +
                  S_AND + DataTableColumns.CONFLICT_TYPE + S_IS_NOT_NULL, null, null,
              new String[] { DataTableColumns.CONFLICT_TYPE }, new String[] { "ASC" }),
          new Object[] { rowId }, null, accessContext);

      if (table.getNumberOfRows() != 2) {
        throw new IllegalStateException(
            "Did not find a server and local row when resolving conflicts for rowId: " + rowId);
      }
      Row localRow = table.getRowAtIndex(0);
      Row serverRow = table.getRowAtIndex(1);

      int localConflictType = Integer
          .parseInt(localRow.getDataByKey(DataTableColumns.CONFLICT_TYPE));

      int serverConflictType = Integer
          .parseInt(serverRow.getDataByKey(DataTableColumns.CONFLICT_TYPE));

      if (localConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES
          && localConflictType != ConflictType.LOCAL_DELETED_OLD_VALUES) {
        throw new IllegalStateException(
            "Did not find local conflict row when resolving conflicts for rowId: " + rowId);
      }

      if (serverConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES
          && serverConflictType != ConflictType.SERVER_DELETED_OLD_VALUES) {
        throw new IllegalStateException(
            "Did not find server conflict row when resolving conflicts for rowId: " + rowId);
      }

      if (localConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES) {
        throw new IllegalStateException(
            "Local row is marked for deletion -- blending does not make sense for rowId: " + rowId);
      }

      HashMap<String,Object> updateValues = new HashMap<String,Object>();
      for (String key : cvValues.keySet()) {
        updateValues.put(key, cvValues.get(key));
      }

      // clean up the incoming map of server values to retain
      cleanUpValuesMap(orderedColumns, updateValues);
      updateValues.put(DataTableColumns.ID, rowId);
      updateValues
          .put(DataTableColumns.ROW_ETAG, serverRow.getDataByKey(DataTableColumns.ROW_ETAG));

      // update what was the local conflict record with the local's changes
      // by the time we apply the update, the local conflict record will be
      // restored to the proper (conflict_type, sync_state) values.
      //
      // No need to specify them here.

      // but take the local's metadata values (i.e., do not change these
      // during the update) ...
      updateValues.put(DataTableColumns.FORM_ID, localRow.getDataByKey(DataTableColumns.FORM_ID));
      updateValues.put(DataTableColumns.LOCALE, localRow.getDataByKey(DataTableColumns.LOCALE));
      updateValues.put(DataTableColumns.SAVEPOINT_TYPE,
          localRow.getDataByKey(DataTableColumns.SAVEPOINT_TYPE));
      updateValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
          localRow.getDataByKey(DataTableColumns.SAVEPOINT_TIMESTAMP));
      updateValues.put(DataTableColumns.SAVEPOINT_CREATOR,
          localRow.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

      // take the server's filter metadata values ...
      TreeMap<String,Object> privilegedUpdateValues = new TreeMap<String,Object>();
      privilegedUpdateValues.put(DataTableColumns.ID, rowId);
      privilegedUpdateValues
          .put(DataTableColumns.FILTER_TYPE, serverRow.getDataByKey(DataTableColumns.FILTER_TYPE));
      privilegedUpdateValues.put(DataTableColumns.FILTER_VALUE,
          serverRow.getDataByKey(DataTableColumns.FILTER_VALUE));
      privilegedUpdateValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
          serverRow.getDataByKey(DataTableColumns.SAVEPOINT_TIMESTAMP));
      privilegedUpdateValues.put(DataTableColumns.SAVEPOINT_CREATOR,
          serverRow.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

      // delete the record of the server row
      deleteServerConflictRowWithId(db, tableId, rowId);

      // move the local conflict back into the normal (null) state

      restoreRowFromConflict(db, tableId, rowId, SyncState.changed, localConflictType);

      // update local with server's changes
      updateRowWithId(db, tableId, orderedColumns, updateValues, activeUser, rolesList,
          locale);

      // update as if user has admin privileges.
      // do this so we can update the filter type and filter value
      updateRowWithId( db, tableId, orderedColumns, privilegedUpdateValues,
          activeUser, RoleConsts.ADMIN_ROLES_LIST, locale);

      if (!inTransaction) {
        db.setTransactionSuccessful();
      }
    } finally {
      if (db != null) {
        if (!inTransaction) {
          db.endTransaction();
        }
      }
    }
  }

  /**
   * Resolve the server conflict by taking the server changes.  This may delete the local row.
   *
   * @param db
   * @param tableId
   * @param rowId
   * @param activeUser
   * @param locale
   */
  public void resolveServerConflictTakeServerRowWithId(OdkConnectionInterface db,
      String tableId, String rowId, String activeUser, String locale) {

    String rolesList = RoleConsts.ADMIN_ROLES_LIST;
    // TODO: incoming rolesList should be the privileged user roles because we are
    // TODO: overwriting our local row with everything from the server.

    // we have no way in the resolve conflicts screen to choose which filter scope
    // to take. Need to allow super-user and above to choose the local filter scope
    // vs just taking what the server has.

    boolean inTransaction = false;
    try {

      inTransaction = db.inTransaction();
      if (!inTransaction) {
        db.beginTransactionNonExclusive();
      }

      OrderedColumns orderedColumns = getUserDefinedColumns(db, tableId);

      AccessContext accessContext = getAccessContext(db, tableId, activeUser,
          RoleConsts.ADMIN_ROLES_LIST);

      // get both conflict records for this row.
      // the local record is always before the server record (due to conflict_type values)
      StringBuilder b = new StringBuilder();
      b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
          .append(DataTableColumns.CONFLICT_TYPE).append(S_IS_NOT_NULL);
      BaseTable table = privilegedQuery(db, tableId,
          QueryUtil.buildSqlStatement(tableId, b.toString(), null, null,
              new String[] { DataTableColumns.CONFLICT_TYPE }, new String[] { "ASC" }),
          new Object[] { rowId }, null, accessContext);

      if (table.getNumberOfRows() != 2) {
        throw new IllegalStateException(
            "Did not find a server and local row when resolving conflicts for rowId: " + rowId);
      }
      Row localRow = table.getRowAtIndex(0);
      Row serverRow = table.getRowAtIndex(1);

      int localConflictType = Integer
          .parseInt(localRow.getDataByKey(DataTableColumns.CONFLICT_TYPE));

      int serverConflictType = Integer
          .parseInt(serverRow.getDataByKey(DataTableColumns.CONFLICT_TYPE));

      if (localConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES
          && localConflictType != ConflictType.LOCAL_DELETED_OLD_VALUES) {
        throw new IllegalStateException(
            "Did not find local conflict row when resolving conflicts for rowId: " + rowId);
      }

      if (serverConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES
          && serverConflictType != ConflictType.SERVER_DELETED_OLD_VALUES) {
        throw new IllegalStateException(
            "Did not find server conflict row when resolving conflicts for rowId: " + rowId);
      }

      if (serverConflictType == ConflictType.SERVER_DELETED_OLD_VALUES) {

        // delete the record of the server row
        deleteServerConflictRowWithId(db, tableId, rowId);

        // move the local record into the 'new_row' sync state
        // so it can be physically deleted.

        if ( !privilegedUpdateRowETagAndSyncState(db, tableId, rowId, null, SyncState.new_row,
            activeUser) ) {
          throw new IllegalArgumentException(
              "row id " + rowId + " does not have exactly 1 row in table " + tableId);
        }

        // and delete the local conflict and all of its associated attachments
        deleteRowWithId(db, tableId, rowId, activeUser, rolesList);

      } else {
        // update the local conflict record with the server's changes
        HashMap<String,Object> updateValues = new HashMap<String,Object>();
        updateValues.put(DataTableColumns.ID, rowId);
        updateValues
            .put(DataTableColumns.ROW_ETAG, serverRow.getDataByKey(DataTableColumns.ROW_ETAG));

        // update what was the local conflict record with the server's changes
        // by the time we apply the update, the local conflict record will be
        // restored to the proper (conflict_type, sync_state) values.
        //
        // No need to specify them here.

        // take the server's metadata values too...
        updateValues.put(DataTableColumns.FILTER_TYPE,
            serverRow.getDataByKey(DataTableColumns.FILTER_TYPE));
        updateValues.put(DataTableColumns.FILTER_VALUE,
            serverRow.getDataByKey(DataTableColumns.FILTER_VALUE));
        updateValues
            .put(DataTableColumns.FORM_ID, serverRow.getDataByKey(DataTableColumns.FORM_ID));
        updateValues.put(DataTableColumns.LOCALE, serverRow.getDataByKey(DataTableColumns.LOCALE));
        updateValues.put(DataTableColumns.SAVEPOINT_TYPE,
            serverRow.getDataByKey(DataTableColumns.SAVEPOINT_TYPE));
        updateValues.put(DataTableColumns.SAVEPOINT_TIMESTAMP,
            serverRow.getDataByKey(DataTableColumns.SAVEPOINT_TIMESTAMP));
        updateValues.put(DataTableColumns.SAVEPOINT_CREATOR,
            serverRow.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

        // take all the data values from the server...
        for (String elementKey : orderedColumns.getRetentionColumnNames()) {
          updateValues.put(elementKey, serverRow.getDataByKey(elementKey));
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
          for (ColumnDefinition cd : orderedColumns.getColumnDefinitions()) {
            if (cd.getType().getDataType() != ElementDataType.rowpath) {
              // not a file attachment
              continue;
            }
            String v = serverRow.getDataByKey(cd.getElementKey());
            if (v != null && v.length() != 0) {
              // non-null file attachment specified on server row
              hasUriFragments = true;
              break;
            }
          }
          newState = hasUriFragments ? SyncState.synced_pending_files : SyncState.synced;
        }

        // delete the record of the server row
        deleteServerConflictRowWithId(db, tableId, rowId);

        // move the local conflict back into either the synced or synced_pending_files
        // state

        restoreRowFromConflict(db, tableId, rowId, newState, localConflictType);

        // update local with server's changes

        updateRowWithId(db, tableId, orderedColumns, updateValues, activeUser, rolesList,
            locale);

        // and reset the sync state to whatever it should be (update will make it changed)
        restoreRowFromConflict(db, tableId, rowId, newState, null);
      }

      if (!inTransaction) {
        db.setTransactionSuccessful();
      }
    } catch (ActionNotAuthorizedException e) {
      WebLogger.getLogger(db.getAppName()).printStackTrace(e);
      throw new IllegalStateException(e);
    } finally {
      if (db != null) {
        if (!inTransaction) {
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
   * @param activeUser
   * @param rolesList
   * @param locale
   */
  public void insertCheckpointRowWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId, String activeUser,
      String rolesList, String locale) throws ActionNotAuthorizedException {

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(
          t + ": No values to add into table for checkpoint" + tableId);
    }

    // these are all managed in the database layer...
    // the user should NOT set them...

    if (cvValues.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
      throw new IllegalArgumentException(
          t + ": No user supplied savepoint timestamp can be included for a checkpoint");
    }

    if (cvValues.containsKey(DataTableColumns.SAVEPOINT_TYPE)) {
      throw new IllegalArgumentException(
          t + ": No user supplied savepoint type can be included for a checkpoint");
    }

    if (cvValues.containsKey(DataTableColumns.ROW_ETAG)) {
      throw new IllegalArgumentException(
          t + ": No user supplied row ETag can be included for a checkpoint");
    }

    if (cvValues.containsKey(DataTableColumns.SYNC_STATE)) {
      throw new IllegalArgumentException(
          t + ": No user supplied sync state can be included for a checkpoint");
    }

    if (cvValues.containsKey(DataTableColumns.CONFLICT_TYPE)) {
      throw new IllegalArgumentException(
          t + ": No user supplied conflict type can be included for a checkpoint");
    }

    if (cvValues.containsKey(DataTableColumns.FILTER_VALUE)) {
      throw new IllegalArgumentException(
          t + ": No user supplied filter value can be included for a checkpoint");
    }

    if (cvValues.containsKey(DataTableColumns.FILTER_TYPE)) {
      throw new IllegalArgumentException(
          t + ": No user supplied filter type can be included for a checkpoint");
    }

    // If a rowId is specified, a cursor will be needed to
    // get the current row to create a checkpoint with the relevant data
    Cursor c = null;
    try {
      // Allow the user to pass in no rowId if this is the first
      // checkpoint row that the user is adding
      if (rowId == null) {

        // TODO: is this even valid any more? I think we disallow this in the AIDL flow.

        String rowIdToUse = LocalizationUtils.genUUID();
        HashMap<String,Object> currValues = new HashMap<String,Object>();
        for (String key : cvValues.keySet()) {
          currValues.put(key, cvValues.get(key));
        }
        currValues.put(DataTableColumns._ID, rowIdToUse);
        currValues.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
        insertCheckpointIntoExistingTable(db, tableId, orderedColumns, currValues, activeUser,
            rolesList, locale, true, null, null);
        return;
      }

      StringBuilder b = new StringBuilder();
      b.append(K_DATATABLE_ID_EQUALS_PARAM).append(S_AND)
          .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(" IN (SELECT MAX(")
              .append(DataTableColumns.SAVEPOINT_TIMESTAMP).append(") FROM ")
                 .append(tableId).append(K_WHERE).append(K_DATATABLE_ID_EQUALS_PARAM).append(")");
      c = db.query(tableId, null, b.toString(), new Object[] { rowId, rowId }, null,
          null, null, null);
      c.moveToFirst();

      if (c.getCount() > 1) {
        throw new IllegalStateException(t + ": More than one checkpoint at a timestamp");
      }

      // Inserting a checkpoint for the first time
      if (c.getCount() <= 0) {
        HashMap<String,Object> currValues = new HashMap<String,Object>();
        for (String key : cvValues.keySet()) {
          currValues.put(key, cvValues.get(key));
        }
        currValues.put(DataTableColumns._ID, rowId);
        currValues.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
        insertCheckpointIntoExistingTable(db, tableId, orderedColumns, currValues, activeUser,
            rolesList, locale, true, null, null);
        return;
      } else {
        // Make sure that the conflict_type of any existing row
        // is null, otherwise throw an exception
        int conflictIndex = c.getColumnIndex(DataTableColumns.CONFLICT_TYPE);
        if (!c.isNull(conflictIndex)) {
          throw new IllegalStateException(
              t + ":  A checkpoint cannot be added for a row that is in conflict");
        }

        HashMap<String,Object> currValues = new HashMap<String,Object>();
        for (String key : cvValues.keySet()) {
          currValues.put(key, cvValues.get(key));
        }

        // This is unnecessary
        // We should only have one row at this point
        //c.moveToFirst();

        String priorFilterType = null;
        String priorFilterValue = null;

        // Get the number of columns to iterate over and add
        // those values to the content values
        for (int i = 0; i < c.getColumnCount(); i++) {
          String name = c.getColumnName(i);

          if (currValues.containsKey(name)) {
            continue;
          }

          // omitting savepoint timestamp will generate a new timestamp.
          if (name.equals(DataTableColumns.SAVEPOINT_TIMESTAMP)) {
            continue;
          }

          // set savepoint type to null to mark this as a checkpoint
          if (name.equals(DataTableColumns.SAVEPOINT_TYPE)) {
            currValues.put(name, null);
            continue;
          }

          // sync state (a non-null field) should either remain 'new_row'
          // or be set to 'changed' for all other existing values.
          if (name.equals(DataTableColumns.SYNC_STATE)) {
            String priorState = c.getString(i);
            if (priorState.equals(SyncState.new_row.name())) {
              currValues.put(name, SyncState.new_row.name());
            } else {
              currValues.put(name, SyncState.changed.name());
            }
            continue;
          }

          if (c.isNull(i)) {
            currValues.put(name, null);
            continue;
          }

          // otherwise, just copy the values over...
          Class<?> theClass = CursorUtils.getIndexDataType(c, i);
          Object object = CursorUtils.getIndexAsType(c, theClass, i);
          insertValueIntoContentValues(currValues, theClass, name, object);

          if (name.equals(DataTableColumns.FILTER_TYPE)) {
            priorFilterType = c.getString(i);
          }

          if (name.equals(DataTableColumns.FILTER_VALUE)) {
            priorFilterValue = c.getString(i);
          }
        }

        insertCheckpointIntoExistingTable(db, tableId, orderedColumns, currValues, activeUser,
            rolesList, locale, false, priorFilterType, priorFilterValue);
      }
    } finally {
      if (c != null && !c.isClosed()) {
        c.close();
      }
    }
  }

  private void insertValueIntoContentValues(Map<String,Object> cv, Class<?> theClass, String name,
      Object obj) {

    if (obj == null) {
      cv.put(name, null);
      return;
    }

    // Couldn't use the CursorUtils.getIndexAsType
    // because assigning the result to Object v
    // would not work for the currValues.put function
    if (theClass == Long.class) {
      cv.put(name, (Long) obj);
    } else if (theClass == Integer.class) {
      cv.put(name, (Integer) obj);
    } else if (theClass == Double.class) {
      cv.put(name, (Double) obj);
    } else if (theClass == String.class) {
      cv.put(name, (String) obj);
    } else if (theClass == Boolean.class) {
      // stored as integers
      Integer v = (Integer) obj;
      cv.put(name, Boolean.valueOf(v != 0));
    } else if (theClass == ArrayList.class) {
      cv.put(name, (String) obj);
    } else if (theClass == HashMap.class) {
      // json deserialization of an object
      cv.put(name, (String) obj);
    } else {
      throw new IllegalStateException(
          "Unexpected data type in SQLite table " + theClass.toString());
    }
  }

  /**
   * Insert the given rowId with the values in the cvValues. All metadata field
   * values must be specified in the cvValues. This is called from Sync for inserting
   * a row verbatim from the server.
   * <p/>
   * If a row with this rowId is present, then an exception is thrown.
   *
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @param activeUser
   * @param locale
   * @param asCsvRequestedChange
   */
  public void privilegedInsertRowWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId, String activeUser,
      String locale, boolean asCsvRequestedChange) {

    String rolesList = RoleConsts.ADMIN_ROLES_LIST;

    if (cvValues == null || cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    HashMap<String,Object> cvDataTableVal = new HashMap<String,Object>();
    cvDataTableVal.put(DataTableColumns.ID, rowId);
    for ( String key : cvValues.keySet() ) {
      cvDataTableVal.put(key, cvValues.get(key));
    }

    // TODO: verify that all fields are specified
    try {
      upsertDataIntoExistingTable(db, tableId, orderedColumns, cvDataTableVal, false, true,
          activeUser, rolesList, locale, asCsvRequestedChange);
    } catch (ActionNotAuthorizedException e) {
      WebLogger.getLogger(db.getAppName()).printStackTrace(e);
      throw new IllegalStateException(e);
    }
  }

  /**
   * Insert the given rowId with the values in the cvValues. If certain metadata
   * values are not specified in the cvValues, then suitable default values may
   * be supplied for them.
   * <p/>
   * If a row with this rowId and certain matching metadata fields is present,
   * then an exception is thrown.
   *
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param rowId
   * @param activeUser
   * @param rolesList
   * @param locale
   */
  public void insertRowWithId(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, ContentValues cvValues, String rowId, String activeUser,
      String rolesList, String locale) throws ActionNotAuthorizedException {

    if (cvValues == null || cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    HashMap<String,Object> cvDataTableVal = new HashMap<String,Object>();
    cvDataTableVal.put(DataTableColumns.ID, rowId);
    for ( String key : cvValues.keySet() ) {
      cvDataTableVal.put(key, cvValues.get(key));
    }

    upsertDataIntoExistingTable(db, tableId, orderedColumns, cvDataTableVal, false, false,
        activeUser, rolesList, locale, false);
  }

  /**
   * Write checkpoint into the database
   *
   * @param db
   * @param tableId
   * @param orderedColumns
   * @param cvValues
   * @param activeUser
   * @param rolesList
   * @param locale
   * @param isNewRow
   */
  private void insertCheckpointIntoExistingTable(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, HashMap<String,Object> cvValues, String activeUser, String rolesList,
      String locale, boolean isNewRow, String priorFilterType, String priorFilterValue)
      throws ActionNotAuthorizedException {
    String rowId = null;

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    HashMap<String,Object> cvDataTableVal = new HashMap<String,Object>();
    cvDataTableVal.putAll(cvValues);

    if (cvDataTableVal.containsKey(DataTableColumns.ID)) {

      rowId = (String) cvDataTableVal.get(DataTableColumns.ID);
      if (rowId == null) {
        throw new IllegalArgumentException(DataTableColumns.ID + ", if specified, cannot be null");
      }
    } else {
      throw new IllegalArgumentException(t
          + ": rowId should not be null in insertCheckpointIntoExistingTable in the ContentValues");
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.ROW_ETAG)
        || cvDataTableVal.get(DataTableColumns.ROW_ETAG) == null) {
      cvDataTableVal.put(DataTableColumns.ROW_ETAG, DataTableColumns.DEFAULT_ROW_ETAG);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.CONFLICT_TYPE)) {
      cvDataTableVal.put(DataTableColumns.CONFLICT_TYPE, null);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.FORM_ID)) {
      cvDataTableVal.put(DataTableColumns.FORM_ID, null);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.LOCALE) || (
        cvDataTableVal.get(DataTableColumns.LOCALE) == null)) {
      cvDataTableVal.put(DataTableColumns.LOCALE, locale);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE) || (
        cvDataTableVal.get(DataTableColumns.SAVEPOINT_TYPE) == null)) {
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, null);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)
        || cvDataTableVal.get(DataTableColumns.SAVEPOINT_TIMESTAMP) == null) {
      String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
    }

    if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR) || (
        cvDataTableVal.get(DataTableColumns.SAVEPOINT_CREATOR) == null)) {
      cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR, activeUser);
    }

    cleanUpValuesMap(orderedColumns, cvDataTableVal);

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      List<String> rolesArray = getRolesArray(rolesList);

      // get the security settings
      TableSecuritySettings tss = getTableSecuritySettings(db, tableId);

      if (isNewRow) {

        // ensure that filter type and value are defined. Use defaults if not.

        if (!cvDataTableVal.containsKey(DataTableColumns.FILTER_TYPE) || (
            cvDataTableVal.get(DataTableColumns.FILTER_TYPE) == null)) {
          cvDataTableVal.put(DataTableColumns.FILTER_TYPE, tss.filterTypeOnCreation);
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.FILTER_VALUE)) {
          cvDataTableVal.put(DataTableColumns.FILTER_VALUE, activeUser);
        }

        cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());

        tss.allowRowChange(activeUser, rolesArray, SyncState.new_row.name(), priorFilterType,
            priorFilterValue, RowChange.NEW_ROW);

      } else {

        // don't allow changes to filter type or value or syncState when inserting checkpoints
        cvDataTableVal.put(DataTableColumns.FILTER_TYPE, priorFilterType);

        cvDataTableVal.put(DataTableColumns.FILTER_VALUE, priorFilterValue);

        // for this call path, syncState is already updated by caller

        tss.allowRowChange(activeUser, rolesArray,
            (String) cvDataTableVal.get(DataTableColumns.SYNC_STATE), priorFilterType,
            priorFilterValue, RowChange.CHANGE_ROW);
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

  private enum RowChange {
    NEW_ROW,
    CHANGE_ROW,
    DELETE_ROW
  }

  private class TableSecuritySettings {
    final String tableId;
    final boolean isLocked;
    final boolean canUnverifiedUserCreateRow;
    final String filterTypeOnCreation;

    public TableSecuritySettings(final String tableId, final boolean isLocked,
        final boolean canUnverifiedUserCreateRow, final String filterTypeOnCreation) {
      this.tableId = tableId;
      this.isLocked = isLocked;
      this.canUnverifiedUserCreateRow = canUnverifiedUserCreateRow;
      this.filterTypeOnCreation = filterTypeOnCreation;
    }

    public void canModifyFilterTypeAndValue(List<String> rolesArray)
        throws ActionNotAuthorizedException {

      if (rolesArray == null) {
        // unverified user

        // throw an exception
        throw new ActionNotAuthorizedException(
            t + ": unverified users cannot modify filterType or filterValue fields in (any) table "
                + tableId);

      } else if (!(rolesArray.contains(RoleConsts.ROLE_SUPER_USER) || rolesArray
          .contains(RoleConsts.ROLE_ADMINISTRATOR))) {
        // not (super-user or administrator)

        // throw an exception
        throw new ActionNotAuthorizedException(t
            + ": user does not have the privileges (super-user or administrator) to modify filterType or filterValue fields in table "
            + tableId);
      }
    }

    public void allowRowChange(String activeUser, List<String> rolesArray,
        String updatedSyncState, String priorFilterType, String priorFilterValue,
        RowChange rowChange) throws ActionNotAuthorizedException {

      switch (rowChange) {
      case NEW_ROW:

        // enforce restrictions:
        // 1. if locked, only super-user and administrator can create rows.
        // 2. otherwise, if unverified user, allow creation based upon unverifedUserCanCreate flag
        if (isLocked) {
          // inserting into a LOCKED table

          if (rolesArray == null) {
            // unverified user

            // throw an exception
            throw new ActionNotAuthorizedException(
                t + ": unverified users cannot create a rows in a locked table " + tableId);
          }

          if (!(rolesArray.contains(RoleConsts.ROLE_SUPER_USER) || rolesArray
              .contains(RoleConsts.ROLE_ADMINISTRATOR))) {
            // bad JSON
            // not a super-user and not an administrator

            // throw an exception
            throw new ActionNotAuthorizedException(t
                + ": user does not have the privileges (super-user or administrator) to create a row in a locked table "
                + tableId);
          }

        } else if (rolesArray == null) {
          // inserting into an UNLOCKED table

          // unverified user
          if (!canUnverifiedUserCreateRow) {

            // throw an exception
            throw new ActionNotAuthorizedException(t
                + ": unverified users do not have the privileges to create a row in this unlocked table "
                + tableId);
          }
        }
        break;
      case CHANGE_ROW:

        // if SyncState is new_row then allow edits in both locked and unlocked tables
        if (!updatedSyncState.equals(SyncState.new_row.name())) {

          if (isLocked) {
            // modifying a LOCKED table

            // disallow edits if:
            // 1. user is unverified
            // 2. existing filterValue is null or does not match the activeUser AND
            //    the activeUser is neither a super-user nor an administrator.

            if (rolesArray == null || rolesArray.isEmpty()) {
              // unverified user

              // throw an exception
              throw new ActionNotAuthorizedException(
                  t + ": unverified users cannot modify rows in a locked table " + tableId);
            }

            // allow if prior filterValue matches activeUser
            if (!(priorFilterValue != null && activeUser.equals(priorFilterValue))) {
              // otherwise...
              // reject if the activeUser is not a super-user or administrator

              if (rolesArray == null || !(rolesArray.contains(RoleConsts.ROLE_SUPER_USER)
                  || rolesArray.contains(RoleConsts.ROLE_ADMINISTRATOR))) {
                // bad JSON or
                // not a super-user and not an administrator

                // throw an exception
                throw new ActionNotAuthorizedException(t
                    + ": user does not have the privileges (super-user or administrator) to modify rows in a locked table "
                    + tableId);
              }
            }
          } else {
            // modifying an UNLOCKED table

            // allow if filterType is MODIFY or DEFAULT
            if (priorFilterType == null || !(
                priorFilterType.equals(RowFilterScope.Type.MODIFY.name()) || priorFilterType
                    .equals(RowFilterScope.Type.DEFAULT.name()))) {
              // otherwise...

              // allow if prior filterValue matches activeUser
              if (priorFilterValue == null || !activeUser.equals(priorFilterValue)) {
                // otherwise...
                // reject if the activeUser is not a super-user or administrator

                if (rolesArray == null || !(rolesArray.contains(RoleConsts.ROLE_SUPER_USER)
                    || rolesArray.contains(RoleConsts.ROLE_ADMINISTRATOR))) {
                  // bad JSON or
                  // not a super-user and not an administrator

                  // throw an exception
                  throw new ActionNotAuthorizedException(t
                      + ": user does not have the privileges (super-user or administrator) to modify hidden or read-only rows in an unlocked table "
                      + tableId);
                }
              }
            }
          }
        }
        break;
      case DELETE_ROW:

        // if SyncState is new_row then allow deletes in both locked and unlocked tables
        if (!updatedSyncState.equals(SyncState.new_row.name())) {

          if (isLocked) {
            // deleting a LOCKED table

            // disallow deletes if:
            // 1. user is unverified
            // 2. user is not a super-user or an administrator

            if (rolesArray == null) {
              // unverified user

              // throw an exception
              throw new ActionNotAuthorizedException(
                  t + ": unverified users cannot delete rows in a locked table " + tableId);
            }

            // reject if the activeUser is not a super-user or administrator

            if (rolesArray == null || !(rolesArray.contains(RoleConsts.ROLE_SUPER_USER)
                || rolesArray.contains(RoleConsts.ROLE_ADMINISTRATOR))) {
              // bad JSON or
              // not a super-user and not an administrator

              // throw an exception
              throw new ActionNotAuthorizedException(t
                  + ": user does not have the privileges (super-user or administrator) to delete rows in a locked table "
                  + tableId);
            }
          } else {
            // delete in an UNLOCKED table

            // allow if filterType is DEFAULT
            if (priorFilterType == null || !(
                priorFilterType.equals(RowFilterScope.Type.DEFAULT.name()))) {
              // otherwise...

              // allow if prior filterValue matches activeUser
              if (priorFilterValue == null || !activeUser.equals(priorFilterValue)) {
                // otherwise...
                // reject if the activeUser is not a super-user or administrator

                if (rolesArray == null || !(rolesArray.contains(RoleConsts.ROLE_SUPER_USER)
                    || rolesArray.contains(RoleConsts.ROLE_ADMINISTRATOR))) {
                  // bad JSON or
                  // not a super-user and not an administrator

                  // throw an exception
                  throw new ActionNotAuthorizedException(t
                      + ": user does not have the privileges (super-user or administrator) to delete hidden or read-only rows in an unlocked table "
                      + tableId);
                }
              }
            }
          }
        }
        break;
      }
    }
  }

  /**
   * Get the table's security settings.
   *
   * @param db
   * @param tableId
   * @return
   */
  private TableSecuritySettings getTableSecuritySettings(OdkConnectionInterface db,
      String tableId) {

    // get the security settings
    List<KeyValueStoreEntry> entries = getTableMetadata(db, tableId,
        KeyValueStoreConstants.PARTITION_TABLE, LocalKeyValueStoreConstants.TableSecurity.ASPECT,
        null).getEntries();

    KeyValueStoreEntry locked = null;
    KeyValueStoreEntry filterTypeOnCreation = null;
    KeyValueStoreEntry unverifiedUserCanCreate = null;
    for (KeyValueStoreEntry entry : entries) {
      if (entry.key.equals(LocalKeyValueStoreConstants.TableSecurity.KEY_FILTER_TYPE_ON_CREATION)) {
        filterTypeOnCreation = entry;
      } else if (entry.key
          .equals(LocalKeyValueStoreConstants.TableSecurity.KEY_UNVERIFIED_USER_CAN_CREATE)) {
        unverifiedUserCanCreate = entry;
      } else if (entry.key.equals(LocalKeyValueStoreConstants.TableSecurity.KEY_LOCKED)) {
        locked = entry;
      }
    }

    Boolean isLocked = (locked != null) ?
        KeyValueStoreUtils.getBoolean(db.getAppName(), locked) :
        null;
    if (isLocked == null) {
      isLocked = false;
    }

    Boolean canUnverifiedUserCreateRow = (unverifiedUserCanCreate != null) ?
        KeyValueStoreUtils.getBoolean(db.getAppName(), unverifiedUserCanCreate) :
        null;
    if (canUnverifiedUserCreateRow == null) {
      canUnverifiedUserCreateRow = true;
    }

    String filterType = (filterTypeOnCreation != null) ? filterTypeOnCreation.value : null;
    if (filterType == null) {
      filterType = DataTableColumns.DEFAULT_FILTER_TYPE;
    }

    return new TableSecuritySettings(tableId, isLocked, canUnverifiedUserCreateRow, filterType);
  }

  /*
   * Write data into a user defined database table
   *
   * TODO: This is broken w.r.t. updates of partial fields
   */
  private void upsertDataIntoExistingTable(OdkConnectionInterface db, String tableId,
      OrderedColumns orderedColumns, Map<String,Object> cvValues, boolean shouldUpdate,
      boolean asServerRequestedChange, String activeUser, String rolesList, String locale,
      boolean asCsvRequestedChange) throws ActionNotAuthorizedException {

    String rowId = null;
    String whereClause = null;
    boolean specifiesConflictType = cvValues.containsKey(DataTableColumns.CONFLICT_TYPE);
    boolean nullConflictType =
        specifiesConflictType && (cvValues.get(DataTableColumns.CONFLICT_TYPE) == null);
    Object[] whereArgs = new Object[specifiesConflictType ? (1 + (nullConflictType ? 0 : 1)) : 1];
    boolean update = false;
    String updatedSyncState = SyncState.new_row.name();
    String priorFilterType = DataTableColumns.DEFAULT_FILTER_TYPE;
    String priorFilterValue = null;

    if (cvValues.size() <= 0) {
      throw new IllegalArgumentException(t + ": No values to add into table " + tableId);
    }

    HashMap<String,Object> cvDataTableVal = new HashMap<String,Object>();
    cvDataTableVal.putAll(cvValues);

    // if this is a server-requested change, all the user fields and admin columns should be specified.
    if (asServerRequestedChange && !asCsvRequestedChange) {
      for (String columnName : orderedColumns.getRetentionColumnNames()) {
        if (!cvDataTableVal.containsKey(columnName)) {
          throw new IllegalArgumentException(
              t + ": Not all user field values are set during server " +
                  (shouldUpdate ? "update" : "insert") + " in table " + tableId + " missing: " +
                  columnName);
        }
      }
      for (String columnName : ADMIN_COLUMNS) {
        if (!cvDataTableVal.containsKey(columnName)) {
          throw new IllegalArgumentException(
              t + ": Not all metadata field values are set during server " +
                  (shouldUpdate ? "update" : "insert") + " in table " + tableId + " missing: " +
                  columnName);
        }
      }
    }

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

        rowId = (String) cvDataTableVal.get(DataTableColumns.ID);
        if (rowId == null) {
          throw new IllegalArgumentException(
              DataTableColumns.ID + ", if specified, cannot be null");
        }

        if (specifiesConflictType) {
          if (nullConflictType) {
            whereClause = K_DATATABLE_ID_EQUALS_PARAM + S_AND + DataTableColumns.CONFLICT_TYPE
                + S_IS_NULL;
            whereArgs[0] = rowId;
          } else {
            whereClause =
                K_DATATABLE_ID_EQUALS_PARAM + S_AND + DataTableColumns.CONFLICT_TYPE + S_EQUALS_PARAM;
            whereArgs[0] = rowId;
            whereArgs[1] = cvValues.get(DataTableColumns.CONFLICT_TYPE);
          }
        } else {
          whereClause = K_DATATABLE_ID_EQUALS_PARAM;
          whereArgs[0] = rowId;
        }

        AccessContext accessContext = getAccessContext(db, tableId, activeUser,
            RoleConsts.ADMIN_ROLES_LIST);

        StringBuilder b = new StringBuilder();
        b.append(K_SELECT_FROM).append(tableId).append(K_WHERE).append(whereClause);
        BaseTable data = privilegedQuery(db, tableId, b.toString(), whereArgs, null, accessContext);

        // There must be only one row in the db for the update to work
        if (shouldUpdate) {
          if (data.getNumberOfRows() == 1) {
            int filterTypeCursorIndex = data.getColumnIndexOfElementKey(DataTableColumns.FILTER_TYPE);
            priorFilterType = data.getRowAtIndex(0).getDataByIndex(filterTypeCursorIndex);
            if (priorFilterType == null) {
              priorFilterType = DataTableColumns.DEFAULT_FILTER_TYPE;
            }
            int filterValueCursorIndex = data.getColumnIndexOfElementKey(DataTableColumns.FILTER_VALUE);
            priorFilterValue =  data.getRowAtIndex(0).getDataByIndex(filterValueCursorIndex);
            int syncStateCursorIndex = data.getColumnIndexOfElementKey(DataTableColumns.SYNC_STATE);
            updatedSyncState = data.getRowAtIndex(0).getDataByIndex(syncStateCursorIndex);

            if (updatedSyncState.equals(SyncState.deleted.name()) || updatedSyncState
                .equals(SyncState.in_conflict.name())) {
              throw new IllegalStateException(
                  t + ": Cannot update a deleted or in-conflict row");
            } else if (updatedSyncState.equals(SyncState.synced.name()) || updatedSyncState
                .equals(SyncState.synced_pending_files.name())) {
              updatedSyncState = SyncState.changed.name();
            }
            update = true;
          } else if (data.getNumberOfRows() > 1) {
            throw new IllegalArgumentException(
                t + ": row id " + rowId + " has more than 1 row in table " + tableId);
          }
        } else {
          if (data.getNumberOfRows() > 0) {
            throw new IllegalArgumentException(
                t + ": row id " + rowId + " is already present in table " + tableId);
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

      List<String> rolesArray = getRolesArray(rolesList);

      // get the security settings
      TableSecuritySettings tss = getTableSecuritySettings(db, tableId);

      if (!asServerRequestedChange) {
        // do not allow filterType or filterValue to be modified in normal workflow
        if (cvDataTableVal.containsKey(DataTableColumns.FILTER_TYPE) || cvDataTableVal
            .containsKey(DataTableColumns.FILTER_VALUE)) {

          tss.canModifyFilterTypeAndValue(rolesArray);
        }
      }

      if (update) {

        // MODIFYING

        if (!cvDataTableVal.containsKey(DataTableColumns.SYNC_STATE) || (
            cvDataTableVal.get(DataTableColumns.SYNC_STATE) == null)) {
          cvDataTableVal.put(DataTableColumns.SYNC_STATE, updatedSyncState);
        }

        if (!asServerRequestedChange) {

          // apply row access restrictions
          // this will throw an IllegalArgumentException
          tss.allowRowChange(activeUser, rolesArray, updatedSyncState, priorFilterType,
              priorFilterValue, RowChange.CHANGE_ROW);

        }

        if (cvDataTableVal.containsKey(DataTableColumns.LOCALE) && (
            cvDataTableVal.get(DataTableColumns.LOCALE) == null)) {
          cvDataTableVal.put(DataTableColumns.LOCALE, locale);
        }

        if (cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE) && (
            cvDataTableVal.get(DataTableColumns.SAVEPOINT_TYPE) == null)) {
          cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)
            || cvDataTableVal.get(DataTableColumns.SAVEPOINT_TIMESTAMP) == null) {
          String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
          cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR) || (
            cvDataTableVal.get(DataTableColumns.SAVEPOINT_CREATOR) == null)) {
          cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR, activeUser);
        }
      } else {

        // INSERTING

        if (!cvDataTableVal.containsKey(DataTableColumns.ROW_ETAG)
            || cvDataTableVal.get(DataTableColumns.ROW_ETAG) == null) {
          cvDataTableVal.put(DataTableColumns.ROW_ETAG, DataTableColumns.DEFAULT_ROW_ETAG);
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.SYNC_STATE) || (
            cvDataTableVal.get(DataTableColumns.SYNC_STATE) == null)) {
          cvDataTableVal.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.CONFLICT_TYPE)) {
          cvDataTableVal.put(DataTableColumns.CONFLICT_TYPE, null);
        }

        if (!asServerRequestedChange) {

          cvDataTableVal.put(DataTableColumns.FILTER_TYPE, tss.filterTypeOnCreation);

          // activeUser
          cvDataTableVal.put(DataTableColumns.FILTER_VALUE, activeUser);

          tss.allowRowChange(activeUser, rolesArray, updatedSyncState, priorFilterType,
              priorFilterValue, RowChange.NEW_ROW);
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.FORM_ID)) {
          cvDataTableVal.put(DataTableColumns.FORM_ID, null);
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.LOCALE) || (
            cvDataTableVal.get(DataTableColumns.LOCALE) == null)) {
          cvDataTableVal.put(DataTableColumns.LOCALE, locale);
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TYPE) || (
            cvDataTableVal.get(DataTableColumns.SAVEPOINT_TYPE) == null)) {
          cvDataTableVal.put(DataTableColumns.SAVEPOINT_TYPE, SavepointTypeManipulator.complete());
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_TIMESTAMP)
            || cvDataTableVal.get(DataTableColumns.SAVEPOINT_TIMESTAMP) == null) {
          String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
          cvDataTableVal.put(DataTableColumns.SAVEPOINT_TIMESTAMP, timeStamp);
        }

        if (!cvDataTableVal.containsKey(DataTableColumns.SAVEPOINT_CREATOR) || (
            cvDataTableVal.get(DataTableColumns.SAVEPOINT_CREATOR) == null)) {
          cvDataTableVal.put(DataTableColumns.SAVEPOINT_CREATOR, activeUser);
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
   * @return true if rowId exists. False otherwise.
   */
  public boolean privilegedUpdateRowETagAndSyncState(OdkConnectionInterface db, String tableId,
      String rowId, String rowETag, SyncState state, String activeUser) {

    String whereClause = K_DATATABLE_ID_EQUALS_PARAM;
    Object[] whereArgs = { rowId };

    TreeMap<String,Object> cvDataTableVal = new TreeMap<String,Object>();

    cvDataTableVal.put(DataTableColumns.ROW_ETAG, rowETag);
    cvDataTableVal.put(DataTableColumns.SYNC_STATE, state.name());

    boolean dbWithinTransaction = db.inTransaction();
    try {
      if (!dbWithinTransaction) {
        db.beginTransactionNonExclusive();
      }

      AccessContext accessContext = getAccessContext(db, tableId, activeUser,
          RoleConsts.ADMIN_ROLES_LIST);

      StringBuilder b = new StringBuilder();
      b.append(K_SELECT_FROM).append(tableId).append(K_WHERE).append(whereClause);
      BaseTable data = privilegedQuery(db, tableId, b.toString(), whereArgs, null, accessContext);

      // There must be only one row in the db
      if (data.getNumberOfRows() != 1) {
        return false;
      }

      db.update(tableId, cvDataTableVal, whereClause, whereArgs);

      if (!dbWithinTransaction) {
        db.setTransactionSuccessful();
      }
      return true;
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
  private void cleanUpValuesMap(OrderedColumns orderedColumns, Map<String,Object> values) {

    TreeMap<String, String> toBeResolved = new TreeMap<String, String>();

    for (String key : values.keySet()) {
      if (DataTableColumns.CONFLICT_TYPE.equals(key)) {
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
        toBeResolved.put(key, (String) values.get(key));
      }
    }

    // remove these non-retained values from the values set...
    for (String key : toBeResolved.keySet()) {
      values.remove(key);
    }

    while (!toBeResolved.isEmpty()) {

      TreeMap<String, String> moreToResolve = new TreeMap<String, String>();

      for (TreeMap.Entry<String, String> entry : toBeResolved.entrySet()) {
        String key = entry.getKey();
        String json = entry.getValue();
        if (json == null) {
          // don't need to do anything
          // since the value is null
          continue;
        }
        ColumnDefinition cp = orderedColumns.find(key);
        try {
          TypeReference<Map<String, Object>> reference = new TypeReference<Map<String, Object>>() {
          };
          Map<String, Object> struct = ODKFileUtils.mapper.readValue(json, reference);
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
    WebLogger.getLogger(db.getAppName())
        .i("commonTableDefn", DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);
    db.execSQL(
        ColumnDefinitionsColumns.getTableCreateSql(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME),
        null);
    WebLogger.getLogger(db.getAppName())
        .i("commonTableDefn", DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);
    db.execSQL(
        KeyValueStoreColumns.getTableCreateSql(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME),
        null);
    WebLogger.getLogger(db.getAppName())
        .i("commonTableDefn", DatabaseConstants.TABLE_DEFS_TABLE_NAME);
    db.execSQL(TableDefinitionsColumns.getTableCreateSql(DatabaseConstants.TABLE_DEFS_TABLE_NAME),
        null);
    WebLogger.getLogger(db.getAppName())
        .i("commonTableDefn", DatabaseConstants.SYNC_ETAGS_TABLE_NAME);
    db.execSQL(SyncETagColumns.getTableCreateSql(DatabaseConstants.SYNC_ETAGS_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName())
        .i("commonTableDefn", DatabaseConstants.CHOICE_LIST_TABLE_NAME);
    db.execSQL(ChoiceListColumns.getTableCreateSql(DatabaseConstants.CHOICE_LIST_TABLE_NAME), null);
    WebLogger.getLogger(db.getAppName()).i("commonTableDefn", "done");
  }
}
