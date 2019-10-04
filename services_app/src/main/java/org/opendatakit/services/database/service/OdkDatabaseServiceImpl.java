/*
 * Copyright (C) 2017 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.services.database.service;

import android.content.ContentValues;
import android.content.Context;

import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.database.RoleConsts;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TableDefinitionEntry;
import org.opendatakit.database.data.TableMetaDataEntries;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.database.queries.QueryBounds;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.service.InternalUserDbInterface;
import org.opendatakit.database.service.TableHealthInfo;
import org.opendatakit.database.service.TableHealthStatus;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;
import org.opendatakit.services.database.utilities.ProviderUtils;
import org.opendatakit.services.database.utilities.SyncETagsUtils;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Extraction of database layer API prior to chunking enforcement.
 * Useful in webservice abstraction.
 *
 * @author mitchellsundt@gmail.com
 */

public final class OdkDatabaseServiceImpl implements InternalUserDbInterface {

   private Context context;

   public OdkDatabaseServiceImpl(Context context) {
      this.context = context;
   }

   private String getInternalDefaultGroup(String appName) {
      PropertiesSingleton props =
          CommonToolProperties.get(context, appName);
      String value = props.getProperty(CommonToolProperties.KEY_DEFAULT_GROUP);
      if ( value != null && value.length() == 0 ) {
         return null;
      } else {
         return value;
      }
   }

   private String getInternalRolesList(String appName) {
      PropertiesSingleton props =
          CommonToolProperties.get(context, appName);
      String value = props.getProperty(CommonToolProperties.KEY_ROLES_LIST);
      if ( value != null && value.length() == 0 ) {
         return null;
      } else {
         return value;
      }
   }

   private String getInternalUsersList(String appName) {
      PropertiesSingleton props =
          CommonToolProperties.get(context, appName);
      String value = props.getProperty(CommonToolProperties.KEY_USERS_LIST);
      if ( value != null && value.length() == 0 ) {
         return null;
      } else {
         return value;
      }
   }

   private String getUserSelectedDefaultLocale(String appName) {
      PropertiesSingleton props =
          CommonToolProperties.get(context, appName);
      return props.getUserSelectedDefaultLocale();
   }

   /**
   * Return the active user or "anonymous" if the user
   * has not been authenticated against the server.
   *
   * @param appName the app name
   *
   * @return the user reported from the server or "anonymous" if
   * server authentication has not been completed.
   */
  @Override
  public String getActiveUser(String appName) {
    PropertiesSingleton props =
        CommonToolProperties.get(context, appName);

    return ODKServicesPropertyUtils.getActiveUser(props);
  }

  /**
   * Return the roles of a verified username or google account.
   * If the username or google account have not been verified,
   * or if the server settings specify to use an anonymous user,
   * then return an empty string.
   *
   * @param appName the app name
   *
   * @return empty string or JSON serialization of an array of ROLES. See RoleConsts for possible values.
   */
   @Override public String getRolesList(String appName) {
      return getInternalRolesList(appName);
   }

  /**
   * Return the current user's default group.
   * This will be an empty string if the server does not support user-defined groups.
   *
   * @return empty string or the name of the default group.
   */
   @Override public String getDefaultGroup(String appName) {
      return getInternalDefaultGroup(appName);
   }

  /**
   * Return the users configured on the server if the current
   * user is verified to have Tables Super-user, Administer Tables or
   * Site Administrator roles. Otherwise, returns information about
   * the current user. If the user is syncing anonymously with the
   * server, this returns an empty string.
   *
   * @param appName the app name
   *
   * @return null or JSON serialization of an array of objects
   * structured as { "user_id": "...", "full_name": "...", "roles": ["...",...] }
   */
   @Override public String getUsersList(String appName) {
    // Realistically, each user would be no more than 500 bytes. 1Mb = 2000 users.
      return getInternalUsersList(appName);
   }

   @Override public DbHandle openDatabase(String appName) {

      OdkConnectionInterface db = null;

      DbHandle dbHandleName = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .generateDatabaseServiceDbHandle();
      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         return dbHandleName;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void closeDatabase(String appName, DbHandle dbHandleName) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         boolean first = true;
         while (db != null && db.isOpen() && db.inTransaction()) {
            if (!first) {
               WebLogger.getLogger(appName).e("closeDatabase",
                   appName + " " + dbHandleName.getDatabaseHandle() + " aborting transaction!");
            }
            first = false;
            // (presumably) abort the outstanding transaction
            db.endTransaction();
         }
      } finally {
         if (db != null) {
            try {
               // release the reference...
               // this will not close the db handle
               // the AppNameSharedStateContainer still holds a reference.
               db.releaseReference();
            } finally {
               // this will release the database from the AppNameSharedStateContainer...
               // this may also not close the connection -- it may be held open by a cursor
               // try {
               OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(appName,
                   dbHandleName);

               // }
               // Enabling dumpInfo will expose a bug in which
               // an open file handle on a directory that has been deleted
               // will prevent the directory to be created for the WebLogger
               // which will throw an exception
               // finally{
               //    OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().dumpInfo(true);
               // }
            }
         }
      }
   }

  /**
   * Create a local only table and prepend the given id with an "L_"
   *
   * @param appName the app name
   * @param dbHandleName a database handle to use
   * @param tableId the table to update
   * @param columns
   * @return
   */
   @Override public OrderedColumns createLocalOnlyTableWithColumns(String appName,
       DbHandle dbHandleName, String tableId, ColumnList columns)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         OrderedColumns results = ODKDatabaseImplUtils.get()
             .createLocalOnlyTableWithColumns(db, tableId, columns.getColumns());

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  /**
   * Drop the given local only table
   *
   * @param appName the app name
   * @param dbHandleName a database handle to use
   * @param tableId the table to update
   */
   @Override public void deleteLocalOnlyTable(String appName, DbHandle dbHandleName, String tableId)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().deleteLocalOnlyTable(db, tableId);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  /**
   * Insert a row into a local only table
   *
   * @param appName the app name
   * @param dbHandleName a database handle to use
   * @param tableId the table to update
   * @param rowValues
   * @throws ActionNotAuthorizedException
   */
   @Override public void insertLocalOnlyRow(String appName, DbHandle dbHandleName, String tableId,
       ContentValues rowValues) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().insertLocalOnlyRow(db, tableId, rowValues);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  /**
   * Update a row in a local only table
   *
   * @param appName the app name
   * @param dbHandleName a database handle to use
   * @param tableId the table to update
   * @param rowValues
   * @param whereClause
   * @param bindArgs
   * @throws ActionNotAuthorizedException
   */
   @Override public void updateLocalOnlyRows(String appName, DbHandle dbHandleName, String tableId,
       ContentValues rowValues, String whereClause, BindArgs bindArgs)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get()
             .updateLocalOnlyRow(db, tableId, rowValues, whereClause,
                 (bindArgs == null) ? null : bindArgs.bindArgs);

      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  /**
   * Delete a row in a local only table
   *
   * @param appName the app name
   * @param dbHandleName a database handle to use
   * @param tableId the table to update
   * @param whereClause
   * @param bindArgs
   * @throws ActionNotAuthorizedException
   */
   @Override public void deleteLocalOnlyRows(String appName, DbHandle dbHandleName, String tableId,
       String whereClause, BindArgs bindArgs) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get()
             .deleteLocalOnlyRow(db, tableId, whereClause,
                 (bindArgs == null) ? null : bindArgs.bindArgs);

      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  /**
   * SYNC Only. ADMIN Privileges
   *
   * @param appName the app name
   * @param dbHandleName a database handle to use
   * @param tableId the table to update
   * @param schemaETag
   * @param tableInstanceFilesUri
     */
   @Override public void privilegedServerTableSchemaETagChanged(String appName,
       DbHandle dbHandleName, String tableId, String schemaETag, String tableInstanceFilesUri)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().serverTableSchemaETagChanged(db, tableId, schemaETag,
             tableInstanceFilesUri);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  /**
   * Compute the app-global choiceListId for this choiceListJSON
   * and register the tuple of (choiceListId, choiceListJSON).
   * Return choiceListId.
   *
   * @param appName -- application name
   * @param dbHandleName -- database handle
   * @param choiceListJSON -- the actual JSON choice list text.
   * @return choiceListId -- the unique code mapping to the choiceListJSON
   */
   @Override public String setChoiceList(String appName, DbHandle dbHandleName,
       String choiceListJSON) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         return ODKDatabaseImplUtils.get().setChoiceList(db, choiceListJSON);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  /**
   * Return the choice list JSON corresponding to the choiceListId
   *
   * @param appName -- application name
   * @param dbHandleName -- database handle
   * @param choiceListId -- the md5 hash of the choiceListJSON
   * @return choiceListJSON -- the actual JSON choice list text.
   */
   @Override public String getChoiceList(String appName, DbHandle dbHandleName, String choiceListId)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         return ODKDatabaseImplUtils.get().getChoiceList(db, choiceListId);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public OrderedColumns createOrOpenTableWithColumns(String appName,
       DbHandle dbHandleName, String tableId, ColumnList columns)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         OrderedColumns results = ODKDatabaseImplUtils.get()
             .createOrOpenTableWithColumns(db, tableId, columns.getColumns());

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public OrderedColumns createOrOpenTableWithColumnsAndProperties(String appName,
       DbHandle dbHandleName, String tableId, ColumnList columns, List<KeyValueStoreEntry> metaData,
       boolean clear) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         OrderedColumns results =
             ODKDatabaseImplUtils.get()
                 .createOrOpenTableWithColumnsAndProperties(db, tableId, columns.getColumns(),
                     metaData, clear);

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable deleteAllCheckpointRowsWithId(String appName, DbHandle dbHandleName,
       String tableId, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         ODKDatabaseImplUtils.get().deleteAllCheckpointRowsWithId(db, tableId, rowId,
             activeUser, rolesList);
         BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable deleteLastCheckpointRowWithId(String appName, DbHandle dbHandleName,
       String tableId, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         ODKDatabaseImplUtils.get().deleteLastCheckpointRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void deleteTableAndAllData(String appName, DbHandle dbHandleName,
       String tableId) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().deleteTableAndAllData(db, tableId);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }

      ProviderUtils.notifyTablesProviderListener(context, appName, tableId);
   }

  @Override public boolean rescanTableFormDefs(String appName, DbHandle dbHandleName,
                                              String tableId) {

    OdkConnectionInterface db = null;

    boolean outcome = false;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      outcome = ODKDatabaseImplUtils.get().rescanTableFormDefs(db, tableId);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    ProviderUtils.notifyFormsProviderListener(context, appName, tableId);
    return outcome;
  }

   @Override public void deleteTableMetadata(String appName, DbHandle dbHandleName, String tableId,
       String partition, String aspect, String key) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().deleteTableMetadata(db, tableId, partition, aspect, key);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable deleteRowWithId(String appName, DbHandle dbHandleName, String tableId,
       String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         ODKDatabaseImplUtils.get().deleteRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  /**
   * SYNC Only. ADMIN Privileges!
   *
   * @param appName the app name
   * @param dbHandleName a database handle to use
   * @param tableId the table to update
   * @param rowId which row in the table to update
   * @return
     */
   @Override public BaseTable privilegedDeleteRowWithId(String appName, DbHandle dbHandleName,
       String tableId, String rowId)
       {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         ODKDatabaseImplUtils.get().privilegedDeleteRowWithId(db, tableId, rowId, activeUser);
         BaseTable t = ODKDatabaseImplUtils.get().privilegedGetMostRecentRowWithId(db, tableId,
             rowId, activeUser);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public String[] getAdminColumns() {

      List<String> cols = ODKDatabaseImplUtils.get().getAdminColumns();
      String[] results = cols.toArray(new String[cols.size()]);

      return results;
   }

   @Override public String[] getAllColumnNames(String appName, DbHandle dbHandleName,
       String tableId) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         String[] results =  ODKDatabaseImplUtils.get().getAllColumnNames(db, tableId);

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public List<String> getAllTableIds(String appName, DbHandle dbHandleName)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         List<String> results = ODKDatabaseImplUtils.get().getAllTableIds(db);

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable getRowsWithId(String appName, DbHandle dbHandleName, String tableId,
       String rowId) {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         BaseTable results = ODKDatabaseImplUtils.get()
             .getRowsWithId(db, tableId, rowId, activeUser, rolesList);

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable privilegedGetRowsWithId(String appName, DbHandle dbHandleName,
       String tableId, String rowId)
       {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         BaseTable results = ODKDatabaseImplUtils.get()
             .privilegedGetRowsWithId(db, tableId, rowId, activeUser);

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable getMostRecentRowWithId(String appName, DbHandle dbHandleName,
       String tableId, String rowId) {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         BaseTable results = ODKDatabaseImplUtils.get()
             .getMostRecentRowWithId(db, tableId, rowId, activeUser, rolesList);

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

  @Override public TableMetaDataEntries getTableMetadata(String appName,
      DbHandle dbHandleName, String tableId, String partition, String aspect, String key)
      {

    OdkConnectionInterface db = null;

    TableMetaDataEntries kvsEntries = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      kvsEntries = ODKDatabaseImplUtils.get()
          .getTableMetadata(db, tableId, partition, aspect, key);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    return kvsEntries;
  }

  @Override
  public TableMetaDataEntries getTableMetadataIfChanged(String appName, DbHandle dbHandleName, String tableId,
      String revId) {

    OdkConnectionInterface db = null;

    TableMetaDataEntries kvsEntries = null;
    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(appName, dbHandleName);
      String currentRevId = ODKDatabaseImplUtils.get().getTableDefinitionRevId(db, tableId);
      if (revId != null && revId.equals(currentRevId)) {
        kvsEntries = new TableMetaDataEntries(tableId, revId);
      } else {
        kvsEntries = ODKDatabaseImplUtils.get().getTableMetadata(db, tableId, null, null, null);
      }
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    return kvsEntries;
  }

   @Override public TableHealthInfo getTableHealthStatus(String appName, DbHandle dbHandleName,
       String tableId) {

     long now = System.currentTimeMillis();
     WebLogger.getLogger(appName).i("getTableHealthStatus",
         appName + " " + dbHandleName.getDatabaseHandle() + " " + tableId + " "
             + "getTableHealthStatus -- searching for conflicts and checkpoints ");

     TableHealthInfo  healthInfo;
     OdkConnectionInterface db = null;
     TableHealthStatus status = TableHealthStatus.TABLE_HEALTH_IS_CLEAN;

     try {
       // +1 referenceCount if db is returned (non-null)
       db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
           .getConnection(appName, dbHandleName);

       int health = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);
       boolean hasConflicts = CursorUtils.getTableHealthHasConflicts(health);
       boolean hasCheckpoints = CursorUtils.getTableHealthHasCheckpoints(health);
       boolean hasChanges = CursorUtils.getTableHealthHasChanges(health);

       if (hasCheckpoints && hasConflicts) {
         status = TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS;
       } else if (hasCheckpoints) {
         status = TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS;
       } else if (hasConflicts) {
         status = TableHealthStatus.TABLE_HEALTH_HAS_CONFLICTS;
       }

       healthInfo = new TableHealthInfo(tableId, status, hasChanges);

       long elapsed = System.currentTimeMillis() - now;
       WebLogger.getLogger(appName).i("getTableHealthStatus",
           appName + " " + dbHandleName.getDatabaseHandle() + " " + tableId + " "
               + "getTableHealthStatus -- full table scan completed: " + Long.toString(elapsed)
               + " ms");

       return healthInfo;
     } finally {
       if (db != null) {
         // release the reference...
         // this does not necessarily close the db handle
         // or terminate any pending transaction
         db.releaseReference();
       }
     }
   }

   @Override public ArrayList<TableHealthInfo> getTableHealthStatuses(String appName,
       DbHandle dbHandleName) {

      long now = System.currentTimeMillis();
      WebLogger.getLogger(appName)
          .i("getTableHealthStatuses", appName + " " + dbHandleName.getDatabaseHandle() + " " +
              "getTableHealthStatuses -- searching for conflicts and checkpoints ");

      ArrayList<TableHealthInfo> problems = new ArrayList<>();
      ArrayList<String> tableIds = null;
      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }

     for (String tableId : tableIds) {
       TableHealthInfo info = getTableHealthStatus(appName, dbHandleName, tableId);
       problems.add(info);
     }

     long elapsed = System.currentTimeMillis() - now;
     WebLogger.getLogger(appName)
         .i("getTableHealthStatuses", appName + " " + dbHandleName.getDatabaseHandle() + " " +
             "getTableHealthStatuses -- full table scan completed: " + Long.toString(elapsed)
             + " ms");

     return problems;
   }

   @Override public String[] getExportColumns() {
      List<String> exports = ODKDatabaseImplUtils.get().getExportColumns();
      String[] results =  exports.toArray(new String[exports.size()]);

      return results;
   }

   @Override public String getSyncState(String appName, DbHandle dbHandleName, String tableId,
       String rowId) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         SyncState state = ODKDatabaseImplUtils.get().getSyncState(db, tableId, rowId);
         return state.name();
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public TableDefinitionEntry getTableDefinitionEntry(String appName,
       DbHandle dbHandleName, String tableId) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         TableDefinitionEntry results = ODKDatabaseImplUtils.get()
             .getTableDefinitionEntry(db, tableId);

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public OrderedColumns getUserDefinedColumns(String appName, DbHandle dbHandleName,
       String tableId) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         OrderedColumns results = ODKDatabaseImplUtils.get()
             .getUserDefinedColumns(db, tableId);

         return results;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public boolean hasTableId(String appName, DbHandle dbHandleName, String tableId)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         return ODKDatabaseImplUtils.get().hasTableId(db, tableId);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable insertCheckpointRowWithId(String appName, DbHandle dbHandleName,
       String tableId, ContentValues cvValues, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);
      String userSelectedDefaultLocale = getUserSelectedDefaultLocale(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
             .getUserDefinedColumns(db, tableId);
         ODKDatabaseImplUtils.get()
             .insertCheckpointRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser,
                 rolesList, userSelectedDefaultLocale);
         BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable insertRowWithId(String appName, DbHandle dbHandleName, String tableId,
       ContentValues cvValues, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);
      String userSelectedDefaultLocale = getUserSelectedDefaultLocale(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
             .getUserDefinedColumns(db, tableId);
         ODKDatabaseImplUtils.get()
             .insertRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser, rolesList,
                 userSelectedDefaultLocale);
         BaseTable t = ODKDatabaseImplUtils.get()
             .getMostRecentRowWithId(db, tableId, rowId, activeUser, rolesList);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   /**
    * SYNC Only. ADMIN Privileges!
    *
    * @param appName the app name
    * @param dbHandleName a database handle to use
    * @param tableId the table to update
    * @param cvValues
    * @param rowId which row in the table to update
    * @param asCsvRequestedChange
    * @return
    */
   @Override public BaseTable privilegedInsertRowWithId(String appName, DbHandle dbHandleName,
       String tableId, ContentValues cvValues, String rowId,
       boolean asCsvRequestedChange) {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String userSelectedDefaultLocale = getUserSelectedDefaultLocale(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
             .getUserDefinedColumns(db, tableId);
         ODKDatabaseImplUtils.get()
             .privilegedInsertRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser,
                 userSelectedDefaultLocale, asCsvRequestedChange);
         BaseTable t = ODKDatabaseImplUtils.get().privilegedGetMostRecentRowWithId(db, tableId,
             rowId, activeUser);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   /**
    * SYNC Only. ADMIN Privileges!
    *
    * @param appName the app name
    * @param dbHandleName a database handle to use
    * @param tableId the table to update
    * @param cvValues  server's field values for this row
    * @param rowId which row in the table to update
    *          expected to be one of ConflictType.LOCAL_DELETED_OLD_VALUES (0) or
    * @return The updated row, in a table
    */
   @Override public BaseTable privilegedPerhapsPlaceRowIntoConflictWithId(String appName,
       DbHandle dbHandleName, String tableId, ContentValues cvValues,
       String rowId) {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getRolesList(appName);
      String userSelectedDefaultLocale = getUserSelectedDefaultLocale(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
             .getUserDefinedColumns(db, tableId);
         ODKDatabaseImplUtils.get()
             .privilegedPerhapsPlaceRowIntoConflictWithId(db, tableId, orderedColumns, cvValues, rowId,
                 activeUser, rolesList, userSelectedDefaultLocale);
         BaseTable t = ODKDatabaseImplUtils.get().privilegedGetRowsWithId(db, tableId,
             rowId, activeUser);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable simpleQuery(String appName, DbHandle dbHandleName, String sqlCommand,
                                          BindArgs bindArgs, QueryBounds sqlQueryBounds, String tableId)
       {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);


         ODKDatabaseImplUtils.AccessContext accessContext =
             ODKDatabaseImplUtils.get().getAccessContext(db, tableId, activeUser, rolesList);

         BaseTable result = ODKDatabaseImplUtils.get()
             .query(db, tableId, sqlCommand,
                 (bindArgs == null) ? null : bindArgs.bindArgs, sqlQueryBounds, accessContext);

         return result;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable privilegedSimpleQuery(String appName, DbHandle dbHandleName,
       String sqlCommand, BindArgs bindArgs, QueryBounds sqlQueryBounds, String tableId)
       {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);

         ODKDatabaseImplUtils.AccessContext accessContext = ODKDatabaseImplUtils.get()
             .getAccessContext(db, tableId, activeUser, RoleConsts.ADMIN_ROLES_LIST);

         BaseTable result = ODKDatabaseImplUtils.get()
             .privilegedQuery(db, tableId, sqlCommand,
                 (bindArgs == null) ? null : bindArgs.bindArgs, sqlQueryBounds,
                 accessContext);

         return result;
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void privilegedExecute(String appName, DbHandle dbHandleName, String sqlCommand,
       BindArgs bindArgs) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);

         ODKDatabaseImplUtils.get()
             .privilegedExecute(db, sqlCommand, (bindArgs == null) ? null : bindArgs.bindArgs);

      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void replaceTableMetadata(String appName, DbHandle dbHandleName,
       KeyValueStoreEntry entry) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().replaceTableMetadata(db, entry);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void replaceTableMetadataList(String appName, DbHandle dbHandleName,
       String tableId, List<KeyValueStoreEntry> entries, boolean clear)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().replaceTableMetadata(db, tableId, entries, clear);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void replaceTableMetadataSubList(String appName, DbHandle dbHandleName,
       String tableId, String partition, String aspect, List<KeyValueStoreEntry> entries)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get()
             .replaceTableMetadataSubList(db, tableId, partition, aspect, entries);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable saveAsIncompleteMostRecentCheckpointRowWithId(String appName,
       DbHandle dbHandleName, String tableId, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         ODKDatabaseImplUtils.get().saveAsIncompleteMostRecentCheckpointRowWithId(db, tableId,
             rowId);
         BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable saveAsCompleteMostRecentCheckpointRowWithId(String appName,
       DbHandle dbHandleName, String tableId, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         ODKDatabaseImplUtils.get().saveAsCompleteMostRecentCheckpointRowWithId(db, tableId, rowId);
         BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   /**
    * SYNC Only. ADMIN Privileges
    *
    * @param appName the app name
    * @param dbHandleName a database handle to use
    * @param tableId the table to update
    * @param schemaETag TODO what?
    * @param lastDataETag TODO what?
    */
   @Override public void privilegedUpdateTableETags(String appName, DbHandle dbHandleName,
       String tableId, String schemaETag, String lastDataETag)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().privilegedUpdateTableETags(db, tableId, schemaETag,
             lastDataETag);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   /**
    * SYNC Only. ADMIN Privileges
    *
    * @param appName the app name
    * @param dbHandleName a database handle to use
    * @param tableId the table to update
    */
   @Override public void privilegedUpdateTableLastSyncTime(String appName, DbHandle dbHandleName,
       String tableId) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         ODKDatabaseImplUtils.get().privilegedUpdateTableLastSyncTime(db, tableId);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public BaseTable updateRowWithId(String appName, DbHandle dbHandleName, String tableId,
       ContentValues cvValues, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);
      String userSelectedDefaultLocale = getUserSelectedDefaultLocale(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         db.beginTransactionExclusive();
         OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
             .getUserDefinedColumns(db, tableId);
         ODKDatabaseImplUtils.get()
             .updateRowWithId(db, tableId, orderedColumns, cvValues, rowId, activeUser, rolesList,
                 userSelectedDefaultLocale);
         BaseTable t = ODKDatabaseImplUtils.get().getMostRecentRowWithId(db, tableId, rowId,
             activeUser, rolesList);
         db.setTransactionSuccessful();
         return t;
      } finally {
         if (db != null) {
            db.endTransaction();
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void resolveServerConflictWithDeleteRowWithId(String appName,
       DbHandle dbHandleName, String tableId, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);

         ODKDatabaseImplUtils.get()
             .resolveServerConflictWithDeleteRowWithId(db, tableId, rowId,
                 activeUser);

      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void resolveServerConflictTakeLocalRowWithId(String appName,
       DbHandle dbHandleName, String tableId, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);
      String userSelectedDefaultLocale = getUserSelectedDefaultLocale(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);

         ODKDatabaseImplUtils.get()
             .resolveServerConflictTakeLocalRowWithId(db, tableId, rowId, activeUser, rolesList,
                 userSelectedDefaultLocale);

      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void resolveServerConflictTakeLocalRowPlusServerDeltasWithId(String appName,
       DbHandle dbHandleName, String tableId, ContentValues cvValues, String rowId)
       throws ActionNotAuthorizedException {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String rolesList = getInternalRolesList(appName);
      String userSelectedDefaultLocale = getUserSelectedDefaultLocale(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);

         ODKDatabaseImplUtils.get()
             .resolveServerConflictTakeLocalRowPlusServerDeltasWithId(db, tableId, cvValues,
                 rowId, activeUser, rolesList, userSelectedDefaultLocale);

      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void resolveServerConflictTakeServerRowWithId(String appName,
       DbHandle dbHandleName, String tableId, String rowId) {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);
      String userSelectedDefaultLocale = getUserSelectedDefaultLocale(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);

         // regardless of the roles available to the user, act as god.
         ODKDatabaseImplUtils.get()
             .resolveServerConflictTakeServerRowWithId(db, tableId, rowId, activeUser,
                 userSelectedDefaultLocale);

      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   /**
    * SYNC Only. ADMIN Privileges!
    *
    * @param appName the app name
    * @param dbHandleName a database handle to use
    * @param tableId the table to update
    * @param rowId which row in the table to update
    * @param rowETag The new etag for the row
    * @param syncState - the SyncState.name()
    */
   @Override public void privilegedUpdateRowETagAndSyncState(String appName, DbHandle dbHandleName,
       String tableId, String rowId, String rowETag, String syncState)
       {

      OdkConnectionInterface db = null;

      String activeUser = getActiveUser(appName);

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         if ( !ODKDatabaseImplUtils.get().privilegedUpdateRowETagAndSyncState(db, tableId, rowId, rowETag,
             SyncState.valueOf(syncState), activeUser) ) {
            throw new IllegalArgumentException(
                "row id " + rowId + " does not have exactly 1 row in table " + tableId);
         }
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void deleteAppAndTableLevelManifestSyncETags(String appName,
       DbHandle dbHandleName) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         SyncETagsUtils.deleteAppAndTableLevelManifestSyncETags(db);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void deleteAllSyncETagsForTableId(String appName, DbHandle dbHandleName,
       String tableId) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         SyncETagsUtils.deleteAllSyncETagsForTableId(db, tableId);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void deleteAllSyncETagsExceptForServer(String appName, DbHandle dbHandleName,
       String verifiedUri) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         SyncETagsUtils.deleteAllSyncETagsExceptForServer(db, verifiedUri);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void deleteAllSyncETagsUnderServer(String appName, DbHandle dbHandleName,
       String verifiedUri) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         SyncETagsUtils.deleteAllSyncETagsUnderServer(db, verifiedUri);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public String getFileSyncETag(String appName, DbHandle dbHandleName,
       String verifiedUri, String tableId, long modificationTimestamp)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         return SyncETagsUtils.getFileSyncETag(db, verifiedUri, tableId, modificationTimestamp);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public String getManifestSyncETag(String appName, DbHandle dbHandleName,
       String verifiedUri, String tableId) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         return SyncETagsUtils.getManifestSyncETag(db, verifiedUri, tableId);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void updateFileSyncETag(String appName, DbHandle dbHandleName,
       String verifiedUri, String tableId, long modificationTimestamp, String eTag)
       {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         SyncETagsUtils.updateFileSyncETag(db, verifiedUri, tableId, modificationTimestamp, eTag);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }

   @Override public void updateManifestSyncETag(String appName, DbHandle dbHandleName,
       String verifiedUri, String tableId, String eTag) {

      OdkConnectionInterface db = null;

      try {
         // +1 referenceCount if db is returned (non-null)
         db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
             .getConnection(appName, dbHandleName);
         SyncETagsUtils.updateManifestSyncETag(db, verifiedUri, tableId, eTag);
      } finally {
         if (db != null) {
            // release the reference...
            // this does not necessarily close the db handle
            // or terminate any pending transaction
            db.releaseReference();
         }
      }
   }
}
