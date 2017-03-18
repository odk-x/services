package org.opendatakit.database.service.test;

import android.content.ContentValues;
import android.content.Intent;
import android.os.IBinder;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.test.ServiceTestCase;
import android.util.Log;
import org.opendatakit.TestConsts;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.database.data.*;
import org.opendatakit.database.service.AidlDbInterface;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.service.UserDbInterface;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.service.OdkDatabaseService;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertNotEquals;

public class GroupsPermissionTest extends ServiceTestCase<OdkDatabaseService> {

   private boolean initialized = false;
   private static final String APPNAME = TestConsts.APPNAME;

   private static final String DB_TABLE_ID = "testtable";
   private static final String COL_STRING_ID = "columnString";
   private static final String COL_INTEGER_ID = "columnInteger";
   private static final String COL_NUMBER_ID = "columnNumber";
   private static final String TEST_STR_i = "TestStri";
   private static final int TEST_INT_i = 0;
   private static final double TEST_NUM_i = 0.1;

   private static final String TEST_GROUP_1 = "GROUP_ONE";
   private static final String TEST_USER_1 = "testUsr1@gmail.com";
   private static final String TEST_PWD_1 = "12345";
   private static final String TEST_USER_2 = "usr2@gmail.com";
   private static final String TEST_PWD_2 = "12345";
   private static final String TEST_GRP_2 = "GROUP_TWO";
   private static final String TEST_USER_3 = "usr3@gmail.com";
   private static final String TEST_PWD_3 = "12345";
   private static final String TEST_GRP_3 = "GROUP_THREE";


   private static final String TEST_STR_1 = "TestStr1";
   private static final int TEST_INT_1 = 1;
   private static final double TEST_NUM_1 = 1.1;

   private static final String FULL_PERMISSION_ROLES = "[\"ROLE_DATA_OWNER\",\"ROLE_USER\","
       + "\"ROLE_SITE_ACCESS_ADMIN\",\"ROLE_SYNCHRONIZE_TABLES\",\"ROLE_ADMINISTER_TABLES\",\"ROLE_DATA_VIEWER\",\"ROLE_SUPER_USER_TABLES\"]";

   private static final String LIMITED_PERMISSION_ROLES_2 = "[\"ROLE_USER\","
       + "\"ROLE_SYNCHRONIZE_TABLES\",\"" + TEST_GRP_2 + "\"]";

   private static final String LIMITED_PERMISSION_ROLES_3 = "[\"ROLE_USER\","
       + "\"ROLE_SYNCHRONIZE_TABLES\",\"" + TEST_GRP_3 + "\"]";

   public GroupsPermissionTest() {
      super(OdkDatabaseService.class);
   }

   public GroupsPermissionTest(Class<OdkDatabaseService> serviceClass) {
      super(serviceClass);
   }

   @Override protected void setUp() throws Exception {
      super.setUp();

      boolean beganUninitialized = !initialized;
      if (beganUninitialized) {
         initialized = true;
         // Used to ensure that the singleton has been initialized properly
         AndroidConnectFactory.configure();
      }
      setupService();
   }

   @Override protected void tearDown() throws Exception {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("GroupPermisisonTest", "tearDown: " + db.getDatabaseHandle());
         verifyNoTablesExistNCleanAllTables(serviceInterface, db);
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
      super.tearDown();
   }

   @Nullable private UserDbInterface bindToDbService() {
      Intent bind_intent = new Intent();
      bind_intent.setClass(getContext(), OdkDatabaseService.class);
      IBinder service = this.bindService(bind_intent);

      UserDbInterface dbInterface;
      try {
         dbInterface = new UserDbInterface(AidlDbInterface.Stub.asInterface(service));
      } catch (IllegalArgumentException e) {
         dbInterface = null;
      }
      return dbInterface;
   }

   @NonNull private List<Column> createColumnList() {
      List<Column> columns = new ArrayList<Column>();

      columns.add(new Column(COL_STRING_ID, "column String", "string", null));
      columns.add(new Column(COL_INTEGER_ID, "column Integer", "integer", null));
      columns.add(new Column(COL_NUMBER_ID, "column Number", "number", null));

      return columns;
   }

   private ContentValues contentValuesTestSeti(int i, String groupType, String groupsList) {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_i + i);
      cv.put(COL_INTEGER_ID, i);
      cv.put(COL_NUMBER_ID, TEST_NUM_i + i);
      cv.put(DataTableColumns.GROUP_TYPE, groupType);
      cv.put(DataTableColumns.GROUPS_LIST, groupsList);
      cv.putNull(DataTableColumns.FILTER_EXT);
      return cv;
   }

   private void verifyRowTestSeti(Row row, int i, String groupType, String groupsList) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_i + i);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Integer.toString(i));
      assertEquals(row.getDataByKey(COL_NUMBER_ID),
          Double.toString(TEST_NUM_i + i));
      assertEquals(groupType, row.getDataByKey(DataTableColumns.GROUP_TYPE));
      assertEquals(groupsList, row.getDataByKey(DataTableColumns.GROUPS_LIST));
   }



   private ContentValues contentValuesTestSet1() {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_1);
      cv.put(COL_INTEGER_ID, TEST_INT_1);
      cv.put(COL_NUMBER_ID, TEST_NUM_1);
      cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.Type.DEFAULT.name());
      cv.put(DataTableColumns.GROUPS_LIST, TEST_GROUP_1);
      cv.putNull(DataTableColumns.FILTER_EXT);

      return cv;
   }

   private void verifyRowTestSet1(Row row) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
      assertEquals(row.getDataByKey(COL_INTEGER_ID), Integer.toString(TEST_INT_1));
      assertEquals(row.getDataByKey(COL_NUMBER_ID), Double.toString(TEST_NUM_1));
      assertEquals(row.getDataByKey(DataTableColumns.GROUP_TYPE),
          RowFilterScope.Type.DEFAULT.name());
      assertEquals(TEST_GROUP_1, row.getDataByKey(DataTableColumns.GROUPS_LIST));
      assertNull(row.getDataByKey(DataTableColumns.FILTER_EXT));
   }

   private KeyValueStoreEntry lockTableProperty() {
      KeyValueStoreEntry prop = new KeyValueStoreEntry();
      prop.tableId = DB_TABLE_ID;
      prop.partition = "Table";
      prop.aspect = "security";
      prop.key = "locked";
      prop.type = "boolean";
      prop.value = "true";
      return prop;
   }

   private KeyValueStoreEntry noAnonCreationProperty() {
      KeyValueStoreEntry prop = new KeyValueStoreEntry();
      prop.tableId = DB_TABLE_ID;
      prop.partition = "Table";
      prop.aspect = "security";
      prop.key = "unverifiedUserCanCreate";
      prop.type = "boolean";
      prop.value = "false";
      return prop;
   }

   private KeyValueStoreEntry defaultPermissionReadOnlyProperty() {
      KeyValueStoreEntry prop = new KeyValueStoreEntry();
      prop.tableId = DB_TABLE_ID;
      prop.partition = "Table";
      prop.aspect = "security";
      prop.key = "filterTypeOnCreation";
      prop.type = "string";
      prop.value = RowFilterScope.Type.READ_ONLY.name();
      return prop;
   }

   private KeyValueStoreEntry defaultPermissionHiddenProperty() {
      KeyValueStoreEntry prop = new KeyValueStoreEntry();
      prop.tableId = DB_TABLE_ID;
      prop.partition = "Table";
      prop.aspect = "security";
      prop.key = "filterTypeOnCreation";
      prop.type = "string";
      prop.value = RowFilterScope.Type.HIDDEN.name();
      return prop;
   }

   private String getActiveUser(String appName) {
      PropertiesSingleton props = CommonToolProperties
          .get(getService().getApplicationContext(), appName);
      return props.getActiveUser();
   }

   private void setActiveUser(String activeUser, String password, String appName) {
      PropertiesSingleton props = CommonToolProperties
          .get(getService().getApplicationContext(), appName);
      // these are stored in devices
      props.setProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE, getContext()
          .getString(org.opendatakit.androidlibrary.R.string.credential_type_google_account));
      props.setProperty(CommonToolProperties.KEY_ACCOUNT, activeUser);
      // this is stored in SharedPreferences
      props.setProperty(CommonToolProperties.KEY_PASSWORD, password);
   }

   private void setFullRolePermission(String appName) {
      PropertiesSingleton props = CommonToolProperties
          .get(getService().getApplicationContext(), appName);
      props.setProperty(CommonToolProperties.KEY_ROLES_LIST, FULL_PERMISSION_ROLES);
   }

   private void setLimitedRolePermissionGroups3(String appName) {
      PropertiesSingleton props = CommonToolProperties
          .get(getService().getApplicationContext(), appName);
      props.setProperty(CommonToolProperties.KEY_ROLES_LIST, LIMITED_PERMISSION_ROLES_3);
   }

   private void setLimitedRolePermissionGroups2(String appName) {
      PropertiesSingleton props = CommonToolProperties
          .get(getService().getApplicationContext(), appName);
      props.setProperty(CommonToolProperties.KEY_ROLES_LIST, LIMITED_PERMISSION_ROLES_2);
   }

   private void switchToUser1() {
      setActiveUser(TEST_USER_1, TEST_PWD_1, APPNAME);
      String verifyName = getActiveUser(APPNAME);
      assertEquals("mailto:" + TEST_USER_1, verifyName);
      setFullRolePermission(APPNAME);
   }

   private void switchToUser2() {
      setActiveUser(TEST_USER_2, TEST_PWD_2, APPNAME);
      String verifyName = getActiveUser(APPNAME);
      assertEquals("mailto:" + TEST_USER_2, verifyName);
      setLimitedRolePermissionGroups2(APPNAME);
   }

   private void switchToUser3() {
      setActiveUser(TEST_USER_3, TEST_PWD_3, APPNAME);
      String verifyName = getActiveUser(APPNAME);
      assertEquals("mailto:" + TEST_USER_3, verifyName);
      setLimitedRolePermissionGroups3(APPNAME);
   }

   private boolean hasNoTablesInDb(UserDbInterface serviceInterface, DbHandle db)
       throws ServicesAvailabilityException {
      List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);
      return (tableIds.size() == 0);
   }

   private void verifyNoTablesExistNCleanAllTables(UserDbInterface serviceInterface, DbHandle db)
       throws ServicesAvailabilityException {
      switchToUser1();

      List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);

      // Drop any leftover table now that the test is done
      for (String id : tableIds) {
         serviceInterface.deleteTableAndAllData(APPNAME, db, id);
      }

      tableIds = serviceInterface.getAllTableIds(APPNAME, db);

      boolean tablesGone = (tableIds.size() == 0);

      assertTrue(tablesGone);
   }

   public void testDbInsertNDeleteSingleRowIntoTable() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         DbHandle db = serviceInterface.openDatabase(APPNAME);

         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testSetUser() throws ActionNotAuthorizedException {
      String testUserName = "test@gmail.com";
      setActiveUser(testUserName, "1235", APPNAME);
      String verifyName = getActiveUser(APPNAME);
      assertEquals("mailto:" + testUserName, verifyName);

      String testUserName2 = "test3@gmail.com";
      setActiveUser(testUserName2, "1235", APPNAME);
      verifyName = getActiveUser(APPNAME);
      assertNotEquals("mailto:" + testUserName, verifyName);

      setActiveUser(testUserName, "1235", APPNAME);
      verifyName = getActiveUser(APPNAME);
      assertEquals("mailto:" + testUserName, verifyName);
   }

   public void testUserAuthorizationSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1();

         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testUserAuthorizationFailure() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1();
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         switchToUser2();

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         fail("Should have thrown a ActionNotAuthorizedException");
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ActionNotAuthorizedException e) {
         // should fail based on security settings
      }

      try {
         switchToUser1();
         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }


      // NOTE: until the sync state is changed no permissions are enforced
      try {
         switchToUser2();
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());


         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      try {
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());
         fail("Should have thrown an ActionNotAuthorizedException");
      } catch (ActionNotAuthorizedException e) {
         // expect a permission exception
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      // clean up
      try {
         switchToUser1();
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         assertEquals(SyncState.deleted.name(), row.getDataByKey(DataTableColumns
             .SYNC_STATE));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testGroupAuthorizationUpdateRow_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1();

         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // change group
         ContentValues cv = new ContentValues();
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.DEFAULT.name());
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         switchToUser2();

         // verify user 2 can read
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(RowFilterScope.GroupType.DEFAULT.name(), row.getDataByKey(DataTableColumns
             .GROUP_TYPE));
         assertEquals(TEST_GRP_2, row.getDataByKey(DataTableColumns.GROUPS_LIST));

         // verify user 2 can change value
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // verify the change
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

         // delete row
         switchToUser1();
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(SyncState.deleted.name(), row.getDataByKey(DataTableColumns
             .SYNC_STATE));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testGroupAuthorizationFailureLocked_ExpectSomeFailures() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1();
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         switchToUser2();

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         fail("Should have thrown a ActionNotAuthorizedException");
      } catch (ActionNotAuthorizedException e) {
         // should fail based on security settings
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      try {
         switchToUser1();
         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }


      // NOTE: until the sync state is changed no permissions are enforced
      try {
         switchToUser2();
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());


         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      // even though user 2 was last change should lose permission when in sync'd state
      try {
         switchToUser2();
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());
         fail("Should have thrown an ActionNotAuthorizedException");
      } catch (ActionNotAuthorizedException e) {
         // expect a permission exception
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      // verify user 3 also fails
      try {
         switchToUser3();
         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());
         fail("Should have thrown an ActionNotAuthorizedException");
      }  catch (ActionNotAuthorizedException e) {
         // expect a permission exception
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      // NOTE: super user can change group
      try {
         switchToUser1();
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_3);
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.DEFAULT.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());


         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("3",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      // verify user 3 now succeeds with the correct groups
      try {
         switchToUser3();
         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 4);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         assertEquals("4",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_3, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }


      // clean up
      try {
         switchToUser1();
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         assertEquals(SyncState.deleted.name(), row.getDataByKey(DataTableColumns
             .SYNC_STATE));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testGroupAuthorizationFailureNotLocked_ExpectSomeFailures() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser2();
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);


         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);


         // NOTE: until the sync state is changed no permissions are enforced
         switchToUser1();
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // user two should still be able to see the row since they created it
         switchToUser2();

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // user three should not be able to see the row
         switchToUser3();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // change so that user 3 has group read access
         switchToUser1();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         cv = new ContentValues();
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_3);
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.READ_ONLY.name());
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // user three should NOW be able to see the row
         switchToUser3();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         // user three attempts to update a read only row
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());
         fail("Should have thrown an ActionNotAuthorizedException");
      } catch (ActionNotAuthorizedException e) {
         // expect a permission exception
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      // verify user 3 also fails
      try {
         // verify no changes
         switchToUser1();
         UserTable table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId
             .toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);
         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // change to allow user 3 to modify row, and lock user 2
         ContentValues cv = new ContentValues();
         cv.put(DataTableColumns.FILTER_VALUE, "mailto:" + TEST_USER_1);
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.MODIFY.name());
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // verify user 3 can update
         switchToUser3();
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId
             .toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);
         assertEquals("3",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_3, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // verify user 2 cannot update
         switchToUser2();
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 4);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());
         fail("Should have thrown an ActionNotAuthorizedException");
      }  catch (ActionNotAuthorizedException e) {
         // expect a permission exception
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }


      // clean up
      try {
         switchToUser1();
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         assertEquals(SyncState.deleted.name(), row.getDataByKey(DataTableColumns
             .SYNC_STATE));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testGroupAuthorizationViewHiddenRowLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1();

         db = serviceInterface.openDatabase(APPNAME);
         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // change group
         ContentValues cv = new ContentValues();
         cv.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.DEFAULT.name());
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         switchToUser2();

         // verify user 2 can read
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(RowFilterScope.GroupType.DEFAULT.name(), row.getDataByKey(DataTableColumns
             .GROUP_TYPE));
         assertEquals(TEST_GRP_2, row.getDataByKey(DataTableColumns.GROUPS_LIST));

         // verify user 3 cannot read the data
         switchToUser3();
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // verify user 2 can change value
         switchToUser2();
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // verify the change
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

         // delete row
         switchToUser1();
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(SyncState.deleted.name(), row.getDataByKey(DataTableColumns
             .SYNC_STATE));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testGroupViewingHiddenLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1();
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         // NOTE: until the sync state is changed no permissions are enforced
         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.DEFAULT.name());
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_2);
         cv.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // user 3 should not be able to see the row
         switchToUser3();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // user 2 should be in the group and be able to see the row
         switchToUser2();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         //  switch the filter value to verify user 1 super user can still see row
         switchToUser1();
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         cv.put(DataTableColumns.FILTER_VALUE, "mailto:" + TEST_USER_2);

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("3",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // verify user 1 (super user) can see row
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         // super user can change group
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 4);
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_3);
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.DEFAULT.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("4",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // verify user 3 now succeeds with the correct groups
         switchToUser3();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("4",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }


      // clean up
      try {
         switchToUser1();
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         assertEquals(SyncState.deleted.name(), row.getDataByKey(DataTableColumns
             .SYNC_STATE));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testGroupAuthorizationViewHiddenRowNotLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1();

         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // change group
         ContentValues cv = new ContentValues();
         cv.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.DEFAULT.name());
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         switchToUser2();

         // verify user 2 can read
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(RowFilterScope.GroupType.DEFAULT.name(), row.getDataByKey(DataTableColumns
             .GROUP_TYPE));
         assertEquals(TEST_GRP_2, row.getDataByKey(DataTableColumns.GROUPS_LIST));

         // verify user 3 cannot read the data
         switchToUser3();
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // verify user 2 can change value
         switchToUser2();
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // verify the change
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

         // delete row
         switchToUser1();
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(SyncState.deleted.name(), row.getDataByKey(DataTableColumns
             .SYNC_STATE));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testGroupViewingHiddenNotLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1();
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         // NOTE: until the sync state is changed no permissions are enforced
         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.DEFAULT.name());
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_2);
         cv.put(DataTableColumns.FILTER_TYPE, RowFilterScope.Type.HIDDEN.name());
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("2",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // user 3 should not be able to see the row
         switchToUser3();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // user 2 should be in the group and be able to see the row
         switchToUser2();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         //  switch the filter value to verify user 1 super user can still see row
         switchToUser1();
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         cv.put(DataTableColumns.FILTER_VALUE, "mailto:" + TEST_USER_2);

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("3",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // verify user 1 (super user) can see row
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         // super user can change group
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 4);
         cv.put(DataTableColumns.GROUPS_LIST, TEST_GRP_3);
         cv.put(DataTableColumns.GROUP_TYPE, RowFilterScope.GroupType.DEFAULT.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("4",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // verify user 3 now succeeds with the correct groups
         switchToUser3();
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals("4",row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.FILTER_VALUE));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }


      // clean up
      try {
         switchToUser1();
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         assertEquals(SyncState.deleted.name(), row.getDataByKey(DataTableColumns
             .SYNC_STATE));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }


   //ContentValues contentValuesTestSeti(int i, String groupType, String groupsList)
   //verifyRowTestSeti(Row row, int i, String groupType, String groupsList)
   public void testGroupViewingMultipleHiddenNotLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {

         switchToUser1();
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionHiddenProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         List<String> groups = new ArrayList<String>();
         groups.add(TEST_GROUP_1);
         groups.add(TEST_GRP_2);
         groups.add(TEST_GRP_3);

         int i=0;
         for(int counter = 0; counter < 10; counter++) {
            for(String group : groups) {
               for(RowFilterScope.GroupType type : RowFilterScope.GroupType.values()) {
                  // insert row
                  UUID rowId = UUID.randomUUID();
                  i++;
                  ContentValues cv = contentValuesTestSeti(i, type.name(), group);
                  serviceInterface
                      .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

                  cv = new ContentValues();
                  cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());
                  serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());
               }
            }
         }

         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, -1, 0);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(120, table.getNumberOfRows());

         switchToUser2();
         table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, -1, 0);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(40, table.getNumberOfRows());
         helperCheckFunction(table, TEST_GRP_2);

         switchToUser3();
         table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, -1, 0);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(40, table.getNumberOfRows());
         helperCheckFunction(table, TEST_GRP_3);
      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

   }

   private void helperCheckFunction(UserTable table, String groupsList) {
      for(int i=0; i < table.getNumberOfRows(); i++) {
         Row row = table.getRowAtIndex(i);
         int iValue = Integer.valueOf(row.getDataByKey(COL_INTEGER_ID));
         if(i % 4 == 0) {
            verifyRowTestSeti(row, iValue, RowFilterScope.GroupType.DEFAULT.name(), groupsList);
         } else if (i % 4 == 1) {
            verifyRowTestSeti(row, iValue, RowFilterScope.GroupType.MODIFY.name(), groupsList);
         } else if (i % 4 == 2) {
            verifyRowTestSeti(row, iValue, RowFilterScope.GroupType.READ_ONLY.name(), groupsList);
         } else if (i % 4 == 3) {
            verifyRowTestSeti(row, iValue, RowFilterScope.GroupType.HIDDEN.name(), groupsList);
         } else {
            fail("Something is wrong");
         }

      }
   }

}
