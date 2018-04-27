package org.opendatakit.database.service;

import android.Manifest;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.os.IBinder;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.test.InstrumentationRegistry;
import android.support.test.rule.GrantPermissionRule;
import android.support.test.runner.AndroidJUnit4;
import android.util.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.opendatakit.TestConsts;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.utilities.ODKServicesPropertyUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(AndroidJUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupsPermissionTest {

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
   private static final String anonymousUser = "anonymous";


   private static final String TEST_STR_1 = "TestStr1";
   private static final int TEST_INT_1 = 1;
   private static final double TEST_NUM_1 = 1.1;

   private static final String FULL_PERMISSION_ROLES = "[\"ROLE_DATA_ROW_OWNER\",\"ROLE_USER\","
       + "\"ROLE_SITE_ACCESS_ADMIN\",\"ROLE_SYNCHRONIZE_TABLES\",\"ROLE_ADMINISTER_TABLES\",\"ROLE_DATA_VIEWER\",\"ROLE_SUPER_USER_TABLES\"]";

   private static final String LIMITED_PERMISSION_ROLES_2 = "[\"ROLE_USER\","
       + "\"ROLE_SYNCHRONIZE_TABLES\",\"" + TEST_GRP_2 + "\"]";

   private static final String LIMITED_PERMISSION_ROLES_3 = "[\"ROLE_USER\","
       + "\"ROLE_SYNCHRONIZE_TABLES\",\"" + TEST_GRP_3 + "\"]";


   @Rule
   public final ODKServiceTestRule mServiceRule = new ODKServiceTestRule();

   @Rule
   public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);

   @Rule
   public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

   @Before
   public void setUp() throws Exception {

      boolean beganUninitialized = !initialized;
      if (beganUninitialized) {
         initialized = true;
         // Used to ensure that the singleton has been initialized properly
         AndroidConnectFactory.configure();

         UserDbInterface serviceInterface = bindToDbService();
         DbHandle dbHandle = null;
         try {
            dbHandle = serviceInterface.openDatabase(APPNAME);

            verifyNoTablesExistNCleanAllTables(serviceInterface, dbHandle);
         } finally {
            if ( dbHandle != null ) {
               serviceInterface.closeDatabase(APPNAME, dbHandle);
            }
         }
      }
   }

   @After
   public void tearDown() throws Exception {
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
   }

   @Nullable private UserDbInterface bindToDbService() {
      Context context = InstrumentationRegistry.getContext();
      Intent bind_intent = new Intent();
      bind_intent.setClassName(IntentConsts.Database.DATABASE_SERVICE_PACKAGE,
          IntentConsts.Database.DATABASE_SERVICE_CLASS);

      int count = 0;
      UserDbInterface dbInterface;
      try {
         IBinder service = null;
         try {
            service = mServiceRule.bindService(bind_intent);
         } catch (TimeoutException e) {
            e.printStackTrace();
         }
         dbInterface = new UserDbInterfaceImpl(
             new InternalUserDbInterfaceAidlWrapperImpl(AidlDbInterface.Stub.asInterface(service)));
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

   private ContentValues contentValuesTestSeti(int i, String groupReadOnly, String groupModify, String groupPrivileged) {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_i + i);
      cv.put(COL_INTEGER_ID, TEST_INT_i + i);
      cv.put(COL_NUMBER_ID, TEST_NUM_i + i);

      if (groupReadOnly != null) {
         cv.put(DataTableColumns.GROUP_READ_ONLY, groupReadOnly);
      }

      if (groupModify != null) {
         cv.put(DataTableColumns.GROUP_MODIFY, groupModify);
      }

      if (groupPrivileged != null) {
         cv.put(DataTableColumns.GROUP_PRIVILEGED, groupPrivileged);
      }

      return cv;
   }

   private void verifyRowTestSeti(TypedRow row, int i, String groupReadOnly, String groupModify,
       String groupPrivileged) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_i + i);
      assertEquals(row.getDataByKey(COL_INTEGER_ID), Long.valueOf(TEST_INT_i + i));
      assertEquals(row.getDataByKey(COL_NUMBER_ID), Double.valueOf(TEST_NUM_i + i));
      assertEquals(groupReadOnly, row.getDataByKey(DataTableColumns.GROUP_READ_ONLY));
      assertEquals(groupModify, row.getDataByKey(DataTableColumns.GROUP_MODIFY));
      assertEquals(groupPrivileged, row.getDataByKey(DataTableColumns.GROUP_PRIVILEGED));
   }



   private ContentValues contentValuesTestSet1() {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_1);
      cv.put(COL_INTEGER_ID, TEST_INT_1);
      cv.put(COL_NUMBER_ID, TEST_NUM_1);
      cv.put(DataTableColumns.DEFAULT_ACCESS, RowFilterScope.Access.FULL.name());
      cv.put(DataTableColumns.GROUP_MODIFY, TEST_GROUP_1);
      cv.putNull(DataTableColumns.GROUP_PRIVILEGED);

      return cv;
   }

   private void verifyRowTestSet1(TypedRow row) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
      assertEquals(row.getDataByKey(COL_INTEGER_ID), Long.valueOf(TEST_INT_1));
      assertEquals(row.getDataByKey(COL_NUMBER_ID), Double.valueOf(TEST_NUM_1));
      assertEquals(row.getRawStringByKey(DataTableColumns.DEFAULT_ACCESS),
              RowFilterScope.Access.FULL.name());
      assertEquals(TEST_GROUP_1, row.getRawStringByKey(DataTableColumns.GROUP_MODIFY));
      assertNull(row.getRawStringByKey(DataTableColumns.GROUP_PRIVILEGED));
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
      prop.key = "defaultAccessOnCreation";
      prop.type = "string";
      prop.value = RowFilterScope.Access.READ_ONLY.name();
      return prop;
   }

   private KeyValueStoreEntry defaultPermissionHiddenProperty() {
      KeyValueStoreEntry prop = new KeyValueStoreEntry();
      prop.tableId = DB_TABLE_ID;
      prop.partition = "Table";
      prop.aspect = "security";
      prop.key = "defaultAccessOnCreation";
      prop.type = "string";
      prop.value = RowFilterScope.Access.HIDDEN.name();
      return prop;
   }


   private void clearActiveUser(String appName) {
      PropertiesSingleton props = CommonToolProperties
              .get(InstrumentationRegistry.getTargetContext(), appName);
      ODKServicesPropertyUtils.clearActiveUser(props);
   }

   private void setActiveUser(String activeUser, String password, String appName, String
       roleListJSONstring,
       String defaultGroup) {
      Log.e("GroupsPermissionTest", "setActiveUser: Setting user to " + activeUser);
      PropertiesSingleton props = CommonToolProperties
          .get(InstrumentationRegistry.getTargetContext(), appName);
      // these are stored in devices
      Map<String,String> properties = new HashMap<String,String>();
      properties.put(CommonToolProperties.KEY_AUTHENTICATION_TYPE,
          InstrumentationRegistry.getTargetContext()
          .getString(org.opendatakit.androidlibrary.R.string.credential_type_username_password));
      properties.put(CommonToolProperties.KEY_USERNAME, activeUser);
      properties.put(CommonToolProperties.KEY_AUTHENTICATED_USER_ID, "mailto:" + activeUser);
      // this is stored in SharedPreferences
      properties.put(CommonToolProperties.KEY_PASSWORD, password);
      properties.put(CommonToolProperties.KEY_ROLES_LIST, roleListJSONstring);
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, defaultGroup);
      props.setProperties(properties);
   }

   private void switchToAnonymousUser(UserDbInterface dbServiceInterface) {
      clearActiveUser(APPNAME);
      //dbServiceInterface.getActiveUser(APPNAME);
      String verifyName = null;
      try {
         verifyName = dbServiceInterface.getActiveUser(APPNAME);
      } catch (ServicesAvailabilityException e) {
         fail("Unable to get verify switched to AnonymousUser");
      }
   }

   private void switchToUser1(UserDbInterface dbServiceInterface) {
      setActiveUser(TEST_USER_1, TEST_PWD_1, APPNAME, FULL_PERMISSION_ROLES, null);
      String verifyName = null;
      try {
         verifyName = dbServiceInterface.getActiveUser(APPNAME);
      } catch (ServicesAvailabilityException e) {
         fail("Unable to get verify switched to User 1");
      }
      assertEquals("failing switchToUser1", "mailto:" + TEST_USER_1, verifyName);
   }

   private void switchToUser2(UserDbInterface dbServiceInterface) {
      setActiveUser(TEST_USER_2, TEST_PWD_2, APPNAME, LIMITED_PERMISSION_ROLES_2, null);
      String verifyName = null;
      try {
         verifyName = dbServiceInterface.getActiveUser(APPNAME);
      } catch (ServicesAvailabilityException e) {
         fail("Unable to get verify switched to User 1");
      }
      assertEquals("failing switchToUser2", "mailto:" + TEST_USER_2, verifyName);
   }

   private void switchToUser3(UserDbInterface dbServiceInterface) {
      setActiveUser(TEST_USER_3, TEST_PWD_3, APPNAME, LIMITED_PERMISSION_ROLES_3, null);
      String verifyName = null;
      try {
         verifyName = dbServiceInterface.getActiveUser(APPNAME);
      } catch (ServicesAvailabilityException e) {
         fail("Unable to get verify switched to User 1");
      }
      assertEquals("failing switchToUser3", "mailto:" + TEST_USER_3, verifyName);
   }

   private boolean hasNoTablesInDb(UserDbInterface serviceInterface, DbHandle db)
       throws ServicesAvailabilityException {
      List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);
      return (tableIds.size() == 0);
   }

   private void verifyNoTablesExistNCleanAllTables(UserDbInterface serviceInterface, DbHandle db)
       throws ServicesAvailabilityException {
      switchToUser1(serviceInterface);

      List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);

      // Drop any leftover table now that the test is done
      for (String id : tableIds) {
         serviceInterface.deleteTableAndAllData(APPNAME, db, id);
      }

      tableIds = serviceInterface.getAllTableIds(APPNAME, db);

      boolean tablesGone = (tableIds.size() == 0);

      assertTrue(tablesGone);
   }

   @Test
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
         TypedRow row = table.getRowAtIndex(0);

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

   @Test
   public void testSetUser() throws ActionNotAuthorizedException {
      UserDbInterface dbServiceInterface = bindToDbService();
      String testUserName = "test@gmail.com";
      setActiveUser(testUserName, "1235", APPNAME, FULL_PERMISSION_ROLES, null);
      String verifyName = null;

      try {
         verifyName = dbServiceInterface.getActiveUser(APPNAME);
      } catch (ServicesAvailabilityException e) {
        fail("Failed to verify user");
      }

      assertEquals("mailto:" + testUserName, verifyName);


      String testUserName2 = "test3@gmail.com";
      setActiveUser(testUserName2, "1235", APPNAME, LIMITED_PERMISSION_ROLES_3, null);
      try {
         verifyName = dbServiceInterface.getActiveUser(APPNAME);
      } catch (ServicesAvailabilityException e) {
         fail("Failed to verify user");
      }

      assertNotEquals("mailto:" + testUserName, verifyName);
      assertEquals("mailto:" + testUserName2, verifyName);

      setActiveUser(testUserName, "1235", APPNAME, FULL_PERMISSION_ROLES, null);

      try {
         verifyName = dbServiceInterface.getActiveUser(APPNAME);
      } catch (ServicesAvailabilityException e) {
         fail("Failed to verify user");
      }
      assertEquals("mailto:" + testUserName, verifyName);
   }

   @Test
   public void testUserAuthorizationSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);

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
         TypedRow row = table.getRowAtIndex(0);

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

   @Test
   public void testUserAuthorizationFailure() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         switchToUser2(serviceInterface);

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
         switchToUser1(serviceInterface);
         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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
         switchToUser2(serviceInterface);
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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

         assertEquals(Long.valueOf(2),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
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
         TypedRow row = table.getRowAtIndex(0);

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
         switchToUser1(serviceInterface);
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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

   @Test
   public void testGroupAuthorizationUpdateRow_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);

         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         columns =serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // change group
         ContentValues cv = new ContentValues();
         cv.put(DataTableColumns.GROUP_PRIVILEGED, TEST_GRP_2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         switchToUser2(serviceInterface);

         // verify user 2 can read
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(TEST_GRP_2, row.getDataByKey(DataTableColumns.GROUP_PRIVILEGED));

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

         assertEquals(Long.valueOf(2),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

         // delete row
         switchToUser1(serviceInterface);
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

   @Test
   public void testGroupAuthorizationAnonymousFailureLocked_ExpectSomeFailures() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         columns = serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
                 colList, metaData, false);

         switchToAnonymousUser(serviceInterface);
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
         switchToUser1(serviceInterface);
         // insert row
         serviceInterface
                 .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                         rowId.toString());

         UserTable table = serviceInterface
                 .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      try {
         switchToAnonymousUser(serviceInterface);
         UserTable table = serviceInterface
                 .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      try {
         switchToUser1(serviceInterface);
         UserTable table = serviceInterface
                 .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getRawStringByKey(DataTableColumns.ID));

         // NOTE: savepoint_creator will change to be the new user unless it is specified in cv.
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());


         table = serviceInterface
                 .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(2), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getRawStringByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_1, row.getRawStringByKey(DataTableColumns.SAVEPOINT_CREATOR));

      } catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

      // clean up
      try {
         switchToUser1(serviceInterface);
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
                 .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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

   @Test
   public void testGroupAuthorizationFailureLocked_ExpectSomeFailures() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(lockTableProperty());
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         columns = serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db,
             DB_TABLE_ID,
             colList, metaData, false);

         switchToUser2(serviceInterface);
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
         switchToUser1(serviceInterface);
         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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
         switchToUser2(serviceInterface);
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         // NOTE: savepoint_creator will change to be the new user unless it is specified in cv.
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());


         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(2),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
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
         switchToUser2(serviceInterface);
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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
         switchToUser3(serviceInterface);
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
         switchToUser1(serviceInterface);
         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         cv.put(DataTableColumns.GROUP_PRIVILEGED, TEST_GRP_3);

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());


         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(3),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
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
         switchToUser3(serviceInterface);
         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 4);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(4), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
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
         switchToUser1(serviceInterface);
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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

   @Test
   public void testGroupAuthorizationFailureNotLocked_ExpectSomeFailures() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser2(serviceInterface);
         db = serviceInterface.openDatabase(APPNAME);

         List<KeyValueStoreEntry> metaData = new ArrayList<KeyValueStoreEntry>();
         metaData.add(noAnonCreationProperty());
         metaData.add(defaultPermissionReadOnlyProperty());

         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, db, DB_TABLE_ID,
             colList, metaData, false);

         ContentValues cv2 = new ContentValues();
         cv2.put(COL_STRING_ID, TEST_STR_1);
         cv2.put(COL_INTEGER_ID, TEST_INT_1);
         cv2.put(COL_NUMBER_ID, TEST_NUM_1);
         cv2.put(DataTableColumns.ROW_OWNER, "mailto:" + TEST_USER_2);
         cv2.put(DataTableColumns.DEFAULT_ACCESS, RowFilterScope.Access.FULL.name());
         cv2.put(DataTableColumns.GROUP_MODIFY, TEST_GROUP_1);

         // insert row
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv2,
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);


         // NOTE: until the sync state is changed no permissions are enforced
         switchToUser1(serviceInterface);
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.DEFAULT_ACCESS, RowFilterScope.Access.HIDDEN.name());
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // user two should still be able to see the row since they created it
         switchToUser2(serviceInterface);

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(2),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // user three should not be able to see the row
         switchToUser3(serviceInterface);
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // change so that user 3 has group read access
         switchToUser1(serviceInterface);
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         cv = new ContentValues();
         cv.put(DataTableColumns.GROUP_READ_ONLY, TEST_GRP_3);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // user three should NOW be able to see the row
         switchToUser3(serviceInterface);
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
         switchToUser1(serviceInterface);
         UserTable table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId
             .toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);
         assertEquals(Long.valueOf(2), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // change to allow user 3 to modify row, and lock user 2
         ContentValues cv = new ContentValues();
         cv.put(DataTableColumns.ROW_OWNER, "mailto:" + TEST_USER_1);
         cv.put(DataTableColumns.GROUP_MODIFY, TEST_GRP_3);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // verify user 3 can update
         switchToUser3(serviceInterface);
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId
             .toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);
         assertEquals(Long.valueOf(3), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_3, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // verify user 2 cannot update
         switchToUser2(serviceInterface);
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
         switchToUser1(serviceInterface);
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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

   @Test
   public void testGroupAuthorizationViewHiddenRowLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);

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
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // change group
         ContentValues cv = new ContentValues();
         cv.put(DataTableColumns.DEFAULT_ACCESS, RowFilterScope.Access.HIDDEN.name());
         cv.put(DataTableColumns.GROUP_PRIVILEGED, TEST_GRP_2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         switchToUser2(serviceInterface);

         // verify user 2 can read
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(TEST_GRP_2, row.getDataByKey(DataTableColumns.GROUP_PRIVILEGED));

         // verify user 3 cannot read the data
         switchToUser3(serviceInterface);
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // verify user 2 can change value
         switchToUser2(serviceInterface);
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // verify the change
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(2),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

         // delete row
         switchToUser1(serviceInterface);
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

   @Test
   public void testGroupViewingHiddenLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);
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
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         // NOTE: until the sync state is changed no permissions are enforced
         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.GROUP_MODIFY, TEST_GRP_2);
         cv.put(DataTableColumns.DEFAULT_ACCESS, RowFilterScope.Access.HIDDEN.name());
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(2),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // user 3 should not be able to see the row
         switchToUser3(serviceInterface);
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // user 2 should be in the group and be able to see the row
         switchToUser2(serviceInterface);
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         //  switch the filter value to verify user 1 super user can still see row
         switchToUser1(serviceInterface);
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         cv.put(DataTableColumns.ROW_OWNER, "mailto:" + TEST_USER_2);

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(3), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.ROW_OWNER));
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
         cv.put(DataTableColumns.GROUP_MODIFY, TEST_GRP_3);

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(4), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // verify user 3 now succeeds with the correct groups
         switchToUser3(serviceInterface);
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(4), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.ROW_OWNER));
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
         switchToUser1(serviceInterface);
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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

   @Test
   public void testGroupAuthorizationViewHiddenRowNotLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);

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
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // change group
         ContentValues cv = new ContentValues();
         cv.put(DataTableColumns.DEFAULT_ACCESS, RowFilterScope.Access.HIDDEN.name());
         cv.put(DataTableColumns.GROUP_MODIFY, TEST_GRP_2);
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         switchToUser2(serviceInterface);

         // verify user 2 can read
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(TEST_GRP_2, row.getDataByKey(DataTableColumns.GROUP_MODIFY));

         // verify user 3 cannot read the data
         switchToUser3(serviceInterface);
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // verify user 2 can change value
         switchToUser2(serviceInterface);
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         // verify the change
         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(2),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.SAVEPOINT_CREATOR));

         // delete row
         switchToUser1(serviceInterface);
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

   @Test
   public void testGroupViewingHiddenNotLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      UUID rowId = UUID.randomUUID();
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {
         switchToUser1(serviceInterface);
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
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         // NOTE: until the sync state is changed no permissions are enforced
         ContentValues cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 2);
         cv.put(DataTableColumns.GROUP_MODIFY, TEST_GRP_2);
         cv.put(DataTableColumns.DEFAULT_ACCESS, RowFilterScope.Access.HIDDEN.name());
         cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(2),row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // user 3 should not be able to see the row
         switchToUser3(serviceInterface);
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // user 2 should be in the group and be able to see the row
         switchToUser2(serviceInterface);
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         //  switch the filter value to verify user 1 super user can still see row
         switchToUser1(serviceInterface);
         cv = new ContentValues();
         cv.put(COL_INTEGER_ID, 3);
         cv.put(DataTableColumns.ROW_OWNER, "mailto:" + TEST_USER_2);

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(3), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.ROW_OWNER));
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
         cv.put(DataTableColumns.GROUP_MODIFY, TEST_GRP_3);

         assertEquals(rowId.toString(), row.getDataByKey
             (DataTableColumns.ID));

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(4), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.ROW_OWNER));
         assertEquals("mailto:" + TEST_USER_1, row.getDataByKey(DataTableColumns
             .SAVEPOINT_CREATOR));

         // verify user 3 now succeeds with the correct groups
         switchToUser3(serviceInterface);
         table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         assertEquals(Long.valueOf(4), row.getDataByKey(COL_INTEGER_ID));
         assertEquals("mailto:" + TEST_USER_2, row.getDataByKey(DataTableColumns.ROW_OWNER));
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
         switchToUser1(serviceInterface);
         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

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

   @Test
   public void testGroupViewingMultipleHiddenNotLocked_ExpectSuccess() {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      List<Column> columnList = createColumnList();
      ColumnList colList = new ColumnList(columnList);
      OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

      try {

         switchToUser1(serviceInterface);
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
               for(RowFilterScope.Access type : RowFilterScope.Access.values()) {
                  // insert row
                  UUID rowId = UUID.randomUUID();
                  i++;
                  ContentValues cv = null;
                  if (type.equals(RowFilterScope.Access.FULL)) {
                     cv = contentValuesTestSeti(i, null, null, group);
                  } else if (type.equals(RowFilterScope.Access.MODIFY)) {
                     cv = contentValuesTestSeti(i, null, group, null);
                  } else if (type.equals(RowFilterScope.Access.READ_ONLY)) {
                     cv = contentValuesTestSeti(i, group, null, null);
                  }

                  if (cv != null) {
                     serviceInterface
                             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

                     cv = new ContentValues();
                     cv.put(DataTableColumns.SYNC_STATE, SyncState.synced.name());
                     serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());
                  }
               }
            }
         }

         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, -1, 0);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(90, table.getNumberOfRows());

         switchToUser2(serviceInterface);
         table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, -1, 0);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(30, table.getNumberOfRows());
         helperCheckFunction(table, TEST_GRP_2);

         switchToUser3(serviceInterface);
         table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, -1, 0);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(30, table.getNumberOfRows());
         helperCheckFunction(table, TEST_GRP_3);
      }  catch (ActionNotAuthorizedException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }

   }

   private void helperCheckFunction(UserTable table, String groupName) {
      for(int i=0; i < table.getNumberOfRows(); i++) {
         TypedRow row = table.getRowAtIndex(i);
         Long longValue = (Long) row.getDataByKey(COL_INTEGER_ID);
         int iValue = longValue.intValue();
         if(i % 3 == 0) {
            verifyRowTestSeti(row, iValue, null, null, groupName);
         } else if (i % 3 == 1) {
            verifyRowTestSeti(row, iValue, null, groupName, null);
         } else if (i % 3 == 2) {
            verifyRowTestSeti(row, iValue, groupName, null, null);
         } else {
            fail("Something is wrong");
         }

      }
   }

}
