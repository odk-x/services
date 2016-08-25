package org.opendatakit.database.service.test;

import android.content.ContentValues;
import android.content.Intent;
import android.os.IBinder;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.test.ServiceTestCase;

import android.util.Log;
import org.opendatakit.TestConsts;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.exception.ActionNotAuthorizedException;
import org.opendatakit.common.android.exception.ServicesAvailabilityException;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.database.OdkDbSerializedInterface;
import org.opendatakit.database.service.*;

import java.util.*;

public class OdkDatabaseServiceTest extends ServiceTestCase<OdkDatabaseService> {

   private static final String APPNAME = TestConsts.APPNAME;
   private static final String DB_TABLE_ID = "testtable";
   private static final String LOCAL_ONLY_DB_TABLE_ID = "L_" + DB_TABLE_ID;
   private static final String COL_STRING_ID = "columnString";
   private static final String COL_INTEGER_ID = "columnInteger";
   private static final String COL_NUMBER_ID = "columnNumber";

   private static final String TEST_STR_1 = "TestStr1";
   private static final int TEST_INT_1 = 1;
   private static final double TEST_NUM_1 = 1.1;
   private static final String TEST_STR_2 = "TestStr2";
   private static final int TEST_INT_2 = 2;
   private static final double TEST_NUM_2 = 2.1;
   private static final String TEST_STR_3 = "TestStr3";
   private static final int TEST_INT_3 = TEST_INT_2;
   private static final double TEST_NUM_3 = 3.1;
   private static final String TEST_STR_i = "TestStri";
   private static final int TEST_INT_i = 0;
   private static final double TEST_NUM_i = 0.1;

   public OdkDatabaseServiceTest() {
      super(OdkDatabaseService.class);
   }

   public OdkDatabaseServiceTest(Class<OdkDatabaseService> serviceClass) {
      super(serviceClass);
   }

   @Override protected void setUp() throws Exception {
      super.setUp();
      setupService();
   }

   @Override protected void tearDown() throws Exception {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "tearDown: " + db.getDatabaseHandle());
         verifyNoTablesExistNCleanAllTables(serviceInterface, db);
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
      super.tearDown();
   }

   @NonNull private List<Column> createColumnList() {
      List<Column> columns = new ArrayList<Column>();

      columns.add(new Column(COL_STRING_ID, "column String", "string", null));
      columns.add(new Column(COL_INTEGER_ID, "column Integer", "integer", null));
      columns.add(new Column(COL_NUMBER_ID, "column Number", "number", null));

      return columns;
   }

   private ContentValues contentValuesTestSet1() {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_1);
      cv.put(COL_INTEGER_ID, TEST_INT_1);
      cv.put(COL_NUMBER_ID, TEST_NUM_1);

      return cv;
   }

   private ContentValues contentValuesTestSet2() {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_2);
      cv.put(COL_INTEGER_ID, TEST_INT_2);
      cv.put(COL_NUMBER_ID, TEST_NUM_2);

      return cv;
   }

   private ContentValues contentValuesTestSeti(int i) {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_i + i);
      cv.put(COL_INTEGER_ID, TEST_INT_i + i);
      cv.put(COL_NUMBER_ID, TEST_NUM_i + i);

      return cv;
   }

   private void verifyRowTestSet1(OdkDbRow row) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Integer.toString(TEST_INT_1));
      assertEquals(row.getDataByKey(COL_NUMBER_ID),
          Double.toString(TEST_NUM_1));
   }

   private void verifyRowTestSet2(OdkDbRow row) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_2);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Integer.toString(TEST_INT_2));
      assertEquals(row.getDataByKey(COL_NUMBER_ID),
          Double.toString(TEST_NUM_2));
   }

   private void verifyRowTestSeti(OdkDbRow row, int i) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_i + i);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Integer.toString(TEST_INT_i + i));
      assertEquals(row.getDataByKey(COL_NUMBER_ID),
          Double.toString(TEST_NUM_i + i));
   }

   private void verifyTableTestSet(OdkDbTable table, int offset) {
      for (int i = 0; i < table.getNumberOfRows(); i++) {
         verifyRowTestSeti(table.getRowAtIndex(i), i + offset);
      }
   }

   private void verifyNoTablesExistNCleanAllTables(OdkDbSerializedInterface serviceInterface,
       OdkDbHandle db) throws ServicesAvailabilityException {
      List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);

      boolean tablesGone = (tableIds.size() == 0);

      // Drop any leftover table now that the test is done
      for (String id : tableIds) {
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);
      }

      assertTrue(tablesGone);
   }

   @Nullable private OdkDbSerializedInterface bindToDbService() {
      Intent bind_intent = new Intent();
      bind_intent.setClass(getContext(), OdkDatabaseService.class);
      IBinder service = this.bindService(bind_intent);

      OdkDbSerializedInterface dbInterface;
      try {
         dbInterface = new OdkDbSerializedInterface(OdkDbInterface.Stub.asInterface(service));
      } catch (IllegalArgumentException e) {
         dbInterface = null;
      }
      return dbInterface;
   }

   private boolean hasNoTablesInDb(OdkDbSerializedInterface serviceInterface, OdkDbHandle db)
       throws ServicesAvailabilityException {
      List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);
      return (tableIds.size() == 0);
   }

   public void testBinding() {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      assertNotNull(serviceInterface.getDbInterface());
      // TODO: database check function?

      // TODO: add a bind with bind_intent.setClassName instead
   }

   public void testDbCreateNDeleteTable() {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnList = new ColumnList(createColumnList());

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, columnList);

         List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);

         // verify single table exists
         assertTrue(tableIds.size() == 1);
         assertTrue(tableIds.contains(DB_TABLE_ID));

         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail();
      }
   }

   public void testDbCreateNDeleteTableWTransactions() {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnList = new ColumnList(createColumnList());

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteTableWTransactions: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, columnList);

         List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);
         // verify single table exists
         assertTrue(tableIds.size() == 1);
         assertTrue(tableIds.contains(DB_TABLE_ID));

         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail();
      }
   }

   public void testDbInsertSingleRowIntoTable() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         OdkDbRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertThrowsIllegalArgumentException()
       throws ActionNotAuthorizedException, ServicesAvailabilityException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      OdkDbHandle db = null;
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         db = serviceInterface.openDatabase(APPNAME);
         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         ContentValues cv = new ContentValues();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv,
                 rowId.toString());

      } catch (IllegalArgumentException e) {
         // expected
         assertTrue("Got what we expected " + e.toString(), true);
      } catch (Exception e) {
         assertTrue("Unexpected " + e.toString(), false);
      } finally {
         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         serviceInterface.closeDatabase(APPNAME, db);
      }
   }

   public void testDbInsertSingleRowIntoTableWSingleTransaction()
       throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         OdkDbRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertSingleRowIntoTableWTwoTransactions()
       throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbInsertSingleRowIntoTableWTwoTransactions: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         OdkDbRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertTwoRowsIntoTable() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertTwoRowsIntoTable: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId1 = UUID.randomUUID();
         UUID rowId2 = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId1.toString());
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet2(),
                 rowId2.toString());

         UserTable table = serviceInterface
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null,
                 new String[] { COL_STRING_ID }, new String[] { "ASC" }, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertTwoRowsIntoTableWSingleTransaction()
       throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);

         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId1 = UUID.randomUUID();
         UUID rowId2 = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId1.toString());
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet2(),
                 rowId2.toString());

         UserTable table = serviceInterface
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null,
                 new String[] { COL_STRING_ID }, new String[] { "ASC" }, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertTwoRowsIntoTableWTwoTransactions() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbInsertTwoRowsIntoTableWTwoTransactions: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId1 = UUID.randomUUID();
         UUID rowId2 = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId1.toString());

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet2(),
                 rowId2.toString());

         UserTable table = serviceInterface
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null,
                 new String[] { COL_STRING_ID }, new String[] { "ASC" }, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbQueryWNoParams() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbQueryWNoParams: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         OdkDbRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   /*
       public void testDbQueryRowWNoColumnsSpecified() {
           OdkDbSerializedInterface serviceInterface = bindToDbService();
           try {

               List<Column> columnList = createColumnList();
               ColumnList colList = new ColumnList(columnList);

               OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
              Log.i("openDatabase", "testDbQueryRowWNoColumnsSpecified: " + db
                  .getDatabaseHandle());
               serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

               OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
               UUID rowId =  UUID.randomUUID();

               serviceInterface.insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

               UserTable table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, null, rowId.toString());

               assertEquals(DB_TABLE_ID, table.getTableId());
               assertEquals(1, table.getNumberOfRows());
               Row row = table.getRowAtIndex(0);

               verifyRowTestSet1(row);

               // clean up
               serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

               // verify no tables left
               assertTrue(hasNoTablesInDb(serviceInterface, db));
               serviceInterface.closeDatabase(APPNAME, db);
           } catch (ServicesAvailabilityException e) {
               e.printStackTrace();
               fail(e.getMessage());
           }
       }
   */
   public void testDbUpdateAllValues() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbUpdateAllValues: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));

         serviceInterface
             .updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet2(),
                 rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         verifyRowTestSet2(table.getRowAtIndex(0));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbUpdateAllValuesWTransactions() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbUpdateAllValuesWTransactions: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));

         serviceInterface
             .updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet2(),
                 rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         verifyRowTestSet2(table.getRowAtIndex(0));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbUpdateSingleValue() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbUpdateSingleValue: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         OdkDbRow row = table.getRowAtIndex(0);
         verifyRowTestSet1(table.getRowAtIndex(0));

         List<Column> singleColumnArray = new ArrayList<Column>();
         singleColumnArray.add(new Column(COL_INTEGER_ID, "column Integer", "integer", null));
         OrderedColumns singleColumn = new OrderedColumns(APPNAME, DB_TABLE_ID, singleColumnArray);

         ContentValues singleValue = new ContentValues();
         int changeValue = 3;
         singleValue.put(COL_INTEGER_ID, changeValue);

         serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, singleColumn, singleValue,
             rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         row = table.getRowAtIndex(0);

         assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
         assertEquals(row.getDataByKey(COL_INTEGER_ID),
             Integer.toString(changeValue));
         assertEquals(row.getDataByKey(COL_NUMBER_ID),
             Double.toString(TEST_NUM_1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertNDeleteSingleRowIntoTable() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertNDeleteSingleRowIntoTable: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

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
         OdkDbRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // delete row
         serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }
   
   public void testDbCreateNDeleteLargeTable() throws ActionNotAuthorizedException {
      final int NUM_ROWS = 10000;
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertNDelete1000RowsIntoTable: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

         Set<UUID> rowIds = new HashSet<>(NUM_ROWS);
         for (int i = 0; i < NUM_ROWS; i++) {
            UUID rowId = UUID.randomUUID();
            rowIds.add(rowId);

            // insert row
            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());
         }

         UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, null, null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(NUM_ROWS, table.getNumberOfRows());
         OdkDbRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // delete row
         Iterator<UUID> rowIdIterator = rowIds.iterator();
         for (int i = 0; i < NUM_ROWS; i++) {
            UUID rowId = rowIdIterator.next();
            serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         }

         table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbUpdateWTwoServiceConnections() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface1 = bindToDbService();
      OdkDbSerializedInterface serviceInterface2 = bindToDbService();

      try {
         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);
         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         OdkDbHandle db1 = serviceInterface1.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbUpdateWTwoServiceConnections db1: " + db1.getDatabaseHandle());

         OdkDbHandle db2 = serviceInterface2.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbUpdateWTwoServiceConnections db2: " + db2.getDatabaseHandle());

         serviceInterface1.createOrOpenDBTableWithColumns(APPNAME, db1, DB_TABLE_ID, colList);

         serviceInterface1
             .insertRowWithId(APPNAME, db1, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table;

         // use service connection 1 to verify db is in correct state
         table = serviceInterface1
             .rawSqlQuery(APPNAME, db1, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet1(table.getRowAtIndex(0));

         // use service connection 2 to verify db is in correct state
         table = serviceInterface2
             .rawSqlQuery(APPNAME, db2, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet1(table.getRowAtIndex(0));

         // use service connection 2 to update values
         serviceInterface2
             .updateRowWithId(APPNAME, db2, DB_TABLE_ID, columns, contentValuesTestSet2(),
                 rowId.toString());

         // use service connection 2 to verify db is in correct state
         table = serviceInterface1
             .rawSqlQuery(APPNAME, db2, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet2(table.getRowAtIndex(0));

         // use service connection 1 to verify db is in correct state
         table = serviceInterface1
             .rawSqlQuery(APPNAME, db1, DB_TABLE_ID, columns, null, null, null, null, null, null,
             null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet2(table.getRowAtIndex(0));

         // clean up
         serviceInterface1.deleteDBTableAndAllData(APPNAME, db1, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface1, db1));
         serviceInterface1.closeDatabase(APPNAME, db1);

      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbLimitNoOffset() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 0);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbLimitOver() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 60 , null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(50, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 0);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbOffsetNoLimit() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, orderByCol,
                 orderByDir, null, 10);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(40, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 10);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbLimitWithOffset() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , 10);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 10);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbQueryResumeForward() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 0);

         table = serviceInterface
             .resumeRawSqlQuery(APPNAME, db, columns, table.resumeQueryForward(10));

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 10);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbQueryResumeForwardEnd() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 60 , null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(50, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 0);

         assertNull(table.resumeQueryForward(1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbQueryResumeBackward() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , 20);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 20);

         table = serviceInterface
             .resumeRawSqlQuery(APPNAME, db, columns, table.resumeQueryBackward(10));

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 10);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbQueryResumeBackwardEnd() throws ActionNotAuthorizedException {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , 10);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 10);

         table = serviceInterface
             .resumeRawSqlQuery(APPNAME, db, columns, table.resumeQueryBackward(10));

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), 0);

         assertNull(table.resumeQueryBackward(1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbCreateNDeleteLocalOnlyTable() {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         OrderedColumns columns = serviceInterface
             .createLocalOnlyDbTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface.deleteLocalOnlyDBTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertSingleRowIntoLocalOnlyTable() {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         OrderedColumns columns = serviceInterface
             .createLocalOnlyDbTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface
             .insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());

         OdkDbTable results = serviceInterface.rawSqlQueryLocalOnlyTables(APPNAME, db,
             DB_TABLE_ID, null, null, null, null, null, null, null, null);

         assertEquals(1, results.getNumberOfRows());
         OdkDbRow row = results.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteLocalOnlyDBTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase",
             "testDbInsertSingleRowIntoLocalOnlyTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertTwoRowsIntoLocalOnlyTable() {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         OrderedColumns columns = serviceInterface
             .createLocalOnlyDbTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());
         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet2());


         OdkDbTable results = serviceInterface.rawSqlQueryLocalOnlyTables(APPNAME, db,
             DB_TABLE_ID, null, null, null, null, null, null, null, null);

         assertEquals(2, results.getNumberOfRows());

         verifyRowTestSet1(results.getRowAtIndex(0));
         verifyRowTestSet2(results.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteLocalOnlyDBTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase",
             "testDbInsertSingleRowIntoLocalOnlyTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbUpdateAllValuesLocalOnlyTable()  {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         OrderedColumns columns = serviceInterface
             .createLocalOnlyDbTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());


         OdkDbTable results = serviceInterface
             .rawSqlQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());
         verifyRowTestSet1(results.getRowAtIndex(0));

         String whereClause = COL_STRING_ID + "=?";
         String[] bindArgs = new String[] { TEST_STR_1 };
         serviceInterface
             .updateLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet2(), whereClause,
                 bindArgs);

         results = serviceInterface
             .rawSqlQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());
         verifyRowTestSet2(results.getRowAtIndex(0));

         // clean up
         serviceInterface.deleteLocalOnlyDBTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase",
             "testDbInsertSingleRowIntoLocalOnlyTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbUpdateSingleValueLocalOnlyTable() {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         OrderedColumns columns = serviceInterface
             .createLocalOnlyDbTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());


         OdkDbTable results = serviceInterface
             .rawSqlQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());
         verifyRowTestSet1(results.getRowAtIndex(0));

         List<Column> singleColumnArray = new ArrayList<>();
         singleColumnArray.add(new Column(COL_INTEGER_ID, "column Integer", "integer", null));
         ContentValues singleValue = new ContentValues();
         int changeValue = 3;
         singleValue.put(COL_INTEGER_ID, changeValue);

         String whereClause = COL_STRING_ID + "=?";
         String[] bindArgs = new String[] { TEST_STR_1 };
         serviceInterface
             .updateLocalOnlyRow(APPNAME, db, DB_TABLE_ID, singleValue, whereClause, bindArgs);

         results = serviceInterface
             .rawSqlQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());

         OdkDbRow row = results.getRowAtIndex(0);
         assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
         assertEquals(row.getDataByKey(COL_INTEGER_ID), Integer.toString(changeValue));
         assertEquals(row.getDataByKey(COL_NUMBER_ID), Double.toString(TEST_NUM_1));

         // clean up
         serviceInterface.deleteLocalOnlyDBTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase",
             "testDbInsertSingleRowIntoLocalOnlyTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertNDeleteSingleRowIntoLocalOnlyTable() {
      OdkDbSerializedInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

         OdkDbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         OrderedColumns columns = serviceInterface
             .createLocalOnlyDbTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());


         OdkDbTable results = serviceInterface
             .rawSqlQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());
         verifyRowTestSet1(results.getRowAtIndex(0));

         String whereClause = COL_STRING_ID + "=?";
         String[] bindArgs = new String[] { TEST_STR_1 };
         serviceInterface.deleteLocalOnlyRow(APPNAME, db, DB_TABLE_ID, whereClause, bindArgs);

         results = serviceInterface
             .rawSqlQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(0, results.getNumberOfRows());

         // clean up
         serviceInterface.deleteLocalOnlyDBTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase",
             "testDbInsertSingleRowIntoLocalOnlyTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

}
