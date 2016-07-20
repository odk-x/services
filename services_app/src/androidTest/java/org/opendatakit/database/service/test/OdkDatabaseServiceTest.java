package org.opendatakit.database.service.test;

import android.content.ContentValues;
import android.content.Intent;
import android.os.IBinder;
import android.os.RemoteException;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.test.ServiceTestCase;

import android.util.Log;
import org.opendatakit.TestConsts;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.Row;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.database.OdkDbSerializedInterface;
import org.opendatakit.database.service.OdkDatabaseService;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.database.service.OdkDbInterface;

import java.util.*;

public class OdkDatabaseServiceTest extends ServiceTestCase<OdkDatabaseService> {

   private static final String APPNAME = TestConsts.APPNAME;
   private static final String DB_TABLE_ID = "testtable";
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
      } catch (RemoteException e) {
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

   private void verifyRowTestSet1(Row row) {
      assertEquals(row.getRawDataOrMetadataByElementKey(COL_STRING_ID), TEST_STR_1);
      assertEquals(row.getRawDataOrMetadataByElementKey(COL_INTEGER_ID),
          Integer.toString(TEST_INT_1));
      assertEquals(row.getRawDataOrMetadataByElementKey(COL_NUMBER_ID),
          Double.toString(TEST_NUM_1));
   }

   private void verifyRowTestSet2(Row row) {
      assertEquals(row.getRawDataOrMetadataByElementKey(COL_STRING_ID), TEST_STR_2);
      assertEquals(row.getRawDataOrMetadataByElementKey(COL_INTEGER_ID),
          Integer.toString(TEST_INT_2));
      assertEquals(row.getRawDataOrMetadataByElementKey(COL_NUMBER_ID),
          Double.toString(TEST_NUM_2));
   }

   private void verifyNoTablesExistNCleanAllTables(OdkDbSerializedInterface serviceInterface,
       OdkDbHandle db) throws RemoteException {
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
       throws RemoteException {
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
      } catch (RemoteException e) {
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
      } catch (RemoteException e) {
         e.printStackTrace();
         fail();
      }
   }

   public void testDbInsertSingleRowIntoTable() {
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
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertSingleRowIntoTableWSingleTransaction() {
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
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertSingleRowIntoTableWTwoTransactions() {
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
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertTwoRowsIntoTable() {
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
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, COL_STRING_ID,
                 "ASC");
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertTwoRowsIntoTableWSingleTransaction() {
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
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, COL_STRING_ID,
                 "ASC");
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertTwoRowsIntoTableWTwoTransactions() {
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
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, COL_STRING_ID,
                 "ASC");
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbQueryWNoParams() {
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
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (RemoteException e) {
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
           } catch (RemoteException e) {
               e.printStackTrace();
               fail(e.getMessage());
           }
       }
   */
   public void testDbUpdateAllValues() {
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
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null);
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
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbUpdateAllValuesWTransactions() {
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
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null);
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
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbUpdateSingleValue() {
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
             .rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         Row row = table.getRowAtIndex(0);
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

         assertEquals(row.getRawDataOrMetadataByElementKey(COL_STRING_ID), TEST_STR_1);
         assertEquals(row.getRawDataOrMetadataByElementKey(COL_INTEGER_ID),
             Integer.toString(changeValue));
         assertEquals(row.getRawDataOrMetadataByElementKey(COL_NUMBER_ID),
             Double.toString(TEST_NUM_1));

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbInsertNDeleteSingleRowIntoTable() {
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
         Row row = table.getRowAtIndex(0);

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
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbCreateNDeleteLargeTable() {
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
             null, null, null, null, null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(NUM_ROWS, table.getNumberOfRows());
         Row row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // delete row
         Iterator<UUID> rowIdIterator = rowIds.iterator();
         for (int i = 0; i < NUM_ROWS; i++) {
            UUID rowId = rowIdIterator.next();
            serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         }

         table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // clean up
         serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   public void testDbUpdateWTwoServiceConnections() {
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
             .rawSqlQuery(APPNAME, db1, DB_TABLE_ID, columns, null, null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet1(table.getRowAtIndex(0));

         // use service connection 2 to verify db is in correct state
         table = serviceInterface2
             .rawSqlQuery(APPNAME, db2, DB_TABLE_ID, columns, null, null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet1(table.getRowAtIndex(0));

         // use service connection 2 to update values
         serviceInterface2
             .updateRowWithId(APPNAME, db2, DB_TABLE_ID, columns, contentValuesTestSet2(),
                 rowId.toString());

         // use service connection 2 to verify db is in correct state
         table = serviceInterface1
             .rawSqlQuery(APPNAME, db2, DB_TABLE_ID, columns, null, null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet2(table.getRowAtIndex(0));

         // use service connection 1 to verify db is in correct state
         table = serviceInterface1
             .rawSqlQuery(APPNAME, db1, DB_TABLE_ID, columns, null, null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet2(table.getRowAtIndex(0));

         // clean up
         serviceInterface1.deleteDBTableAndAllData(APPNAME, db1, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface1, db1));
         serviceInterface1.closeDatabase(APPNAME, db1);

      } catch (RemoteException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

}
