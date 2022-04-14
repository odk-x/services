package org.opendatakit.database.service;

import android.content.ContentValues;
import android.util.Log;

import androidx.annotation.NonNull;
import androidx.test.filters.LargeTest;

import org.joda.time.DateTime;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.utilities.DateUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OdkDatabaseServiceTest extends OdkDatabaseTestAbstractBase {

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
   private static final String TEST_STR_i = "TestStri";
   private static final int TEST_INT_i = 0;
   private static final double TEST_NUM_i = 0.1;


   // The number of columns to put into the list, divided by the 5 column types to repeat.
   private static final int MANY_COL_NUM_COLUMNS = 100 / 5;
   private static final String MANY_COL_STRING_ID = "columnString";
   private static final String MANY_COL_INTEGER_ID = "columnInteger";
   private static final String MANY_COL_NUMBER_ID = "columnNumber";
   private static final String MANY_COL_BOOL_ID = "columnBool";
   private static final String MANY_COL_DATE_ID = "columnDate";
   private static final DateUtils date = new DateUtils(Locale.US, null);
   private static final String dateString = date.formatDateTimeForDb(DateTime.parse
       ("2016-09-28T21:26:22+00:00"));

   protected void setUpBefore() {
      return;
   }

   protected void tearDownBefore() {
      return;
   }

   @NonNull private List<Column> createColumnList() {
      List<Column> columns = new ArrayList<>();

      columns.add(new Column(COL_STRING_ID, "column String", "string", null));
      columns.add(new Column(COL_INTEGER_ID, "column Integer", "integer", null));
      columns.add(new Column(COL_NUMBER_ID, "column Number", "number", null));

      return columns;
   }

   @NonNull private List<Column> createManyColumnList() {
      List<Column> columns = new ArrayList<>();

      for (int i = 0; i < MANY_COL_NUM_COLUMNS; i++) {
         columns.add(
             new Column(MANY_COL_STRING_ID + i, "column String", ElementDataType.string.name(),
                 null));
         columns.add(
             new Column(MANY_COL_INTEGER_ID + i, "column Integer", ElementDataType.integer.name(),
                 null));
         columns.add(
             new Column(MANY_COL_NUMBER_ID + i, "column Number", ElementDataType.number.name(),
                 null));
         columns.add(
             new Column(MANY_COL_BOOL_ID + i, "column Bool", ElementDataType.bool.name(), null));
         columns.add(
             new Column(MANY_COL_DATE_ID + i, "column Date", ElementDataType.string.name(), null));
      }
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

   private ContentValues contentValuesTestSetManyColumns(int i) {
      ContentValues cv = new ContentValues();


      for (int j = 0; j < MANY_COL_NUM_COLUMNS; j++) {
         cv.put(MANY_COL_STRING_ID + j, "STR_" + j + "_" + i);
         cv.put(MANY_COL_INTEGER_ID + j, i+j);
         cv.put(MANY_COL_NUMBER_ID + j, i*j);
         cv.put(MANY_COL_BOOL_ID + j, ((i+j)%2 != 0));
         cv.put(MANY_COL_DATE_ID + j, dateString);
      }

      return cv;
   }

   private void verifyRowTestSet1(TypedRow row) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Long.valueOf(TEST_INT_1));
      assertEquals(row.getDataByKey(COL_NUMBER_ID),
          Double.valueOf(TEST_NUM_1));
   }

   private void verifyRowTestSet2(TypedRow row) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_2);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Long.valueOf(TEST_INT_2));
      assertEquals(row.getDataByKey(COL_NUMBER_ID),
          Double.valueOf(TEST_NUM_2));
   }

   private void verifyRowTestSeti(TypedRow row, int i) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_i + i);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Long.valueOf(TEST_INT_i + i));
      assertEquals(row.getDataByKey(COL_NUMBER_ID),
          Double.valueOf(TEST_NUM_i + i));
   }

   private void veriftyRowTestSetManyColumns(TypedRow row, int i) {
      for (int j = 0; j < MANY_COL_NUM_COLUMNS; j++) {
         assertEquals(row.getDataByKey(MANY_COL_STRING_ID + j), "STR_" + j + "_" + i);
         assertEquals(row.getDataByKey(MANY_COL_INTEGER_ID + j), Long.valueOf(i + j));
         assertEquals(row.getDataByKey(MANY_COL_NUMBER_ID + j), Double.valueOf(i * j));
         assertEquals(row.getDataByKey(MANY_COL_BOOL_ID + j), Boolean.valueOf(((i+j)%2)==1));
         assertEquals(row.getDataByKey(MANY_COL_DATE_ID + j), dateString);
      }
   }

   private void verifyTableTestSet(BaseTable table, OrderedColumns columns, int offset) {
      for (int i = 0; i < table.getNumberOfRows(); i++) {
         TypedRow typedRow = new TypedRow(table.getRowAtIndex(i), columns);
         verifyRowTestSeti(typedRow, i + offset);
      }
   }

   @Test
   public void testBinding() {
      UserDbInterfaceImpl serviceInterface = bindToDbService();
       assertNotNull(serviceInterface);
       assertNotNull( "bind did not succeed",
          ((InternalUserDbInterfaceAidlWrapperImpl) serviceInterface.getInternalUserDbInterface()).getDbInterface());
      // TODO: database check function?

      // TODO: add a bind with bind_intent.setClassName instead
   }

   @Test
   public void testDbCreateNDeleteTable() {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnList = new ColumnList(createColumnList());

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, columnList);

         List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);

         // verify single table exists
          assertEquals(1, tableIds.size());
         assertTrue(tableIds.contains(DB_TABLE_ID));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail();
      }
   }

   @Test
   public void testDbCreateNDeleteTableWTransactions() {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnList = new ColumnList(createColumnList());

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteTableWTransactions: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, columnList);

         List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);
         // verify single table exists
          assertEquals(1, tableIds.size());
         assertTrue(tableIds.contains(DB_TABLE_ID));

         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail();
      }
   }

   @Test
   public void testDbInsertSingleRowIntoTable() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbInsertThrowsIllegalArgumentException()
       throws ActionNotAuthorizedException, ServicesAvailabilityException {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          db = serviceInterface.openDatabase(APPNAME);
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

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
          fail("Unexpected " + e.toString());
      } finally {
         // clean up
          assertNotNull(serviceInterface);
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         serviceInterface.closeDatabase(APPNAME, db);
      }
   }

   @Test
   public void testDbInsertSingleRowIntoTableWSingleTransaction()
       throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbInsertSingleRowIntoTableWTwoTransactions()
       throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbInsertSingleRowIntoTableWTwoTransactions: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbInsertTwoRowsIntoTable() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertTwoRowsIntoTable: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

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
             .simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null,
                 new String[] { COL_STRING_ID }, new String[] { "ASC" }, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbInsertTwoRowsIntoTableWSingleTransaction()
       throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);

         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

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
             .simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null,
                 new String[] { COL_STRING_ID }, new String[] { "ASC" }, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbInsertTwoRowsIntoTableWTwoTransactions() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbInsertTwoRowsIntoTableWTwoTransactions: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

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
             .simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null,
                 new String[] { COL_STRING_ID }, new String[] { "ASC" }, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(2, table.getNumberOfRows());

         verifyRowTestSet1(table.getRowAtIndex(0));
         verifyRowTestSet2(table.getRowAtIndex(1));

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbQueryWNoParams() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbQueryWNoParams: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSet1(row);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   /*
       @Test
       public void testDbQueryRowWNoColumnsSpecified() {
           UserDbInterface serviceInterface = bindToDbService();
           try {

               List<Column> columnList = createColumnList();
               ColumnList colList = new ColumnList(columnList);

               DbHandle db = serviceInterface.openDatabase(APPNAME);
              Log.i("openDatabase", "testDbQueryRowWNoColumnsSpecified: " + db
                  .getDatabaseHandle());
               serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

               OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
               UUID rowId =  UUID.randomUUID();

               serviceInterface.insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

               UserTable table = serviceInterface.getRowsWithId(APPNAME, db, DB_TABLE_ID, null, rowId.toString());

               assertEquals(DB_TABLE_ID, table.getTableId());
               assertEquals(1, table.getNumberOfRows());
               TypedRow row = table.getRowAtIndex(0);

               verifyRowTestSet1(row);

               // clean up
               serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

               // verify no tables left
               assertTrue(hasNoTablesInDb(serviceInterface, db));
               serviceInterface.closeDatabase(APPNAME, db);
           } catch (ServicesAvailabilityException e) {
               e.printStackTrace();
               fail(e.getMessage());
           }
       }
   */
   @Test
   public void testDbUpdateAllValues() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbUpdateAllValues: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null,
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
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbUpdateAllValuesWTransactions() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbUpdateAllValuesWTransactions: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null,
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
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbUpdateSingleValue() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      try {
         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbUpdateSingleValue: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table = serviceInterface
             .simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());

         TypedRow row = table.getRowAtIndex(0);
         verifyRowTestSet1(table.getRowAtIndex(0));

         List<Column> singleColumnArray = new ArrayList<>();
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
             Long.valueOf(changeValue));
         assertEquals(row.getDataByKey(COL_NUMBER_ID),
             Double.valueOf(TEST_NUM_1));

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
      if(db != null) {
         try {
            // clean up
            serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);
            // verify no tables left
            assertTrue(hasNoTablesInDb(serviceInterface, db));
            serviceInterface.closeDatabase(APPNAME, db);
         } catch (ServicesAvailabilityException e) {
            // do nothing
         }
      }
   }
   }

   @Test
   public void testDbInsertNDeleteSingleRowIntoTable() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertNDeleteSingleRowIntoTable: " + db.getDatabaseHandle());

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
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   private void internalTestDbCreateNDeleteLargeTable(final int NUM_ROWS) throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      if (false) {
         return;
      }
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLargeTable: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

         Set<UUID> rowIds = new HashSet<>(NUM_ROWS);
         for (int i = 0; i < NUM_ROWS; i++) {
            if ( i %100 == 0 ) {
               Log.i("openDatabase", "testDbCreateNDeleteLargeTable: inserting row " + i );
            }

            UUID rowId = UUID.randomUUID();
            rowIds.add(rowId);

            // insert row
            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, null, null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(NUM_ROWS, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSeti(row, 0);

         // delete row
         Iterator<UUID> rowIdIterator = rowIds.iterator();
         for (int i = 0; i < NUM_ROWS; i++) {
            if ( i %100 == 0 ) {
               Log.i("openDatabase", "testDbCreateNDeleteLargeTable: deleting row " + i );
            }

            UUID rowId = rowIdIterator.next();
            serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         }

         table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(0, table.getNumberOfRows());

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbCreateNDeleteSmallTable() throws ActionNotAuthorizedException {
      final int NUM_ROWS = 300;
      internalTestDbCreateNDeleteLargeTable(NUM_ROWS);
   }

   @LargeTest
   public void testDbCreateNDeleteLargeTable() throws ActionNotAuthorizedException {
      final int NUM_ROWS = 10000;
      internalTestDbCreateNDeleteLargeTable(NUM_ROWS);
   }

   private void internalTestDbCreateNVerifyNDeleteLargeTableManyColumns(final int NUM_ROWS)
       throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      try {

         List<Column> columnList = createManyColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
         db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbCreateNVerifyNDeleteLargeTableManyColumns: " + db.getDatabaseHandle());

         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);

         Set<UUID> rowIds = new HashSet<>(NUM_ROWS);
         for (int i = 0; i < NUM_ROWS; i++) {
            if ( i %100 == 0 ) {
               Log.i("openDatabase", "testDbCreateNVerifyNDeleteLargeTableManyColumns: inserting row " + i );
            }

            UUID rowId = UUID.randomUUID();
            rowIds.add(rowId);

            // insert row
            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns,
                    contentValuesTestSetManyColumns(i), rowId.toString());
         }

         UserTable table1 = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, null, null);
         UserTable table2 = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, null, null);

         assertEquals(DB_TABLE_ID, table1.getTableId());
         assertEquals(DB_TABLE_ID, table2.getTableId());
         assertEquals(NUM_ROWS, table1.getNumberOfRows());
         assertEquals(NUM_ROWS, table2.getNumberOfRows());

         for (int i = 0; i < NUM_ROWS; i++) {
            TypedRow row1 = table1.getRowAtIndex(i);
            veriftyRowTestSetManyColumns(row1, i);

            TypedRow row2 = table2.getRowAtIndex(i);
            veriftyRowTestSetManyColumns(row2, i);
         }

         // delete row
         Iterator<UUID> rowIdIterator = rowIds.iterator();
         for (int i = 0; i < NUM_ROWS; i++) {
            if ( i %100 == 0 ) {
               Log.i("openDatabase", "testDbCreateNDeleteLargeTable: deleting row " + i );
            }

            UUID rowId = rowIdIterator.next();
            serviceInterface.deleteRowWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
         }

         table1 = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null,
             null, null, null, null, null, null, null);
         assertEquals(DB_TABLE_ID, table1.getTableId());
         assertEquals(0, table1.getNumberOfRows());


      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         if(db != null) {
            try {
            // clean up
            serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);
            // verify no tables left
            assertTrue(hasNoTablesInDb(serviceInterface, db));
            serviceInterface.closeDatabase(APPNAME, db);
            } catch (ServicesAvailabilityException e) {
               // do nothing
            }
         }
      }
   }

   @Test
   public void testDbCreateNVerifyNDeleteSmallTableManyColumns()
       throws ActionNotAuthorizedException {
      final int NUM_ROWS = 300;
      internalTestDbCreateNVerifyNDeleteLargeTableManyColumns(NUM_ROWS);
   }

   @LargeTest
   public void testDbCreateNVerifyNDeleteLargeTableManyColumns()
       throws ActionNotAuthorizedException {
      final int NUM_ROWS = 2500;
      internalTestDbCreateNVerifyNDeleteLargeTableManyColumns(NUM_ROWS);
   }

   @Test
   public void testDbUpdateWTwoServiceConnections() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface1 = bindToDbService();
      UserDbInterface serviceInterface2 = bindToDbService();

      try {
         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);
         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

          assertNotNull(serviceInterface1);
         DbHandle db1 = serviceInterface1.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbUpdateWTwoServiceConnections db1: " + db1.getDatabaseHandle());

          assertNotNull(serviceInterface2);
          DbHandle db2 = serviceInterface2.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbUpdateWTwoServiceConnections db2: " + db2.getDatabaseHandle());

         serviceInterface1.createOrOpenTableWithColumns(APPNAME, db1, DB_TABLE_ID, colList);

         serviceInterface1
             .insertRowWithId(APPNAME, db1, DB_TABLE_ID, columns, contentValuesTestSet1(),
                 rowId.toString());

         UserTable table;

         // use service connection 1 to verify db is in correct state
         table = serviceInterface1
             .simpleQuery(APPNAME, db1, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet1(table.getRowAtIndex(0));

         // use service connection 2 to verify db is in correct state
         table = serviceInterface2
             .simpleQuery(APPNAME, db2, DB_TABLE_ID, columns, null, null, null, null, null, null,
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
             .simpleQuery(APPNAME, db2, DB_TABLE_ID, columns, null, null, null, null, null, null,
                 null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet2(table.getRowAtIndex(0));

         // use service connection 1 to verify db is in correct state
         table = serviceInterface1
             .simpleQuery(APPNAME, db1, DB_TABLE_ID, columns, null, null, null, null, null, null,
             null, null);
         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         verifyRowTestSet2(table.getRowAtIndex(0));

         // clean up
         serviceInterface1.deleteTableAndAllData(APPNAME, db1, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface1, db1));
         serviceInterface1.closeDatabase(APPNAME, db1);

      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbLimitNoOffset() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbLimitNoOffset: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,0);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbLimitNoOffset: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }


   @Test
   public void testDbLimitNoOffsetArbitraryQuery() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbLimitNoOffsetArbitraryQuery: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         BaseTable table = serviceInterface.arbitrarySqlQuery(APPNAME, db, DB_TABLE_ID,
             "SELECT * FROM " + DB_TABLE_ID, null, 10, null);
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table, columns,0);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbLimitNoOffsetArbitraryQuery: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbLimitOver() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbLimitOver: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 60 , null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(50, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,0);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbLimitOver: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbLimitOverArbitraryQuery() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbLimitOverArbitraryQuery: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         BaseTable table = serviceInterface.arbitrarySqlQuery(APPNAME, db, DB_TABLE_ID,
             "SELECT * FROM " + DB_TABLE_ID, null, 60, null);

         assertEquals(50, table.getNumberOfRows());

         verifyTableTestSet(table, columns,0);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbLimitOverArbitraryQuery: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbOffsetNoLimit() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbOffsetNoLimit: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
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
             .simpleQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, orderByCol,
                 orderByDir, null, 10);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(40, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,10);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbOffsetNoLimit: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbOffsetNoLimitArbitraryQuery() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbOffsetNoLimitArbitraryQuery: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         BaseTable table = serviceInterface.arbitrarySqlQuery(APPNAME, db, DB_TABLE_ID,
             "SELECT * FROM " + DB_TABLE_ID, null, null, 10);

         assertEquals(40, table.getNumberOfRows());

         verifyTableTestSet(table, columns, 10);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbOffsetNoLimitArbitraryQuery: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbLimitWithOffset() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbLimitWithOffset: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , 10);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,10);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbLimitWithOffset: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbLimitWithOffsetArbitraryQuery() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbLimitWithOffsetArbitraryQuery: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         BaseTable table = serviceInterface.arbitrarySqlQuery(APPNAME, db, DB_TABLE_ID,
             "SELECT * FROM " + DB_TABLE_ID, null, 10, 10);

         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table, columns,10);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbLimitWithOffsetArbitraryQuery: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbQueryResumeForward() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,0);

         table = serviceInterface
             .resumeSimpleQuery(APPNAME, db, columns, table.resumeQueryForward(10));

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,10);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbQueryResumeForwardEnd() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 60 , null);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(50, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,0);

         assertNull(table.resumeQueryForward(1));

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbQueryResumeBackward() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , 20);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,20);

         table = serviceInterface
             .resumeSimpleQuery(APPNAME, db, columns, table.resumeQueryBackward(10));

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,10);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbQueryResumeBackwardEnd() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db,
             DB_TABLE_ID, colList);

         for (int i = 0; i < 50; i++) {
            UUID rowId = UUID.randomUUID();

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(i),
                    rowId.toString());
         }

         String[] orderByCol = new String[] { COL_INTEGER_ID };
         String[] orderByDir = new String[] { "ASC" };
         UserTable table = serviceInterface.simpleQuery(APPNAME, db, DB_TABLE_ID, columns,
             null, null, null, null, orderByCol, orderByDir, 10 , 10);

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns,10);

         table = serviceInterface
             .resumeSimpleQuery(APPNAME, db, columns, table.resumeQueryBackward(10));

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(10, table.getNumberOfRows());

         verifyTableTestSet(table.getBaseTable(), columns, 0);

         assertNull(table.resumeQueryBackward(1));

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbCreateNDeleteLocalOnlyTable() {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         OrderedColumns columns = serviceInterface
             .createLocalOnlyTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface.deleteLocalOnlyTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbInsertSingleRowIntoLocalOnlyTable() {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbCreateNDeleteLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
         OrderedColumns columns = serviceInterface
             .createLocalOnlyTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         // ensure the table is empty
         serviceInterface.deleteLocalOnlyRows(APPNAME, db, DB_TABLE_ID, null, null);

         serviceInterface
             .insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());

         BaseTable results = serviceInterface.simpleQueryLocalOnlyTables(APPNAME, db,
             DB_TABLE_ID, null, null, null, null, null, null, null, null);

         assertEquals(1, results.getNumberOfRows());

         verifyRowTestSet1(new TypedRow(results.getRowAtIndex(0), columns));

         // clean up
         serviceInterface.deleteLocalOnlyTable(APPNAME, db, columns.getTableId());

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

   @Test
   public void testDbInsertTwoRowsIntoLocalOnlyTable() {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);

         OrderedColumns columns = serviceInterface
             .createLocalOnlyTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());
         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet2());


         BaseTable results = serviceInterface.simpleQueryLocalOnlyTables(APPNAME, db,
             DB_TABLE_ID, null, null, null, null, null, null, null, null);

         assertEquals(2, results.getNumberOfRows());

         verifyRowTestSet1(new TypedRow(results.getRowAtIndex(0), columns));
         verifyRowTestSet2(new TypedRow(results.getRowAtIndex(1), columns));

         // clean up
         serviceInterface.deleteLocalOnlyTable(APPNAME, db, columns.getTableId());

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

   @Test
   public void testDbUpdateAllValuesLocalOnlyTable()  {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);

         OrderedColumns columns = serviceInterface
             .createLocalOnlyTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         // ensure the table is empty
         serviceInterface.deleteLocalOnlyRows(APPNAME, db, DB_TABLE_ID, null, null);

         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());


         BaseTable results = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());
         verifyRowTestSet1(new TypedRow(results.getRowAtIndex(0), columns));

         String whereClause = COL_STRING_ID + "=?";
         BindArgs bindArgs = new BindArgs(new Object[] { TEST_STR_1 });
         serviceInterface
             .updateLocalOnlyRows(APPNAME, db, DB_TABLE_ID, contentValuesTestSet2(), whereClause,
                 bindArgs);

         results = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());
         verifyRowTestSet2(new TypedRow(results.getRowAtIndex(0), columns));

         // clean up
         serviceInterface.deleteLocalOnlyTable(APPNAME, db, columns.getTableId());

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

   @Test
   public void testDbUpdateSingleValueLocalOnlyTable() {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);

         OrderedColumns columns = serviceInterface
             .createLocalOnlyTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         // ensure the table is empty
         serviceInterface.deleteLocalOnlyRows(APPNAME, db, DB_TABLE_ID, null, null);

         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());


         BaseTable results = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());
         verifyRowTestSet1(new TypedRow(results.getRowAtIndex(0), columns));

         List<Column> singleColumnArray = new ArrayList<>();
         singleColumnArray.add(new Column(COL_INTEGER_ID, "column Integer", "integer", null));
         ContentValues singleValue = new ContentValues();
         int changeValue = 3;
         singleValue.put(COL_INTEGER_ID, changeValue);

         String whereClause = COL_STRING_ID + "=?";
         BindArgs bindArgs = new BindArgs(new Object[] { TEST_STR_1 });
         serviceInterface
             .updateLocalOnlyRows(APPNAME, db, DB_TABLE_ID, singleValue, whereClause, bindArgs);

         results = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());

         TypedRow row = new TypedRow(results.getRowAtIndex(0),columns);
         assertEquals(row.getRawStringByKey(COL_STRING_ID), TEST_STR_1);
         assertEquals(row.getRawStringByKey(COL_INTEGER_ID), Long.toString(changeValue));
         assertEquals(row.getRawStringByKey(COL_NUMBER_ID), Double.toString(TEST_NUM_1));

         // clean up
         serviceInterface.deleteLocalOnlyTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbInsertNDeleteSingleRowIntoLocalOnlyTable() {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

          assertNotNull(serviceInterface);
          DbHandle db = serviceInterface.openDatabase(APPNAME);
         OrderedColumns columns = serviceInterface
             .createLocalOnlyTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);

         assertEquals(LOCAL_ONLY_DB_TABLE_ID, columns.getTableId());

         serviceInterface.insertLocalOnlyRow(APPNAME, db, DB_TABLE_ID, contentValuesTestSet1());


         BaseTable results = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(1, results.getNumberOfRows());
         verifyRowTestSet1(new TypedRow(results.getRowAtIndex(0), columns));

         String whereClause = COL_STRING_ID + "=?";
         BindArgs bindArgs = new BindArgs(new Object[] { TEST_STR_1 });
         serviceInterface.deleteLocalOnlyRows(APPNAME, db, DB_TABLE_ID, whereClause, bindArgs);

         results = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, db, DB_TABLE_ID, null, null, null, null, null,
                 null, null, null);

         assertEquals(0, results.getNumberOfRows());

         // clean up
         serviceInterface.deleteLocalOnlyTable(APPNAME, db, columns.getTableId());

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);

      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

}
