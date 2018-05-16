package org.opendatakit.database.service;

import android.content.ContentValues;
import android.support.annotation.NonNull;
import android.util.Log;
import org.junit.Test;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.exception.ServicesAvailabilityException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OdkDatabaseTypesTest extends OdkDatabaseTestAbstractBase {

   private static final String LOCAL_ONLY_DB_TABLE_ID = "L_" + DB_TABLE_ID;

   private static final String COL_INTEGER_ID = "columnInteger";
   private static final String COL_NUMBER_ID = "columnNumber";
   private static final String COL_BOOL_ID = "columnBool";
   private static final String COL_ROWPATH_ID = "columnRowPath";
   private static final String COL_CONFIGPATH_ID = "columnConfigPath";
   private static final String COL_STRING_ID = "columnString";
   private static final String COL_ARRAY_ID = "columnArrayPath";
   private static final String COL_ITEM_STRING_ID = "columnArrayPath_items";
   private static final String COL_GEO_OBJ_ID = "columnGeopointObject";
   private static final String COL_GEO_OBJ_ID_LAT = "columnGeopointObject_lat";
   private static final String COL_GEO_OBJ_ID_LONG = "columnGeopointObject_long";
   private static final String COL_GEO_OBJ_ID_ALT = "columnGeopointObject_alt";
   private static final String COL_GEO_OBJ_ID_ACC = "columnGeopointObject_acc";

   private static final String TEST_STR_1 = "TestBoolStr1";
   private static final int TEST_INT_1 = 1;
   private static final double TEST_NUM_1 = 1.0;
   private static final boolean TEST_BOOL_1 = true;

   private static final int TEST_INT_i = 0;
   private static final double TEST_NUM_i = 0.0;
   private static final String TEST_ROWPATH_i = "TestRowPath";
   private static final String TEST_CONFIGPATH_i = "TestConfigPath";
   private static final String TEST_STR_i = "TestBoolStr";
   private static final String TEST_ARRAY_i = "[\"Test1\",\"Test2\"]";

   private static final ArrayList<String> TEST_ARRAY_i_CHECK = new ArrayList<String>();

   static {
      TEST_ARRAY_i_CHECK.addAll(Arrays.asList("Test1", "Test2"));
   }

   @Override protected void setUpBefore() {
      return;
   }

   @Override protected void tearDownBefore() {
      return;
   }

   @NonNull private List<Column> createColumnList() {
      List<Column> columns = new ArrayList<Column>();

      columns
          .add(new Column(COL_INTEGER_ID, "column Integer", ElementDataType.integer.name(), null));
      columns.add(new Column(COL_NUMBER_ID, "column Number", ElementDataType.number.name(), null));
      columns.add(new Column(COL_BOOL_ID, "column Bool", ElementDataType.bool.name(), null));
      columns
          .add(new Column(COL_ROWPATH_ID, "column Rowpath", ElementDataType.rowpath.name(), null));
      columns.add(
          new Column(COL_CONFIGPATH_ID, "column Configpath", ElementDataType.configpath.name(),
              null));
      columns.add(new Column(COL_STRING_ID, "columnString", ElementDataType.string.name(), null));

      columns.add(new Column(COL_ITEM_STRING_ID, "items", ElementDataType.string.name(), null));
      columns.add(new Column(COL_ARRAY_ID, "columnArrayPath", ElementDataType.array.name(),
          "[\"" + COL_ITEM_STRING_ID + "\"]"));

      columns.add(new Column(COL_GEO_OBJ_ID_ACC, "acc", ElementDataType.number.name(), null));
      columns.add(new Column(COL_GEO_OBJ_ID_ALT, "alt", ElementDataType.number.name(), null));
      columns.add(new Column(COL_GEO_OBJ_ID_LAT, "lat", ElementDataType.number.name(), null));
      columns.add(new Column(COL_GEO_OBJ_ID_LONG, "long", ElementDataType.number.name(), null));
      columns.add(new Column(COL_GEO_OBJ_ID, COL_GEO_OBJ_ID, ElementDataType.object.name(),
          "[\"" + COL_GEO_OBJ_ID_ACC + "\",\"" + COL_GEO_OBJ_ID_ALT + "\",\"" + COL_GEO_OBJ_ID_LAT
              + "\",\"" + COL_GEO_OBJ_ID_LONG + "\"]"));

      return columns;
   }

   private ContentValues contentValuesTestSet1() {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_1);
      cv.put(COL_INTEGER_ID, TEST_INT_1);
      cv.put(COL_BOOL_ID, TEST_BOOL_1);

      cv.put(COL_GEO_OBJ_ID_ACC, TEST_NUM_1);
      cv.put(COL_GEO_OBJ_ID_ALT, TEST_NUM_1);
      cv.put(COL_GEO_OBJ_ID_LAT, TEST_NUM_1);
      cv.put(COL_GEO_OBJ_ID_LONG, TEST_NUM_1);

      return cv;
   }

   private ContentValues contentValuesTestSeti(int i) {
      ContentValues cv = new ContentValues();

      cv.put(COL_INTEGER_ID, TEST_INT_i + i);
      cv.put(COL_NUMBER_ID, TEST_NUM_i + i);
      cv.put(COL_BOOL_ID, (i % 2 != 0));
      cv.put(COL_ROWPATH_ID, TEST_ROWPATH_i + i);
      cv.put(COL_CONFIGPATH_ID, TEST_CONFIGPATH_i + i);
      cv.put(COL_ARRAY_ID, TEST_ARRAY_i);
      cv.put(COL_STRING_ID, TEST_STR_i + i);
      cv.put(COL_GEO_OBJ_ID_ACC, TEST_NUM_i + i);
      cv.put(COL_GEO_OBJ_ID_ALT, TEST_NUM_i + i);
      cv.put(COL_GEO_OBJ_ID_LAT, TEST_NUM_i + i);
      cv.put(COL_GEO_OBJ_ID_LONG, TEST_NUM_i + i);

      return cv;
   }

   private void verifyRowTestSet1(TypedRow row) {
      assertEquals(row.getDataByKey(COL_INTEGER_ID), Long.valueOf(TEST_INT_1));
      assertNull(row.getDataByKey(COL_NUMBER_ID));
      assertEquals(row.getDataByKey(COL_BOOL_ID), Boolean.valueOf(TEST_BOOL_1));
      assertNull(row.getDataByKey(COL_ROWPATH_ID));
      assertNull(row.getDataByKey(COL_CONFIGPATH_ID));
      assertNull(row.getDataByKey(COL_ARRAY_ID));
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);

      assertEquals(row.getDataByKey(COL_GEO_OBJ_ID_ACC), Double.valueOf(TEST_NUM_1));
      assertEquals(row.getDataByKey(COL_GEO_OBJ_ID_ALT), Double.valueOf(TEST_NUM_1));
      assertEquals(row.getDataByKey(COL_GEO_OBJ_ID_LAT), Double.valueOf(TEST_NUM_1));
      assertEquals(row.getDataByKey(COL_GEO_OBJ_ID_LONG), Double.valueOf(TEST_NUM_1));

   }

   private void verifyRowTestSeti(TypedRow row, int i) {
      assertEquals(row.getDataByKey(COL_INTEGER_ID), Long.valueOf(TEST_INT_i + i));
      assertEquals(row.getDataByKey(COL_NUMBER_ID), Double.valueOf(TEST_NUM_i + i));
      assertEquals(row.getDataByKey(COL_BOOL_ID), Boolean.valueOf((i % 2 != 0)));
      assertEquals(row.getDataByKey(COL_ROWPATH_ID), TEST_ROWPATH_i + i);
      assertEquals(row.getDataByKey(COL_CONFIGPATH_ID), TEST_CONFIGPATH_i + i);
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_i + i);

      assertEquals(row.getDataByKey(COL_ARRAY_ID), TEST_ARRAY_i_CHECK);
      assertEquals(row.getDataByKey(COL_GEO_OBJ_ID_ACC), Double.valueOf(TEST_NUM_i + i));
      assertEquals(row.getDataByKey(COL_GEO_OBJ_ID_ALT), Double.valueOf(TEST_NUM_i + i));
      assertEquals(row.getDataByKey(COL_GEO_OBJ_ID_LAT), Double.valueOf(TEST_NUM_i + i));
      assertEquals(row.getDataByKey(COL_GEO_OBJ_ID_LONG), Double.valueOf(TEST_NUM_i + i));
   }

   @Test public void testDbInsertSingleRowIntoTable() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         db = serviceInterface.openDatabase(APPNAME);
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
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         if (db != null) {
            try {
               // clean up
               serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

               // verify no tables left
               assertTrue(hasNoTablesInDb(serviceInterface, db));
               serviceInterface.closeDatabase(APPNAME, db);
            } catch (ServicesAvailabilityException e) {
               // let exception proceed
            }
         }
      }
   }

   @Test public void testDbInsertTwoRowsIntoTable() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         db = serviceInterface.openDatabase(APPNAME);
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId1 = UUID.randomUUID();
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(1),
                 rowId1.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId1.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         verifyRowTestSeti(row, 1);

         UUID rowId2 = UUID.randomUUID();
         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSeti(2),
                 rowId2.toString());

         table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId2.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         row = table.getRowAtIndex(0);

         verifyRowTestSeti(row, 2);

         // clean up
         serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

         // verify no tables left
         assertTrue(hasNoTablesInDb(serviceInterface, db));
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         if (db != null) {
            try {
               // clean up
               serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

               // verify no tables left
               assertTrue(hasNoTablesInDb(serviceInterface, db));
               serviceInterface.closeDatabase(APPNAME, db);
            } catch (ServicesAvailabilityException e) {
               // let exception proceed
            }
         }
      }
   }

   @Test public void testDbInsertCheckpointRowWithBooleanIntoTable()
       throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbInsertCheckpointRowWithBooleanIntoTable: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         serviceInterface
             .insertCheckpointRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(),
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

   @Test public void testArbitraryQueryUsingAliasColumnName() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      DbHandle db = null;
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);
         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase",
             "testDbInsertCheckpointRowWithBooleanIntoTable: " + db.getDatabaseHandle());

         for(int numRows = 10; numRows < 300; numRows = numRows + 25) {
            serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

            for (int i = 0; i < numRows; ++i) {
               serviceInterface.insertCheckpointRowWithId(APPNAME, db, DB_TABLE_ID, columns,
                   contentValuesTestSeti(i), UUID.randomUUID().toString());
            }

            BaseTable table = serviceInterface
                .arbitrarySqlQuery(APPNAME, db, DB_TABLE_ID, "SELECT " + "* FROM " + DB_TABLE_ID
                        + " ORDER BY " + COL_INTEGER_ID + " ASC",
                    null, null, null);

            assertEquals(numRows, table.getNumberOfRows());

            for (int i = 0; i < numRows; ++i) {
               TypedRow row = new TypedRow(table.getRowAtIndex(i), columns);
               verifyRowTestSeti(row, i);
            }

            table = serviceInterface.arbitrarySqlQuery(APPNAME, db, DB_TABLE_ID,
                "SELECT " + "count(*) AS Total FROM " + DB_TABLE_ID, null, null, null);

            TypedRow row = new TypedRow(table.getRowAtIndex(0), columns);
            Object value = row.getDataByKey("Total");
            if (value instanceof String) {
               int count = Integer.valueOf((String) value);
               assertEquals(numRows, count);
            } else {
               fail("Should have returned a string because type of column was unknown");
            }

            // clean up
            serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

            // verify no tables left
            assertTrue(hasNoTablesInDb(serviceInterface, db));
         }
         serviceInterface.closeDatabase(APPNAME, db);
         Log.i("closeDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         if (db != null) {
            try {
               // clean up
               serviceInterface.deleteTableAndAllData(APPNAME, db, DB_TABLE_ID);

               // verify no tables left
               assertTrue(hasNoTablesInDb(serviceInterface, db));
               serviceInterface.closeDatabase(APPNAME, db);
            } catch (ServicesAvailabilityException e) {
               // let exception proceed
            }
         }
      }
   }
}
