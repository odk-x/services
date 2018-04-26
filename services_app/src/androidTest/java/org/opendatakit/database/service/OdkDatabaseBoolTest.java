package org.opendatakit.database.service;

import android.content.ContentValues;
import android.support.annotation.NonNull;
import android.util.Log;
import org.junit.Test;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.exception.ServicesAvailabilityException;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OdkDatabaseBoolTest extends OdkDatabaseTestAbstractBase {

   private static final String LOCAL_ONLY_DB_TABLE_ID = "L_" + DB_TABLE_ID;

   private static final String COL_INTEGER_ID = "columnInteger";
   private static final String COL_BOOL_ID = "columnBool";
   private static final String COL_STRING_ID = "columnString";

   private static final String TEST_STR_1 = "TestBoolStr1";
   private static final int TEST_INT_1 = 1;
   private static final boolean TEST_BOOL_1 = true;
   private static final String TEST_STR_2 = "TestBoolStrStr2";
   private static final int TEST_INT_2 = 2;
   private static final boolean TEST_BOOL_2 = false;
   private static final String TEST_STR_i = "TestBoolStr";
   private static final int TEST_INT_i = 0;

   @Override protected void setUpBefore() {
      return;
   }

   @Override protected void tearDownBefore() {
      return;
   }

   @NonNull private List<Column> createColumnList() {
      List<Column> columns = new ArrayList<Column>();

      columns.add(new Column(COL_INTEGER_ID, "column Integer", "integer", null));
      columns.add(new Column(COL_BOOL_ID, "column Bool", "boolean", null));
      columns.add(new Column(COL_STRING_ID, "column String", "string", null));

      return columns;
   }

   private ContentValues contentValuesTestSet1() {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_1);
      cv.put(COL_INTEGER_ID, TEST_INT_1);
      cv.put(COL_BOOL_ID, TEST_BOOL_1);

      return cv;
   }

   private ContentValues contentValuesTestSet2() {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_2);
      cv.put(COL_INTEGER_ID, TEST_INT_2);
      cv.put(COL_BOOL_ID, TEST_BOOL_2);

      return cv;
   }

   private ContentValues contentValuesTestSeti(int i) {
      ContentValues cv = new ContentValues();

      cv.put(COL_STRING_ID, TEST_STR_i + i);
      cv.put(COL_INTEGER_ID, TEST_INT_i + i);
      cv.put(COL_BOOL_ID, (i%2 != 0));

      return cv;
   }

   private void verifyRowTestSet1(TypedRow row) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
      assertEquals(row.getDataByKey(COL_INTEGER_ID), Long.valueOf(TEST_INT_1));
      assertEquals(row.getDataByKey(COL_BOOL_ID), Boolean.valueOf(TEST_BOOL_1));
   }

   private void verifyRowTestSet2(TypedRow row) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_2);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Long.valueOf(TEST_INT_2));
      assertEquals(row.getDataByKey(COL_BOOL_ID),
          Boolean.valueOf(TEST_BOOL_2));
   }

   private void verifyRowTestSeti(TypedRow row, int i) {
      assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_i + i);
      assertEquals(row.getDataByKey(COL_INTEGER_ID),
          Long.valueOf(TEST_INT_i + i));
      assertEquals(row.getDataByKey(COL_BOOL_ID),
          Boolean.valueOf((i%2 != 0)));
   }

   @Test
   public void testBinding() {
      UserDbInterfaceImpl serviceInterface = bindToDbService();
      assertNotNull( "bind did not succeed",
          ((InternalUserDbInterfaceAidlWrapperImpl) serviceInterface.getInternalUserDbInterface()).getDbInterface());
   }

   @Test
   public void testDbInsertSingleRowIntoTable() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

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
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbInsertNullRowIntoTable() throws ActionNotAuthorizedException {
      UserDbInterface serviceInterface = bindToDbService();
      try {

         List<Column> columnList = createColumnList();
         ColumnList colList = new ColumnList(columnList);

         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertSingleRowIntoTable: " + db.getDatabaseHandle());
         serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

         OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
         UUID rowId = UUID.randomUUID();

         ContentValues cv = new ContentValues();

         cv.put(COL_STRING_ID, TEST_STR_1);
         cv.put(COL_INTEGER_ID, TEST_INT_1);

         serviceInterface
             .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, cv, rowId.toString());

         UserTable table = serviceInterface
             .getRowsWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

         assertEquals(DB_TABLE_ID, table.getTableId());
         assertEquals(1, table.getNumberOfRows());
         TypedRow row = table.getRowAtIndex(0);

         assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
         assertEquals(row.getDataByKey(COL_INTEGER_ID), Long.valueOf(TEST_INT_1));
         Boolean value = (Boolean) row.getDataByKey(COL_BOOL_ID);
         assertEquals(value, null);

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
   public void testDbInsertTwoRowsIntoLocalOnlyTable() {
      UserDbInterface serviceInterface = bindToDbService();
      try {
         ColumnList columnListObj = new ColumnList(createColumnList());

         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("openDatabase", "testDbInsertTwoRowsIntoLocalOnlyTable: " + db.getDatabaseHandle());
         // TODO: why do we have a dbHandle and APPNAME?
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
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDbUpdateSingleValue() throws ActionNotAuthorizedException {
      {
         UserDbInterface serviceInterface = bindToDbService();
         DbHandle db = null;
         try {
            ColumnList columnListObj = new ColumnList(createColumnList());
            db = serviceInterface.openDatabase(APPNAME);
            Log.i("openDatabase", "testDbUpdateSingleValue: " + db.getDatabaseHandle());
            OrderedColumns columns = serviceInterface.createOrOpenTableWithColumns(APPNAME, db, DB_TABLE_ID, columnListObj);
            UUID rowId = UUID.randomUUID();

            assertEquals(DB_TABLE_ID, columns.getTableId());

            serviceInterface
                .insertRowWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

            BaseTable results = serviceInterface
                .simpleQuery(APPNAME, db, DB_TABLE_ID, null, null, null, null, null, null, null,
                    null);

            assertEquals(1, results.getNumberOfRows());
            verifyRowTestSet1(new TypedRow(results.getRowAtIndex(0), columns));

            ContentValues singleValue = new ContentValues();
            int changeValue = 3;
            singleValue.put(COL_INTEGER_ID, changeValue);

            serviceInterface.updateRowWithId(APPNAME, db, DB_TABLE_ID, columns, singleValue, rowId.toString());

            results = serviceInterface
                .simpleQuery(APPNAME, db, DB_TABLE_ID, null, null, null, null, null, null, null,
                    null);

            assertEquals(1, results.getNumberOfRows());

            TypedRow row = new TypedRow(results.getRowAtIndex(0), columns);
            assertEquals(row.getDataByKey(COL_STRING_ID), TEST_STR_1);
            assertEquals(row.getDataByKey(COL_INTEGER_ID), Long.valueOf(changeValue));
            assertEquals(row.getDataByKey(COL_BOOL_ID), Boolean.valueOf(TEST_BOOL_1));

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
                  // let exception proceed
               }
            }
         }
      }
   }
}

