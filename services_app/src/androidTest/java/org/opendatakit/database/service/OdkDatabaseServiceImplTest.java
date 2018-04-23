package org.opendatakit.database.service;

import android.content.ContentValues;
import android.database.Cursor;
import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.RoleConsts;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnList;
import org.opendatakit.database.data.KeyValueStoreEntry;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.database.data.Row;
import org.opendatakit.database.data.TableMetaDataEntries;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.provider.ChoiceListColumns;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.provider.KeyValueStoreColumns;
import org.opendatakit.provider.TableDefinitionsColumns;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static android.text.TextUtils.join;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(AndroidJUnit4.class) public class OdkDatabaseServiceImplTest
    extends OdkDatabaseTestAbstractBase {
   private static final String TAG = OdkDatabaseServiceImplTest.class.getSimpleName();
   private static final double delta = 0.00001;

   public static final String SHOULD_THROW_EXCEPTION = "Should have thrown an exception";
   public static final String TABLE_NAME1 = "tbl";
   public static final String TABLE_NAME2 = "tb2";
   public static final String TABLE_LOCAL = "tablelocal";
   public static final String TEA_HOUSES_TBL_NAME = "tea_houses";

   public static final String CHOICE_LIST_JSON = "[{\"choice_list_name\":\"climate_types\","
       + "\"data_value\":\"moderate\",\"display\":{\"title\":{\"text\":{\"default\":\"Moderate\",\"es\":\"Moderate\"}}},\"_row_num\":41},{\"choice_list_name\":\"climate_types\",\"data_value\":\"temperate\",\"display\":{\"title\":{\"text\":{\"default\":\"Temperate\",\"es\":\"Templado\"}}},\"_row_num\":42},{\"choice_list_name\":\"climate_types\",\"data_value\":\"hot\",\"display\":{\"title\":{\"text\":{\"default\":\"Hot\",\"es\":\"Caliente\"}}},\"_row_num\":43}]";
   public static final String FAKE_LIST_OF_ROLES = "['list', 'of', 'roles']";
   public static final String TEST_USERNAME = "test";
   public static final String TEST_DEFAULT_GROUP = "default group here";
   public static final String PARTITION = "partition";
   public static final String ASPECT = "aspect";
   public static final String COLUMN_ID1 = "columnId";
   public static final String COLUMN_ID3 = "columnId3";
   public static final String COLUMN_ID2 = "columnId2";
   public static final String SYNC_STATE_COLNAME = "_sync_state";
   public static final String CONFLICT_TYPE_COLNAME = "_conflict_type";
   public static final String SAVEPOINT_TYPE_COLNAME = "_savepoint_type";

   private UserDbInterface serviceInterface;
   private PropertiesSingleton props;
   private DbHandle dbHandle;

   protected void setUpBefore() {
      try {
         serviceInterface = bindToDbService();
         dbHandle = serviceInterface.openDatabase(APPNAME);

         props = CommonToolProperties.get(InstrumentationRegistry.getTargetContext(), APPNAME);
         props.clearSettings();
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   protected void tearDownBefore() {
      try {
         props.clearSettings();
         if (dbHandle != null) {
            serviceInterface.closeDatabase(APPNAME, dbHandle);
         }
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testGetActiveUser() {
      try {
         String str = serviceInterface.getActiveUser(APPNAME);
         assertEquals(str, "anonymous");
         setProp(props, CommonToolProperties.KEY_AUTHENTICATION_TYPE,
             props.CREDENTIAL_TYPE_USERNAME_PASSWORD);
         setProp(props, CommonToolProperties.KEY_USERNAME, "insert username here");
         setProp(props, CommonToolProperties.KEY_AUTHENTICATED_USER_ID, "user_id_here");
         setProp(props, CommonToolProperties.KEY_ROLES_LIST, FAKE_LIST_OF_ROLES);
         assertEquals(serviceInterface.getActiveUser(APPNAME), "user_id_here");
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         props.clearSettings();
      }
   }

   @Test public void testGetRolesList() {
      try {
         assertNull(serviceInterface.getRolesList(APPNAME));
         setProp(props, CommonToolProperties.KEY_ROLES_LIST, FAKE_LIST_OF_ROLES);
         assertEquals(serviceInterface.getRolesList(APPNAME), FAKE_LIST_OF_ROLES);
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         props.clearSettings();
      }
   }

   @Test public void testGetDefaultGroup() {
      try {
         String defaultGroup = serviceInterface.getDefaultGroup(APPNAME);
         assertTrue(defaultGroup == null || defaultGroup.isEmpty());
         setProp(props, CommonToolProperties.KEY_DEFAULT_GROUP, TEST_DEFAULT_GROUP);
         assertEquals(serviceInterface.getDefaultGroup(APPNAME), TEST_DEFAULT_GROUP);
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         props.clearSettings();
      }

   }

   @Test public void testGetUsersList() {
      try {
         assertNull(serviceInterface.getUsersList(APPNAME));
         setProp(props, CommonToolProperties.KEY_USERS_LIST, TEST_USERNAME);
         assertEquals(serviceInterface.getUsersList(APPNAME), TEST_USERNAME);
         props.clearSettings();
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         props.clearSettings();
      }
   }

   @Test public void testCreateLocalOnlyTableWithColumns() {

      try {

         serviceInterface.createLocalOnlyTableWithColumns(APPNAME, dbHandle, TABLE_LOCAL,
             getLocalTableColumnList());

         ContentValues cv = new ContentValues();
         cv.put(COLUMN_ID1, "ayy lmao");
         cv.put(COLUMN_ID2, 3);

         serviceInterface.insertLocalOnlyRow(APPNAME, dbHandle, TABLE_LOCAL, cv);

         BaseTable baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         List<Row> rows = baseTable.getRows();
         assertEquals(rows.size(), 1);
         for (Row row : rows) {
            assertEquals(row.getRawStringByKey(COLUMN_ID1), "ayy lmao");
            Long value = row.getDataType(COLUMN_ID2, Long.class);
            assertTrue(value.intValue() == 3);
         }
         assertColType("L_" + TABLE_LOCAL, COLUMN_ID2, "INTEGER");
         assertColType("L_" + TABLE_LOCAL, COLUMN_ID3, "REAL");

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            // cleanup
            serviceInterface.deleteLocalOnlyTable(APPNAME, dbHandle, TABLE_LOCAL);
         } catch (ServicesAvailabilityException e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testDeleteLocalOnlyTable() {
      try {
         serviceInterface.createLocalOnlyTableWithColumns(APPNAME, dbHandle, TABLE_LOCAL,
             getLocalTableColumnList());
         OdkConnectionInterface db = OdkConnectionFactorySingleton
             .getOdkConnectionFactoryInterface().getConnection(APPNAME, dbHandle);
         // Should not throw exception, table exists
         db.rawQuery("PRAGMA table_info(" + TABLE_LOCAL + ");", new String[0]).close();
         serviceInterface.deleteLocalOnlyTable(APPNAME, dbHandle, TABLE_LOCAL);
         boolean worked = false;
         try {
            Cursor c = db.rawQuery("PRAGMA table_info(" + TABLE_LOCAL + ");", new String[0]);
            if (c == null)
               throw new Exception("no results, success");
            c.moveToFirst();
            get(c, "type"); // should throw out of bounds exception
            c.close();
         } catch (Exception ignored) {
            worked = true;
         }
         assertTrue(worked);
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testInsertLocalOnlyRow() {

      try {
         serviceInterface.createLocalOnlyTableWithColumns(APPNAME, dbHandle, TABLE_LOCAL,
             getLocalTableColumnList());

         serviceInterface
             .insertLocalOnlyRow(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("test", 15, 3.1415));

         BaseTable baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         verifyRowExistsInLocalTable(1, baseTable, "test", 15, 3.1415);

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            serviceInterface.deleteLocalOnlyTable(APPNAME, dbHandle, TABLE_LOCAL);
         } catch (ServicesAvailabilityException e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }

   }

   private void verifyRowExistsInLocalTable(int expectedNumRows, BaseTable baseTable, String id, int intValue,
       double doubleValue) {
      List<Row> rows = baseTable.getRows();
      assertEquals(expectedNumRows, rows.size());
      for (Row row : rows) {
         if (row.getRawStringByKey(COLUMN_ID1).equals(id)) {
            assertEquals(row.getRawStringByKey(COLUMN_ID1), id);
            Long value = row.getDataType(COLUMN_ID2, Long.class);
            assertTrue(value.intValue() == intValue);
            Double val3 = row.getDataType(COLUMN_ID3, Double.class);
            assertEquals(val3, doubleValue, delta);
         }
      }
   }

   @Test public void testUpdateLocalOnlyRow() {
      try {
         serviceInterface.createLocalOnlyTableWithColumns(APPNAME, dbHandle, TABLE_LOCAL,
             getLocalTableColumnList());
         serviceInterface
             .insertLocalOnlyRow(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("test2", 15, 3.1415));
         serviceInterface
             .insertLocalOnlyRow(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("test", 15, 3.1415));

         serviceInterface
             .updateLocalOnlyRows(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("test", 16, 3.1415),
                 "columnId = ?", new BindArgs(new String[] { "test" }));

         BaseTable baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         verifyRowExistsInLocalTable(2, baseTable, "test", 16, 3.1415);
         verifyRowExistsInLocalTable(2, baseTable, "test2", 15, 3.1415);

         ContentValues cv = new ContentValues();
         cv.put(COLUMN_ID3, 9.5);
         serviceInterface.updateLocalOnlyRows(APPNAME, dbHandle, TABLE_LOCAL, cv, "columnId2 = ?",
             new BindArgs(new Object[] { 16 }));

         baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         verifyRowExistsInLocalTable(2, baseTable, "test", 16, 9.5);
         verifyRowExistsInLocalTable(2, baseTable, "test2", 15, 3.1415);

         serviceInterface
             .insertLocalOnlyRow(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("a", 2, 3.1415));
         baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         verifyRowExistsInLocalTable(3, baseTable, "test", 16, 9.5);
         verifyRowExistsInLocalTable(3, baseTable, "test2", 15, 3.1415);
         verifyRowExistsInLocalTable(3, baseTable, "a", 2, 3.1415);

         serviceInterface
             .updateLocalOnlyRows(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("test", 16, 3.1415),
                 "columnId = ?", new BindArgs(new String[] { "test" }));

         baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         verifyRowExistsInLocalTable(3, baseTable, "test", 16, 3.1415);
         verifyRowExistsInLocalTable(3, baseTable, "test2", 15, 3.1415);
         verifyRowExistsInLocalTable(3, baseTable, "a", 2, 3.1415);

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            serviceInterface.deleteLocalOnlyTable(APPNAME, dbHandle, TABLE_LOCAL);
         } catch (ServicesAvailabilityException e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testDeleteLocalOnlyRow() {
      try {
         serviceInterface.createLocalOnlyTableWithColumns(APPNAME, dbHandle, TABLE_LOCAL,
             getLocalTableColumnList());

         serviceInterface
             .insertLocalOnlyRow(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("test", 15, 3.1415));
         serviceInterface.deleteLocalOnlyRows(APPNAME, dbHandle, TABLE_LOCAL, "columnId = ?",
             new BindArgs(new String[] { "test" }));

         BaseTable baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         List<Row> rows = baseTable.getRows();
         assertEquals(0, rows.size());

         serviceInterface
             .insertLocalOnlyRow(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("test", 15, 3.1415));
         serviceInterface
             .insertLocalOnlyRow(APPNAME, dbHandle, TABLE_LOCAL, makeTblCvs("test2", 18, 9.813793));
         serviceInterface.deleteLocalOnlyRows(APPNAME, dbHandle, TABLE_LOCAL, "columnId = ?",
             new BindArgs(new String[] { "test" }));

         baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         verifyRowExistsInLocalTable(1, baseTable, "test2", 18, 9.813793);

         baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);
         verifyRowExistsInLocalTable(1, baseTable, "test2", 18, 9.813793);

         serviceInterface.deleteLocalOnlyRows(APPNAME, dbHandle, TABLE_LOCAL, "columnId = ?",
             new BindArgs(new String[] { "test2" }));

         baseTable = serviceInterface
             .simpleQueryLocalOnlyTables(APPNAME, dbHandle, TABLE_LOCAL, null, null, null, null,
                 null, null, null, null);

         rows = baseTable.getRows();
         assertEquals(0, rows.size());

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            serviceInterface.deleteLocalOnlyTable(APPNAME, dbHandle, TABLE_LOCAL);
         } catch (ServicesAvailabilityException e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }

   }

   @Test public void testSetChoiceList() {

      try {

         OdkConnectionInterface db = OdkConnectionFactorySingleton
             .getOdkConnectionFactoryInterface().getConnection(APPNAME, dbHandle);

         String id = serviceInterface.setChoiceList(APPNAME, dbHandle, "[]");
         assertEquals(id, "d751713988987e9331980363e24189ce");
         Cursor c = db.rawQuery(
             "SELECT * FROM " + DatabaseConstants.CHOICE_LIST_TABLE_NAME + " WHERE "
                 + ChoiceListColumns.CHOICE_LIST_ID + " = ?;", new String[] { id });
         c.moveToFirst();
         assertEquals(get(c, ChoiceListColumns.CHOICE_LIST_JSON), "[]");
         c.close();
         id = serviceInterface.setChoiceList(APPNAME, dbHandle, "['a', 'b']");
         assertEquals(id, "ce75bd1cd721f68dd62b65e2868a1951");
         c = db.rawQuery("SELECT * FROM " + DatabaseConstants.CHOICE_LIST_TABLE_NAME + " WHERE "
             + ChoiceListColumns.CHOICE_LIST_ID + " = ?;", new String[] { id });
         c.moveToFirst();
         assertEquals(get(c, ChoiceListColumns.CHOICE_LIST_JSON), "['a', 'b']");
         c.close();

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testSetChoiceListNullList() {

      try {
         assertNull(serviceInterface.setChoiceList(APPNAME, dbHandle, null));
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testSetChoiceListEmptyList() {
      try {
         assertNull(serviceInterface.setChoiceList(APPNAME, dbHandle, ""));
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testSetChoiceListTrailingWhitespace() {

      try {
         serviceInterface.setChoiceList(APPNAME, dbHandle, "[] ");
         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalArgumentException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testGetChoiceList() {
      try {
         String choiceId = serviceInterface.setChoiceList(APPNAME, dbHandle, CHOICE_LIST_JSON);
         assertEquals(serviceInterface.getChoiceList(APPNAME, dbHandle, choiceId),
             CHOICE_LIST_JSON);
         assertNull(serviceInterface.getChoiceList(APPNAME, dbHandle, null));
         assertNull(serviceInterface.getChoiceList(APPNAME, dbHandle, ""));

         // TODO: figure out how to remove choice list
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testCreateOrOpenTableWithColumns() {
      try {
         // create
         List<Column> cols = makeCols();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
         } catch (ServicesAvailabilityException e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsBadColumnName() {
      try {
         // create
         List<Column> cols = makeCols();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }

         cols = makeCols();
         cols.set(1,
             new Column(COLUMN_ID3, "new Column Name", ElementDataType.number.name(), "[]"));
         serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));

         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalStateException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsMissingColumn() {
      try {
         // create
         List<Column> cols = makeCols();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME2, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME2, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }

         cols = makeCols();
         cols.remove(1);
         serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME2, new ColumnList(cols));
         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalStateException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME2);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsExtraColumn() {
      try {
         // create
         List<Column> cols = makeCols();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }

         cols = makeCols();
         cols.add(new Column("columnId4", "Column Name 4", ElementDataType.string.name(), "[]"));
         serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalStateException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsBadType() {
      try {
         // create
         List<Column> cols = makeCols();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         res = serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }

         cols = makeCols();
         cols.set(1, new Column(COLUMN_ID3, "Column Name", ElementDataType.integer.name(), "[]"));
         serviceInterface
             .createOrOpenTableWithColumns(APPNAME, dbHandle, TABLE_NAME1, new ColumnList(cols));
         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalStateException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsAndProperties() {
      try {
         // create
         List<Column> cols = makeCols();
         List<KeyValueStoreEntry> md = makeMetadata();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         md.get(1).value = "will be set";
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertMdat("some key", "some value");
         assertMdat("some key 2", "will be set");
         md.get(1).value = "will be reset";
         md.remove(0);
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, true);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertNoMdat("some key");
         assertMdat("some key 2", "will be reset");
         // For use in other tests
         md = makeMetadata();
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsAndPropertiesBadColumnName() {
      try {
         // create
         List<Column> cols = makeCols();
         List<KeyValueStoreEntry> md = makeMetadata();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         md.get(1).value = "will be set";
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertMdat("some key", "some value");
         assertMdat("some key 2", "will be set");
         md.get(1).value = "will be reset";
         md.remove(0);
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, true);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertNoMdat("some key");
         assertMdat("some key 2", "will be reset");
         // For use in other tests
         md = makeMetadata();
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);

         cols = makeCols();
         md = makeMetadata();
         cols.set(1,
             new Column(COLUMN_ID3, "new Column Name", ElementDataType.number.name(), "[]"));
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);
         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalStateException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsAndPropertiesMissingColumn() {
      try {
         // create
         List<Column> cols = makeCols();
         List<KeyValueStoreEntry> md = makeMetadata();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         md.get(1).value = "will be set";
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertMdat("some key", "some value");
         assertMdat("some key 2", "will be set");
         md.get(1).value = "will be reset";
         md.remove(0);
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, true);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertNoMdat("some key");
         assertMdat("some key 2", "will be reset");
         // For use in other tests
         md = makeMetadata();
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);

         cols = makeCols();
         cols.remove(1);
         md = makeMetadata();
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);

         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalStateException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsAndPropertiesExtraColumn() {
      try {
         // create
         List<Column> cols = makeCols();
         List<KeyValueStoreEntry> md = makeMetadata();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         md.get(1).value = "will be set";
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertMdat("some key", "some value");
         assertMdat("some key 2", "will be set");
         md.get(1).value = "will be reset";
         md.remove(0);
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, true);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertNoMdat("some key");
         assertMdat("some key 2", "will be reset");
         // For use in other tests
         md = makeMetadata();
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);

         cols = makeCols();
         cols.add(new Column("columnId4", "Column Name 4", ElementDataType.string.name(), "[]"));
         md = makeMetadata();
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);

         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalStateException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testCreateOrOpenTableWithColumnsAndPropertiesBadType() {
      try {
         // create
         List<Column> cols = makeCols();
         List<KeyValueStoreEntry> md = makeMetadata();
         OrderedColumns res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         // open
         md.get(1).value = "will be set";
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, false);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertMdat("some key", "some value");
         assertMdat("some key 2", "will be set");
         md.get(1).value = "will be reset";
         md.remove(0);
         res = serviceInterface
             .createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
                 new ColumnList(cols), md, true);
         for (int i = 0; i < res.getColumns().size(); i++) {
            assertEquals(cols.get(i), res.getColumns().get(i));
         }
         assertNoMdat("some key");
         assertMdat("some key 2", "will be reset");
         // For use in other tests
         md = makeMetadata();
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);

         cols = makeCols();
         cols.set(1, new Column(COLUMN_ID3, "Column Name", ElementDataType.integer.name(), "[]"));
         md = makeMetadata();
         serviceInterface.createOrOpenTableWithColumnsAndProperties(APPNAME, dbHandle, TABLE_NAME1,
             new ColumnList(cols), md, true);
         fail(SHOULD_THROW_EXCEPTION);
      } catch (IllegalStateException e1) {
         // expected result

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TABLE_NAME1);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testDeleteAllCheckpointRowsWithIdNotAllowed() {
      try {
         createTeaHouses();
         thInsert("t1", null);
         serviceInterface
             .deleteAllCheckpointRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");

         fail(SHOULD_THROW_EXCEPTION);
      } catch (ActionNotAuthorizedException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }
      }
   }

   @Test public void testDeleteAllCheckpointRowsWithId() {
      try {

         File instanceFolder = makeTemp("t3");
         createTeaHouses();
         setSuperuser();
         thInsert("t1", SavepointTypeManipulator.complete());
         thInsert("t2", SavepointTypeManipulator.incomplete());
         thInsert("t3", null);
         //thSet("t3", "_savepoint_type", null);

         serviceInterface
             .deleteAllCheckpointRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");
         serviceInterface
             .deleteAllCheckpointRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t2");
         serviceInterface
             .deleteAllCheckpointRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t3");
         thAssertPresent("t1", null, false);
         thAssertPresent("t2", null, false);
         thAssertGone("t3");
         assertFalse(instanceFolder.exists());
         deleteTeaHouses();

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testDeleteLastCheckpointRowWithId() {

      try {

         File instanceFolder = makeTemp("t3");
         createTeaHouses();
         setSuperuser();
         thInsert("t3", SavepointTypeManipulator.incomplete());
         thInsert("t3", null);
         serviceInterface
             .deleteLastCheckpointRowWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t3");
         thAssertPresent("t3", null, false);
         assertTrue(instanceFolder.exists());
         thSet("t3", SAVEPOINT_TYPE_COLNAME, null);
         serviceInterface
             .deleteLastCheckpointRowWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t3");
         thAssertGone("t3");
         assertFalse(instanceFolder.exists());
         deleteTeaHouses();

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testDeleteTableMetadata() {
      try {
         OdkConnectionInterface db = OdkConnectionFactorySingleton
             .getOdkConnectionFactoryInterface().getConnection(APPNAME, dbHandle);

         createTeaHouses();
         insertMetadata(TEA_HOUSES_TBL_NAME, PARTITION, ASPECT, "key", "value");
         insertMetadata(TEA_HOUSES_TBL_NAME, PARTITION, ASPECT, "some_other_key", "value");
         serviceInterface
             .deleteTableMetadata(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, PARTITION, ASPECT, "key");
         Cursor c = db.rawQuery(
             "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
                 + KeyValueStoreColumns.KEY + " = ?;", new String[] { "key" });
         assertEquals(c.getCount(), 0);
         c.close();
         c = db.rawQuery(
             "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
                 + KeyValueStoreColumns.KEY + " = ?;", new String[] { "some_other_key" });
         assertEquals(c.getCount(), 1);
         c.close();
         deleteTeaHouses();

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testDeleteRowWithId() {
      try {
         OdkConnectionInterface db = OdkConnectionFactorySingleton
             .getOdkConnectionFactoryInterface().getConnection(APPNAME, dbHandle);

         setSuperuser();
         createTeaHouses();
         thInsert("t1", null);
         thInsert("t2", null);
         serviceInterface.deleteRowWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");
         Cursor c = db.rawQuery("SELECT * FROM Tea_houses;", null);
         c.moveToFirst();
         assertEquals(get(c, "_id"), "t2");
         c.close();
         deleteTeaHouses();

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test public void testDeleteRowWithIdNoAccess() {
      try {
         createTeaHouses();
         thInsert("t1", null);
         serviceInterface.deleteRowWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");

         fail(SHOULD_THROW_EXCEPTION);
      } catch (ActionNotAuthorizedException e1) {
         // expected result
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testPrivilegedDeleteRowWithId() {
      try {
         OdkConnectionInterface db = OdkConnectionFactorySingleton
             .getOdkConnectionFactoryInterface().getConnection(APPNAME, dbHandle);

         createTeaHouses();
         thInsert("t1", null);
         thInsert("t2", null);
         Cursor c;
         //      c = db.rawQuery("SELECT * FROM Tea_houses;", null);
         //      assertEquals(c.getCount(), 2);
         //      c.close();
         serviceInterface
             .privilegedDeleteRowWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");
         c = db.rawQuery("SELECT * FROM " + TEA_HOUSES_TBL_NAME + ";", null);
         assertEquals(c.getCount(), 1);
         c.moveToFirst();
         assertEquals(get(c, "_id"), "t2");
         c.close();

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testGetAdminColumns() {
      try {
         createTeaHouses();
         String[] cols = serviceInterface.getAdminColumns();
         for (String col : cols) {
            // Should exist in tea houses
            assertColType(TEA_HOUSES_TBL_NAME, col, null);
         }

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testGetAllColumnNames() {
      try {
         createTeaHouses();
         String[] cols = serviceInterface.getAllColumnNames(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME);
         for (String col : cols) {
            // Should exist in tea houses
            assertColType(TEA_HOUSES_TBL_NAME, col, null);
         }

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testGetAllTableIds() {
      try {
         serviceInterface.createOrOpenTableWithColumns(APPNAME, dbHandle, "breathcounter",
             new ColumnList(new ArrayList<Column>()));
         serviceInterface.createOrOpenTableWithColumns(APPNAME, dbHandle, "fields",
             new ColumnList(new ArrayList<Column>()));

         List<String> tables = serviceInterface.getAllTableIds(APPNAME, dbHandle);
         assertTrue(tables.size() == 2);
         // Test that it returns list in sorted order
         assertEquals(tables.get(0), "breathcounter");
         assertEquals(tables.get(1), "fields");

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, "breathcounter");
               serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, "fields");
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testGetRowsWithIdNoAccess() {
      try {
         createTeaHouses();
         thInsert("t1", null);
         thInsert("t1", SavepointTypeManipulator.complete());
         thInsert("t2", null);
         UserTable t = serviceInterface
             .getRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");
         assertEquals(t.getNumberOfRows(), 0);

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testGetRowsWithId() {
      try {
         setSuperuser();
         createTeaHouses();
         thInsert("t1", null);
         thInsert("t1", SavepointTypeManipulator.complete());
         thInsert("t2", null);
         UserTable t = serviceInterface
             .getRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");
         assertEquals(t.getNumberOfRows(), 2);
         String a = t.getRowAtIndex(0).getRawStringByKey(SAVEPOINT_TYPE_COLNAME);
         String b = t.getRowAtIndex(1).getRawStringByKey(SAVEPOINT_TYPE_COLNAME);
         if (a == null) {
            assertEquals(b, SavepointTypeManipulator.complete());
         } else {
            assertNull(b);
            assertEquals(a, SavepointTypeManipulator.complete());
         }

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testPrivilegedGetRowsWithId() {
      try {
         createTeaHouses();
         thInsert("t1", null);
         thInsert("t1", SavepointTypeManipulator.complete());
         thInsert("t2", null);
         UserTable t = serviceInterface
             .privilegedGetRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");
         assertEquals(t.getNumberOfRows(), 2);
         String a = t.getRowAtIndex(0).getRawStringByKey(SAVEPOINT_TYPE_COLNAME);
         String b = t.getRowAtIndex(1).getRawStringByKey(SAVEPOINT_TYPE_COLNAME);
         if (a == null) {
            assertEquals(b, SavepointTypeManipulator.complete());
         } else {
            assertNull(b);
            assertEquals(a, SavepointTypeManipulator.complete());
         }

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testGetMostRecentRowWithId() {
      try {
         createTeaHouses();
         thInsert("t1", SavepointTypeManipulator.incomplete());
         thSet("t1", DataTableColumns.SAVEPOINT_TIMESTAMP, "bbb");
         thSet("t1", "_default_access", "FULL");
         thInsert("t2", SavepointTypeManipulator.complete());
         thSet("t2", DataTableColumns.SAVEPOINT_TIMESTAMP, "aaa");
         thSet("t2", "_default_access", "FULL");
         thSet("t2", "_id", "t1");
         UserTable result = serviceInterface
             .getMostRecentRowWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, "t1");
         assertEquals(result.getNumberOfRows(), 1);
         TypedRow row = result.getRowAtIndex(0);
         assertEquals(row.getRawStringByKey(SAVEPOINT_TYPE_COLNAME), SavepointTypeManipulator
             .incomplete());

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testGetTableMetadata() {
      try {
         String key = "key";
         String key2 = "key2";

         createTeaHouses();
         insertMetadata(TEA_HOUSES_TBL_NAME, PARTITION, ASPECT, key, "val");
         insertMetadata(TEA_HOUSES_TBL_NAME, PARTITION, ASPECT, key2, "other val");
         TableMetaDataEntries result = serviceInterface
             .getTableMetadata(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, PARTITION, ASPECT, key,
                 null);

         assertEquals(result.getEntries().size(), 1);
         assertEquals(result.getEntries().get(0).value, "val");
         result = serviceInterface
             .getTableMetadata(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, null, null, null);

         assertEquals(result.getEntries().size(), 2);
         String a = result.getEntries().get(0).value;
         String b = result.getEntries().get(1).value;
         if (a.equals("val")) {
            assertEquals(b, "other val");
         } else {
            assertEquals(a, "other val");
            assertEquals(b, "val");
         }
         result = serviceInterface
             .getTableMetadata(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, PARTITION, ASPECT, "k3",
                 null);
         assertEquals(result.getEntries().size(), 0);
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
               //serviceInterface
               //    .deleteTableMetadata(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, null, null);
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   @Test public void testGetTableHealthStatus() throws Exception {
      try {
         //_savepoint_type is null -> + 1 checkpoint
         //_conflict_type not is null -> + 1 conflict
         //_sync_state not in [synced, pending_files] -> + 1 changes
         createTeaHouses();
         List<TableHealthInfo> healthInfo = serviceInterface
             .getTableHealthStatuses(APPNAME, dbHandle);
         assertEquals(1, healthInfo.size());
         TableHealthInfo res = healthInfo.get(0);

         assertEquals(res.getHealthStatus(), TableHealthStatus.TABLE_HEALTH_IS_CLEAN);
         thInsert("t1", SyncState.new_row, ConflictType.LOCAL_UPDATED_UPDATED_VALUES);
         thSet("t1", CONFLICT_TYPE_COLNAME, null);
         thSet("t1", SAVEPOINT_TYPE_COLNAME, SavepointTypeManipulator.complete());

         healthInfo = serviceInterface
             .getTableHealthStatuses(APPNAME, dbHandle);
         assertEquals(1, healthInfo.size());
         res = healthInfo.get(0);

         assertEquals(res.getHealthStatus(), TableHealthStatus.TABLE_HEALTH_IS_CLEAN);
         assertTrue(res.hasChanges());

         deleteTeaHouses();
         createTeaHouses();

         thInsert("t1", SyncState.synced, ConflictType.LOCAL_UPDATED_UPDATED_VALUES);
         thSet("t1", CONFLICT_TYPE_COLNAME, ConflictType.LOCAL_DELETED_OLD_VALUES);
         thSet("t1", SAVEPOINT_TYPE_COLNAME, SavepointTypeManipulator.complete());

         healthInfo = serviceInterface
             .getTableHealthStatuses(APPNAME, dbHandle);
         assertEquals(1, healthInfo.size());
         res = healthInfo.get(0);

         assertEquals(res.getHealthStatus(), TableHealthStatus.TABLE_HEALTH_HAS_CONFLICTS);
         assertFalse(res.hasChanges());

         thSet("t1", SAVEPOINT_TYPE_COLNAME, null);
         thSet("t1", SYNC_STATE_COLNAME, SyncState.changed);

         healthInfo = serviceInterface
             .getTableHealthStatuses(APPNAME, dbHandle);
         assertEquals(1, healthInfo.size());
         res = healthInfo.get(0);

         assertEquals(res.getHealthStatus(),
             TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS);
         assertTrue(res.hasChanges());

         thSet("t1", SYNC_STATE_COLNAME, SyncState.synced_pending_files);

         healthInfo = serviceInterface
             .getTableHealthStatuses(APPNAME, dbHandle);
         assertEquals(1, healthInfo.size());
         res = healthInfo.get(0);

         assertEquals(res.getHealthStatus(),
             TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS);
         assertFalse(res.hasChanges());
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      } finally {
         try {
            if (dbHandle != null) {
               deleteTeaHouses();
            }
         } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
         }

      }
   }

   /////////////////////////////////////////////////////////
   /////////////      HELPER FUNCTIONS        //////////////
   /////////////////////////////////////////////////////////

   private static String get(Cursor c, String col) {
      return c.getString(c.getColumnIndexOrThrow(col));
   }

   private void assertColType(String table, String column, String expectedType) throws Exception {
      OdkConnectionInterface db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(APPNAME, dbHandle);

      Cursor c = db.rawQuery("PRAGMA table_info(" + table + ");", null);
      c.moveToFirst();
      while (!get(c, "name").equals(column)) {
         c.moveToNext();
      }
      if (expectedType != null) {
         assertEquals(get(c, "type"), expectedType);
      }
      c.close();
   }

   private static double getDouble(Cursor c, String col) {
      return c.getDouble(c.getColumnIndexOrThrow(col));
   }

   private static int getInt(Cursor c, String col) throws Exception {
      return c.getInt(c.getColumnIndexOrThrow(col));
   }

   private static ContentValues makeTblCvs(String a, Integer b, Double c) {
      ContentValues v = new ContentValues();
      v.put(COLUMN_ID1, a);
      v.put(COLUMN_ID2, b);
      v.put(COLUMN_ID3, c);
      return v;
   }

   private static List<Column> makeCols() {
      List<Column> cols = new ArrayList<>();
      cols.add(new Column(COLUMN_ID1, "Column Name", ElementDataType.string.name(), "[]"));
      cols.add(new Column(COLUMN_ID2, "Second Column Name", ElementDataType.integer.name(), "[]"));
      cols.add(new Column(COLUMN_ID3, "Column Name", ElementDataType.number.name(), "[]"));
      return cols;
   }

   private static List<KeyValueStoreEntry> makeMetadata() {
      List<KeyValueStoreEntry> l = new ArrayList<>();
      KeyValueStoreEntry kvse = new KeyValueStoreEntry();
      kvse.type = "TEXT";
      kvse.tableId = TABLE_NAME1;
      kvse.aspect = ASPECT;
      kvse.partition = PARTITION;
      kvse.key = "some key";
      kvse.value = "some value";
      l.add(kvse);
      kvse = new KeyValueStoreEntry();
      kvse.type = "TEXT";
      kvse.tableId = TABLE_NAME1;
      kvse.aspect = ASPECT;
      kvse.partition = PARTITION;
      kvse.key = "some key 2";
      kvse.value = "some value 2";
      l.add(kvse);
      return l;
   }

   private static File makeTemp(String id) throws Exception {
      File instanceFolder = new File(
          ODKFileUtils.getInstanceFolder(APPNAME, TEA_HOUSES_TBL_NAME, id));
      if (!instanceFolder.exists() && !instanceFolder.mkdirs()) {
         throw new Exception("Should have been able to create " + instanceFolder.getPath());
      }
      assertTrue(instanceFolder.exists());
      FileOutputStream f = new FileOutputStream(new File(instanceFolder.getPath() + "/temp"));
      f.write(new byte[] { 97, 98, 99 });
      f.close();
      return instanceFolder;
   }

   private void insertMetadata(String table, String partition, String aspect, String key,
       String value) throws ServicesAvailabilityException {

      KeyValueStoreEntry kvsEntry = new KeyValueStoreEntry();
      kvsEntry.tableId = table;
      kvsEntry.partition = partition;
      kvsEntry.aspect = aspect;
      kvsEntry.key = key;
      kvsEntry.type = "string";
      kvsEntry.value = value;

      serviceInterface.replaceTableMetadata(APPNAME, dbHandle, kvsEntry);
   }

   private void assertNoMdat(String k) {
      OdkConnectionInterface db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(APPNAME, dbHandle);
      Cursor c = db.rawQuery(
          "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
              + KeyValueStoreColumns.KEY + "= ?;", new String[] { k });
      assertEquals(c.getCount(), 0);
      c.close();
   }

   private void assertMdat(String k, String v) {
      OdkConnectionInterface db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(APPNAME, dbHandle);
      Cursor c = db.rawQuery(
          "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
              + KeyValueStoreColumns.KEY + "= ?;", new String[] { k });
      assertEquals(c.getCount(), 1);
      c.moveToFirst();
      assertEquals(get(c, KeyValueStoreColumns.VALUE), v);
      c.close();
   }

   private void thSetRevid(String revId) throws Exception {
      String cId = TableDefinitionsColumns.TABLE_ID;
      String cSchema = TableDefinitionsColumns.SCHEMA_ETAG;
      String cData = TableDefinitionsColumns.LAST_DATA_ETAG;
      String cTime = TableDefinitionsColumns.LAST_SYNC_TIME;
      String cRev = TableDefinitionsColumns.REV_ID;
      String[] all = { cId, cSchema, cData, cTime, cRev };

      OdkConnectionInterface db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(APPNAME, dbHandle);

      db.rawQuery("DELETE FROM " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + ";", null);
      db.rawQuery("INSERT INTO " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + " (" + join(", ", all)
              + ") VALUES (?, ?, ?, ?, ?);",
          new String[] { TEA_HOUSES_TBL_NAME, "schema etag", "data etag", "time", revId });
   }

   private void thSet(String id, String col, Object val) throws Exception {
      OdkConnectionInterface db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(APPNAME, dbHandle);
      db.rawQuery("UPDATE Tea_houses SET " + col + " = " + (val == null ? "NULL" : "?") + " "
          + "WHERE _id = ?", val == null ? new String[] { id } : new Object[] { val, id });
   }

   private void setSuperuser() throws Exception {
      setProp(props, CommonToolProperties.KEY_AUTHENTICATION_TYPE,
          props.CREDENTIAL_TYPE_USERNAME_PASSWORD);
      setProp(props, CommonToolProperties.KEY_USERNAME, "insert username here");
      setProp(props, CommonToolProperties.KEY_AUTHENTICATED_USER_ID, "user_id_here");
      setProp(props, CommonToolProperties.KEY_ROLES_LIST, RoleConsts.SUPER_USER_ROLES_LIST);
   }

   private void thInsert(String id, String savepointType) {
      OdkConnectionInterface db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(APPNAME, dbHandle);
      db.execSQL("INSERT INTO Tea_houses (_id, _sync_state, _savepoint_timestamp, _savepoint_type) "
          + "VALUES (?, ?, ?, ?)", new Object[] { id, SyncState.synced.name(), "", savepointType });
   }

   private void thInsert(String id, @SuppressWarnings("TypeMayBeWeakened") SyncState syncState,
       int conflictType) {
      OdkConnectionInterface db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(APPNAME, dbHandle);
      db.execSQL("INSERT INTO Tea_houses (_id, _savepoint_timestamp, _sync_state, "
              + "_conflict_type) VALUES (?, ?, ?, ?)",
          new Object[] { id, "", syncState.name(), conflictType });
   }

   private void createTeaHouses() throws Exception {
      List<Column> columns = new ArrayList<Column>();

      columns.add(new Column("Customers", "Customers", "integer", null));
      columns.add(new Column("Date_Opened", "Date_Opened", ElementDataType.string.name(), null));
      columns.add(new Column("District", "District", ElementDataType.string.name(), null));
      columns.add(new Column("Hot", "Hot", ElementDataType.string.name(), null));
      columns.add(new Column("Iced", "Iced", ElementDataType.string.name(), null));
      columns.add(new Column("Location_accuracy", "Accuracy", ElementDataType.number.name(), null));
      columns.add(new Column("Location_altitude", "Altitude", ElementDataType.number.name(), null));
      columns.add(new Column("Location_latitude", "Latitude", ElementDataType.number.name(), null));
      columns
          .add(new Column("Location_longitude", "Longitude", ElementDataType.number.name(), null));
      columns.add(new Column("Name", "Name", ElementDataType.string.name(), null));
      columns.add(new Column("Neighborhood", "Neighborhood", ElementDataType.string.name(), null));
      columns.add(new Column("Phone_Number", "Phone_Number", ElementDataType.string.name(), null));
      columns.add(new Column("Region", "Region", ElementDataType.string.name(), null));
      columns.add(
          new Column("Specialty_Type_id", "Specialty_Type_id", ElementDataType.string.name(),
              null));
      columns.add(new Column("State", "State", ElementDataType.string.name(), null));
      columns.add(new Column("Store_Owner", "Store_Owner", ElementDataType.string.name(), null));
      columns.add(new Column("Visits", "Visits", ElementDataType.string.name(), null));
      columns.add(new Column("WiFi", "WiFi", ElementDataType.string.name(), null));

      ColumnList cl = new ColumnList(columns);

      serviceInterface.createOrOpenTableWithColumns(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, cl);

   }

   private void deleteTeaHouses() throws ServicesAvailabilityException {
      serviceInterface.deleteTableAndAllData(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME);
      serviceInterface
          .deleteTableMetadata(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, null, null);
   }

   private void setProp(PropertiesSingleton props, String a, String b) throws Exception {
      Map<String, String> m = new HashMap<>();
      m.put(a, b);
      props.setProperties(m);
   }

   private void thAssertGone(String id) throws Exception {
      UserTable table = serviceInterface
          .getRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, id);

      assertEquals(table.getNumberOfRows(), 0);

   }

   private void thAssertPresent(String id, @SuppressWarnings("TypeMayBeWeakened") SyncState state,
       boolean conflictTypeShouldBeNull) throws ServicesAvailabilityException {

      UserTable table = serviceInterface
          .getRowsWithId(APPNAME, dbHandle, TEA_HOUSES_TBL_NAME, null, id);

      assertEquals(table.getNumberOfRows(), 1);

      TypedRow row = table.getRowAtIndex(0);

      if (state != null) {
         assertEquals(row.getRawStringByKey(SYNC_STATE_COLNAME), state.name());
      }
      if (conflictTypeShouldBeNull) {
         String val = row.getRawStringByKey(CONFLICT_TYPE_COLNAME);
         assertTrue(val == null || val.isEmpty() || val.equalsIgnoreCase("null"));
      }
   }

   private ColumnList getLocalTableColumnList() {
      return new ColumnList(Arrays.asList(
          new Column[] { new Column(COLUMN_ID1, "Column Name", ElementDataType.string.name(), "[]"),
              new Column(COLUMN_ID3, "Column Name", ElementDataType.number.name(), "[]"),
              new Column(COLUMN_ID2, "Second Column Name", ElementDataType.integer.name(),
                  "[]") }));
   }
}
