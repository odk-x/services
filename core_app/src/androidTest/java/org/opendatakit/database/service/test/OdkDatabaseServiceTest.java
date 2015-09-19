package org.opendatakit.database.service.test;

import android.content.ContentValues;
import android.content.Intent;
import android.os.IBinder;
import android.os.RemoteException;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.test.ServiceTestCase;

import org.opendatakit.TestConsts;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.Row;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.database.service.OdkDatabaseService;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.database.service.OdkDbInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

    public OdkDatabaseServiceTest() {
        super(OdkDatabaseService.class);
    }

    public OdkDatabaseServiceTest(Class<OdkDatabaseService> serviceClass) {
        super(serviceClass);
    }

    @Override
    protected void setUp () throws Exception {
        super.setUp();
        setupService();
    }

    @Override
    protected void tearDown() throws Exception {
        OdkDbInterface serviceInterface = bindToDbService();
        try {
            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            verifyNoTablesExistNCleanAllTables(serviceInterface, db);
            serviceInterface.closeDatabase(APPNAME, db);
        } catch (RemoteException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        super.tearDown();
    }

    @NonNull
    private List<Column> createColumnList() {
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
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_INTEGER_ID), Integer.toString(TEST_INT_1));
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_NUMBER_ID), Double.toString(TEST_NUM_1));
    }

    private void verifyRowTestSet2(Row row) {
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_STRING_ID), TEST_STR_2);
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_INTEGER_ID), Integer.toString(TEST_INT_2));
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_NUMBER_ID), Double.toString(TEST_NUM_2));
    }

    private void verifyNoTablesExistNCleanAllTables(OdkDbInterface serviceInterface, OdkDbHandle db) throws RemoteException {
        List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);

        boolean tablesGone = (tableIds.size() == 0);

        // Drop any leftover table now that the test is done
        for(String id : tableIds) {
            serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_ID);
        }

        assertTrue(tablesGone);
    }

    @Nullable
    private OdkDbInterface bindToDbService() {
        Intent bind_intent = new Intent();
        bind_intent.setClass(getContext(), OdkDatabaseService.class);
        IBinder service = this.bindService(bind_intent);
        return OdkDbInterface.Stub.asInterface(service);
    }

    private boolean hasNoTablesInDb(OdkDbInterface serviceInterface, OdkDbHandle db) throws RemoteException {
        List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);
        return (tableIds.size() == 0);
    }

    public void testBinding() {
        OdkDbInterface serviceInterface = bindToDbService();
        assertNotNull(serviceInterface);
        // TODO: database check function?

        // TODO: add a bind with bind_intent.setClassName instead
    }

    public void testDbCreateNDeleteTable() {
        OdkDbInterface serviceInterface = bindToDbService();
        try {
            ColumnList columnList = new ColumnList(createColumnList());

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
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
        OdkDbInterface serviceInterface = bindToDbService();
        try {

            List<Column> columnList = createColumnList();
            ColumnList colList = new ColumnList(columnList);

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

            OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
            UUID rowId =  UUID.randomUUID();

            serviceInterface.insertDataIntoExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

            UserTable table = serviceInterface.getDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

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
        OdkDbInterface serviceInterface = bindToDbService();
        try {

            List<Column> columnList = createColumnList();
            ColumnList colList = new ColumnList(columnList);

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

            OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
            UUID rowId1 = UUID.randomUUID();
            UUID rowId2 = UUID.randomUUID();

            serviceInterface.insertDataIntoExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId1.toString());
            serviceInterface.insertDataIntoExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet2(), rowId2.toString());

            UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, COL_STRING_ID, "ASC");
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
        OdkDbInterface serviceInterface = bindToDbService();
        try {

            List<Column> columnList = createColumnList();
            ColumnList colList = new ColumnList(columnList);

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

            OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
            UUID rowId =  UUID.randomUUID();

            serviceInterface.insertDataIntoExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

            UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null);

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
        OdkDbInterface serviceInterface = bindToDbService();
        try {

            List<Column> columnList = createColumnList();
            ColumnList colList = new ColumnList(columnList);

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

            OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
            UUID rowId =  UUID.randomUUID();

            serviceInterface.insertDataIntoExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

            UserTable table = serviceInterface.getDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, null, rowId.toString());

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
        OdkDbInterface serviceInterface = bindToDbService();
        try {
            List<Column> columnList = createColumnList();
            ColumnList colList = new ColumnList(columnList);

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

            OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
            UUID rowId =  UUID.randomUUID();

            serviceInterface.insertDataIntoExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

            UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null,null, null,null,null);
            assertEquals(DB_TABLE_ID, table.getTableId());
            assertEquals(1, table.getNumberOfRows());

            verifyRowTestSet1(table.getRowAtIndex(0));

            serviceInterface.updateDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet2(), rowId.toString());

            table = serviceInterface.getDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
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
        OdkDbInterface serviceInterface = bindToDbService();
        try {
            List<Column> columnList = createColumnList();
            ColumnList colList = new ColumnList(columnList);

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

            OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
            UUID rowId =  UUID.randomUUID();

            serviceInterface.insertDataIntoExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

            UserTable table = serviceInterface.rawSqlQuery(APPNAME, db, DB_TABLE_ID, columns, null, null, null, null, null, null);
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

            serviceInterface.updateDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, singleColumn, singleValue, rowId.toString());

            table = serviceInterface.getDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

            assertEquals(DB_TABLE_ID, table.getTableId());
            assertEquals(1, table.getNumberOfRows());

            row = table.getRowAtIndex(0);

            assertEquals(row.getRawDataOrMetadataByElementKey(COL_STRING_ID), TEST_STR_1);
            assertEquals(row.getRawDataOrMetadataByElementKey(COL_INTEGER_ID), Integer.toString(changeValue));
            assertEquals(row.getRawDataOrMetadataByElementKey(COL_NUMBER_ID), Double.toString(TEST_NUM_1));

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
        OdkDbInterface serviceInterface = bindToDbService();
        try {

            List<Column> columnList = createColumnList();
            ColumnList colList = new ColumnList(columnList);

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_ID, colList);

            OrderedColumns columns = new OrderedColumns(APPNAME, DB_TABLE_ID, columnList);
            UUID rowId =  UUID.randomUUID();

            // insert row
            serviceInterface.insertDataIntoExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, contentValuesTestSet1(), rowId.toString());

            UserTable table = serviceInterface.getDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

            assertEquals(DB_TABLE_ID, table.getTableId());
            assertEquals(1, table.getNumberOfRows());
            Row row = table.getRowAtIndex(0);

            verifyRowTestSet1(row);

            // delete row
            serviceInterface.deleteDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, rowId.toString());

            table = serviceInterface.getDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());
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
}
