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

        cv.put(COL_STRING_ID, "TestStr1");
        cv.put(COL_INTEGER_ID, 1);
        cv.put(COL_NUMBER_ID, 1.1);

        return cv;
    }

    private ContentValues contentValuesTestSet2() {
        ContentValues cv = new ContentValues();

        cv.put(COL_STRING_ID, "TestStr2");
        cv.put(COL_INTEGER_ID, 2);
        cv.put(COL_NUMBER_ID, 2.1);

        return cv;
    }

    private void verifyRowTestSet1(Row row) {
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_STRING_ID), "TestStr1");
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_INTEGER_ID), Integer.toString(1));
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_NUMBER_ID), Double.toString(1.1));
    }

    private void verifyRowTestSet2(Row row) {
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_STRING_ID), "TestStr2");
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_INTEGER_ID), Integer.toString(2));
        assertEquals(row.getRawDataOrMetadataByElementKey(COL_NUMBER_ID), Double.toString(2.1));
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



   //         UserTable table = serviceInterface.getDataInExistingDBTableWithId(APPNAME, db, DB_TABLE_ID, columns, rowId.toString());

    //        assertEquals(DB_TABLE_ID, table.getTableId());
    //        assertEquals(1, table.getNumberOfRows());

    //        Row row = table.getRowAtIndex(0);



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
