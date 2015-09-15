package org.opendatakit.database.service.test;

import android.content.Intent;
import android.os.IBinder;
import android.os.RemoteException;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.test.ServiceTestCase;

import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.database.service.OdkDatabaseService;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.database.service.OdkDbInterface;

import java.util.ArrayList;
import java.util.List;

public class OdkDatabaseServiceTest extends ServiceTestCase<OdkDatabaseService> {

    public static final String APPNAME = "unittest";
    public static final String DB_TABLE_NAME = "testtable";

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
    private ColumnList createColumnList() {
        List<Column> columns = new ArrayList<Column>();

        columns.add(new Column("columnString", "column String", "string", null));
        columns.add(new Column("columnInteger", "column Integer", "integer", null));
        columns.add(new Column("columnNumber", "column Number", "number", null));

        return new ColumnList(columns);
    }

    @Nullable
    private OdkDbInterface bindToDbService() {
        Intent bind_intent = new Intent();
        bind_intent.setClass(getContext(), OdkDatabaseService.class);
        IBinder service = this.bindService(bind_intent);
        return OdkDbInterface.Stub.asInterface(service);
    }

    public void testBinding() {
        OdkDbInterface serviceInterface = bindToDbService();
        assertNotNull(serviceInterface);
        // TODO: database check function?

        // TODO: add a bind with bind_intent.setClassName instead
    }

    public void testDbCreateTable() {
        OdkDbInterface serviceInterface = bindToDbService();
        try {
            ColumnList columnList = createColumnList();

            OdkDbHandle db = serviceInterface.openDatabase(APPNAME, false);
            // TODO: why do we have a dbHandle and APPNAME?
            serviceInterface.createOrOpenDBTableWithColumns(APPNAME, db, DB_TABLE_NAME, columnList);

            List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);

            // verify only 1 table
            assertTrue(tableIds.size() == 1);
            assertTrue(tableIds.contains(DB_TABLE_NAME));

            serviceInterface.deleteDBTableAndAllData(APPNAME, db, DB_TABLE_NAME);

            tableIds = serviceInterface.getAllTableIds(APPNAME, db);
            // verify no tables
            assertTrue(tableIds.size() == 0);
        } catch (RemoteException e) {
            e.printStackTrace();
            fail();
        }
    }



}
