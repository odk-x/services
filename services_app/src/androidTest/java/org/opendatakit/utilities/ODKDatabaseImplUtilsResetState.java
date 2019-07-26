package org.opendatakit.utilities;

import android.util.Log;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.provider.ColumnDefinitionsColumns;
import org.opendatakit.provider.KeyValueStoreColumns;
import org.opendatakit.provider.TableDefinitionsColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;

import java.io.File;

/**
 * Created by wrb on 9/21/2015.
 *
 * Put the actual unit tests in the Abstract class as there are two setups.
 *
 * In ODKDatabaseImplUtilsKeepState it keeps the database initalized between tests whereas
 * in ODKDatabaseImplUtilsResetState, it wipes the database from the file system between each test
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODKDatabaseImplUtilsResetState extends AbstractODKDatabaseUtilsTest {

    private DbHandle uniqueKey;

    protected String getAppName() {
        return "test-" + uniqueKey.getDatabaseHandle().substring(6);
    }

    private static class DatabaseInitializer {


        public static void onCreate(OdkConnectionInterface db) {
            String createColCmd = ColumnDefinitionsColumns
                    .getTableCreateSql(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);

            try {
                db.execSQL(createColCmd, null);
            } catch (Exception e) {
                Log.e("test", "Error while creating table "
                        + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);
                e.printStackTrace();
            }

            String createTableDefCmd = TableDefinitionsColumns
                    .getTableCreateSql(DatabaseConstants.TABLE_DEFS_TABLE_NAME);

            try {
                db.execSQL(createTableDefCmd, null);
            } catch (Exception e) {
                Log.e("test", "Error while creating table " + DatabaseConstants.TABLE_DEFS_TABLE_NAME);
                e.printStackTrace();
            }

            String createKVSCmd = KeyValueStoreColumns
                    .getTableCreateSql(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);

            try {
                db.execSQL(createKVSCmd, null);
            } catch (Exception e) {
                Log.e("test", "Error while creating table "
                        + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);
                e.printStackTrace();
            }
        }
    }

    @Before
    public synchronized void setUp() throws Exception {

        //StaticStateManipulator.get().reset();

        // Just to initialize this
        AndroidConnectFactory.configure();
        uniqueKey = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();

        OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeAllConnections();
        ODKFileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(getAppName())));

        ODKFileUtils.verifyExternalStorageAvailability();

        ODKFileUtils.assertDirectoryStructure(getAppName());

        // +1 referenceCount if db is returned (non-null)
        db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(getAppName(), uniqueKey);

        DatabaseInitializer.onCreate(db);
    }

    @After
    public void tearDown() throws Exception {

        if (db != null) {
            db.releaseReference();
        }

        OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeConnection(
            getAppName(), uniqueKey);
        OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().removeAllConnections();
        // give a chance for GC to happen so that we
        // release and close database handles in the
        // C++ layer that were orphaned in Java
        Thread.sleep(100L);
        try {
            ODKFileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(getAppName())));
        } catch ( Exception e) {
           // ignore
        }
    }
}
