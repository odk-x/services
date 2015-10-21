package org.opendatakit.common.android.utilities.test;

import android.test.RenamingDelegatingContext;
import android.util.Log;

import org.apache.commons.io.FileUtils;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.database.DatabaseConstants;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.provider.TableDefinitionsColumns;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.database.service.OdkDbHandle;

import java.io.File;

/**
 * Created by wrb on 9/21/2015.
 *
 * Put the actual unit tests in the Abstract class as there are two setups.
 *
 * In ODKDatabaseImplUtilsKeepState it keeps the database initalized between tests whereas
 * in ODKDatabaseImplUtilsResetState, it wipes the database from the file system between each test
 */
public class ODKDatabaseImplUtilsResetState extends AbstractODKDatabaseUtilsTest {
    private static final String TEST_FILE_PREFIX = "test_";
    private static final String DATABASE_NAME = "test.db";

    private OdkDbHandle uniqueKey;

    protected String getAppName() {
        return "test-" + uniqueKey.getDatabaseHandle().substring(6);
    }

    private static class DatabaseInitializer {


        public static void onCreate(OdkConnectionInterface db) {
            String createColCmd = ColumnDefinitionsColumns
                    .getTableCreateSql(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);

            try {
                db.execSQL(createColCmd);
            } catch (Exception e) {
                Log.e("test", "Error while creating table "
                        + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);
                e.printStackTrace();
            }

            String createTableDefCmd = TableDefinitionsColumns
                    .getTableCreateSql(DatabaseConstants.TABLE_DEFS_TABLE_NAME);

            try {
                db.execSQL(createTableDefCmd);
            } catch (Exception e) {
                Log.e("test", "Error while creating table " + DatabaseConstants.TABLE_DEFS_TABLE_NAME);
                e.printStackTrace();
            }

            String createKVSCmd = KeyValueStoreColumns
                    .getTableCreateSql(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);

            try {
                db.execSQL(createKVSCmd);
            } catch (Exception e) {
                Log.e("test", "Error while creating table "
                        + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);
                e.printStackTrace();
            }
        }
    }

    @Override
    protected synchronized void setUp() throws Exception {
        super.setUp();

        //StaticStateManipulator.get().reset();

        // Just to initialize this
        AndroidConnectFactory.configure();
        uniqueKey = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().generateInternalUseDbHandle();

        RenamingDelegatingContext context = new RenamingDelegatingContext(getContext(),
                TEST_FILE_PREFIX);

      OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().releaseAllDatabases();
        FileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(getAppName())));

        ODKFileUtils.verifyExternalStorageAvailability();

        ODKFileUtils.assertDirectoryStructure(getAppName());

        // +1 referenceCount if db is returned (non-null)
        db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(getAppName(), uniqueKey);

        DatabaseInitializer.onCreate(db);
    }

    @Override
    protected void tearDown() throws Exception {

        if (db != null) {
            db.releaseReference();
        }

        RenamingDelegatingContext context = new RenamingDelegatingContext(getContext(), TEST_FILE_PREFIX);
        OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().releaseDatabase(getAppName(), uniqueKey);
        OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().releaseAllDatabases();
        // give a chance for GC to happen so that we
        // release and close database handles in the
        // C++ layer that were orphaned in Java
        Thread.sleep(100L);
        try {
           FileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(getAppName())));
        } catch ( Exception e) {
           // ignore
        }

        super.tearDown();
    }
}
