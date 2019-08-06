package org.opendatakit.utilities;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.opendatakit.TestConsts;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.utilities.ODKDatabaseImplUtils;

import java.util.List;

/**
 * Created by wrb on 9/21/2015.
 *
 * Put the actual unit tests in the Abstract class as there are two setups.
 *
 * In ODKDatabaseImplUtilsKeepState it keeps the database initalized between tests whereas
 * in ODKDatabaseImplUtilsResetState, it wipes the database from the file system between each test
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ODKDatabaseImplUtilsKeepState extends AbstractODKDatabaseUtilsTest {

   private static boolean initialized = false;
    private static final String APPNAME = TestConsts.APPNAME;
    private static final DbHandle uniqueKey = new DbHandle(AbstractODKDatabaseUtilsTest.class.getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);

    @Override
    protected String getAppName() {
        return APPNAME;
    }

    @Before
    public synchronized void setUp() throws Exception {
       ODKFileUtils.verifyExternalStorageAvailability();
       ODKFileUtils.assertDirectoryStructure(APPNAME);

       boolean beganUninitialized = !initialized;
       if (beganUninitialized) {
          initialized = true;
          // Used to ensure that the singleton has been initialized properly
          AndroidConnectFactory.configure();
       }

       // +1 referenceCount if db is returned (non-null)
       db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
           .getConnection(getAppName(), uniqueKey);
       if (beganUninitialized) {
          // start clean
          List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

          // Drop any leftover table now that the test is done
          for (String id : tableIds) {
             ODKDatabaseImplUtils.get().deleteTableAndAllData(db, id);
          }
       } else {
          verifyNoTablesExistNCleanAllTables();
       }
    }

    @After
    public void tearDown() throws Exception {
      verifyNoTablesExistNCleanAllTables();

      if (db != null) {
        db.releaseReference();
      }
    }
}
