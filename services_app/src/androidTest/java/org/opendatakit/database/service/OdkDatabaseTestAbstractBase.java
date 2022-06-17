package org.opendatakit.database.service;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import android.Manifest;
import android.content.Context;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.GrantPermissionRule;
import androidx.test.rule.ServiceTestRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.opendatakit.TestConsts;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.services.database.AndroidConnectFactory;

import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Created by wrb on 9/26/2017.
 */

abstract class OdkDatabaseTestAbstractBase {
   private static final String LOGTAG = OdkDatabaseTestAbstractBase.class.getName();
   protected static final String APPNAME = TestConsts.APPNAME;
   protected static final String DB_TABLE_ID = "testtable";

   private static int bindToDbServiceCount = 0;

   @Rule
   public final ServiceTestRule mServiceRule = new ServiceTestRule();
   @Rule
   public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);
   @Rule
   public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

   private boolean initialized = false;

   abstract protected void setUpBefore();

   abstract protected void tearDownBefore();

   @Before
   public void setUp() throws Exception {
      boolean beganUninitialized = !initialized;
      if (beganUninitialized) {
         initialized = true;
         // Used to ensure that the singleton has been initialized properly
         AndroidConnectFactory.configure();
      }
      setUpBefore();
   }

   @After
   public void tearDown() throws Exception {
      tearDownBefore();
      UserDbInterface serviceInterface = bindToDbService();
      try {
          assertNotNull(serviceInterface);
         DbHandle db = serviceInterface.openDatabase(APPNAME);
         Log.i("OdkDatabaseServiceTest", "tearDown: " + db.getDatabaseHandle());
         verifyNoTablesExistNCleanAllTables(serviceInterface, db);
         serviceInterface.closeDatabase(APPNAME, db);
      } catch (ServicesAvailabilityException e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Nullable protected UserDbInterfaceImpl bindToDbService() {
      Context context = InstrumentationRegistry.getInstrumentation().getContext();

      ++bindToDbServiceCount;
      Intent bind_intent = new Intent();
      bind_intent.setClassName(IntentConsts.Database.DATABASE_SERVICE_PACKAGE,
          IntentConsts.Database.DATABASE_SERVICE_CLASS);

      int count = 0;
      UserDbInterfaceImpl dbInterface;
      try {
         IBinder service = null;
         while ( service == null ) {
            try {
               service = mServiceRule.bindService(bind_intent);
            } catch (TimeoutException e) {
               dbInterface = null;
            }
            if ( service == null ) {
               ++count;
               if ( count % 20 == 0 ) {
                  Log.i(LOGTAG, "bindToDbService failed for " + count +
                      " tries so far on bindToDbServiceCount " + bindToDbServiceCount);
               }
               try {
                  Thread.sleep(10);
               } catch (InterruptedException e) {
               }
            }
         }
         dbInterface = new UserDbInterfaceImpl(
             new InternalUserDbInterfaceAidlWrapperImpl(IDbInterface.Stub.asInterface(service)));
      } catch (IllegalArgumentException e) {
         dbInterface = null;
      }
      return dbInterface;
   }

   private void verifyNoTablesExistNCleanAllTables(UserDbInterface serviceInterface,
       DbHandle db) throws ServicesAvailabilityException {
      List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);

      boolean tablesGone = (tableIds.size() == 0);

      // Drop any leftover table now that the test is done
      for (String id : tableIds) {
         Log.e(LOGTAG, "Left over table with ID:" + id);
         serviceInterface.deleteTableAndAllData(APPNAME, db, id);
         serviceInterface.deleteTableMetadata(APPNAME, db, id, null, null, null);
      }

      assertTrue(tablesGone);
   }

   protected boolean hasNoTablesInDb(UserDbInterface serviceInterface, DbHandle db)
       throws ServicesAvailabilityException {
      List<String> tableIds = serviceInterface.getAllTableIds(APPNAME, db);
      return (tableIds.size() == 0);
   }
}
