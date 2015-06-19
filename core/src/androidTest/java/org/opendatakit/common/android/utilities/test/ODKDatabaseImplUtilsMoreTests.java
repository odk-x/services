/*
 * Copyright (C) 2015 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.common.android.utilities.test;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.database.OdkDatabase;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.database.service.OdkDbHandle;

import android.test.AndroidTestCase;
import android.test.RenamingDelegatingContext;

public class ODKDatabaseImplUtilsMoreTests extends AndroidTestCase {

  private static final String TEST_FILE_PREFIX = "test_";

  private OdkDbHandle uniqueKey;
  OdkDatabase db = null;

  private static class DatabaseInitializer {

    public static void onCreate(OdkDatabase db) {
      
    }
  }

  /*
   * Set up the database for the tests(non-Javadoc)
   * 
   * @see android.test.AndroidTestCase#setUp()
   */
  @Override
  protected synchronized void setUp() throws Exception {
    super.setUp();

    StaticStateManipulator.get().reset();

    uniqueKey = DatabaseFactory.get().generateInternalUseDbHandle();

    RenamingDelegatingContext context = new RenamingDelegatingContext(getContext(),
        TEST_FILE_PREFIX);

    DatabaseFactory.get().releaseAllDatabases(context);
    FileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(getAppName())));
    
    ODKFileUtils.verifyExternalStorageAvailability();

    ODKFileUtils.assertDirectoryStructure(getAppName());
    
    db = DatabaseFactory.get().getDatabase(context, getAppName(), uniqueKey);

    DatabaseInitializer.onCreate(db);
  }

  protected String getAppName() {
    return "test-" + uniqueKey.getDatabaseHandle().substring(6);
  }
  
  /*
   * Destroy all test data once tests are done(non-Javadoc)
   * 
   * @see android.test.AndroidTestCase#tearDown()
   */
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();

    if (db != null) {
      db.close();
    }
      
    RenamingDelegatingContext context = new RenamingDelegatingContext(getContext(),
        TEST_FILE_PREFIX);
    DatabaseFactory.get().releaseDatabase(context, getAppName(), uniqueKey);
    DatabaseFactory.get().releaseAllDatabases(context);
    FileUtils.deleteDirectory(new File(ODKFileUtils.getAppFolder(getAppName())));
  }


  /*
   * Test query when there is no data
   */
  public void testQueryWithNoData_ExpectFail() {
    String tableId = "badTable";
    boolean thrown = false;

    try {
      ODKDatabaseImplUtils.get().query(db, tableId, null, null, null, null, null, null, null);
    } catch (Exception e) {
      thrown = true;
      e.printStackTrace();
    }

    assertTrue(thrown);
  }


}
