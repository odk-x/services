package org.opendatakit.common.android.utilities.test;

import android.content.ContentValues;
import android.database.Cursor;
import android.test.AndroidTestCase;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.TestConsts;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.database.AndroidConnectFactory;
import org.opendatakit.common.android.database.OdkConnectionFactorySingleton;
import org.opendatakit.common.android.database.OdkConnectionInterface;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.database.service.OdkDbHandle;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by clarice on 2/10/16.
 */
public class ODKDatabaseImplUtilsPerformance extends AndroidTestCase {

  private static boolean initialized = false;
  private static final String APPNAME = TestConsts.APPNAME;
  private static final OdkDbHandle uniqueKey = new OdkDbHandle(AbstractODKDatabaseUtilsTest.class.getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);

  private static final String TAG = "AbstractODKDatabaseUtilsTest";
  private static final String testLogFileName = "test-Android.log";

  private static final String testTable = "testTable";

  private static final String elemKey = "_element_key";
  private static final String elemName = "_element_name";
  private static final String listChildElemKeys = "_list_child_element_keys";

  private static long testStartTime;
  private static long testExecutionTime;

  protected OdkConnectionInterface db;

  protected String getAppName() {
    return APPNAME;
  }

  /*
   * Set up the database for the tests(non-Javadoc)
   *
   * @see android.test.AndroidTestCase#setUp()
   */
  @Override
  protected synchronized void setUp() throws Exception {
    super.setUp();
    ODKFileUtils.verifyExternalStorageAvailability();
    ODKFileUtils.assertDirectoryStructure(APPNAME);

    boolean beganUninitialized = !initialized;
    if ( beganUninitialized ) {
      initialized = true;
      // Used to ensure that the singleton has been initialized properly
      AndroidConnectFactory.configure();
    }

    // +1 referenceCount if db is returned (non-null)
    db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface().getConnection(getAppName(), uniqueKey, getContext());
    if ( beganUninitialized ) {
      // start clean
      List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

      // Drop any leftover table now that the test is done
      for(String id : tableIds) {
        ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), id);
      }
    } else {
      verifyNoTablesExistNCleanAllTables();
    }


  }


  /*
   * Destroy all test data once tests are done(non-Javadoc)
   *
   * @see android.test.AndroidTestCase#tearDown()
   */
  @Override
  protected void tearDown() throws Exception {
    testExecutionTime = System.currentTimeMillis() - testStartTime;

    verifyNoTablesExistNCleanAllTables();

    if (db != null) {
      db.releaseReference();
    }

    super.tearDown();
  }

  protected void verifyNoTablesExistNCleanAllTables() {
    /* NOTE: if there is a problem it might be the fault of the previous test if the assertion
       failure is happening in the setUp function as opposed to the tearDown function */

    List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);

    boolean tablesGone = (tableIds.size() == 0);

    // Drop any leftover table now that the test is done
    for(String id : tableIds) {
      ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), id);
    }

    assertTrue(tablesGone);
  }

  protected void verifyNoTablesExist() {
    List<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);
    assertTrue(tableIds.size() == 0);
  }

  protected void writeTestInformationToFile(String testName, long timeInMillis) {
    String loggingDir = ODKFileUtils.getLoggingFolder(getAppName());

    // Create log test file if necessary
    File testLogFile = new File(loggingDir + File.separator + testLogFileName);

    try {
      if (!testLogFile.exists()) {
        testLogFile.createNewFile();
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.print("Error while trying to write test output information to log file");
    }

    // Write test data into file
    try {
      Date currentDate = new Date();
      FileOutputStream fo = new FileOutputStream(testLogFile, true);
      OutputStreamWriter logFile = new OutputStreamWriter(new BufferedOutputStream(fo), CharEncoding.UTF_8);
      logFile.write(currentDate.toString() + ": " + testName + ": " + timeInMillis + " ms\n");
      logFile.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.out.print("Exception while trying to write data to log file");
    }
  }

  /*
   * Check that the database is setup
   */
  public void testPreConditions() {
    assertNotNull(db);
  }

  // Test inserting 1000 rows with 1 query to confirm all data inserted
  public void testInserting1000Rows_ExpectPass() {
    String testName = "testInserting1000Rows_ExpectPass";
    String tableId = "testTable";
    int numOfIterations = 1000;
    String testColType = ElementDataType.integer.name();
    int numOfColumns = 10;

    testStartTime = System.currentTimeMillis();

    // Create the test table
    List<Column> columns = new ArrayList<Column>();
    for (int i = 0; i < numOfColumns; ++i) {
      String testCol = "testColumn_" + Integer.toString(i);
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert 1000 rows
    for (int i = 0 ; i < numOfIterations ; i++ ) {
      String rowId = ODKDataUtils.genUUID();

      ContentValues cvValues = new ContentValues();
      for (int j = 0; j < numOfColumns; j++) {
        String testCol = "testColumn_" + Integer.toString(j);
        //String testVal = "testVal_" + Integer.toString(j);
        int testVal = i;
        cvValues.put(testCol, testVal);
      }

      ODKDatabaseImplUtils.get()
              .insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);
    }

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId;
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, null);

    assertEquals(cursor.getCount(), numOfIterations);

    cursor.close();

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);

    testExecutionTime = System.currentTimeMillis() - testStartTime;
    writeTestInformationToFile(testName, testExecutionTime);
  }

  // Test querying 1000 times
  public void testQuerying1000Rows_ExpectPass() {
    String testName = "testQuerying1000Rows_ExpectPass";
    String tableId = "testTable";
    int numOfIterations = 1000;
    String testColType = ElementDataType.integer.name();
    int numOfColumns = 10;

    testStartTime = System.currentTimeMillis();

    // Create the test table
    List<Column> columns = new ArrayList<Column>();
    for (int i = 0; i < numOfColumns; ++i) {
      String testCol = "testColumn_" + Integer.toString(i);
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert 1000 rows
    for (int i = 0 ; i < numOfIterations ; i++ ) {
      ContentValues cvValues = new ContentValues();
      String rowId = ODKDataUtils.genUUID();
      for (int j = 0; j < numOfColumns; j++) {
        String testCol = "testColumn_" + Integer.toString(j);
        //String testVal = "testVal_" + Integer.toString(j);
        int testVal = i;
        cvValues.put(testCol, testVal);
      }

      ODKDatabaseImplUtils.get()
              .insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);
    }

    // Select everything out of the table
    Cursor cursor = null;
    for (int k = 0; k < numOfIterations; k++) {
      String queryCol = "testColumn_" + Integer.toString(0);
      String sel = "SELECT * FROM " + tableId + " WHERE " + queryCol
              + "= ?";
      String[] selArgs = {Integer.toString(k)};
      cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, selArgs);

      assertEquals(cursor.getCount(), 1);

      int val = 0;
      while (cursor.moveToNext()) {
        int ind = cursor.getColumnIndex(queryCol);
        int type = cursor.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        val = cursor.getInt(ind);
        assertEquals(val, k);
      }
    }

    if (cursor != null) {
      cursor.close();
    }

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);

    testExecutionTime = System.currentTimeMillis() - testStartTime;
    writeTestInformationToFile(testName, testExecutionTime);
  }

  // Test deleting 1000 rows in random order
  public void testDeleting1000Rows_ExpectPass() {
    String testName = "testDeleting1000Rows_ExpectPass";
    String tableId = "testTable";
    int numOfIterations = 1000;
    String testColType = ElementDataType.integer.name();
    int numOfColumns = 10;

    testStartTime = System.currentTimeMillis();

    // Create the test table
    List<Column> columns = new ArrayList<Column>();
    for (int i = 0; i < numOfColumns; ++i) {
      String testCol = "testColumn_" + Integer.toString(i);
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert 1000 rows
    insertRows(tableId, 0, numOfIterations, numOfColumns, orderedColumns);

    // Now delete all rows in random order
    try {
      randomlyDeleteValueInRange(tableId, 0, numOfIterations, numOfIterations);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.print("testDeleting1000Rows_ExpectPass: Error in randomlyDeleteValueInRange");
    }

    // Select everything out of the table
    String sel = "SELECT * FROM " + tableId;
    Cursor cursorFinal = ODKDatabaseImplUtils.get().rawQuery(db, sel, null);

    assertEquals(cursorFinal.getCount(), 0);

    cursorFinal.close();

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);

    testExecutionTime = System.currentTimeMillis() - testStartTime;
    writeTestInformationToFile(testName, testExecutionTime);
  }

  // Test 100 insertions follows by 1 query and 1 deletion x10
  public void testInsertionQueryDeletion_ExpectPass() {
    String testName = "testInsertionQueryDeletion_ExpectPass";
    String tableId = "testTable";
    int numOfIterations = 1000;
    String testColType = ElementDataType.integer.name();
    int numOfColumns = 10;
    int incVal = 100;
    int attempts = 0;

    testStartTime = System.currentTimeMillis();

    // Create the test table
    List<Column> columns = new ArrayList<Column>();
    for (int i = 0; i < numOfColumns; ++i) {
      String testCol = "testColumn_" + Integer.toString(i);
      columns.add(new Column(testCol, testCol, testColType, "[]"));
    }
    OrderedColumns orderedColumns = ODKDatabaseImplUtils.get()
            .createOrOpenDBTableWithColumns(db, getAppName(), tableId, columns);

    // Insert Rows
    for (int i = 0; i < numOfIterations; i+=incVal) {
      int start = i;
      int end = i + incVal;
      insertRows(tableId, start, end, numOfColumns, orderedColumns);

      // Delete one row randomly in the range
      try {
        randomlyDeleteValueInRange(tableId, start, end, 1);
      } catch (Exception e) {
        e.printStackTrace();
        System.out.print("testInsertionQueryDeletion_ExpectPass: Error in randomlyDeleteValueInRange");
      }
      attempts++;
    }

    String sel = "SELECT * FROM " + tableId;
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, null);

    assertEquals(cursor.getCount(), numOfIterations - attempts);

    // Drop the table now that the test is done
    ODKDatabaseImplUtils.get().deleteDBTableAndAllData(db, getAppName(), tableId);

    testExecutionTime = System.currentTimeMillis() - testStartTime;
    writeTestInformationToFile(testName, testExecutionTime);
  }

  // Randomly deletes database values in range from high to low
  // Meant to be called once with original array placeholders
  // for all rows in db, but can be called after some rows have already been
  // deleted from db table as long as the range of values has never
  // been used before
  private void randomlyDeleteValueInRange(String tableId, int low, int high, int numOfIterations) throws Exception {
    int numOfRowsInTable = 0;
    int initVal = -1;
    long randSeed = 150L;
    int rangeOfDelVals = high - low;

    // Get the number of rows in the table currently
    String sel = "SELECT * FROM " + tableId;
    Cursor initCursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, null);
    numOfRowsInTable = initCursor.getCount();
    initCursor.close();

    // Test parameter validity
    if (low < 0 || rangeOfDelVals > numOfRowsInTable) {
      throw new Exception ("low can't be lower than 0 and (high - low) can't be greater than number of rows in table");
    }

    if (numOfIterations > numOfRowsInTable) {
      throw new Exception ("numOfIterations can't be more than number of rows in table");
    }

    // Now delete all rows in random order
    Random randGen = new Random(randSeed);
    int [] placeHolder = new int[high];
    Arrays.fill(placeHolder, initVal);

    for (int k = 0; k < numOfIterations; k++) {
      Cursor cursorRowId = null;
      Cursor cursorDel = null;

      int randVal = randGen.nextInt(high - low) + low;

      while (placeHolder[randVal] != initVal && ArrayUtils.contains(placeHolder, initVal)) {
        randVal = randGen.nextInt(high - low) + low;
      }

      // No empty slots left
      if (!ArrayUtils.contains(placeHolder, initVal)) {
        return;
      }

      placeHolder[randVal] = randVal;

      // Select everything out of the table for a certain row
      String queryCol = "testColumn_" + Integer.toString(0);
      String selRowId = "SELECT * FROM " + tableId + " WHERE " + queryCol
              + " = ?";
      String[] selArgsRowId = {Integer.toString(randVal)};
      cursorRowId = ODKDatabaseImplUtils.get().rawQuery(db, selRowId, selArgsRowId);

      assertEquals(cursorRowId.getCount(), 1);

      int val = 0;
      while (cursorRowId.moveToNext()) {
        int ind = cursorRowId.getColumnIndex(queryCol);
        int type = cursorRowId.getType(ind);
        assertEquals(type, Cursor.FIELD_TYPE_INTEGER);
        val = cursorRowId.getInt(ind);
        assertEquals(val, randVal);

        // Get the savepoint_type
        int indId = cursorRowId.getColumnIndex(DataTableColumns.ID);
        int typeId = cursorRowId.getType(indId);
        assertEquals(typeId, Cursor.FIELD_TYPE_STRING);
        String rowId = cursorRowId.getString(indId);

        ODKDatabaseImplUtils.get().deleteRowWithId(db, getAppName(), tableId, rowId);

        // Check that the row was actually deleted
        String selDel = "SELECT * FROM " + tableId + " WHERE " + DataTableColumns.ID
                + " = ?";
        String[] selArgsDel = {rowId};
        cursorDel = ODKDatabaseImplUtils.get().rawQuery(db, selDel, selArgsDel);

        assertEquals(cursorDel.getCount(), 0);
      }

      if (cursorRowId != null) {
        cursorRowId.close();
      }

      if (cursorDel != null) {
        cursorDel.close();
      }
    }
  }

  private void insertRows(String tableId, int start, int stop, int numOfColumns, OrderedColumns orderedColumns) {
    // Get the number of rows currently in the table if any
    String sel = "SELECT * FROM " + tableId;
    Cursor cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, null);
    int currNumOfRows = cursor.getCount();

    // Insert rows
    for (int i = start ; i < stop ; i++ ) {
      ContentValues cvValues = new ContentValues();
      String rowId = ODKDataUtils.genUUID();
      for (int j = 0; j < numOfColumns; j++) {
        String testCol = "testColumn_" + Integer.toString(j);
        int testVal = i;
        cvValues.put(testCol, testVal);
      }

      ODKDatabaseImplUtils.get()
              .insertRowWithId(db, tableId, orderedColumns, cvValues, rowId);
    }

    // Select everything out of the table
    cursor = ODKDatabaseImplUtils.get().rawQuery(db, sel, null);

    assertEquals(cursor.getCount(), currNumOfRows + (stop - start));

    cursor.close();
  }
}
