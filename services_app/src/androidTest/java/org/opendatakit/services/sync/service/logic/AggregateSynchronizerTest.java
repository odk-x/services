package org.opendatakit.services.sync.service.logic;

import android.content.Context;
import android.support.test.InstrumentationRegistry;
import android.support.test.rule.ServiceTestRule;
import android.support.test.runner.AndroidJUnit4;
import android.test.ApplicationTestCase;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.*;
import org.opendatakit.application.ToolAwareApplication;
import org.opendatakit.database.data.BaseTable;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.httpclientandroidlib.HttpStatus;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.R;
import org.opendatakit.services.application.Services;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.services.sync.service.exceptions.HttpClientWebException;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncNotification;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by clarice on 5/6/16.
 */
@RunWith(AndroidJUnit4.class)
public class AggregateSynchronizerTest {
  String agg_url;
  String appId;
  String absolutePathOfTestFiles;
  String host;
  String userName;
  String password;
  String appName;
  int batchSize;
  int version;

  @Rule
  public final ServiceTestRule mServiceRule = new ServiceTestRule();

  @Before
  public void setUp() throws Exception {

    agg_url = "https://test.appspot.com";
    appId = "odktables/default";
    absolutePathOfTestFiles = "testfiles/test/";
    batchSize = 1000;
    userName = "";
    password = "";
    appName = "test";
    URL url = new URL(agg_url);
    host = url.getHost();
    version = 2;
  }

  private SyncExecutionContext getSyncExecutionContext() {
    Context context = InstrumentationRegistry.getTargetContext();
    SyncNotification syncProg = new SyncNotification(context, appName);
    SyncOverallResult syncRes = new SyncOverallResult();

    PropertiesSingleton props = CommonToolProperties.get(context, appName);
    props.setProperty(CommonToolProperties.KEY_SYNC_SERVER_URL, agg_url);
    if ( userName.length() == 0 ) {
      props.setProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE,
          context.getString(R.string.credential_type_none));
      props.setProperty(CommonToolProperties.KEY_USERNAME, userName);
      props.setProperty(CommonToolProperties.KEY_PASSWORD, password);
      props.setProperty(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      props.setProperty(CommonToolProperties.KEY_ROLES_LIST, "");
      props.setProperty(CommonToolProperties.KEY_USERS_LIST, "");
    } else {
      props.setProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE,
          context.getString(R.string.credential_type_username_password));
      props.setProperty(CommonToolProperties.KEY_USERNAME, userName);
      props.setProperty(CommonToolProperties.KEY_PASSWORD, password);
      props.setProperty(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      props.setProperty(CommonToolProperties.KEY_ROLES_LIST, "");
      props.setProperty(CommonToolProperties.KEY_USERS_LIST, "");
    }

    SyncExecutionContext syncExecutionContext = new SyncExecutionContext(context,
        ((ToolAwareApplication) context).getVersionCodeString(), appName, syncProg, syncRes);

    return syncExecutionContext;
  }

  BaseTable buildBaseTable(OrderedColumns orderedColumns, int tableSize) {

    String[] primaryKey = {TableConstants.ID};
    ArrayList<String> elementKeys = new ArrayList<String>();
    elementKeys.addAll(TableConstants.CLIENT_ONLY_COLUMN_NAMES);
    elementKeys.addAll(TableConstants.SHARED_COLUMN_NAMES);
    elementKeys.addAll(orderedColumns.getRetentionColumnNames());
    Collections.sort(elementKeys);
    String[] elementKeysToIndex = new String[elementKeys.size()];
    elementKeysToIndex = elementKeys.toArray(elementKeysToIndex);
    BaseTable refTable = new BaseTable(primaryKey, elementKeysToIndex, null, tableSize);

    return refTable;
  }

  void appendRowContent(BaseTable baseTable, OrderedColumns orderedColumns, String rowId,
      String fieldValue, String savepointTimestamp) {

    String[] rowData = new String[baseTable.getElementKeyForIndex().length];

    // store the fieldValue into the user data column
    int countFields = 0;
    for (ColumnDefinition column : orderedColumns.getColumnDefinitions()) {
      if (column.isUnitOfRetention()) {
        ++countFields;
        String elementKey = column.getElementKey();
        rowData[baseTable.getColumnIndexOfElementKey(elementKey)] = fieldValue;
      }
    }
    assertEquals(1, countFields);

    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.ID)] = rowId;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.ROW_ETAG)] = null;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.FORM_ID)] = null;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.LOCALE)] = Locale.getDefault().getLanguage();
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.SYNC_STATE)] = SyncState.new_row.name();
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.CONFLICT_TYPE)] = null;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.SAVEPOINT_TYPE)] =
        SavepointTypeManipulator.complete();
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.SAVEPOINT_TIMESTAMP)] = savepointTimestamp;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.SAVEPOINT_CREATOR)] = userName;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.DEFAULT_ACCESS)] = null;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.ROW_OWNER)] = userName;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.GROUP_READ_ONLY)] = null;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.GROUP_MODIFY)] = null;
    ++countFields;
    rowData[baseTable.getColumnIndexOfElementKey(TableConstants.GROUP_PRIVILEGED)] = null;

    assertEquals(baseTable.getElementKeyForIndex().length, countFields);

    org.opendatakit.database.data.Row theRow =
        new org.opendatakit.database.data.Row(rowData, baseTable);

    baseTable.addRow(theRow);
  }

  /*
   * Perform tear down for tests if necessary
   */
  @After
  public void tearDown() throws Exception {
    // no-op
  }

  @Test
  public void testNoOp() {
     // no-op
  }

//  /*
//   * Test getting the app level file manifest with no files
//   */
//  @Test
//  public void testGetAppLevelFileManifestWithNoFiles_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      FileManifestDocument tableManifestEntries =
//          synchronizer.getAppLevelFileManifest(null, null, true);
//      assertEquals(tableManifestEntries.entries.size(), 0);
//
//    } catch (Exception e) {
//      fail("testGetAppLevelFileManifestWithNoFiles_ExpectPass: expected pass but got exception");
//      e.printStackTrace();
//
//    }
//  }
//
//  /*
//   * Test get list of tables when no tables exist
//   */
//  @Test
//  public void testGetTablesWhenNoTablesExist_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResourceList tables = synchronizer.getTables(null);
//
//      assertEquals(tables.getTables().size(), 0);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetTablesWhenNoTablesExist_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test get table definitions when no tables exist
//   */
//  @Test
//  public void testGetTableDefinitions_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test0";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testGetTableDefinitions_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      assertEquals(tableDefRes.getTableId(), testTableId);
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetTableDefinitions_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test upload config file
//   */
//  @Test
//  public void testUploadConfigFileWithTextFile_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      String fileName = "testFile.txt";
//
//      String destDir = ODKFileUtils.getConfigFolder(appName);
//
//      File destFile = new File(destDir, fileName);
//
//      // Now copy this file over to the correct place on the device
//      FileUtils.writeStringToFile(destFile, "This is a test", Charsets.UTF_8);
//
//      synchronizer.uploadConfigFile(destFile);
//
//      synchronizer.deleteConfigFile(destFile);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testuploadConfigFileWithTextFile_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test delete config file
//   */
//  @Test
//  public void testDeleteConfigFile_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      String fileName = "testFile.txt";
//
//      String destDir = ODKFileUtils.getConfigFolder(appName);
//
//      File destFile = new File(destDir, fileName);
//
//      // Now copy this file over to the correct place on the device
//      FileUtils.writeStringToFile(destFile, "This is a test", Charsets.UTF_8);
//
//      synchronizer.uploadConfigFile(destFile);
//
//      synchronizer.deleteConfigFile(destFile);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testDeleteConfigFile_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test create table with string
//   */
//  @Test
//  public void testCreateTableWithString_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test0";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testCreateTableWithString_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testCreateTableWithString_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test delete table
//   */
//  @Test
//  public void testDeleteTable_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test1";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testDeleteTable_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testDeleteTable_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test get change sets
//   */
//  @Test
//  public void testGetChangeSetsWhenThereAreNoChanges_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test2";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testGetChangeSetsWhenThereAreNoChanges_ExpectPass";
//    String tableDataETag = null;
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      tableDataETag = testTableRes.getDataETag();
//
//      // Need to get all tables again
//      // Maybe that tableResource will have the right dataETag
//
//      ChangeSetList changeSetList = synchronizer.getChangeSets(testTableRes, tableDataETag);
//
//      ArrayList<String> changeSets = changeSetList.getChangeSets();
//
//      assertEquals(changeSets.size(), 0);
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetChangeSets_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test get change set
//   */
//  @Test
//  public void testGetChangeSetWhenThereAreNoChanges_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test3";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testGetChangeSetWhenThereAreNoChanges_ExpectPass";
//    String tableDataETag = null;
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      tableDataETag = testTableRes.getDataETag();
//
//      RowResourceList rowResourceList = synchronizer.getChangeSet(testTableRes, tableDataETag, true, null);
//
//      ArrayList<RowResource> rowRes = rowResourceList.getRows();
//
//      assertEquals(rowRes.size(), 0);
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetChangeSetWhenThereAreNoChanges_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test get updates when there are no changes
//   */
//  @Test
//  public void testGetUpdatesWhenThereAreNoChanges_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test4";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testGetUpdatesWhenThereAreNoChanges_ExpectPass";
//    String tableDataETag = null;
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      tableDataETag = testTableRes.getDataETag();
//
//      RowResourceList rowResourceList = synchronizer.getUpdates(testTableRes, tableDataETag,
//          null, 100);
//
//      ArrayList<RowResource> rowRes = rowResourceList.getRows();
//
//      assertEquals(rowRes.size(), 0);
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetUpdatesWhenThereAreNoChanges_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test get updates when there are changes
//   */
//  @Test
//  public void testGetUpdatesWhenThereAreUpdates_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test41";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testGetUpdatesWhenThereAreUpdates_ExpectPass";
//    String tableDataETag = null;
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 1);
//
//      tableDataETag = testTableRes.getDataETag();
//
//      // Insert a row
//      String val = "test value for table " + testTableId;
//
//      String rowId = "uuid:" + UUID.randomUUID().toString();
//      String ts = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
//
//      appendRowContent(refTable, orderedColumns, rowId, val, ts);
//
//      ArrayList<org.opendatakit.database.data.Row> listOfRowsToCreate =
//          new ArrayList<org.opendatakit.database.data.Row>();
//
//      listOfRowsToCreate.addAll(refTable.getRows());
//
//      RowOutcomeList outcomes = synchronizer.pushLocalRows(testTableRes, orderedColumns,
//          listOfRowsToCreate);
//
//      RowResourceList rowResourceList = synchronizer.getUpdates(testTableRes, tableDataETag,
//          null, 100);
//
//      ArrayList<RowResource> rowRes = rowResourceList.getRows();
//
//      assertEquals(rowRes.size(), 1);
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetUpdatesWhenThereAreUpdates_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
// * Test get updates when there are over 2000 changes
// */
//  @Test
//  public void testGetUpdatesWhenThereAre2001Updates_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test42";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testGetUpdatesWhenThereAre2001Updates_ExpectPass";
//    String tableDataETag = null;
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 2001);
//      tableDataETag = testTableRes.getDataETag();
//
//      // Insert over 2001 rows
//
//      String val, rowId, ts;
//      for (int i = 0; i < 2001; i++) {
//        val = "test value for table " + i + " for table " + testTableId;
//
//        rowId = "uuid:" + UUID.randomUUID().toString();
//        ts = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
//
//        appendRowContent(refTable, orderedColumns, rowId, val, ts);
//      }
//
//      ArrayList<org.opendatakit.database.data.Row> listOfRowsToCreate =
//          new ArrayList<org.opendatakit.database.data.Row>();
//
//      listOfRowsToCreate.addAll(refTable.getRows());
//
//      RowOutcomeList outcomes = synchronizer.pushLocalRows(testTableRes, orderedColumns,
//          listOfRowsToCreate);
//
//      RowResourceList rowResourceList = synchronizer.getUpdates(testTableRes, tableDataETag,
//          null, 3000);
//
//      ArrayList<RowResource> rowRes = rowResourceList.getRows();
//
//      assertEquals(rowRes.size(), 2000);
//
//      assertEquals(rowResourceList.isHasMoreResults(), true);
//
//      String webSafeCursor = rowResourceList.getWebSafeResumeCursor();
//
//      RowResourceList rowResourceList2 = synchronizer.getUpdates(testTableRes, "5",
//          webSafeCursor, 100);
//
//      ArrayList<RowResource> rowRes2 = rowResourceList2.getRows();
//
//      assertEquals(rowRes2.size(), 1);
//
//      assertEquals(rowResourceList2.isHasMoreResults(), false);
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetUpdatesWhenThereAre2001Updates_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test alter rows with an inserted row
//   */
//  @Test
//  public void testAlterRowsWithInsertedRow_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test5";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String RowId = "uuid:" + UUID.randomUUID().toString();
//
//    String utf_val = "Test value";
//
//    String testTableSchemaETag = "testAlterRowsWithInsertedRow_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 1);
//
//      // Insert a row
//      String rowId = "uuid:" + UUID.randomUUID().toString();
//      String ts = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
//
//      appendRowContent(refTable, orderedColumns, rowId, utf_val, ts);
//
//      ArrayList<org.opendatakit.database.data.Row> listOfRowsToCreate =
//          new ArrayList<org.opendatakit.database.data.Row>();
//
//      listOfRowsToCreate.addAll(refTable.getRows());
//
//      RowOutcomeList rowOutList = synchronizer.pushLocalRows(testTableRes, orderedColumns,
//          listOfRowsToCreate);
//
//      ArrayList<RowOutcome> rowOuts = rowOutList.getRows();
//
//      assertEquals(rowOuts.size(), 1);
//
//      synchronizer.deleteTable(testTableRes);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testAlterRowsWithInsertedRow_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test getting the table level file manifest with no files
//   */
//  @Test
//  public void testGetTableLevelFileManifest_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test6";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testGetTableLevelFileManifest_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 0);
//
//      FileManifestDocument tableManifest =
//              synchronizer.getTableLevelFileManifest(testTableId,
//                      testTableRes.getTableLevelManifestETag(), null, false);
//      assertEquals(tableManifest.entries.size(), 0);
//
//      synchronizer.deleteTable(testTableRes);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetTableLevelFileManifest_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test getting the row level file manifest with no files
//   */
//  @Test
//  public void testGetRowLevelFileManifestWhenThereAreNoFiles_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test7";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testGetRowLevelFileManifest_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 1);
//
//      // Insert a row
//      String val = "test value for table " + testTableId;
//
//      String rowId = "uuid:" + UUID.randomUUID().toString();
//      String ts = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
//
//      appendRowContent(refTable, orderedColumns, rowId, val, ts);
//
//      ArrayList<org.opendatakit.database.data.Row> listOfRowsToCreate =
//          new ArrayList<org.opendatakit.database.data.Row>();
//
//      listOfRowsToCreate.addAll(refTable.getRows());
//
//      RowOutcomeList rowOutList = synchronizer.pushLocalRows(testTableRes, orderedColumns,
//          listOfRowsToCreate);
//
//      FileManifestDocument rowManifest =
//        synchronizer.getRowLevelFileManifest(testTableRes.getInstanceFilesUri(), testTableId,
//            rowId, SyncAttachmentState.SYNC, null);
//      assertEquals(rowManifest.entries.size(), 0);
//
//      synchronizer.deleteTable(testTableRes);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testGetRowLevelFileManifest_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test upload instance file
//   */
//  @Test
//  public void testUploadInstanceFile_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test8";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testUploadInstanceFile_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      String fileName = "testFile.txt";
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 1);
//
//      // Insert a row
//      String val = "test value for table " + testTableId;
//      String rowId = "uuid:" + UUID.randomUUID().toString();
//      String ts = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
//
//      appendRowContent(refTable, orderedColumns, rowId, val, ts);
//
//      ArrayList<org.opendatakit.database.data.Row> listOfRowsToCreate =
//          new ArrayList<org.opendatakit.database.data.Row>();
//
//      listOfRowsToCreate.addAll(refTable.getRows());
//
//      RowOutcomeList rowOutList = synchronizer.pushLocalRows(testTableRes, orderedColumns,
//          listOfRowsToCreate);
//
//      String destDir = ODKFileUtils.getInstanceFolder(appName, testTableId, rowId);
//
//      File destFile = new File(destDir, fileName);
//
//      FileUtils.writeStringToFile(destFile, "This is a test", Charsets.UTF_8);
//
//      CommonFileAttachmentTerms cat1 = synchronizer.createCommonFileAttachmentTerms(testTableRes.getInstanceFilesUri(),
//              testTableId, rowId, ODKFileUtils.asRowpathUri(appName, testTableId, rowId, destFile));
//
//      synchronizer.uploadInstanceFile(destFile, cat1.instanceFileDownloadUri);
//
//      synchronizer.deleteTable(testTableRes);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testUploadInstanceFile_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test downloading file
//   */
//  @Test
//  public void testDownloadFile_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test9";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testUploadInstanceFile_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 1);
//
//      // Insert a row
//      String val = "test value for table " + testTableId;
//      String rowId = "uuid:" + UUID.randomUUID().toString();
//      String ts = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
//
//      appendRowContent(refTable, orderedColumns, rowId, val, ts);
//
//      ArrayList<org.opendatakit.database.data.Row> listOfRowsToCreate =
//          new ArrayList<org.opendatakit.database.data.Row>();
//
//      listOfRowsToCreate.addAll(refTable.getRows());
//
//      RowOutcomeList rowOutList = synchronizer.pushLocalRows(testTableRes, orderedColumns,
//          listOfRowsToCreate);
//
//      String fileName = "testFile.txt";
//
//      String destDir = ODKFileUtils.getInstanceFolder(appName, testTableId, rowId);
//
//      File destFile = new File(destDir, fileName);
//
//      FileUtils.writeStringToFile(destFile, "This is a test", Charsets.UTF_8);
//
//      CommonFileAttachmentTerms cat1 = synchronizer.createCommonFileAttachmentTerms(testTableRes.getInstanceFilesUri(),
//          testTableId, rowId, ODKFileUtils.asRowpathUri(appName, testTableId, rowId, destFile));
//
//      synchronizer.uploadInstanceFile(destFile, cat1.instanceFileDownloadUri);
//
//      String fileName2 = "testFile2.txt";
//
//      String destDir2 = ODKFileUtils.getInstanceFolder(appName, testTableId, rowId);
//
//      File destFile2 = new File(destDir2, fileName2);
//      synchronizer.downloadFile(destFile2, cat1.instanceFileDownloadUri);
//
//      synchronizer.deleteTable(testTableRes);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testDownloadFile_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test upload batch
//   */
//  @Test
//  public void testUploadBatch_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test10";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String RowId = "uuid:" + UUID.randomUUID().toString();
//
//    String utf_val = "तुरंत अस्पताल रेफर करें व रास्ते मैं शिशु को ओ. आर. एस देतेरहें";
//
//    String testTableSchemaETag = "testUploadBatch_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 1);
//
//      // Create a row of data to attach the batch of files
//      String ts = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
//
//      appendRowContent(refTable, orderedColumns, RowId, utf_val, ts);
//
//      ArrayList<org.opendatakit.database.data.Row> listOfRowsToCreate =
//          new ArrayList<org.opendatakit.database.data.Row>();
//
//      listOfRowsToCreate.addAll(refTable.getRows());
//
//      RowOutcomeList rowOutList = synchronizer.pushLocalRows(testTableRes, orderedColumns,
//          listOfRowsToCreate);
//
//      ArrayList<CommonFileAttachmentTerms> listOfCats =
//              new ArrayList<CommonFileAttachmentTerms>();
//
//      // Create two test files
//      String fileName = "testFile.txt";
//      String destDir = ODKFileUtils.getInstanceFolder(appName, testTableId, RowId);
//      File destFile = new File(destDir, fileName);
//      FileUtils.writeStringToFile(destFile, "This is a test", Charsets.UTF_8);
//
//      CommonFileAttachmentTerms cat1 = synchronizer.createCommonFileAttachmentTerms(testTableRes.getInstanceFilesUri(),
//          testTableId, RowId, ODKFileUtils.asRowpathUri(appName, testTableId, RowId, destFile));
//      listOfCats.add(cat1);
//
//
//      String fileName2 = "testFile2.txt";
//      String destDir2 = ODKFileUtils.getInstanceFolder(appName, testTableId, RowId);
//      File destFile2 = new File(destDir2, fileName2);
//      FileUtils.writeStringToFile(destFile2, "This is a test 2", Charsets.UTF_8);
//
//      CommonFileAttachmentTerms cat2 = synchronizer.createCommonFileAttachmentTerms(testTableRes.getInstanceFilesUri(),
//          testTableId, RowId, ODKFileUtils.asRowpathUri(appName, testTableId, RowId, destFile2));
//      listOfCats.add(cat2);
//
//      synchronizer.uploadInstanceFileBatch(listOfCats, testTableRes.getInstanceFilesUri(), RowId,
//          testTableId);
//
//      synchronizer.deleteTable(testTableRes);
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testUploadBatch_ExpectPass: expected pass but got exception");
//    }
//  }
//
//  /*
//   * Test batch downloading of files
//   */
//  @Test
//  public void testDownloadBatch_ExpectPass() {
//    SyncExecutionContext sharedContext = getSyncExecutionContext();
//
//    String testTableId = "test11";
//    String colName = "test_col1";
//    String colKey = "test_col1";
//    String colType = "string";
//
//    String testTableSchemaETag = "testDownloadBatch_ExpectPass";
//    String listOfChildElements = "[]";
//
//    ArrayList<Column> columns = new ArrayList<Column>();
//
//    ArrayList<CommonFileAttachmentTerms> listOfCats =
//            new ArrayList<CommonFileAttachmentTerms>();
//
//    columns.add(new Column(colKey, colName, colType, listOfChildElements));
//
//    try {
//      AggregateSynchronizer synchronizer = new AggregateSynchronizer(sharedContext);
//
//      TableResource testTableRes = synchronizer.createTable(testTableId, testTableSchemaETag, columns);
//
//      assertNotNull(testTableRes);
//
//      assertEquals(testTableRes.getTableId(), testTableId);
//
//      TableDefinitionResource tableDefRes = synchronizer.getTableDefinition(testTableRes.getDefinitionUri());
//
//      ArrayList<Column> cols = tableDefRes.getColumns();
//
//      for (int i = 0; i < cols.size(); i++) {
//        Column col = cols.get(i);
//        assertEquals(col.getElementKey(), colKey);
//      }
//
//      OrderedColumns orderedColumns = new OrderedColumns(appName, testTableId, cols);
//      BaseTable refTable = buildBaseTable(orderedColumns, 1);
//
//      // Insert a row
//      String val = "test value for table " + testTableId;
//      String rowId = "uuid:" + UUID.randomUUID().toString();
//      String ts = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
//
//      appendRowContent(refTable, orderedColumns, rowId, val, ts);
//
//      ArrayList<org.opendatakit.database.data.Row> listOfRowsToCreate =
//          new ArrayList<org.opendatakit.database.data.Row>();
//
//      listOfRowsToCreate.addAll(refTable.getRows());
//
//      RowOutcomeList rowOutList = synchronizer.pushLocalRows(testTableRes, orderedColumns,
//          listOfRowsToCreate);
//
//      String fileName = "testFile.txt";
//      String destDir = ODKFileUtils.getInstanceFolder(appName, testTableId, rowId);
//      File destFile = new File(destDir, fileName);
//      FileUtils.writeStringToFile(destFile, "This is a test", Charsets.UTF_8);
//
//
//      CommonFileAttachmentTerms cat1 = synchronizer.createCommonFileAttachmentTerms(testTableRes.getInstanceFilesUri(),
//          testTableId, rowId, ODKFileUtils.asRowpathUri(appName, testTableId, rowId, destFile));
//      listOfCats.add(cat1);
//
//      synchronizer.uploadInstanceFile(destFile, cat1.instanceFileDownloadUri);
//
//      String fileName2 = "testFile2.txt";
//      String destDir2 = ODKFileUtils.getInstanceFolder(appName, testTableId, rowId);
//      File destFile2 = new File(destDir2, fileName2);
//      FileUtils.writeStringToFile(destFile2, "This is a test 2", Charsets.UTF_8);
//      CommonFileAttachmentTerms cat2 = synchronizer.createCommonFileAttachmentTerms(testTableRes.getInstanceFilesUri(),
//          testTableId, rowId, ODKFileUtils.asRowpathUri(appName, testTableId, rowId, destFile2));
//      listOfCats.add(cat2);
//
//     synchronizer.uploadInstanceFile(destFile2, cat2.instanceFileDownloadUri);
//
//      synchronizer.downloadInstanceFileBatch(listOfCats, testTableRes.getInstanceFilesUri(),
//          rowId, testTableId);
//
//      synchronizer.deleteTable(testTableRes);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      fail("testDownloadBatch_ExpectPass: expected pass but got exception");
//    }
//  }
}