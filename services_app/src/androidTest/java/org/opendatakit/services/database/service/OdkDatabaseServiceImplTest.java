package org.opendatakit.services.database.service;

import android.content.ContentValues;
import android.database.Cursor;
import android.support.test.runner.AndroidJUnit4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.RoleConsts;
import org.opendatakit.database.data.*;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.database.service.TableHealthInfo;
import org.opendatakit.database.service.TableHealthStatus;
import org.opendatakit.exception.ActionNotAuthorizedException;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.provider.*;
import org.opendatakit.services.application.Services;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;

import static android.text.TextUtils.join;
import static org.junit.Assert.*;

/**
 * Created by Niles on 7/6/17.
 */
@RunWith(AndroidJUnit4.class)
public class OdkDatabaseServiceImplTest {
  private static final String TAG = OdkDatabaseServiceImplTest.class.getSimpleName();
  private static final double delta = 0.00001;
  private static boolean initialized = false;
  private OdkDatabaseServiceImpl d;
  private PropertiesSingleton props;
  private DbHandle dbHandle;
  private OdkConnectionInterface db;

  public static String get(Cursor c, String col) {
    return c.getString(c.getColumnIndexOrThrow(col));
  }

  private static String getAppName() {
    return "default";
  }

  private static double getDouble(Cursor c, String col) {
    return c.getDouble(c.getColumnIndexOrThrow(col));
  }

  private static int getInt(Cursor c, String col) throws Exception {
    return c.getInt(c.getColumnIndexOrThrow(col));
  }

  private static ContentValues makeTblCvs(String a, Integer b, Double c) {
    ContentValues v = new ContentValues();
    v.put("columnId", a);
    v.put("columnId2", b);
    v.put("columnId3", c);
    return v;
  }

  private static List<Column> makeCols() {
    List<Column> cols = new ArrayList<>();
    cols.add(new Column("columnId", "Column Name", ElementDataType.string.name(), "[]"));
    cols.add(new Column("columnId2", "Second Column Name", ElementDataType.integer.name(), "[]"));
    cols.add(new Column("columnId3", "Column Name", ElementDataType.number.name(), "[]"));
    return cols;
  }

  private static List<KeyValueStoreEntry> makeMetadata() {
    List<KeyValueStoreEntry> l = new ArrayList<>();
    KeyValueStoreEntry kvse = new KeyValueStoreEntry();
    kvse.type = "TEXT";
    kvse.tableId = "tbl";
    kvse.aspect = "aspect";
    kvse.partition = "partition";
    kvse.key = "some key";
    kvse.value = "some value";
    l.add(kvse);
    kvse = new KeyValueStoreEntry();
    kvse.type = "TEXT";
    kvse.tableId = "tbl";
    kvse.aspect = "aspect";
    kvse.partition = "partition";
    kvse.key = "some key 2";
    kvse.value = "some value 2";
    l.add(kvse);
    return l;
  }

  private static File makeTemp(String id) throws Exception {
    File instanceFolder = new File(ODKFileUtils.getInstanceFolder(getAppName(), "Tea_houses", id));
    if (!instanceFolder.exists() && !instanceFolder.mkdirs()) {
      throw new Exception("Should have been able to create " + instanceFolder.getPath());
    }
    assertTrue(instanceFolder.exists());
    FileOutputStream f = new FileOutputStream(new File(instanceFolder.getPath() + "/temp"));
    f.write(new byte[] { 97, 98, 99 });
    f.close();
    return instanceFolder;
  }

  @Before
  public void setUp() throws Exception {
    props = CommonToolProperties.get(Services._please_dont_use_getInstance(), getAppName());
    props.clearSettings();
    ODKFileUtils.assertDirectoryStructure(getAppName());
    if (!initialized) {
      initialized = true;
      AndroidConnectFactory.configure();
    }
    DbHandle uniqueKey = new DbHandle(
        getClass().getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .getConnection(getAppName(), uniqueKey);
    d = new OdkDatabaseServiceImpl(Services._please_dont_use_getInstance());
    dbHandle = d.openDatabase(getAppName());
    db.execSQL("DELETE FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + ";", null);
  }

  @After
  public void cleanUp() throws Exception {
    d.closeDatabase(getAppName(), dbHandle);
  }

  @Test
  public void testGetActiveUser() throws Exception {
    assertEquals(d.getActiveUser(getAppName()), "anonymous");
    setProp(CommonToolProperties.KEY_AUTHENTICATION_TYPE, props.CREDENTIAL_TYPE_USERNAME_PASSWORD);
    setProp(CommonToolProperties.KEY_USERNAME, "insert username here");
    setProp(CommonToolProperties.KEY_AUTHENTICATED_USER_ID, "user_id_here");
    setProp(CommonToolProperties.KEY_ROLES_LIST, "['list', 'of', 'roles']");
    assertEquals(d.getActiveUser(getAppName()), "user_id_here");
  }

  private Object setProp(String a, String b) throws Exception {
    Map<String, String> m = new HashMap<>();
    m.put(a, b);
    props.setProperties(m);

    return null;
    /*
    Field gpf = props.getClass().getDeclaredField("mGeneralProps");
    gpf.setAccessible(true);
    Properties gp = (Properties) gpf.get(props);
    Field spf = props.getClass().getDeclaredField("mSecureProps");
    spf.setAccessible(true);
    Properties sp = (Properties) spf.get(props);
    Field dpf = props.getClass().getDeclaredField("mDeviceProps");
    dpf.setAccessible(true);
    Properties dp = (Properties) dpf.get(props);
    Method ispm = props.getClass().getDeclaredMethod("isSecureProperty", String.class);
    ispm.setAccessible(true);
    Method idpm = props.getClass().getDeclaredMethod("isDeviceProperty", String.class);
    idpm.setAccessible(true);
    if ((boolean) ispm.invoke(props, a)) {
      return sp.setProperty(a, b);
    } else if ((boolean) idpm.invoke(props, a)) {
      return dp.setProperty(a, b);
    }
    return gp.setProperty(a, b);
    */
  }

  @Test
  public void testGetRolesList() throws Exception {
    assertNull(d.getRolesList(getAppName()));
    setProp(CommonToolProperties.KEY_ROLES_LIST, "['list', 'of', 'roles']");
    assertEquals(d.getRolesList(getAppName()), "['list', 'of', 'roles']");
  }

  @Test
  public void testGetDefaultGroup() throws Exception {
    String defaultGroup = d.getDefaultGroup(getAppName());
    assertTrue(defaultGroup == null || defaultGroup.isEmpty());
    setProp(CommonToolProperties.KEY_DEFAULT_GROUP, "default group here");
    assertEquals(d.getDefaultGroup(getAppName()), "default group here");
  }

  @Test
  public void testGetUsersList() throws Exception {
    assertNull(d.getUsersList(getAppName()));
    setProp(CommonToolProperties.KEY_USERS_LIST, "test");
    assertEquals(d.getUsersList(getAppName()), "test");
  }

  @Test
  public void testOpenDatabase() throws Exception {
    // Should not throw an exception
    String qualifier = d.openDatabase(getAppName()).getDatabaseHandle();
    assertNotNull(qualifier);
    assertFalse(qualifier.isEmpty());
    d.closeDatabase(getAppName(), new DbHandle(qualifier));
  }

  @Test
  public void testCloseDatabase() throws Exception {
    // Should not throw an exception
    d.closeDatabase(getAppName(), d.openDatabase(getAppName()));
  }

  @Test
  public void testCreateLocalOnlyTableWithColumns() throws Exception {
    drop("tbl");
    d.createLocalOnlyTableWithColumns(getAppName(), dbHandle, "tbl", new ColumnList(Arrays.asList(
        new Column[] { new Column("columnId", "Column Name", ElementDataType.string.name(), "[]"),
            new Column("columnId3", "Column Name", ElementDataType.number.name(), "[]"),
            new Column("columnId2", "Second Column Name", ElementDataType.integer.name(),
                "[]") })));
    db.execSQL("INSERT INTO tbl (columnId, columnId2) VALUES (?, ?);",
        new Object[] { "ayy lmao", 3 });
    Cursor c = db.rawQuery("SELECT * FROM tbl", new String[0]);
    assertNotNull(c);
    c.moveToFirst();
    assertEquals(get(c, "columnId"), "ayy lmao");
    assertEquals(getDouble(c, "columnId2"), 3, delta);
    c.close();
    assertColType("tbl", "columnId2", "INTEGER");
    assertColType("tbl", "columnId3", "REAL");
    // For use in other tests
    truncate("tbl");
  }

  private void assertColType(String table, String column, String expectedType) throws Exception {
    Cursor c = db.rawQuery("PRAGMA table_info(" + table + ");", new String[0]);
    c.moveToFirst();
    while (!get(c, "name").equals(column)) {
      c.moveToNext();
    }
    if (expectedType != null) {
      assertEquals(get(c, "type"), expectedType);
    }
    c.close();
  }

  @Test
  public void testDeleteLocalOnlyTable() throws Exception {
    testCreateLocalOnlyTableWithColumns();
    // Should not throw exception, table exists
    db.rawQuery("PRAGMA table_info(tbl);", new String[0]).close();
    d.deleteLocalOnlyTable(getAppName(), dbHandle, "tbl");
    boolean worked = false;
    try {
      Cursor c = db.rawQuery("PRAGMA table_info(tbl);", new String[0]);
      if (c == null)
        throw new Exception("no results, success");
      c.moveToFirst();
      get(c, "type"); // should throw out of bounds exception
      c.close();
    } catch (Exception ignored) {
      worked = true;
    }
    assertTrue(worked);
  }

  @Test
  public void testInsertLocalOnlyRow() throws Exception {
    testCreateLocalOnlyTableWithColumns();

    d.insertLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("test", 15, 3.1415));
    Cursor c = db.rawQuery("SELECT * FROM tbl;", new String[0]);
    c.moveToFirst();
    assertEquals(get(c, "columnId"), "test");
    assertEquals(getDouble(c, "columnId3"), 3.1415, delta);
    assertEquals(getInt(c, "columnId2"), 15);
  }

  @Test
  public void testUpdateLocalOnlyRow() throws Exception {
    testCreateLocalOnlyTableWithColumns();
    d.insertLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("test2", 15, 3.1415));
    d.insertLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("test", 15, 3.1415));
    d.updateLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("test", 16, 3.1415),
        "columnId = ?", new BindArgs(new String[] { "test" }));
    Cursor c = db.rawQuery("SELECT * FROM tbl WHERE columnId = ?", new String[] { "test" });
    c.moveToFirst();
    assertEquals(getInt(c, "columnId2"), 16);
    c.close();
    c = db.rawQuery("SELECT * FROM tbl WHERE columnId = ?", new String[] { "test2" });
    c.moveToFirst();
    assertEquals(getInt(c, "columnId2"), 15);
    c.close();
    ContentValues cv = new ContentValues();
    cv.put("columnId3", 9.5);
    d.updateLocalOnlyRow(getAppName(), dbHandle, "tbl", cv, "columnId2 = ?",
        new BindArgs(new Object[] { 16 }));
    c = db.rawQuery("SELECT * FROM tbl WHERE columnId = ?", new String[] { "test" });
    c.moveToFirst();
    assertEquals(getDouble(c, "columnId3"), 9.5, delta);
    c.close();
    c = db.rawQuery("SELECT * FROM tbl WHERE columnId = ?", new String[] { "test2" });
    c.moveToFirst();
    assertEquals(getDouble(c, "columnId3"), 3.1415, delta);
    c.close();
    d.updateLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("a", 2, 3.1415), null, null);
    c = db.rawQuery("SELECT * FROM tbl WHERE columnId = ?", new String[] { "a" });
    assertEquals(c.getCount(), 2);
    c.close();
    d.updateLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("test", 16, 3.1415),
        "columnId = ?", new BindArgs(new String[] { "test" }));
    c = db.rawQuery("SELECT * FROM tbl WHERE columnId = ?", new String[] { "a" });
    assertEquals(c.getCount(), 2);
    c.close();
  }

  @Test
  public void testDeleteLocalOnlyRow() throws Exception {
    testCreateLocalOnlyTableWithColumns();
    d.insertLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("test", 15, 3.1415));
    d.deleteLocalOnlyRow(getAppName(), dbHandle, "tbl", "columnId = ?",
        new BindArgs(new String[] { "test" }));
    Cursor c = db.rawQuery("SELECT * FROM tbl;", new String[0]);
    assertEquals(c.getCount(), 0);
    c.close();
    d.insertLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("test", 15, 3.1415));
    d.insertLocalOnlyRow(getAppName(), dbHandle, "tbl", makeTblCvs("test2", 18, 9.813793));
    d.deleteLocalOnlyRow(getAppName(), dbHandle, "tbl", "columnId = ?",
        new BindArgs(new String[] { "test" }));
    c = db.rawQuery("SELECT * FROM tbl;", new String[0]);
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(getInt(c, "columnId2"), 18);
    c.close();
    d.deleteLocalOnlyRow(getAppName(), dbHandle, "tbl", null, null);
    c = db.rawQuery("SELECT * FROM tbl;", new String[0]);
    assertEquals(c.getCount(), 0);
    c.close();
  }

  private void createTeaHouses() throws Exception {
    drop("Tea_houses");
    db.execSQL("CREATE TABLE Tea_houses (_conflict_type INTEGER NULL, "
        + "_default_access TEXT NULL, _form_id TEXT NULL, _group_modify TEXT NULL, "
        + "_group_privileged TEXT NULL, _group_read_only TEXT NULL, _id TEXT NOT NULL, _locale "
        + "TEXT NULL, _row_etag TEXT NULL, _row_owner TEXT NULL, _savepoint_creator TEXT NULL, "
        + "_savepoint_timestamp TEXT NOT NULL, _savepoint_type TEXT NULL, _sync_state TEXT NOT "
        + "NULL, Customers INTEGER NULL, Date_Opened TEXT NULL, District TEXT NULL, Hot TEXT "
        + "NULL, Iced TEXT NULL, Location_accuracy REAL NULL, Location_altitude REAL NULL, "
        + "Location_latitude REAL NULL, Location_longitude REAL NULL, Name TEXT NULL, "
        + "Neighborhood TEXT NULL, Phone_Number TEXT NULL, Region TEXT NULL, Specialty_Type_id "
        + "TEXT NULL, State TEXT NULL, Store_Owner TEXT NULL, Visits INTEGER NULL, WiFi TEXT "
        + "NULL)", new String[0]);
  }

  @Test
  public void testPrivilegedServerTableSchemaETagChanged() throws Exception {
    truncate(DatabaseConstants.TABLE_DEFS_TABLE_NAME);
    createTeaHouses();
    db.rawQuery("INSERT INTO " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + " (" + join(", ",
        new String[] { TableDefinitionsColumns.TABLE_ID, TableDefinitionsColumns.LAST_DATA_ETAG,
            TableDefinitionsColumns.LAST_SYNC_TIME, TableDefinitionsColumns.SCHEMA_ETAG,
            TableDefinitionsColumns.REV_ID }) + ") VALUES (?, ?, ?, ?, ?);",
        new String[] { "Tea_houses", "data etag", "sync time", "schema etag", "revid" }).close();

    // TODO test that it deletes old files
    thInsert("id1", SyncState.synced, ConflictType.LOCAL_DELETED_OLD_VALUES);
    thInsert("id2", SyncState.changed, ConflictType.LOCAL_DELETED_OLD_VALUES);
    thInsert("id3", SyncState.new_row, ConflictType.LOCAL_UPDATED_UPDATED_VALUES);
    thInsert("id4", SyncState.synced_pending_files, ConflictType.LOCAL_UPDATED_UPDATED_VALUES);
    thInsert("id5", SyncState.synced, ConflictType.SERVER_DELETED_OLD_VALUES);
    thInsert("id6", SyncState.changed, ConflictType.SERVER_DELETED_OLD_VALUES);
    thInsert("id7", SyncState.new_row, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
    thInsert("id8", SyncState.synced_pending_files, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
    thInsert("id9", SyncState.in_conflict, ConflictType.LOCAL_UPDATED_UPDATED_VALUES);
    thInsert("id10", SyncState.in_conflict, ConflictType.LOCAL_DELETED_OLD_VALUES);
    thInsert("id11", SyncState.in_conflict, ConflictType.SERVER_UPDATED_UPDATED_VALUES);
    thInsert("id12", SyncState.in_conflict, ConflictType.SERVER_DELETED_OLD_VALUES);
    //delete where sync state is in_conflict and conflict_type is S_D_O or S_U_U
    //set sync state to deleted, conflict type to null where conflict type = L_D_O
    //set sync state to changed, conflict type to null where conflict type = L_U_U
    //set sync state to new_row where sync state is synced
    //set sync state to new row where sync state is pending_files
    d.privilegedServerTableSchemaETagChanged(getAppName(), dbHandle, "Tea_houses", "new_etag",
        null);
    Cursor c = db.rawQuery("SELECT * FROM " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + " WHERE "
        + TableDefinitionsColumns.TABLE_ID + " = ?;", new String[] { "Tea_houses" });
    c.moveToFirst();
    assertEquals(get(c, TableDefinitionsColumns.SCHEMA_ETAG), "new_etag");
    c.close();
    thAssertPresent("id1", SyncState.deleted, true);
    thAssertPresent("id2", SyncState.deleted, true);
    thAssertPresent("id3", SyncState.changed, true);
    thAssertPresent("id4", SyncState.changed, true);
    thAssertPresent("id5", SyncState.new_row, false);
    thAssertPresent("id6", SyncState.changed, false);
    thAssertPresent("id7", SyncState.new_row, false);
    thAssertPresent("id8", SyncState.new_row, false);
    thAssertPresent("id9", SyncState.changed, true);
    thAssertPresent("id10", SyncState.deleted, true);
    thAssertGone("id11");
    thAssertGone("id12");
  }

  private void thAssertGone(String id) throws Exception {
    Cursor c = db.rawQuery("SELECT * FROM Tea_houses WHERE _id = ?;", new String[] { id });
    assertEquals(c.getCount(), 0);
  }

  private void thAssertPresent(String id, @SuppressWarnings("TypeMayBeWeakened") SyncState state,
                               boolean conflictTypeShouldBeNull) {
    Cursor c = db.rawQuery("SELECT * FROM Tea_houses WHERE _id = ?;", new String[] { id });
    c.moveToFirst();
    if (state != null) {
      assertEquals(get(c, "_sync_state"), state.name());
    }
    if (conflictTypeShouldBeNull) {
      String val = get(c, "_conflict_type");
      assertTrue(val == null || val.isEmpty() || val.equalsIgnoreCase("null"));
    }
    assertEquals(c.getCount(), 1);
  }

  private void thInsert(String id, String savepointType) {
    db.execSQL("INSERT INTO Tea_houses (_id, _sync_state, _savepoint_timestamp, _savepoint_type) "
        + "VALUES (?, ?, ?, ?)", new Object[] { id, SyncState.synced.name(), "", savepointType });
  }

  private void thInsert(String id, @SuppressWarnings("TypeMayBeWeakened") SyncState syncState,
                        int conflictType) {
    db.execSQL("INSERT INTO Tea_houses (_id, _savepoint_timestamp, _sync_state, "
            + "_conflict_type) VALUES (?, ?, ?, ?)",
        new Object[] { id, "", syncState.name(), conflictType });
  }

  @Test
  public void testSetChoiceList() throws Exception {
    String id = d.setChoiceList(getAppName(), dbHandle, "[]");
    assertEquals(id, "d751713988987e9331980363e24189ce");
    Cursor c = db.rawQuery("SELECT * FROM " + DatabaseConstants.CHOICE_LIST_TABLE_NAME + " WHERE "
        + ChoiceListColumns.CHOICE_LIST_ID + " = ?;", new String[] { id });
    c.moveToFirst();
    assertEquals(get(c, ChoiceListColumns.CHOICE_LIST_JSON), "[]");
    c.close();
    id = d.setChoiceList(getAppName(), dbHandle, "['a', 'b']");
    assertEquals(id, "ce75bd1cd721f68dd62b65e2868a1951");
    c = db.rawQuery("SELECT * FROM " + DatabaseConstants.CHOICE_LIST_TABLE_NAME + " WHERE "
        + ChoiceListColumns.CHOICE_LIST_ID + " = ?;", new String[] { id });
    c.moveToFirst();
    assertEquals(get(c, ChoiceListColumns.CHOICE_LIST_JSON), "['a', 'b']");
    c.close();
  }

  @Test
  public void testSetChoiceListNullList() throws Exception {
    assertNull(d.setChoiceList(getAppName(), dbHandle, null));
  }

  @Test
  public void testSetChoiceListEmptyList() throws Exception {
    assertNull(d.setChoiceList(getAppName(), dbHandle, ""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetChoiceListTrailingWhitespace() throws Exception {
    d.setChoiceList(getAppName(), dbHandle, "[] ");
  }

  @Test
  public void testGetChoiceList() throws Exception {
    truncate(DatabaseConstants.CHOICE_LIST_TABLE_NAME);
    db.rawQuery("INSERT INTO " + DatabaseConstants.CHOICE_LIST_TABLE_NAME + " ("
        + ChoiceListColumns.CHOICE_LIST_ID + ", " + ChoiceListColumns.CHOICE_LIST_JSON + ") VALUES "
        + "(?, ?);", new String[] { "3fb5e8ac474a9076c6be893398b03a8f", "['val']" }).close();
    assertEquals(d.getChoiceList(getAppName(), dbHandle, "3fb5e8ac474a9076c6be893398b03a8f"),
        "['val']");
    assertNull(d.getChoiceList(getAppName(), dbHandle, null));
    assertNull(d.getChoiceList(getAppName(), dbHandle, ""));
  }

  @Test
  public void testCreateOrOpenTableWithColumns() throws Exception {
    drop("tbl");
    // create
    List<Column> cols = makeCols();
    OrderedColumns res = d
        .createOrOpenTableWithColumns(getAppName(), dbHandle, "tbl", new ColumnList(cols));
    for (int i = 0; i < res.getColumns().size(); i++) {
      assertEquals(cols.get(i), res.getColumns().get(i));
    }
    // open
    res = d.createOrOpenTableWithColumns(getAppName(), dbHandle, "tbl", new ColumnList(cols));
    for (int i = 0; i < res.getColumns().size(); i++) {
      assertEquals(cols.get(i), res.getColumns().get(i));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateOrOpenTableWithColumnsBadColumnName() throws Exception {
    testCreateOrOpenTableWithColumns();
    List<Column> cols = makeCols();
    cols.set(1, new Column("columnId3", "new Column Name", ElementDataType.number.name(), "[]"));
    d.createOrOpenTableWithColumns(getAppName(), dbHandle, "tbl", new ColumnList(cols));
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateOrOpenTableWithColumnsMissingColumn() throws Exception {
    testCreateOrOpenTableWithColumns();
    List<Column> cols = makeCols();
    cols.remove(1);
    d.createOrOpenTableWithColumns(getAppName(), dbHandle, "tbl", new ColumnList(cols));
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateOrOpenTableWithColumnsExtraColumn() throws Exception {
    testCreateOrOpenTableWithColumns();
    List<Column> cols = makeCols();
    cols.add(new Column("columnId4", "Column Name 4", ElementDataType.string.name(), "[]"));
    d.createOrOpenTableWithColumns(getAppName(), dbHandle, "tbl", new ColumnList(cols));
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateOrOpenTableWithColumnsBadType() throws Exception {
    testCreateOrOpenTableWithColumns();
    List<Column> cols = makeCols();
    cols.set(1, new Column("columnId3", "Column Name", ElementDataType.integer.name(), "[]"));
    d.createOrOpenTableWithColumns(getAppName(), dbHandle, "tbl", new ColumnList(cols));
  }

  private void drop(String table) {
    try {
      db.rawQuery("DROP TABLE " + table + ";", new String[0]).close();
    } catch (Exception ignored) {
    }
  }

  private void truncate(String table) {
    try {
      db.rawQuery("DELETE FROM " + table + ";", new String[0]).close();
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testCreateOrOpenTableWithColumnsAndProperties() throws Exception {
    drop("tbl");
    truncate(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);
    // create
    List<Column> cols = makeCols();
    List<KeyValueStoreEntry> md = makeMetadata();
    OrderedColumns res = d.createOrOpenTableWithColumnsAndProperties(getAppName(), dbHandle, "tbl",
        new ColumnList(cols), md, false);
    for (int i = 0; i < res.getColumns().size(); i++) {
      assertEquals(cols.get(i), res.getColumns().get(i));
    }
    // open
    md.get(1).value = "will be set";
    res = d.createOrOpenTableWithColumnsAndProperties(getAppName(), dbHandle, "tbl",
        new ColumnList(cols), md, false);
    for (int i = 0; i < res.getColumns().size(); i++) {
      assertEquals(cols.get(i), res.getColumns().get(i));
    }
    assertMdat("some key", "some value");
    assertMdat("some key 2", "will be set");
    md.get(1).value = "will be reset";
    md.remove(0);
    res = d.createOrOpenTableWithColumnsAndProperties(getAppName(), dbHandle, "tbl",
        new ColumnList(cols), md, true);
    for (int i = 0; i < res.getColumns().size(); i++) {
      assertEquals(cols.get(i), res.getColumns().get(i));
    }
    assertNoMdat("some key");
    assertMdat("some key 2", "will be reset");
    // For use in other tests
    md = makeMetadata();
    d.createOrOpenTableWithColumnsAndProperties(getAppName(), dbHandle, "tbl", new ColumnList(cols),
        md, true);
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateOrOpenTableWithColumnsAndPropertiesBadColumnName() throws Exception {
    testCreateOrOpenTableWithColumnsAndProperties();
    List<Column> cols = makeCols();
    List<KeyValueStoreEntry> md = makeMetadata();
    cols.set(1, new Column("columnId3", "new Column Name", ElementDataType.number.name(), "[]"));
    d.createOrOpenTableWithColumnsAndProperties(getAppName(), dbHandle, "tbl", new ColumnList(cols),
        md, true);
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateOrOpenTableWithColumnsAndPropertiesMissingColumn() throws Exception {
    testCreateOrOpenTableWithColumnsAndProperties();
    List<Column> cols = makeCols();
    cols.remove(1);
    List<KeyValueStoreEntry> md = makeMetadata();
    d.createOrOpenTableWithColumnsAndProperties(getAppName(), dbHandle, "tbl", new ColumnList(cols),
        md, true);
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateOrOpenTableWithColumnsAndPropertiesExtraColumn() throws Exception {
    testCreateOrOpenTableWithColumnsAndProperties();
    List<Column> cols = makeCols();
    cols.add(new Column("columnId4", "Column Name 4", ElementDataType.string.name(), "[]"));
    List<KeyValueStoreEntry> md = makeMetadata();
    d.createOrOpenTableWithColumnsAndProperties(getAppName(), dbHandle, "tbl", new ColumnList(cols),
        md, true);
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateOrOpenTableWithColumnsAndPropertiesBadType() throws Exception {
    testCreateOrOpenTableWithColumnsAndProperties();
    List<Column> cols = makeCols();
    cols.set(1, new Column("columnId3", "Column Name", ElementDataType.integer.name(), "[]"));
    List<KeyValueStoreEntry> md = makeMetadata();
    d.createOrOpenTableWithColumnsAndProperties(getAppName(), dbHandle, "tbl", new ColumnList(cols),
        md, true);
  }

  private void assertNoMdat(String k) {
    Cursor c = db.rawQuery(
        "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
            + KeyValueStoreColumns.KEY + "= ?;", new String[] { k });
    assertEquals(c.getCount(), 0);
    c.close();
  }

  private void assertMdat(String k, String v) {
    Cursor c = db.rawQuery(
        "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
            + KeyValueStoreColumns.KEY + "= ?;", new String[] { k });
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(get(c, KeyValueStoreColumns.VALUE), v);
    c.close();
  }

  @Test(expected = ActionNotAuthorizedException.class)
  public void testDeleteAllCheckpointRowsWithIdNotAllowed() throws Exception {
    createTeaHouses();
    thInsert("t1", null);
    d.deleteAllCheckpointRowsWithId(getAppName(), dbHandle, "Tea_houses", "t1");
  }

  private void setSuperuser() throws Exception {
    setProp(CommonToolProperties.KEY_AUTHENTICATION_TYPE, props.CREDENTIAL_TYPE_USERNAME_PASSWORD);
    setProp(CommonToolProperties.KEY_USERNAME, "insert username here");
    setProp(CommonToolProperties.KEY_AUTHENTICATED_USER_ID, "user_id_here");
    setProp(CommonToolProperties.KEY_ROLES_LIST, RoleConsts.SUPER_USER_ROLES_LIST);
  }

  @Test
  public void testDeleteAllCheckpointRowsWithId() throws Exception {
    File instanceFolder = makeTemp("t3");
    createTeaHouses();
    setSuperuser();
    thInsert("t1", SavepointTypeManipulator.complete());
    thInsert("t2", SavepointTypeManipulator.incomplete());
    thInsert("t3", null);
    //thSet("t3", "_savepoint_type", null);
    d.deleteAllCheckpointRowsWithId(getAppName(), dbHandle, "Tea_houses", "t1");
    d.deleteAllCheckpointRowsWithId(getAppName(), dbHandle, "Tea_houses", "t2");
    d.deleteAllCheckpointRowsWithId(getAppName(), dbHandle, "Tea_houses", "t3");
    thAssertPresent("t1", null, false);
    thAssertPresent("t2", null, false);
    thAssertGone("t3");
    assertFalse(instanceFolder.exists());
  }

  private void thSet(String id, String col, Object val) throws Exception {
    db.rawQuery("UPDATE Tea_houses SET " + col + " = " + (val == null ? "NULL" : "?") + " "
        + "WHERE _id = ?", val == null ? new String[] { id } : new Object[] { val, id });
  }

  @Test
  public void testDeleteLastCheckpointRowWithId() throws Exception {
    File instanceFolder = makeTemp("t3");
    createTeaHouses();
    setSuperuser();
    thInsert("t3", SavepointTypeManipulator.incomplete());
    thInsert("t3", null);
    d.deleteLastCheckpointRowWithId(getAppName(), dbHandle, "Tea_houses", "t3");
    thAssertPresent("t3", null, false);
    assertTrue(instanceFolder.exists());
    thSet("t3", "_savepoint_type", null);
    d.deleteLastCheckpointRowWithId(getAppName(), dbHandle, "Tea_houses", "t3");
    thAssertGone("t3");
    assertFalse(instanceFolder.exists());
  }

  @Test
  public void testDeleteTableAndAllData() throws Exception {
    db.rawQuery("CREATE TABLE IF NOT EXISTS _uploads (_tableId);", new String[0]).close();
    createTeaHouses();
    truncate(DatabaseConstants.TABLE_DEFS_TABLE_NAME);
    truncate(DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME);
    truncate(DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME);
    // Should not throw exception
    assertColType("Tea_houses", "_id", "TEXT");
    thInsert("t1", SavepointTypeManipulator.checkpoint());
    db.rawQuery("INSERT INTO " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + " (" + join(", ",
        new String[] { TableDefinitionsColumns.TABLE_ID, TableDefinitionsColumns.LAST_DATA_ETAG,
            TableDefinitionsColumns.LAST_SYNC_TIME, TableDefinitionsColumns.SCHEMA_ETAG,
            TableDefinitionsColumns.REV_ID }) + ") VALUES (?, ?, ?, ?, ?);",
        new String[] { "Tea_houses", "data etag", "sync time", "schema etag", "revid" }).close();
    thAssertPresent("t1", null, false);
    insertMetadata("Tea_houses", "partition", "aspect", "key", "value");
    db.rawQuery("INSERT INTO " + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME + " (" + join(", ",
        new String[] { ColumnDefinitionsColumns.TABLE_ID,
            ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS, ColumnDefinitionsColumns.ELEMENT_KEY,
            ColumnDefinitionsColumns.ELEMENT_NAME, ColumnDefinitionsColumns.ELEMENT_TYPE }) + ") "
            + "VALUES (?, ?, ?, ?, ?);",
        new String[] { "Tea_houses", "[]", "Customers", "Customers", "integer" });
    d.deleteTableAndAllData(getAppName(), dbHandle, "Tea_houses");
    Cursor c = db.rawQuery("SELECT * FROM " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + " WHERE "
        + TableDefinitionsColumns.TABLE_ID + " = ?;", new String[] { "Tea_houses" });
    assertEquals(c.getCount(), 0);
    c.close();
    c = db.rawQuery(
        "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
            + KeyValueStoreColumns.TABLE_ID + " = ?;", new String[] { "Tea_houses" });
    assertEquals(c.getCount(), 0);
    c.close();
    c = db.rawQuery("SELECT * FROM " + DatabaseConstants.COLUMN_DEFINITIONS_TABLE_NAME + " WHERE "
        + ColumnDefinitionsColumns.TABLE_ID + " = ?;", new String[] { "Tea_houses" });
    assertEquals(c.getCount(), 0);
    c.close();
    boolean worked = false;
    try {
      // Table should not exist
      assertColType("Tea_houses", "_id", "TEXT");
    } catch (Exception ignored) {
      worked = true;
    }
    assertTrue(worked);
  }

  public void insertMetadata(String table, String partition, String aspect, String key,
                             String value) {
    db.rawQuery(
        "INSERT OR REPLACE INTO " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " ("
            + join(", ",
            new String[] { KeyValueStoreColumns.TABLE_ID, KeyValueStoreColumns.PARTITION,
                KeyValueStoreColumns.ASPECT, KeyValueStoreColumns.KEY, KeyValueStoreColumns.VALUE,
                KeyValueStoreColumns.VALUE_TYPE }) + ") VALUES (?, ?, ?, ?, ?, ?);",
        new String[] { table, partition, aspect, key, value, "TEXT" });
  }

  @Test
  public void testDeleteTableMetadata() throws Exception {
    createTeaHouses();
    insertMetadata("Tea_houses", "partition", "aspect", "key", "value");
    insertMetadata("Tea_houses", "partition", "aspect", "some_other_key", "value");
    d.deleteTableMetadata(getAppName(), dbHandle, "Tea_houses", "partition", "aspect", "key");
    Cursor c = db.rawQuery(
        "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
            + KeyValueStoreColumns.KEY + " = ?;", new String[] { "key" });
    assertEquals(c.getCount(), 0);
    c.close();
    c = db.rawQuery(
        "SELECT * FROM " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " WHERE "
            + KeyValueStoreColumns.KEY + " = ?;", new String[] { "some_other_key" });
    assertEquals(c.getCount(), 1);
    c.close();
  }

  @Test
  public void testDeleteRowWithId() throws Exception {
    setSuperuser();
    createTeaHouses();
    thInsert("t1", null);
    thInsert("t2", null);
    d.deleteRowWithId(getAppName(), dbHandle, "Tea_houses", "t1");
    Cursor c = db.rawQuery("SELECT * FROM Tea_houses;", null);
    c.moveToFirst();
    assertEquals(get(c, "_id"), "t2");
    c.close();
  }

  @Test(expected = ActionNotAuthorizedException.class)
  public void testDeleteRowWithIdNoAccess() throws Exception {
    createTeaHouses();
    thInsert("t1", null);
    d.deleteRowWithId(getAppName(), dbHandle, "Tea_houses", "t1");
  }

  @Test
  public void testPrivilegedDeleteRowWithId() throws Exception {
    createTeaHouses();
    thInsert("t1", null);
    thInsert("t2", null);
    d.privilegedDeleteRowWithId(getAppName(), dbHandle, "Tea_houses", "t1");
    Cursor c = db.rawQuery("SELECT * FROM Tea_houses;", null);
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(get(c, "_id"), "t2");
    c.close();
  }

  @Test
  public void testGetAdminColumns() throws Exception {
    createTeaHouses();
    String[] cols = d.getAdminColumns();
    for (String col : cols) {
      // Should exist in tea houses
      assertColType("Tea_houses", col, null);
    }
  }

  @Test
  public void testGetAllColumnNames() throws Exception {
    createTeaHouses();
    String[] cols = d.getAllColumnNames(getAppName(), dbHandle, "Tea_houses");
    for (String col : cols) {
      // Should exist in tea houses
      assertColType("Tea_houses", col, null);
    }
  }

  @Test
  public void testGetAllTableIds() throws Exception {
    truncate(DatabaseConstants.TABLE_DEFS_TABLE_NAME);
    db.rawQuery("INSERT INTO " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + " (" + join(", ",
        new String[] { TableDefinitionsColumns.TABLE_ID, TableDefinitionsColumns.LAST_DATA_ETAG,
            TableDefinitionsColumns.LAST_SYNC_TIME, TableDefinitionsColumns.SCHEMA_ETAG,
            TableDefinitionsColumns.REV_ID }) + ") VALUES (?, ?, ?, ?, ?);",
        new String[] { "fields", "data etag", "sync time", "schema etag", "revid" }).close();
    db.rawQuery("INSERT INTO " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + " (" + join(", ",
        new String[] { TableDefinitionsColumns.TABLE_ID, TableDefinitionsColumns.LAST_DATA_ETAG,
            TableDefinitionsColumns.LAST_SYNC_TIME, TableDefinitionsColumns.SCHEMA_ETAG,
            TableDefinitionsColumns.REV_ID }) + ") VALUES (?, ?, ?, ?, ?);",
        new String[] { "breathcounter", "data etag", "sync time", "schema etag", "revid" }).close();
    List<String> tables = d.getAllTableIds(getAppName(), dbHandle);
    assertTrue(tables.size() == 2);
    // Test that it returns list in sorted order
    assertEquals(tables.get(0), "breathcounter");
    assertEquals(tables.get(1), "fields");
  }

  @Test
  public void testGetRowsWithIdNoAccess() throws Exception {
    createTeaHouses();
    thInsert("t1", null);
    thInsert("t1", SavepointTypeManipulator.complete());
    thInsert("t2", null);
    BaseTable t = d.getRowsWithId(getAppName(), dbHandle, "Tea_houses", "t1");
    assertEquals(t.getNumberOfRows(), 0);
  }

  @Test
  public void testGetRowsWithId() throws Exception {
    setSuperuser();
    createTeaHouses();
    thInsert("t1", null);
    thInsert("t1", SavepointTypeManipulator.complete());
    thInsert("t2", null);
    BaseTable t = d.getRowsWithId(getAppName(), dbHandle, "Tea_houses", "t1");
    assertEquals(t.getNumberOfRows(), 2);
    String a = t.getRowAtIndex(0).getDataByKey("_savepoint_type");
    String b = t.getRowAtIndex(1).getDataByKey("_savepoint_type");
    if (a == null) {
      assertEquals(b, SavepointTypeManipulator.complete());
    } else {
      assertNull(b);
      assertEquals(a, SavepointTypeManipulator.complete());
    }
  }

  @Test
  public void testPrivilegedGetRowsWithId() throws Exception {
    createTeaHouses();
    thInsert("t1", null);
    thInsert("t1", SavepointTypeManipulator.complete());
    thInsert("t2", null);
    BaseTable t = d.privilegedGetRowsWithId(getAppName(), dbHandle, "Tea_houses", "t1");
    assertEquals(t.getNumberOfRows(), 2);
    String a = t.getRowAtIndex(0).getDataByKey("_savepoint_type");
    String b = t.getRowAtIndex(1).getDataByKey("_savepoint_type");
    if (a == null) {
      assertEquals(b, SavepointTypeManipulator.complete());
    } else {
      assertNull(b);
      assertEquals(a, SavepointTypeManipulator.complete());
    }
  }

  @Test
  public void testGetMostRecentRowWithId() throws Exception {
    createTeaHouses();
    thInsert("t1", SavepointTypeManipulator.incomplete());
    thSet("t1", DataTableColumns.SAVEPOINT_TIMESTAMP, "bbb");
    thSet("t1", "_default_access", "FULL");
    thInsert("t2", SavepointTypeManipulator.complete());
    thSet("t2", DataTableColumns.SAVEPOINT_TIMESTAMP, "aaa");
    thSet("t2", "_default_access", "FULL");
    thSet("t2", "_id", "t1");
    BaseTable result = d.getMostRecentRowWithId(getAppName(), dbHandle, "Tea_houses", "t1");
    assertEquals(result.getNumberOfRows(), 1);
    assertEquals(result.getRowAtIndex(0).getDataByKey("_savepoint_type"),
        SavepointTypeManipulator.incomplete());
    truncate("Tea_houses");
    result = d.getMostRecentRowWithId(getAppName(), dbHandle, "Tea_houses", "t1");
    assertEquals(result.getNumberOfRows(), 0);
  }

  private void thSetRevid(String revId) throws Exception {
    String cId = TableDefinitionsColumns.TABLE_ID;
    String cSchema = TableDefinitionsColumns.SCHEMA_ETAG;
    String cData = TableDefinitionsColumns.LAST_DATA_ETAG;
    String cTime = TableDefinitionsColumns.LAST_SYNC_TIME;
    String cRev = TableDefinitionsColumns.REV_ID;
    String[] all = { cId, cSchema, cData, cTime, cRev };

    db.rawQuery("DELETE FROM " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + ";", null);
    db.rawQuery("INSERT INTO " + DatabaseConstants.TABLE_DEFS_TABLE_NAME + " (" + join(", ",
        all) + ") VALUES (?, ?, ?, ?, ?);",
        new String[] { "Tea_houses", "schema etag", "data etag", "time", revId });
  }

  @Test
  public void testGetTableMetadata() throws Exception {
    insertMetadata("Tea_houses", "partition", "aspect", "key", "val");
    insertMetadata("Tea_houses", "partition", "aspect", "key2", "other val");
    TableMetaDataEntries result = d
        .getTableMetadata(getAppName(), dbHandle, "Tea_houses", "partition", "aspect", "key");
    assertEquals(result.getEntries().size(), 1);
    assertEquals(result.getEntries().get(0).value, "val");
    result = d.getTableMetadata(getAppName(), dbHandle, null, null, null, null);
    assertEquals(result.getEntries().size(), 2);
    String a = result.getEntries().get(0).value;
    String b = result.getEntries().get(1).value;
    if (a.equals("val")) {
      assertEquals(b, "other val");
    } else {
      assertEquals(a, "other val");
      assertEquals(b, "val");
    }
    result = d.getTableMetadata(getAppName(), dbHandle, "Tea_houses", "partition", "aspect", "k3");
    assertEquals(result.getEntries().size(), 0);
  }

  @Test
  public void testGetTableMetadataIfChangedChanged() throws Exception {
    createTeaHouses();
    insertMetadata("Tea_houses", "partition", "aspect", "key", "val");
    insertMetadata("Tea_houses", "partition", "aspect", "key2", "other val");
    TableMetaDataEntries result = d
        .getTableMetadataIfChanged(getAppName(), dbHandle, "Tea_houses", "old and invalid revId");
    assertEquals(result.getEntries().size(), 2);
    String a = result.getEntries().get(0).value;
    String b = result.getEntries().get(1).value;
    if (a.equals("val")) {
      assertEquals(b, "other val");
    } else {
      assertEquals(a, "other val");
      assertEquals(b, "val");
    }
    thSetRevid("new revid");
    result = d.getTableMetadataIfChanged(getAppName(), dbHandle, "Tea_houses", "new revid");
    assertEquals(result.getEntries().size(), 0);
    result = d.getTableMetadataIfChanged(getAppName(), dbHandle, "Tea_houses", "old revid again");
    assertEquals(result.getEntries().size(), 2);
    result = d.getTableMetadataIfChanged(getAppName(), dbHandle, "table_dne", "revid");
    assertEquals(result.getEntries().size(), 0);
  }

  @Test
  public void testGetTableHealthStatus() throws Exception {
    //_savepoint_type is null -> + 1 checkpoint
    //_conflict_type not is null -> + 1 conflict
    //_sync_state not in [synced, pending_files] -> + 1 changes
    createTeaHouses();
    TableHealthInfo res = d.getTableHealthStatus(getAppName(), dbHandle, "Tea_houses");
    assertEquals(res.getHealthStatus(), TableHealthStatus.TABLE_HEALTH_IS_CLEAN);
    thInsert("t1", SyncState.new_row, ConflictType.LOCAL_UPDATED_UPDATED_VALUES);
    thSet("t1", "_conflict_type", null);
    thSet("t1", "_savepoint_type", SavepointTypeManipulator.complete());
    res = d.getTableHealthStatus(getAppName(), dbHandle, "Tea_houses");
    assertEquals(res.getHealthStatus(), TableHealthStatus.TABLE_HEALTH_IS_CLEAN);
    assertTrue(res.hasChanges());

    truncate("Tea_houses");
    thInsert("t1", SyncState.synced, ConflictType.LOCAL_UPDATED_UPDATED_VALUES);
    thSet("t1", "_conflict_type", ConflictType.LOCAL_DELETED_OLD_VALUES);
    thSet("t1", "_savepoint_type", SavepointTypeManipulator.complete());
    res = d.getTableHealthStatus(getAppName(), dbHandle, "Tea_houses");
    assertEquals(res.getHealthStatus(), TableHealthStatus.TABLE_HEALTH_HAS_CONFLICTS);
    assertFalse(res.hasChanges());

    thSet("t1", "_savepoint_type", null);
    thSet("t1", "_sync_state", SyncState.changed);
    res = d.getTableHealthStatus(getAppName(), dbHandle, "Tea_houses");
    assertEquals(res.getHealthStatus(),
        TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS);
    assertTrue(res.hasChanges());

    thSet("t1", "_sync_state", SyncState.synced_pending_files);
    res = d.getTableHealthStatus(getAppName(), dbHandle, "Tea_houses");
    assertEquals(res.getHealthStatus(),
        TableHealthStatus.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS);
    assertFalse(res.hasChanges());
  }

  /*
  @Test
  public void testGetTableHealthStatuses() throws Exception {

  }

  @Test
  public void testGetExportColumns() throws Exception {

  }

  @Test
  public void testGetSyncState() throws Exception {

  }

  @Test
  public void testGetTableDefinitionEntry() throws Exception {

  }

  @Test
  public void testGetUserDefinedColumns() throws Exception {

  }

  @Test
  public void testHasTableId() throws Exception {

  }

  @Test
  public void testInsertCheckpointRowWithId() throws Exception {

  }

  @Test
  public void testInsertRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedInsertRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedPerhapsPlaceRowIntoConflictWithId() throws Exception {

  }

  @Test
  public void testSimpleQuery() throws Exception {

  }

  @Test
  public void testPrivilegedSimpleQuery() throws Exception {

  }

  @Test
  public void testPrivilegedExecute() throws Exception {

  }

  @Test
  public void testReplaceTableMetadata() throws Exception {

  }

  @Test
  public void testReplaceTableMetadataList() throws Exception {

  }

  @Test
  public void testReplaceTableMetadataSubList() throws Exception {

  }

  @Test
  public void testSaveAsIncompleteMostRecentCheckpointRowWithId() throws Exception {

  }

  @Test
  public void testSaveAsCompleteMostRecentCheckpointRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedUpdateTableETags() throws Exception {

  }

  @Test
  public void testPrivilegedUpdateTableLastSyncTime() throws Exception {

  }

  @Test
  public void testUpdateRowWithId() throws Exception {

  }

  @Test
  public void testResolveServerConflictWithDeleteRowWithId() throws Exception {

  }

  @Test
  public void testResolveServerConflictTakeLocalRowWithId() throws Exception {

  }

  @Test
  public void testResolveServerConflictTakeLocalRowPlusServerDeltasWithId() throws Exception {

  }

  @Test
  public void testResolveServerConflictTakeServerRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedUpdateRowETagAndSyncState() throws Exception {

  }

  @Test
  public void testDeleteAppAndTableLevelManifestSyncETags() throws Exception {

  }

  @Test
  public void testDeleteAllSyncETagsForTableId() throws Exception {

  }

  @Test
  public void testDeleteAllSyncETagsExceptForServer() throws Exception {

  }

  @Test
  public void testDeleteAllSyncETagsUnderServer() throws Exception {

  }

  @Test
  public void testGetFileSyncETag() throws Exception {

  }

  @Test
  public void testGetManifestSyncETag() throws Exception {

  }

  @Test
  public void testUpdateFileSyncETag() throws Exception {

  }

  @Test
  public void testUpdateManifestSyncETag() throws Exception {

  }
  */

}
