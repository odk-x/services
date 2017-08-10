package org.opendatakit.services.tables.provider;

import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.net.Uri;
import android.support.test.runner.AndroidJUnit4;
import org.junit.*;
import org.junit.runner.RunWith;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.provider.TableDefinitionsColumns;
import org.opendatakit.provider.TablesProviderAPI;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.forms.provider.FormsProviderTest;
import org.opendatakit.utilities.ODKFileUtils;

import static android.text.TextUtils.join;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Niles on 7/6/17.
 */
@RunWith(AndroidJUnit4.class)
public class TablesProviderTest {

  private static boolean initialized = false;
  private static Uri uri = new Uri.Builder().appendPath(getAppName()).build();
  private static String tTable = DatabaseConstants.TABLE_DEFS_TABLE_NAME;
  private static String cId = TableDefinitionsColumns.TABLE_ID;
  private static String cSchema = TableDefinitionsColumns.SCHEMA_ETAG;
  private static String cData = TableDefinitionsColumns.LAST_DATA_ETAG;
  private static String cTime = TableDefinitionsColumns.LAST_SYNC_TIME;
  private static String cRev = TableDefinitionsColumns.REV_ID;
  private static String[] all = { cId, cSchema, cData, cTime, cRev };

  private static OdkConnectionInterface db;
  private static TablesProvider p;

  public static String getAppName() {
    return "default";
  }

  private static Uri makeUri(String id) {
    return new Uri.Builder().appendPath(getAppName()).appendPath(id).build();
  }

  public static String get(Cursor c, String col) {
    return c.getString(c.getColumnIndexOrThrow(col));
  }

  @Before
  public void setUp() throws Exception {
    ODKFileUtils.assertDirectoryStructure(getAppName());
    if (!initialized) {
      initialized = true;
      AndroidConnectFactory.configure();
    }
    DbHandle uniqueKey = new DbHandle(
        FormsProviderTest.class.getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .getConnection(getAppName(), uniqueKey);
    p = new TablesProvider();
    db.execSQL("DELETE FROM " + tTable + ";", new String[0]);
    test = new FormsProviderTest();
    test.setUp();
  }
  FormsProviderTest test;
  @After
  public void after() throws Exception {
    test.after();
  }
  private static void insertTable(String id) {
    db.execSQL("INSERT INTO " + tTable + " (" + join(", ", all) + ") VALUES (?, ?, ?, ?, ?);",
        new String[] { id, "schema etag here", "data etag here", "timestamp here", "revId here" });
  }

  @Test(expected = CursorIndexOutOfBoundsException.class)
  public void testQueryNoTables() throws Exception {
    Cursor c = p.query(uri, null, null, null, null);
    if (c == null)
      throw new Exception("null cursor");
    c.moveToFirst();
    get(c, cId);
    c.close();
  }

  @Test(expected = CursorIndexOutOfBoundsException.class)
  public void testQueryNoTable() throws Exception {
    insertTable("test");
    Cursor c = p.query(makeUri("Tea_houses"), null, null, null, null);
    if (c == null)
      throw new Exception("null cursor");
    c.moveToFirst();
    get(c, cId);
    c.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryNoAppName() throws Exception {
    p.query(new Uri.Builder().build(), null, null, null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testQueryTooManyPaths() throws Exception {
    p.query(
        new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").appendPath("Tea_houses")
            .build(), null, null, null, null);
  }

  @Test
  public void testQuery() throws Exception {
    insertTable("Tea_houses");
    Cursor c = query(makeUri("Tea_houses"), all, null, null, null);
    c.moveToFirst();
    assertEquals(get(c, cId), "Tea_houses");
    assertEquals(get(c, cData), "data etag here");
    c.close();
    c = query(uri, all, null, null, null);
    c.moveToFirst();
    assertEquals(get(c, cId), "Tea_houses");
    assertEquals(get(c, cData), "data etag here");
    c.close();
    insertTable("test");
    db.execSQL("UPDATE " + tTable + " SET " + cData + " = ? WHERE " + cId + " = ?;",
        new String[] { "new data etag", "test" });
    c = query(uri, all, null, null, null);
    assertEquals(c.getCount(), 2);
    c.close();
    c = query(uri, all, cId + " = ?", new String[] { "test" }, null);
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(get(c, cData), "new data etag");
    c.close();
    c = query(makeUri("test"), all, null, null, null);
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(get(c, cData), "new data etag");
    c.close();
    c = query(makeUri("test"), all, cData + " = ?", new String[] { "new data etag" }, null);
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(get(c, cData), "new data etag");
    c.close();
    c = query(makeUri("test"), all, cData + " = ?", new String[] { "data etag here" }, null);
    assertEquals(c.getCount(), 0);
    c.close();
  }

  private Cursor query(Uri uri, String[] columns, String where, String[] whereArgs,
      String sortOrder) throws Exception {
    Cursor result = p.query(uri, columns, where, whereArgs, sortOrder);
    if (result == null)
      throw new Exception("Null cursor");
    return result;
  }

  @Test
  public void testGetType() throws Exception {
    assertEquals(p.getType(uri), TableDefinitionsColumns.CONTENT_TYPE);
    assertEquals(p.getType(makeUri("Tea_houses")), TableDefinitionsColumns.CONTENT_ITEM_TYPE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTypeNoAppName() throws Exception {
    p.getType(new Uri.Builder().build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTypeTooManyPaths() throws Exception {
    p.getType(
        new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").appendPath("Tea_houses")
            .build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeleteNoAppName() throws Exception {
    p.delete(new Uri.Builder().build(), null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDeleteTooManyPaths() throws Exception {
    p.delete(
        new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").appendPath("Tea_houses")
            .build(), null, null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInsert() throws Exception {
    p.insert(uri, null);
  }

  @Test
  public void testDelete() throws Exception {
    insertTables("Tea_houses", "test");
    assertEquals(p.delete(uri, null, null), 2);
    expectGone("Tea_houses");
    expectGone("test");
    //
    insertTables("Tea_houses", "test");
    assertEquals(p.delete(makeUri("test"), null, null), 1);
    expectPresent("Tea_houses");
    expectGone("test");
    //
    setUp();
    insertTables("Tea_houses", "test");
    assertEquals(p.delete(makeUri("test"), cData + " = ?", new String[] { "data etag here" }), 1);
    expectPresent("Tea_houses");
    expectGone("test");
    //
    setUp();
    insertTables("Tea_houses", "test");
    db.execSQL("UPDATE " + tTable + " SET " + cData + " = ? WHERE " + cId + " = ?;",
        new String[] { "new data etag", "test" });
    assertEquals(p.delete(uri, cData + " = ?", new String[] { "new data etag" }), 1);
    expectPresent("Tea_houses");
    expectGone("test");
    //
    setUp();
    insertTables("Tea_houses", "test");
    db.execSQL("UPDATE " + tTable + " SET " + cData + " = ? WHERE " + cId + " = ?;",
        new String[] { "new data etag", "test" });
    assertEquals(p.delete(makeUri("test"), cData + " = ?", new String[] { "data etag here" }), 0);
    expectPresent("Tea_houses");
    expectPresent("test");
  }

  private void insertTables(String... ids) throws Exception {
    for (String id : ids) {
      insertTable(id);
    }
  }

  private void expectGone(String id) throws Exception {
    Cursor c = db
        .rawQuery("SELECT * FROM " + tTable + " WHERE " + cId + " = ?;", new String[] { id });
    assertEquals(c.getCount(), 0);
    c.close();
  }

  private void expectPresent(String id) throws Exception {
    Cursor c = db
        .rawQuery("SELECT * FROM " + tTable + " WHERE " + cId + " = ?;", new String[] { id });
    c.moveToFirst();
    assertTrue(c.getCount() > 0);
    assertEquals(get(c, cId), id);
    assertEquals(get(c, cRev), "revId here");
    c.close();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUpdate() throws Exception {
    p.update(uri, null, null, null);
  }

  /**
   * Silliest test ever
   */
  @Test
  public void testGetTablesAuthority() {
    assertEquals(TablesProvider.getTablesAuthority(), TablesProviderAPI.AUTHORITY);
  }

  @Test
  public void testOnCreate() {
    p.onCreate(); // shouldn't throw an exception
  }

}