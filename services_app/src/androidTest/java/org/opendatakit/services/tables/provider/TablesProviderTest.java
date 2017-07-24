package org.opendatakit.services.tables.provider;

import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.net.Uri;
import android.support.test.runner.AndroidJUnit4;
import org.junit.*;
import org.junit.runner.RunWith;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.provider.TableDefinitionsColumns;
import org.opendatakit.provider.TablesProviderAPI;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.forms.provider.FormsProviderTest;
import org.opendatakit.utilities.ODKFileUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.all;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.cData;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.cId;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.cRev;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.get;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.getAppName;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.makeUri;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.tTable;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.uri;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.insertTable;
import static org.opendatakit.services.tables.provider.TablesProviderColumns.query;

/**
 * Created by Niles on 7/6/17.
 */
@RunWith(AndroidJUnit4.class)
public class TablesProviderTest {

  private static boolean initialized = false;

  private static OdkConnectionInterface db;
  private static TablesProvider p;
  FormsProviderTest test;

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
  @After
  public void after() throws Exception {
    test.after();
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
    insertTable(db, "test");
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
    insertTable(db, "Tea_houses");
    Cursor c = query(p, all, null, null, null, makeUri("Tea_houses"));
    c.moveToFirst();
    assertEquals(get(c, cId), "Tea_houses");
    assertEquals(get(c, cData), "data etag here");
    c.close();
    c = query(p, all, null, null, null, uri);
    c.moveToFirst();
    assertEquals(get(c, cId), "Tea_houses");
    assertEquals(get(c, cData), "data etag here");
    c.close();
    insertTable(db, "test");
    db.execSQL("UPDATE " + tTable + " SET " + cData + " = ? WHERE " + cId + " = ?;",
        new String[]{ "new data etag", "test" });
    c = query(p, all, null, null, null, uri);
    assertEquals(c.getCount(), 2);
    c.close();
    c = query(p, all, cId + " = ?", new String[]{ "test" }, null, uri);
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(get(c, cData), "new data etag");
    c.close();
    c = query(p, all, null, null, null, makeUri("test"));
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(get(c, cData), "new data etag");
    c.close();
    c = query(p, all, cData + " = ?", new String[]{ "new data etag" }, null, makeUri("test"));
    assertEquals(c.getCount(), 1);
    c.moveToFirst();
    assertEquals(get(c, cData), "new data etag");
    c.close();
    c = query(p, all, cData + " = ?", new String[]{ "data etag here" }, null, makeUri("test"));
    assertEquals(c.getCount(), 0);
    c.close();
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
    assertEquals(p.delete(makeUri("test"), cData + " = ?", new String[]{ "data etag here" }), 1);
    expectPresent("Tea_houses");
    expectGone("test");
    //
    setUp();
    insertTables("Tea_houses", "test");
    db.execSQL("UPDATE " + tTable + " SET " + cData + " = ? WHERE " + cId + " = ?;",
        new String[]{ "new data etag", "test" });
    assertEquals(p.delete(uri, cData + " = ?", new String[]{ "new data etag" }), 1);
    expectPresent("Tea_houses");
    expectGone("test");
    //
    setUp();
    insertTables("Tea_houses", "test");
    db.execSQL("UPDATE " + tTable + " SET " + cData + " = ? WHERE " + cId + " = ?;",
        new String[]{ "new data etag", "test" });
    assertEquals(p.delete(makeUri("test"), cData + " = ?", new String[]{ "data etag here" }), 0);
    expectPresent("Tea_houses");
    expectPresent("test");
  }

  private void insertTables(String... ids) throws Exception {
    for (String id : ids) {
      insertTable(db, id);
    }
  }

  private void expectGone(String id) throws Exception {
    Cursor c = db
        .rawQuery("SELECT * FROM " + tTable + " WHERE " + cId + " = ?;", new String[]{ id });
    assertEquals(c.getCount(), 0);
    c.close();
  }

  private void expectPresent(String id) throws Exception {
    Cursor c = db
        .rawQuery("SELECT * FROM " + tTable + " WHERE " + cId + " = ?;", new String[]{ id });
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
   * Silliest test in the history of man
   */
  @Test
  public void testGetTablesAuthority() {
    assertEquals(TablesProvider.getTablesAuthority(), TablesProviderAPI.AUTHORITY);
  }

  @Test
  public void testOnCreate() {
    p.onCreate(); // shouldn't throw an exception
  }

  public TablesProvider getProvider() {
    return p;
  }
}