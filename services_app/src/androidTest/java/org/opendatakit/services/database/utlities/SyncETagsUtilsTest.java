package org.opendatakit.services.database.utlities;

import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.support.test.runner.AndroidJUnit4;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.provider.SyncETagColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.utilities.ODKFileUtils;

import static org.apache.commons.lang3.StringUtils.join;
import static org.junit.Assert.*;

/**
 * Created by Niles on 6/30/17.
 */
@RunWith(AndroidJUnit4.class)
public class SyncETagsUtilsTest {
  private static boolean initialized = false;
  private static SyncETagsUtils s = new SyncETagsUtils();
  private static String table = DatabaseConstants.SYNC_ETAGS_TABLE_NAME;
  private static String cTable = SyncETagColumns.TABLE_ID;
  private static String cManifest = SyncETagColumns.IS_MANIFEST;
  private static String cUrl = SyncETagColumns.URL;
  private static String cModified = SyncETagColumns.LAST_MODIFIED_TIMESTAMP;
  private static String cMd5 = SyncETagColumns.ETAG_MD5_HASH;
  private OdkConnectionInterface db;

  public static String getAppName() {
    return "default";
  }

  @Before
  public void setUp() {
    ODKFileUtils.assertDirectoryStructure(getAppName());
    if (!initialized) {
      initialized = true;
      AndroidConnectFactory.configure();
    }
    DbHandle uniqueKey = new DbHandle(
        getClass().getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .getConnection(getAppName(), uniqueKey);
    db.rawQuery("DELETE FROM " + table + ";", new Object[0]).close();
  }

  private void insert(String id) {
    insert(id, 1);
  }

  private void insert(String id, int bool) {
    String[] all = new String[] { cTable, cManifest, cUrl, cModified, cMd5 };
    db.rawQuery("INSERT INTO " + table + " (" + join(all, ", ") + ") VALUES (?, ?, ?, ?, ?);",
        new Object[] { id, bool, "http://url.here", "date goes here", "md5 goes here" }).close();
  }

  private Cursor get(String id) {
    Cursor result = db
        .rawQuery("SELECT * FROM " + table + " WHERE " + cTable + " =?;", new Object[] { id });
    if (result == null)
      throw new IllegalStateException("Bad cursor");
    result.moveToFirst();
    return result;
  }

  @Test
  public void testDeleteAllSyncETagsForTableId() {
    insert("Tea_houses");
    insert("some table id");
    s.deleteAllSyncETagsForTableId(db, "Tea_houses");
    expectGone("Tea_houses");
    expectPresent("some table id");
  }

  @Test
  public void testDeleteAppAndTableLevelManifestSyncETags() {
    // If the bool is 1, it should be deleted
    insert("Tea_houses");
    s.deleteAppAndTableLevelManifestSyncETags(db);
    expectGone("Tea_houses");

    // If the bool is 0, it should not be deleted
    insert("Tea_houses", 0);
    s.deleteAppAndTableLevelManifestSyncETags(db);
    expectPresent("Tea_houses");
  }

  @Test
  public void testDeleteAllSyncETagsExceptForServer() {
    // Null should delete everything
    insert("Tea_houses");
    s.deleteAllSyncETagsExceptForServer(db, null);
    expectGone("Tea_houses");

    // giving it a uri should delete everything without that base uri
    insert("Tea_houses");
    insertWithUrl("test", "https://new.url");
    s.deleteAllSyncETagsExceptForServer(db, "http://url.here");
    expectPresent("Tea_houses");
    expectGone("test");

    // only the URI's scheme and host should make a difference
    insertWithUrl("Tea_houses", "http://url.here/abcd");
    insertWithUrl("test", "https://new.url/efgh");
    s.deleteAllSyncETagsExceptForServer(db, "http://url.here/ijkl");
    expectPresent("Tea_houses");
    expectGone("test");

  }

  @Test
  public void testDeleteAllSyncETagsUnderServer() {
    insert("Tea_houses");
    insertWithUrl("test", "https://new.url/abcdef");
    s.deleteAllSyncETagsUnderServer(db, "https://new.url/");
    expectPresent("Tea_houses");
    expectGone("test");
    boolean worked = false;
    try {
      s.deleteAllSyncETagsUnderServer(db, null);
    } catch (IllegalArgumentException ignored) {
      worked = true;
    }
    assertTrue(worked);
  }

  @Test
  public void testGetManifestSyncETag() {
    insert("Tea_houses");
    String md5 = s.getManifestSyncETag(db, "http://url.here", "Tea_houses");
    assertEquals(md5, "md5 goes here");
    assertNull(s.getManifestSyncETag(db, "http://other.url", "Tea_houses"));
    ///
    insert("test", 0);
    assertNull(s.getManifestSyncETag(db, "http://url.here", "test"));
  }

  @Test
  public void testUpdateManifestSyncETag() {
    insert("Tea_houses");
    s.updateManifestSyncETag(db, "http://url.here", "Tea_houses", "new etag");
    Cursor c = db.rawQuery("SELECT * FROM " + table + ";", new String[0]);
    c.moveToFirst();
    assertEquals(gets(c, cMd5), "new etag");
    c.close();
    // does not update when manifest = false
    insert("test", 0);
    s.updateManifestSyncETag(db, "http://url.here", "test", "new etag");
    c = db
        .rawQuery("SELECT * FROM " + table + " WHERE " + cManifest + " =?;", new String[] { "0" });
    c.moveToFirst();
    assertEquals(gets(c, cMd5), "md5 goes here");
    c.close();

  }

  @Test
  public void testGetFileSyncETag() {
    insert("Tea_houses");
    setDate("Tea_houses", 6L);
    insert("test", 0);
    setDate("test", 5L);
    assertNull(s.getFileSyncETag(db, "http://url.here", "Tea_houses", 6));
    assertEquals(s.getFileSyncETag(db, "http://url.here", "test", 5), "md5 goes here");
    // returns null if given a bad date or bad url
    assertNull(s.getFileSyncETag(db, "http://wrong.url", "test", 5));
    assertNull(s.getFileSyncETag(db, "http://url.here", "test", 9));
  }

  @Test
  public void testUpdateFileSyncETag() {
    // should not set with manifest = 1
    insert("Tea_houses");
    setDate("Tea_houses", 6L);
    s.updateFileSyncETag(db, "http://url.here", "Tea_houses", 6, "new etag");
    Cursor c = db
        .rawQuery("SELECT * FROM " + table + " WHERE " + cManifest + " =?;", new Object[] { "1" });
    c.moveToFirst();
    assertEquals(gets(c, cMd5), "md5 goes here");
    setUp();
    // should set with manifest = 0
    insert("Tea_houses", 0);
    setDate("Tea_houses", 6L);
    s.updateFileSyncETag(db, "http://url.here", "Tea_houses", 6, "new etag");
    c = db.rawQuery("SELECT * FROM " + table + ";", new Object[0]);
    c.moveToFirst();
    assertEquals(gets(c, cMd5), "new etag");
    setUp();
    // Should have no effect with wrong url
    insert("Tea_houses", 0);
    setDate("Tea_houses", 6L);
    s.updateFileSyncETag(db, "http://wrong.url", "Tea_houses", 6, "new etag");
    c = db.rawQuery("SELECT * FROM " + table + ";", new Object[0]);
    c.moveToFirst();
    assertEquals(gets(c, cMd5), "md5 goes here");
    setUp();
    // Should update date
    insert("Tea_houses", 0);
    setDate("Tea_houses", 6L);
    s.updateFileSyncETag(db, "http://url.here", "Tea_houses", 7, "new etag");
    c = db.rawQuery("SELECT * FROM " + table + ";", new Object[0]);
    c.moveToFirst();
    assertEquals(gets(c, cMd5), "new etag");
    assertEquals(gets(c, cModified), TableConstants.nanoSecondsFromMillis(7L));
  }

  private void setDate(String id, Long date) {
    db.execSQL("UPDATE " + table + " SET " + cModified + " =? WHERE " + cTable + " =?;",
        new String[] { TableConstants.nanoSecondsFromMillis(date), id });

  }

  private void insertWithUrl(String id, String url) {
    insert(id);
    db.rawQuery("UPDATE " + table + " SET " + cUrl + " = ? WHERE " + cTable + " = ?;",
        new Object[] { url, id }).close();
  }

  public void expectGone(String id) {
    boolean worked = false;
    try {
      Cursor c = get(id);
      c.getString(c.getColumnIndexOrThrow(cTable));
      c.close();
    } catch (CursorIndexOutOfBoundsException ignored) {
      worked = true;
    }
    assertTrue(worked);
  }

  public void expectPresent(String id) {
    Cursor c = get(id);
    assertEquals(gets(c, cTable), id);
    assertEquals(gets(c, cModified), "date goes here");
    c.close();
  }

  public String gets(Cursor c, String col) {
    return c.getString(idx(c, col));
  }

  public int idx(Cursor c, String col) {
    return c.getColumnIndexOrThrow(col);
  }

}