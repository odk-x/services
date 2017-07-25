package org.opendatakit.services.database.utlities;

import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.support.test.runner.AndroidJUnit4;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.provider.SyncETagColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.utilities.ODKFileUtils;

import static org.apache.commons.lang3.StringUtils.join;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  public void setUp() throws Throwable {
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
        new Object[] { bool, "md5 goes here", "date goes here", id, "http://url.here" }).close();
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
    insert("Tea_houses");
    s.deleteAllSyncETagsExceptForServer(db, null);
    expectGone("Tea_houses");
    insert("Tea_houses");
    insert("test");
    db.rawQuery("UPDATE " + table + " SET " + cUrl + " = ? WHERE " + cTable + " = ?;",
        new Object[] { "https://new.url", "test" }).close();
    if (true) return; // TODO
    s.deleteAllSyncETagsExceptForServer(db, "new u");
    expectPresent("test");
    expectGone("Tea_houses");
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