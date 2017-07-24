package org.opendatakit.services.tables.provider;

import android.database.Cursor;
import android.net.Uri;

import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.provider.TableDefinitionsColumns;
import org.opendatakit.services.database.OdkConnectionInterface;

import static android.text.TextUtils.join;

/**
 * Utilities clsas created at Waylon's request
 */

public final class TablesProviderColumns {
  public static Uri uri = new Uri.Builder().appendPath(getAppName()).build();
  public static String tTable = DatabaseConstants.TABLE_DEFS_TABLE_NAME;
  public static String cId = TableDefinitionsColumns.TABLE_ID;
  public static String cSchema = TableDefinitionsColumns.SCHEMA_ETAG;
  public static String cData = TableDefinitionsColumns.LAST_DATA_ETAG;
  public static String cTime = TableDefinitionsColumns.LAST_SYNC_TIME;
  public static String cRev = TableDefinitionsColumns.REV_ID;
  public static String[] all = { cId, cSchema, cData, cTime, cRev };

  /**
   * Do not instantiate this class
   */
  private TablesProviderColumns() {
  }

  public static String getAppName() {
    return "default";
  }

  public static Uri makeUri(String id) {
    return new Uri.Builder().appendPath(getAppName()).appendPath(id).build();
  }

  public static String get(Cursor c, String col) {
    return c.getString(c.getColumnIndexOrThrow(col));
  }
  public static void insertTable(OdkConnectionInterface db, String id) {
    db.execSQL("INSERT INTO " + tTable + " (" + join(", ", all) + ") VALUES (?, ?, ?, ?, ?);",
        new String[]{ id, "schema etag here", "data etag here", "timestamp here", "revId here" });
  }
  public static Cursor query(TablesProvider p, String[] columns, String where, String[] whereArgs, String sortOrder, Uri uri) throws Exception {
    Cursor result = p.query(uri, columns, where, whereArgs, sortOrder);
    if (result == null)
      throw new Exception("Null cursor");
    return result;
  }
}
