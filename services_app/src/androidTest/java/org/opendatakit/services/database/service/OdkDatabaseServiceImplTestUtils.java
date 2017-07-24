package org.opendatakit.services.database.service;

import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.provider.KeyValueStoreColumns;
import org.opendatakit.services.database.OdkConnectionInterface;

import static android.text.TextUtils.join;

/**
 * Created by Niles on 7/24/17.
 */

public class OdkDatabaseServiceImplTestUtils {
  public static void insertMetadata(OdkConnectionInterface db, String table, String partition, String aspect, String key,
                                    String value) {
    db.rawQuery(
        "INSERT INTO " + DatabaseConstants.KEY_VALUE_STORE_ACTIVE_TABLE_NAME + " (" + join(", ",
            new String[]{ KeyValueStoreColumns.TABLE_ID, KeyValueStoreColumns.PARTITION,
                KeyValueStoreColumns.ASPECT, KeyValueStoreColumns.KEY, KeyValueStoreColumns.VALUE,
                KeyValueStoreColumns.VALUE_TYPE }) + ") VALUES (?, ?, ?, "
            + "?, ?, ?);",
        new String[]{ table, partition, aspect, key, value, "TEXT" });
  }

}
