package org.opendatakit.utilities;

import android.Manifest;

import androidx.test.rule.GrantPermissionRule;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.provider.ChoiceListColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utilities.ChoiceListUtils;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Niles on 6/29/17.
 */


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ChoiceListUtilsTest {

  private static final String key = "key that's unlikely to be in use";

  private OdkConnectionInterface db;
  private static boolean initialized = false;

  @Rule
  public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);

  @Rule
  public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

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

    Collection<String> bindArgs = new ArrayList<>();
    String query = "DELETE FROM " + "\"" + DatabaseConstants.CHOICE_LIST_TABLE_NAME + "\" WHERE "
        + ChoiceListColumns.CHOICE_LIST_ID + "=?";
    bindArgs.add(key);
    boolean inTransaction = db.inTransaction();
    if (!inTransaction) {
      db.beginTransactionNonExclusive();
    }
    db.execSQL(query, bindArgs.toArray(new String[bindArgs.size()]));
    if (!inTransaction) {
      db.setTransactionSuccessful();
    }
  }

  private static String getAppName() {
    return "some app name";
  }

  @Test
  public void testSetChoiceList() {
    ChoiceListUtils.setChoiceList(db, key, "my json");
    assertEquals(ChoiceListUtils.getChoiceList(db, key), "my json");
  }

  @Test
  public void testGetChoiceListOnEmpty() {
      assertNull(ChoiceListUtils.getChoiceList(db, key));
  }

  @After
  public void tearDown() throws Exception {
    if (db != null) {
      db.releaseReference();
    }
  }

}
