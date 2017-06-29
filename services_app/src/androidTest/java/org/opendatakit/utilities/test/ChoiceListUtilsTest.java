package org.opendatakit.utilities.test;

import android.support.test.runner.AndroidJUnit4;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.provider.ChoiceListColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utlities.ChoiceListUtils;
import org.opendatakit.utilities.ODKFileUtils;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Created by Niles on 6/29/17.
 */

@RunWith(AndroidJUnit4.class)
public class ChoiceListUtilsTest {

  private static final String key = "key that's unlikely to be in use";

  private OdkConnectionInterface db;
  private static boolean initialized = false;

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
  public void testSetChoiceList() throws Throwable {
    new ChoiceListUtils().setChoiceList(db, key, "my json");
    assertEquals(ChoiceListUtils.getChoiceList(db, key), "my json");
  }

  @Test
  public void testGetChoiceListOnEmpty() throws Throwable {
    assertEquals(ChoiceListUtils.getChoiceList(db, key), null);
  }

  @After
  public void tearDown() throws Exception {
    if (db != null) {
      db.releaseReference();
    }
  }

}
