package org.opendatakit.services.forms;

import android.database.Cursor;
import android.net.Uri;
import android.os.Environment;
import android.support.test.runner.AndroidJUnit4;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.forms.provider.FormsProvider;
import org.opendatakit.services.forms.provider.FormsProviderTest;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.io.IOException;

import static android.text.TextUtils.join;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opendatakit.services.forms.provider.FormsProviderTest.getAppName;
import static org.opendatakit.services.forms.provider.FormsProviderTest.getCvs;
//import static org.opendatakit.services.forms.provider.FormsProviderTest.getAppName;

/**
 * Created by Niles on 6/29/17.
 */

@RunWith(AndroidJUnit4.class)
public class FormInfoTest {
  private FormInfo info;
  private boolean initialized = false;
  private OdkConnectionInterface db;

  @Test
  public void testCursorConstructor() throws Throwable {
    insert("breathcounter");
    Cursor c = db.rawQuery("SELECT " + join(", ", FormsColumns.formsDataColumnNames) + " FROM "
            + DatabaseConstants.FORMS_TABLE_NAME + " WHERE " + FormsColumns.FORM_ID + " =?",
        new String[] { "breathcounter" });
    c.moveToFirst();
    info = new FormInfo("default", c, false);
    assertEquals(info.tableId, "breathcounter");
    assertEquals(info.formVersion, "20130408");
    assertEquals(info.formDef, null);
  }

//  @Test
//  public void testCursorConstructorNotJson() throws Exception {
//    testCursorConstructorBadFormdef(new ArgRunnable() {
//      @Override
//      public void run(File a) throws Exception {
//        OutputStream f = new FileOutputStream(a);
//        f.write("Not a valid json file".getBytes());
//        f.close();
//      }
//    });
//  }

  public void testCursorConstructorBadFormdef(ArgRunnable r) throws Exception {
    AndroidConnectFactory.configure();
    DbHandle uniqueKey = new DbHandle(
        getClass().getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    OdkConnectionInterface db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .getConnection("default", uniqueKey);
    File a = new File(ODKFileUtils.getFormFolder(getAppName(), "Tea_houses", "Tea_houses") + '/'
        + ODKFileUtils.FORMDEF_JSON_FILENAME);
    File b = new File(ODKFileUtils.getAppFolder(getAppName()) + "/formDef-backup.json");
    if (b.exists() && !b.delete()) {
      throw new IOException("should have been able to delete temporary copy of formdef");
    }
    ODKFileUtils.copyFile(a, b);
    try {
      new FormsProvider()
          .insert(new Uri.Builder().appendPath(getAppName()).build(), getCvs("Tea_houses"));
    } catch (Exception ignored) {
      // ignore
    }
    r.run(a);
    Cursor c = db.rawQuery(
        "SELECT * FROM " + DatabaseConstants.FORMS_TABLE_NAME + " WHERE " + FormsColumns.FORM_ID
            + " = ?;", new String[] { "Tea_houses" });
    boolean failed = false;
    boolean worked = false;
    if (c == null) {
      failed = true;
    } else {
      c.moveToFirst();
      try {
        //noinspection ResultOfObjectAllocationIgnored
        new FormInfo("default", c, true);
      } catch (IllegalArgumentException ignored) {
        worked = true;
      }
      c.close();
    }
    if (!a.getParentFile().exists() && !a.getParentFile().mkdirs()) {
      throw new IOException("should have been able to recreate tables/Tea_houses/forms/Tea_houses");
    }
    ODKFileUtils.copyFile(b, a);
    if (!b.delete()) {
      throw new IOException("should have been able to delete temporary copy of formdef");
    }
    if (failed)
      throw new Exception("Null cursor");
    assertTrue(worked);
  }

//  @Test
//  public void testCursorConstructorFormdefDoesntExist() throws Exception {
//    testCursorConstructorBadFormdef(new ArgRunnable() {
//      @Override
//      public void run(File a) throws Exception {
//        if (!a.delete()) {
//          throw new IOException("Should have been able to delete real formdef");
//        }
//      }
//    });
//  }

  @Before
  public void setUp() {
    setUp("default", "Tea_houses_editable");
    if (!initialized) {
      AndroidConnectFactory.configure();
      DbHandle uniqueKey = new DbHandle(
          getClass().getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection("default", uniqueKey);
      initialized = true;
    }
  }

  public void setUp(String app, String form) {
    insert(form);
    info = new FormInfo(null, "default", new File(
        Environment.getExternalStorageDirectory().getPath() + "/opendatakit/" + app
            + "/config/tables/" + form + "/forms" + "/" + form + "/formDef.json"));
  }

  public void insert(String form) {
    try {
      new FormsProviderTest().setUp();
      // make sure it's in the database when we try to query it
      new FormsProvider().insert(new Uri.Builder().appendPath(getAppName()).build(),
          FormsProviderTest.getCvs(form));
    } catch (Throwable ignored) {
      // ignore
    }
  }

  @Test
  public void testAsRowValues() {
    String[] res = info.asRowValues(new String[] { "displayName", "defaultFormLocale", "tableId" });
    String local = LocalizationUtils.getLocalizedDisplayName("default", res[2], res[1], res[0]);
    assertEquals(local, "Tea Houses Editable");
    assertEquals(res[1], "default");
    assertEquals(res[2], "Tea_houses_editable");
    res = info.asRowValues(null);
    assertEquals(res.length, FormsColumns.formsDataColumnNames.length);
  }

  @Test
  public void testProperties() {
    assertEquals(info.appName, "default");
    assertEquals(info.defaultLocale, "default");
    assertEquals(info.formId, "Tea_houses_editable");
    assertEquals(info.formVersion, "20140204");
    setUp("default", "twoColumn");
    assertEquals(info.appName, "default");
    assertEquals(info.defaultLocale, "default");
    assertEquals(info.formId, "twoColumn");
    assertEquals(info.formVersion, "20130408");
    assertEquals(info.instanceName, null);
    assertEquals(info.formTitle, "{\"text\":\"Two Column Form\"}");
    setUp("default", "complex_validate_test");
    assertEquals(info.appName, "default");
    assertEquals(info.defaultLocale, "default");
    assertEquals(info.tableId, "complex_validate_test");
    assertEquals(info.formId, "complex_validate_test");
    assertEquals(info.formVersion, "20130808");
    assertEquals(info.instanceName, null);
    assertEquals(info.formTitle, "{\"text\":{\"default\":\"Two Part Validation Test\","
        + "\"hindi\":\"दो भाग मान्यकरण टेस्ट\"}}");
    assertEquals(
        LocalizationUtils.getLocalizedDisplayName("default", info.tableId, "hindi", info.formTitle),
        "दो भाग मान्यकरण टेस्ट");
    assertEquals(LocalizationUtils
            .getLocalizedDisplayName("default", info.tableId, "default", info.formTitle),
        "Two Part Validation Test");
  }

  private interface ArgRunnable {
    void run(File a) throws Exception;
  }
}
