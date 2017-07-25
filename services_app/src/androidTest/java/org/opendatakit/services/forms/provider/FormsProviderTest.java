package org.opendatakit.services.forms.provider;

import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.support.test.runner.AndroidJUnit4;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.provider.FormsColumns;
import org.opendatakit.services.database.AndroidConnectFactory;
import org.opendatakit.utilities.LocalizationUtils;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Niles on 6/29/17.
 */
@RunWith(AndroidJUnit4.class)
public class FormsProviderTest {
  private static Uri uri = new Uri.Builder().appendPath(getAppName()).build();
  //private OdkConnectionInterface db;
  private static boolean initialized = false;
  private FormsProvider p;

  public static ContentValues getCvs(String id) {
    ContentValues c = new ContentValues();
    c.put(FormsColumns.TABLE_ID, id);
    c.put(FormsColumns.FORM_ID, id);
    return c;
  }

  public static String getAppName() {
    return "default";
  }

  private static void assertTeaHouses(Cursor result) throws Exception {
    if (result == null)
      throw new Exception("null cursor");
    result.moveToFirst();
    assertEquals(LocalizationUtils.getLocalizedDisplayName(getAppName(), "Tea_houses",
        result.getString(result.getColumnIndexOrThrow(FormsColumns.DEFAULT_FORM_LOCALE)),
        result.getString(result.getColumnIndexOrThrow(FormsColumns.DISPLAY_NAME))), "Tea Houses");
    result.close();
  }

//  @Test
//  public void testQueryBlankFormId() throws Exception {
//    // Should pull default form id from the database/KVS
//    Cursor result = p.query(
//        new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").appendPath("").build(),
//        new String[] { FormsColumns.DISPLAY_NAME, FormsColumns.DEFAULT_FORM_LOCALE }, null, null,
//        null);
//    assertTeaHouses(result);
//  }

  @Before
  public void setUp() throws Throwable {
    ODKFileUtils.assertDirectoryStructure(getAppName());
    if (!initialized) {
      initialized = true;
      AndroidConnectFactory.configure();
    }
    /*
    DbHandle uniqueKey = new DbHandle(
        getClass().getSimpleName() + AndroidConnectFactory.INTERNAL_TYPE_SUFFIX);
    db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
        .getConnection(getAppName(), uniqueKey);
    */

    p = new FormsProvider();
  }

  @Test(expected = android.database.SQLException.class)
  public void testInsertExistingForm() throws Exception {
    // Try the insert once and it might succeed if the user never opened tables before (i.e jenkins)
    try {
      p.insert(uri, getCvs("Tea_houses_editable"));
    } catch (Throwable ignored) {
      // ignore
    }
    p.insert(uri, getCvs("Tea_houses_editable"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertBadUri() throws Exception {
    p.insert(new Uri.Builder().build(), null);
  }

//  @Test
//  public void testDeleteExistingAndInsertNewFormUsingWhereClause() throws Exception {
//    deleteExistingAndInsertNewForm(new Runnable() {
//      @Override
//      public void run() {
//        p.delete(uri, FormsColumns.TABLE_ID + " =?", new String[] { "Tea_houses" });
//      }
//    });
//  }

//  @Test
//  public void testDeleteExistingAndInsertNewFormUsingUri() throws Exception {
//    deleteExistingAndInsertNewForm(new Runnable() {
//      @Override
//      public void run() {
//        p.delete(new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses")
//            .appendPath("Tea_houses").build(), null, null);
//      }
//    });
//  }

//  @Test
//  public void testDeleteExistingAndInsertNewFormNoFormId() throws Exception {
//    deleteExistingAndInsertNewForm(new Runnable() {
//      @Override
//      public void run() {
//        p.delete(new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").appendPath("")
//            .build(), null, null);
//      }
//    });
//  }

  private void deleteExistingAndInsertNewForm(Runnable r) throws Exception {
    File a = new File(ODKFileUtils.getFormFolder(getAppName(), "Tea_houses", "Tea_houses") + '/'
        + ODKFileUtils.FORMDEF_JSON_FILENAME);
    File b = new File(ODKFileUtils.getAppFolder(getAppName()) + "/formDef-backup.json");
    if (b.exists() && !b.delete()) {
      throw new IOException("should have been able to delete temporary copy of formdef");
    }
    ODKFileUtils.copyFile(a, b);
    r.run();
    // make sure it was actually removed
    Cursor c = p.query(new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").build(),
        FormsColumns.formsDataColumnNames, null, null, null);
    if (c != null) {
      assertEquals(c.getCount(), 0);
      c.close();
    }
    if (!a.getParentFile().exists() && !a.getParentFile().mkdirs()) {
      throw new IOException("should have been able to recreate tables/Tea_houses/forms/Tea_houses");
    }
    ODKFileUtils.copyFile(b, a);
    p.insert(uri, getCvs("Tea_houses"));
    if (!b.delete()) {
      throw new IOException("should have been able to delete temporary copy of formdef");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsertNonExistingForm() throws Exception {
    p.insert(uri, getCvs("form_id_with_no_formdef_json"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTypeShortUri() throws Exception {
    p.getType(new Uri.Builder().build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTypeMixedUri() throws Exception {
    Uri testUri = new Uri.Builder().appendPath(getAppName()).appendPath("1234")
        .appendPath("too_long").build();
    p.getType(testUri);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetTypeLongUri() throws Exception {
    Uri testUri = new Uri.Builder().appendPath(getAppName()).appendPath("1234")
        .appendPath("too_long").appendPath("way too long").build();
    p.getType(testUri);
  }

  @Test
  public void testGetType() throws Exception {
    Uri testUri = new Uri.Builder().appendPath(getAppName()).appendPath("1234").build();
    assertEquals(p.getType(testUri), FormsColumns.CONTENT_ITEM_TYPE);

    testUri = new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").build();
    assertEquals(p.getType(testUri), FormsColumns.CONTENT_TYPE);

    testUri = new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses")
        .appendPath("Tea_houses").build();
    assertEquals(p.getType(testUri), FormsColumns.CONTENT_ITEM_TYPE);
  }

//  @Test
//  public void testQueryExistingForm() throws Exception {
//    // just app name
//    Cursor result = p.query(uri, FormsColumns.formsDataColumnNames, FormsColumns.TABLE_ID + " =?",
//        new String[] { "Tea_houses" }, FormsColumns.TABLE_ID);
//    assertTeaHouses(result);
//    // app name + table id
//    result = p.query(new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").build(),
//        FormsColumns.formsDataColumnNames, FormsColumns.DEFAULT_FORM_LOCALE + " =?",
//        new String[] { "default" }, FormsColumns.TABLE_ID);
//    assertTeaHouses(result);
//    // app name + table id + form id
//    result = p.query(
//        new Uri.Builder().appendPath(getAppName()).appendPath("Tea_houses").appendPath("Tea_houses")
//            .build(), FormsColumns.formsDataColumnNames, FormsColumns.DEFAULT_FORM_LOCALE + " =?",
//        new String[] { "default" }, FormsColumns.TABLE_ID);
//    assertTeaHouses(result);
//    // TODO app name + table id + numeric form id
//  }

  @Test
  public void testQueryNonExistingForm() throws Throwable {
    Cursor c = p.query(new Uri.Builder().appendPath(getAppName()).build(),
        FormsColumns.formsDataColumnNames, FormsColumns.TABLE_ID + " =?",
        new String[] { "this table shouldn't exist" }, FormsColumns.FORM_ID);
    if (c == null)
      throw new Exception("Unexpected null");
    assertEquals(c.getCount(), 0);
    c.close();
  }

//  @Test
//  public void testUpdate() throws Exception {
//    Cursor c = p.query(uri, new String[0], null, null, FormsColumns.FORM_ID);
//    if (c == null)
//      throw new Exception("Null cursor");
//    int expected = c.getCount();
//    c.close();
//    assertEquals(p.update(uri, null, null, null), expected);
//  }

  @Test
  public void testOnCreate() throws Exception {
    // Should not throw an exception
    p.onCreate();
  }
}
