package org.opendatakit.database.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import android.util.Log;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.database.DatabaseConstants;
import org.opendatakit.database.data.TypedRow;
import org.opendatakit.database.data.UserTable;
import org.opendatakit.database.queries.BindArgs;
import org.opendatakit.exception.ServicesAvailabilityException;
import org.opendatakit.provider.SyncETagColumns;

import java.util.Locale;

/**
 * Created by Niles on 6/30/17.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SyncETagsUtilsTest extends OdkDatabaseTestAbstractBase {

   private static final String LOGTAG = SyncETagsUtilsTest.class.getSimpleName();

   private static final String TEA_HOUSES_TID = "Tea_houses";
   private static final String TEST_ID = "test";

   private static final int DEFAULT_TIMESTAMP = 1234567;
   private static final String DEFAULT_URL = "http://url.here";
   private static final String DEFAULT_MD5 = "md5 goes here";
   public static final String EXPT_MSG = "threw an exception:";

   private UserDbInterface serviceInterface;
   private DbHandle dbHandle;

   protected void setUpBefore() {
      try {
         serviceInterface = bindToDbService();
         assertNotNull(serviceInterface);
         dbHandle = serviceInterface.openDatabase(APPNAME);

      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   protected void tearDownBefore() {
      try {
         UserTable result = serviceInterface
                 .simpleQuery(APPNAME, dbHandle, DatabaseConstants.SYNC_ETAGS_TABLE_NAME, null,
                         null, null, null, null, null, null, null, null);

         if (result == null)
            throw new IllegalStateException("Bad query");

         if (result.getNumberOfRows() > 0) {
            for (int i = 0; i < result.getNumberOfRows(); i++) {
               TypedRow row = result.getRowAtIndex(i);
               Log.e(LOGTAG, "Found Leftover Sync ETAG:");
               Log.e(LOGTAG, "-TABLE_ID: " + row.getRawStringByKey(SyncETagColumns.TABLE_ID));
               Log.e(LOGTAG, "-IS_MANIFEST: " + row.getRawStringByKey(SyncETagColumns.IS_MANIFEST));
               Log.e(LOGTAG, "-ETAG_MD5_HASH: " + row.getRawStringByKey(SyncETagColumns.ETAG_MD5_HASH));
               Log.e(LOGTAG, "-URL: " + row.getRawStringByKey(SyncETagColumns.URL));
               serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, row.getRawStringByKey(SyncETagColumns.TABLE_ID));
            }
            fail("DID NOT CLEAN SYNC_ETAGS!");
         }

         if (dbHandle != null) {
            serviceInterface.closeDatabase(APPNAME, dbHandle);
         }
      } catch (Exception e) {
         e.printStackTrace();
         fail(e.getMessage());
      }
   }

   @Test
   public void testDeleteAllSyncETagsForTableId() {
      String tid = "some table id";
      try {
         insertManifestSyncETag(TEA_HOUSES_TID);
         insertManifestSyncETag(tid);
         serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);
         expectGone(TEA_HOUSES_TID, true);
         expectPresent(tid, true);
         serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, tid);
      } catch (ServicesAvailabilityException e) {
         fail(EXPT_MSG + e.getMessage());
      } finally {
         try {
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, tid);
         } catch (ServicesAvailabilityException e) {
            fail(EXPT_MSG + e.getMessage());
         }
      }
   }

   @Test
   public void testDeleteAppAndTableLevelManifestSyncETags() {
      try {
         // If the bool is 1, it should be deleted
         insertManifestSyncETag(TEA_HOUSES_TID);
         serviceInterface.deleteAppAndTableLevelManifestSyncETags(APPNAME, dbHandle);
         expectGone(TEA_HOUSES_TID, true);
         expectGone(TEA_HOUSES_TID, false);

         // If the bool is 0, it should not be deleted
         insertFileSyncETag(TEA_HOUSES_TID);
         serviceInterface.deleteAppAndTableLevelManifestSyncETags(APPNAME, dbHandle);
         expectPresent(TEA_HOUSES_TID, false);
         expectGone(TEA_HOUSES_TID, true);

      } catch (ServicesAvailabilityException e) {
         fail(EXPT_MSG + e.getMessage());
      } finally {
         try {
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);
         } catch (ServicesAvailabilityException e) {
            fail(EXPT_MSG + e.getMessage());
         }
      }
   }

   @Test
   public void testDeleteAllSyncETagsExceptForServer() {
      try {
         // Null should delete everything
         insertManifestSyncETag(TEA_HOUSES_TID);
         serviceInterface.deleteAllSyncETagsExceptForServer(APPNAME, dbHandle, null);
         expectGone(TEA_HOUSES_TID, true);

         // giving it a uri should delete everything without that base uri
         insertManifestSyncETag(TEA_HOUSES_TID);
         insertManifestSyncETagWithUrl(TEST_ID, "https://new.url");
         serviceInterface.deleteAllSyncETagsExceptForServer(APPNAME, dbHandle, DEFAULT_URL);
         expectPresent(TEA_HOUSES_TID, true);
         expectGone(TEST_ID, true);

         // only the URI's scheme and host should make a difference
         insertManifestSyncETagWithUrl(TEA_HOUSES_TID, "http://url.here/abcd");
         insertManifestSyncETagWithUrl(TEST_ID, "https://new.url/efgh");
         serviceInterface
                 .deleteAllSyncETagsExceptForServer(APPNAME, dbHandle, "http://url.here/ijkl");

         expectPresent(TEA_HOUSES_TID, true);
         expectGone(TEST_ID, true);

      } catch (ServicesAvailabilityException e) {
         fail(EXPT_MSG + e.getMessage());
      } finally {
         try {
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEST_ID);
         } catch (ServicesAvailabilityException e) {
            fail(EXPT_MSG + e.getMessage());
         }
      }
   }

   @Test
   public void testDeleteAllSyncETagsUnderServer() {
      try {
         insertManifestSyncETag(TEA_HOUSES_TID);
         insertManifestSyncETagWithUrl(TEST_ID, "https://new.url/abcdef");
         serviceInterface.deleteAllSyncETagsUnderServer(APPNAME, dbHandle, "https://new.url/");
         expectPresent(TEA_HOUSES_TID, true);
         expectGone(TEST_ID, true);
         boolean worked = false;
         try {
            serviceInterface.deleteAllSyncETagsUnderServer(APPNAME, dbHandle, null);
         } catch (IllegalArgumentException ignored) {
            worked = true;
         }
         assertTrue(worked);
      } catch (ServicesAvailabilityException e) {
         fail(EXPT_MSG + e.getMessage());
      } finally {
         try {
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEST_ID);
         } catch (ServicesAvailabilityException e) {
            fail(EXPT_MSG + e.getMessage());
         }
      }
   }

   @Test
   public void testGetManifestSyncETag() {
      try {
         insertManifestSyncETag(TEA_HOUSES_TID);
         String md5 = serviceInterface
                 .getManifestSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEA_HOUSES_TID);
         assertEquals(md5, DEFAULT_MD5);
         assertNull(serviceInterface
                 .getManifestSyncETag(APPNAME, dbHandle, "http://other.url", TEA_HOUSES_TID));
         ///
         insertFileSyncETag(TEST_ID);
         assertNull(serviceInterface.getManifestSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEST_ID));
      } catch (ServicesAvailabilityException e) {
         fail(EXPT_MSG + e.getMessage());
      } finally {
         try {
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEST_ID);
         } catch (ServicesAvailabilityException e) {
            fail(EXPT_MSG + e.getMessage());
         }
      }

   }

   @Test
   public void testUpdateManifestSyncETag() {
      try {
         String newEtag = "new etag";

         insertManifestSyncETag(TEA_HOUSES_TID);

         serviceInterface
                 .updateManifestSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEA_HOUSES_TID, newEtag);
         UserTable result = get(TEA_HOUSES_TID, true);
         TypedRow row = result.getRowAtIndex(0);
         assertEquals(row.getRawStringByKey(SyncETagColumns.ETAG_MD5_HASH), newEtag);

         // does not update when manifest = false (file instead of manifest)
         insertManifestSyncETag(TEST_ID);
         serviceInterface.updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEST_ID, 3L, newEtag);

         result = get(TEST_ID, true);
         row = result.getRowAtIndex(0);
         assertEquals(row.getRawStringByKey(SyncETagColumns.ETAG_MD5_HASH), DEFAULT_MD5);
      } catch (ServicesAvailabilityException e) {
         fail(EXPT_MSG + e.getMessage());
      } finally {
         try {
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEST_ID);
         } catch (ServicesAvailabilityException e) {
            fail(EXPT_MSG + e.getMessage());
         }
      }

   }

   @Test
   public void testGetFileSyncETag() {
      try {
         insertManifestSyncETag(TEST_ID);
         insertFileSyncETag(TEST_ID);

         serviceInterface
                 .updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEST_ID, 5, DEFAULT_MD5);
         assertEquals(
                 serviceInterface.getFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEST_ID, 5),
                 DEFAULT_MD5);

         // returns null if given a bad date or bad url
         assertNull(
                 serviceInterface.getFileSyncETag(APPNAME, dbHandle, "http://wrong.url", TEST_ID, 5));
         assertNull(
                 serviceInterface.getFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEST_ID, 9));

      } catch (ServicesAvailabilityException e) {
         fail(EXPT_MSG + e.getMessage());
      } finally {
         try {
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEST_ID);
         } catch (ServicesAvailabilityException e) {
            fail(EXPT_MSG + e.getMessage());
         }
      }

   }

   @Test
   public void testUpdateFileSyncETag() {
      try {
         String revisedETag = "new etag";

         // should not set with manifest = 1
         insertManifestSyncETag(TEA_HOUSES_TID);
         serviceInterface
                 .updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEA_HOUSES_TID, 6, revisedETag);

         UserTable result = get(TEA_HOUSES_TID, true);
         TypedRow row = result.getRowAtIndex(0);
         assertEquals(row.getRawStringByKey(SyncETagColumns.ETAG_MD5_HASH), DEFAULT_MD5);

         serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);

         // should set with manifest = 0
         insertFileSyncETag(TEA_HOUSES_TID);
         serviceInterface
                 .updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEA_HOUSES_TID, 6, DEFAULT_MD5);
         serviceInterface
                 .updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEA_HOUSES_TID, 6, revisedETag);

         result = get(TEA_HOUSES_TID, false);
         row = result.getRowAtIndex(0);
         assertEquals(row.getRawStringByKey(SyncETagColumns.ETAG_MD5_HASH), revisedETag);

         serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);

         // Should have no effect with wrong url
         insertFileSyncETag(TEA_HOUSES_TID);
         serviceInterface
                 .updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEA_HOUSES_TID, 6, DEFAULT_MD5);
         serviceInterface
                 .updateFileSyncETag(APPNAME, dbHandle, "http://wrong.url", TEA_HOUSES_TID, 6,
                         revisedETag);
         result = get(TEA_HOUSES_TID, false);
         row = result.getRowAtIndex(0);
         assertEquals(row.getRawStringByKey(SyncETagColumns.ETAG_MD5_HASH), DEFAULT_MD5);

         serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);

         // Should update date
         insertFileSyncETag(TEA_HOUSES_TID);
         serviceInterface
                 .updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEA_HOUSES_TID, 6, DEFAULT_MD5);
         serviceInterface
                 .updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, TEA_HOUSES_TID, 7, revisedETag);

         result = get(TEA_HOUSES_TID, false);
         row = result.getRowAtIndex(0);
         assertEquals(row.getRawStringByKey(SyncETagColumns.ETAG_MD5_HASH), revisedETag);
         assertEquals(row.getRawStringByKey(SyncETagColumns.LAST_MODIFIED_TIMESTAMP),
                 TableConstants.nanoSecondsFromMillis(7L, Locale.ROOT));

      } catch (ServicesAvailabilityException e) {
         fail(EXPT_MSG + e.getMessage());
      }  finally {
         try {
            serviceInterface.deleteAllSyncETagsForTableId(APPNAME, dbHandle, TEA_HOUSES_TID);
         } catch (ServicesAvailabilityException e) {
            fail(EXPT_MSG + e.getMessage());
         }
      }
   }

   /////////////////////////////////////////////////////////////////////////
   ///////////////////     Private Helper Functions      ///////////////////
   /////////////////////////////////////////////////////////////////////////

   private void insertFileSyncETag(String id) throws ServicesAvailabilityException {
      serviceInterface
              .updateFileSyncETag(APPNAME, dbHandle, DEFAULT_URL, id, DEFAULT_TIMESTAMP, DEFAULT_MD5);
   }

   private void insertManifestSyncETag(String id) throws ServicesAvailabilityException {
      serviceInterface.updateManifestSyncETag(APPNAME, dbHandle, DEFAULT_URL, id, DEFAULT_MD5);
   }

   private void insertManifestSyncETagWithUrl(String id, String url)
           throws ServicesAvailabilityException {
      serviceInterface.updateManifestSyncETag(APPNAME, dbHandle, url, id, DEFAULT_MD5);
   }

   private UserTable get(String id, boolean isManifest) throws ServicesAvailabilityException {
      // OrderedColumns columns = new OrderedColumns(APPNAME, table, SyncETagColumns.getColumnList
      // ());
      String whereClause =
              SyncETagColumns.TABLE_ID + " =? AND " + SyncETagColumns.IS_MANIFEST + " =?;";
      Object[] bindArgs = new Object[2];
      bindArgs[0] = id;
      bindArgs[1] = isManifest;
      BindArgs args = new BindArgs(bindArgs);
      UserTable result = serviceInterface
              .simpleQuery(APPNAME, dbHandle, DatabaseConstants.SYNC_ETAGS_TABLE_NAME, null,
                      whereClause, args, null, null, null, null, null, null);

      if (result == null)
         throw new IllegalStateException("Bad query");

      return result;
   }

   private void expectGone(String id, boolean isManifest) throws ServicesAvailabilityException {
      UserTable c = get(id, isManifest);
      assertEquals(0, c.getNumberOfRows());
   }

   private void expectPresent(String id, boolean isManifest) throws ServicesAvailabilityException {
      UserTable c = get(id, isManifest);
      assertTrue(c.getNumberOfRows() > 0);
   }
}