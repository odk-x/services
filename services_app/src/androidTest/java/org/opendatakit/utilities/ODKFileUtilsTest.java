package org.opendatakit.utilities;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Environment;
import android.provider.MediaStore;

import androidx.test.core.app.ApplicationProvider;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.rule.GrantPermissionRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.services.utilities.FileSystemHelper;

@RunWith(AndroidJUnit4.class)
public class ODKFileUtilsTest {

    private static final String ODK_FOLDER_NAME = "opendatakit";
    private FileSystemHelper fileSystemHelper;
    private Context context;

    @Rule
    public GrantPermissionRule permissionRule = GrantPermissionRule.grant(android.Manifest.permission.WRITE_EXTERNAL_STORAGE);

    @Before
    public void setUp() {
        context = ApplicationProvider.getApplicationContext();
        fileSystemHelper = new FileSystemHelper(context);
    }

    @Test
    public void testCreateDirectory() {
        String folderName = "testFolder";
        String relativePath = Environment.DIRECTORY_DOCUMENTS + "/" + ODK_FOLDER_NAME + "/" + folderName;

        Uri directoryUri = fileSystemHelper.getFolderUri(relativePath);
        if (directoryUri == null) {
            directoryUri = fileSystemHelper.createDirectory(relativePath);
        }
        assertNotNull("Directory URI should not be null after creation", directoryUri);

        Uri externalUri = MediaStore.Files.getContentUri("external");
        String[] projection = {MediaStore.Files.FileColumns.RELATIVE_PATH};
        String selection = MediaStore.Files.FileColumns.RELATIVE_PATH + "=?";
        String[] selectionArgs = {relativePath + "/"};
        try (Cursor cursor = context.getContentResolver().query(externalUri, projection, selection, selectionArgs, null)) {
            boolean directoryExists = cursor != null && cursor.getCount() > 0;
            assertTrue("Directory should exist after creation", directoryExists);
        }
    }

    @Test
    public void testGetOdkFolderUri() {
        Uri resultUri = fileSystemHelper.getOdkFolder();
        assertNotNull(resultUri);
        String[] projection = {MediaStore.MediaColumns.RELATIVE_PATH};
        String selection = MediaStore.MediaColumns.RELATIVE_PATH + "=?";
        String[] selectionArgs = {Environment.DIRECTORY_DOCUMENTS + "/" + ODK_FOLDER_NAME + "/"};

        Cursor cursor = context.getContentResolver().query(
                MediaStore.Files.getContentUri("external"),
                projection,
                selection,
                selectionArgs,
                null
        );
        assertNotNull(cursor);
        assertTrue(cursor.getCount() > 0);
        cursor.close();
    }


    @Test
    public void testAssertDirectoryStructure() {
        String appName = ODK_FOLDER_NAME;

        try {
            fileSystemHelper.assertDirectoryStructure(appName);
        } catch (Exception e) {
            fail("Exception thrown while asserting directory structure: " + e.getMessage());
        }

        String[] folderNames = {"config", "data", "output", "system", "permanent"};
        for (String folderName : folderNames) {
            String relativePath = Environment.DIRECTORY_DOCUMENTS + "/" + appName + "/" + folderName;

            Uri folderUri = fileSystemHelper.getFolderUri(relativePath);
            assertNotNull("Folder " + folderName + " does not exist", folderUri);

//            Uri nomediaUri = fileSystemHelper.getFileUri(relativePath, ".nomedia");
//            assertNotNull(".nomedia file does not exist in folder " + folderName, nomediaUri);
        }
    }


}


