package org.opendatakit.services.utilities;

import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Environment;
import android.provider.DocumentsContract;
import android.provider.MediaStore;

public class FileSystemHelper {
    private static final String ODK_FOLDER_NAME = "opendatakit";
    private final Context context;

    public FileSystemHelper(Context context) {
        this.context = context;
    }

    public Uri getOdkFolder() {
        ContentValues values = new ContentValues();
        values.put(MediaStore.MediaColumns.RELATIVE_PATH, Environment.DIRECTORY_DOCUMENTS + "/" + ODK_FOLDER_NAME);
        Uri folderUri = context.getContentResolver().insert(MediaStore.Files.getContentUri("external"), values);
        return folderUri;
    }

    public void assertDirectoryStructure(String appName) {
        String[] folderNames = {"config", "data", "output", "system", "permanent"};

        // Create each directory and check for existence
        for (String folderName : folderNames) {
            String relativePath = Environment.DIRECTORY_DOCUMENTS + "/" + appName + "/" + folderName;
            if (getFolderUri(relativePath) == null) {
                createDirectory(relativePath);
            }

            // Ensure .nomedia file is created inside each folder
            String nomediaFilePath = relativePath;
            if (getFileUri(nomediaFilePath, ".nomedia") == null) {
                createNomediaFile(nomediaFilePath);
            }
        }
    }

    // Helper method to create a .nomedia file inside a specific directory
    private void createNomediaFile(String relativePath) {
        ContentValues values = new ContentValues();
        values.put(MediaStore.MediaColumns.DISPLAY_NAME, ".nomedia");
        values.put(MediaStore.MediaColumns.RELATIVE_PATH, relativePath + "/");
        values.put(MediaStore.MediaColumns.MIME_TYPE, "application/octet-stream"); // MIME type for generic binary data

        context.getContentResolver().insert(MediaStore.Files.getContentUri("external"), values);
    }

    // Helper method to create a directory using MediaStore
    public Uri createDirectory(String relativePath) {
        ContentValues values = new ContentValues();
        values.put(MediaStore.MediaColumns.DISPLAY_NAME, relativePath.substring(relativePath.lastIndexOf("/") + 1)); // Directory name
        values.put(MediaStore.MediaColumns.RELATIVE_PATH, relativePath);
        values.put(MediaStore.MediaColumns.MIME_TYPE, DocumentsContract.Document.MIME_TYPE_DIR); // MIME type for a directory

        return context.getContentResolver().insert(MediaStore.Files.getContentUri("external"), values);
    }

    // Helper method to get the folder Uri using MediaStore
    public Uri getFolderUri(String relativePath) {
        String[] projection = {MediaStore.MediaColumns._ID};
        String selection = MediaStore.MediaColumns.RELATIVE_PATH + "=?";
        String[] selectionArgs = {relativePath + "/"};

        try (Cursor cursor = context.getContentResolver().query(
                MediaStore.Files.getContentUri("external"),
                projection,
                selection,
                selectionArgs,
                null
        )) {
            if (cursor != null && cursor.moveToFirst()) {
                long id = cursor.getLong(cursor.getColumnIndexOrThrow(MediaStore.MediaColumns._ID));
                return ContentUris.withAppendedId(MediaStore.Files.getContentUri("external"), id);
            }
        }
        return null;
    }

    // Helper method to get the file Uri using MediaStore
    public Uri getFileUri(String relativePath, String fileName) {
        String[] projection = {MediaStore.MediaColumns._ID};
        String selection = MediaStore.MediaColumns.RELATIVE_PATH + "=? AND " + MediaStore.MediaColumns.DISPLAY_NAME + "=?";
        String[] selectionArgs = {relativePath + "/", fileName};

        try (Cursor cursor = context.getContentResolver().query(
                MediaStore.Files.getContentUri("external"),
                projection,
                selection,
                selectionArgs,
                null
        )) {
            if (cursor != null && cursor.moveToFirst()) {
                long id = cursor.getLong(cursor.getColumnIndexOrThrow(MediaStore.MediaColumns._ID));
                return ContentUris.withAppendedId(MediaStore.Files.getContentUri("external"), id);
            }
        }
        return null;
    }

}
