/*
 * Copyright (C) 2012 The Android Open Source Project
 * Copyright (C) 2009-2013 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.common.android.utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.common.android.provider.FormsColumns;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Environment;
import android.util.Log;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Static methods used for common file operations.
 *
 * @author Carl Hartung (carlhartung@gmail.com)
 */
public class ODKFileUtils {
  private final static String t = "ODKFileUtils";

  // base path
  private static final String ODK_FOLDER_NAME = "opendatakit";

  // 1st level -- appId

  // 2nd level -- directories

  private static final String CONFIG_FOLDER_NAME = "config";
  
  private static final String DATA_FOLDER_NAME = "data";
  
  private static final String OUTPUT_FOLDER_NAME = "output";
  
  private static final String SYSTEM_FOLDER_NAME = "system";

  // 3rd level -- directories

  // under config
  
  private static final String ASSETS_FOLDER_NAME = "assets";

  // under config and data
  
  private static final String TABLES_FOLDER_NAME = "tables";

  // under data
  
  private static final String WEB_DB_FOLDER_NAME = "webDb";

  private static final String GEO_CACHE_FOLDER_NAME = "geoCache";

  private static final String APP_CACHE_FOLDER_NAME = "appCache";

  // under output and config/assets
  
  private static final String CSV_FOLDER_NAME = "csv";

  // under output
  
  private static final String LOGGING_FOLDER_NAME = "logging";

  /** The name of the folder where the debug objects are written. */
  private static final String DEBUG_FOLDER_NAME = "debug";

  // under system
  
  private static final String STALE_TABLES_FOLDER_NAME = "tables.deleting";

  private static final String PENDING_TABLES_FOLDER_NAME = "tables.pending";

  // 4th level 
  
  // under the config/tables directory...
  
  private static final String FORMS_FOLDER_NAME = "forms";
  
  private static final String COLLECT_FORMS_FOLDER_NAME = "collect-forms";
  
  // under the data/tables directory...
  // and under the output/csv/tableId.qual/ and config/assets/csv/tableId.qual/ directories
  private static final String INSTANCES_FOLDER_NAME = "instances";

  // under data/webDb
  private static final String DATABASE_NAME = "sqlite.db";

  /**
   * Miscellaneous well-known file names
   */

  /** Filename for the top-level configuration file (in assets) */
  private static final String ODK_TABLES_INIT_FILENAME =
      "tables.init";

  /** Filename for the ODK Tables home screen (in assets) */
  private static final String ODK_TABLES_HOME_SCREEN_FILE_NAME =
      "index.html";

  /**
   * Filename of the config/tables/tableId/properties.csv file
   * that holds all kvs properties for this tableId.
   */
  private static final String PROPERTIES_CSV = "properties.csv";

  /**
   * Filename of the config/tables/tableId/definition.csv file
   * that holds the table schema for this tableId.
   */
  private static final String DEFINITION_CSV = "definition.csv";

  /**
   * directories within an application that are inaccessible via the
   * getAsFile() API.
   */
  private static final Set<String> topLevelWebServerExclusions;
  static {

    TreeSet<String> temp;

    temp = new TreeSet<String>();
    temp.add(SYSTEM_FOLDER_NAME);
    temp.add(OUTPUT_FOLDER_NAME);
    topLevelWebServerExclusions = Collections.unmodifiableSet(temp);
  }

  public static Set<String> getDirectoriesToExcludeFromWebServer() {
    return topLevelWebServerExclusions;
  }

  /**
   * Get the name of the logging folder, without a path.
   * @return
   */
  private static String getNameOfLoggingFolder() {
    return LOGGING_FOLDER_NAME;
  }

  /**
   * Get the name of the data folder, without a path.
   * @return
   */
  private static String getNameOfDataFolder() {
    return DATA_FOLDER_NAME;
  }

  /**
   * Get the name of the system folder, without a path.
   * @return
   */
  private static String getNameOfSystemFolder() {
    return SYSTEM_FOLDER_NAME;
  }

  /**
   * Get the name of the instances folder, without a path.
   * @return
   */
  public static String getNameOfInstancesFolder() {
    return INSTANCES_FOLDER_NAME;
  }

  public static String getNameOfSQLiteDatabase() {
    return DATABASE_NAME;
  }
  
  public static final ObjectMapper mapper = new ObjectMapper();

  public static final String MD5_COLON_PREFIX = "md5:";

  // filename of the xforms.xml instance and bind definitions if there is one.
  // NOTE: this file may be missing if the form was not downloaded
  // via the ODK1 compatibility path.
  public static final String FILENAME_XFORMS_XML = "xforms.xml";

  // special filename
  public static final String FORMDEF_JSON_FILENAME = "formDef.json";

  // Used to validate and display valid form names.
  public static final String VALID_FILENAME = "[ _\\-A-Za-z0-9]*.x[ht]*ml";

  public static void verifyExternalStorageAvailability() {
    String cardstatus = Environment.getExternalStorageState();
    if (cardstatus.equals(Environment.MEDIA_REMOVED)
        || cardstatus.equals(Environment.MEDIA_UNMOUNTABLE)
        || cardstatus.equals(Environment.MEDIA_UNMOUNTED)
        || cardstatus.equals(Environment.MEDIA_MOUNTED_READ_ONLY)
        || cardstatus.equals(Environment.MEDIA_SHARED)) {
      RuntimeException e = new RuntimeException("ODK reports :: SDCard error: "
          + Environment.getExternalStorageState());
      throw e;
    }
  }

  public static boolean createFolder(String path) {
    boolean made = true;
    File dir = new File(path);
    if (!dir.exists()) {
      made = dir.mkdirs();
    }
    return made;
  }

  public static String getOdkFolder() {
    String path = Environment.getExternalStorageDirectory() + File.separator + ODK_FOLDER_NAME;
    return path;
  }

  public static File[] getAppFolders() {
    File odk = new File(getOdkFolder());

    File[] results = odk.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        if (!pathname.isDirectory())
          return false;
        return true;
      }
    });

    return results;
  }

  public static void assertDirectoryStructure(String appName) {
    if ( !appName.equals("tables") ) {
      int i=0;
      ++i;
    }
    String[] dirs = { getAppFolder(appName),
        getConfigFolder(appName),
        getDataFolder(appName),
        getOutputFolder(appName),
        getSystemFolder(appName),
        // under Config
        getAssetsFolder(appName),
        getTablesFolder(appName),
        // under Data
        getAppCacheFolder(appName),
        getGeoCacheFolder(appName), 
        getWebDbFolder(appName),
        getTableDataFolder(appName),
        // under Output
        getLoggingFolder(appName),
        getOutputCsvFolder(appName),
        getTablesDebugObjectFolder(appName),
        // under System
        getPendingDeletionTablesFolder(appName),
        getPendingInsertionTablesFolder(appName)};

    for (String dirName : dirs) {
      File dir = new File(dirName);
      if (!dir.exists()) {
        if (!dir.mkdirs()) {
          RuntimeException e = new RuntimeException("Cannot create directory: " + dirName);
          throw e;
        }
      } else {
        if (!dir.isDirectory()) {
          RuntimeException e = new RuntimeException(dirName + " exists, but is not a directory");
          throw e;
        }
      }
    }
  }

  public static void assertConfiguredSurveyApp(String appName, String apkVersion) {
    assertConfiguredOdkApp(appName, "survey.version", apkVersion);
  }

  public static void assertConfiguredTablesApp(String appName, String apkVersion) {
    assertConfiguredOdkApp(appName, "tables.version", apkVersion);
  }

  public static void assertConfiguredScanApp(String appName, String apkVersion) {
    assertConfiguredOdkApp(appName, "scan.version", apkVersion);
  }

  public static void assertConfiguredOdkApp(String appName, String odkAppVersionFile, String apkVersion) {
    File versionFile = new File(getDataFolder(appName), odkAppVersionFile);

    if ( !versionFile.exists() ) {
      versionFile.getParentFile().mkdirs();
    }

    FileOutputStream fs = null;
    OutputStreamWriter w = null;
    BufferedWriter bw = null;
    try {
      fs = new FileOutputStream(versionFile, false);
      w = new OutputStreamWriter(fs, Charsets.UTF_8);
      bw = new BufferedWriter(w);
      bw.write(apkVersion);
      bw.write("\n");
    } catch (IOException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
    } finally {
      if ( bw != null ) {
        try {
          bw.flush();
          bw.close();
        } catch (IOException e) {
          WebLogger.getLogger(appName).printStackTrace(e);
        }
      }
      if ( w != null ) {
        try {
          w.close();
        } catch (IOException e) {
          WebLogger.getLogger(appName).printStackTrace(e);
        }
      }
      try {
        fs.close();
      } catch (IOException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
      }
    }
  }

  public static boolean isConfiguredSurveyApp(String appName, String apkVersion) {
    return isConfiguredOdkApp(appName, "survey.version", apkVersion);
  }

  public static boolean isConfiguredTablesApp(String appName, String apkVersion) {
    return isConfiguredOdkApp(appName, "tables.version", apkVersion);
  }

  public static boolean isConfiguredScanApp(String appName, String apkVersion) {
    return isConfiguredOdkApp(appName, "scan.version", apkVersion);
  }

  private static boolean isConfiguredOdkApp(String appName, String odkAppVersionFile, String apkVersion) {
    File versionFile = new File(getDataFolder(appName), odkAppVersionFile);

    if ( !versionFile.exists() ) {
      return false;
    }

    String versionLine = null;
    FileInputStream fs = null;
    InputStreamReader r = null;
    BufferedReader br = null;
    try {
      fs = new FileInputStream(versionFile);
      r = new InputStreamReader(fs, Charsets.UTF_8);
      br = new BufferedReader(r);
      versionLine = br.readLine();
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    } finally {
      if ( br != null ) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if ( r != null ) {
        try {
          r.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      try {
        fs.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    String[] versionRange = versionLine.split(";");
    for ( String version : versionRange ) {
      if ( version.trim().equals(apkVersion) ) {
        return true;
      }
    }
    return false;
  }

  public static File fromAppPath(String appPath) {
    String[] terms = appPath.split(File.separator);
    if (terms == null || terms.length < 1) {
      return null;
    }
    File f = new File(new File(getOdkFolder()), appPath);
    return f;
  }

  public static String toAppPath(String fullpath) {
    String path = getOdkFolder() + File.separator;
    if (fullpath.startsWith(path)) {
      String partialPath = fullpath.substring(path.length());
      String[] app = partialPath.split(File.separator);
      if (app == null || app.length < 1) {
        Log.w(t, "Missing file path (nothing under '" + ODK_FOLDER_NAME + "'): " + fullpath);
        return null;
      }
      return partialPath;
    } else {

      String[] parts = fullpath.split(File.separator);
      int i = 0;
      while (parts.length > i && !parts[i].equals(ODK_FOLDER_NAME)) {
        ++i;
      }
      if (i == parts.length) {
        Log.w(t, "File path is not under expected '" + ODK_FOLDER_NAME +
            "' Folder (" + path + ") conversion failed for: " + fullpath);
        return null;
      }
      int len = 0; // trailing slash
      while (i >= 0) {
        len += parts[i].length() + 1;
        --i;
      }

      String partialPath = fullpath.substring(len);
      String[] app = partialPath.split(File.separator);
      if (app == null || app.length < 1) {
        Log.w(t, "File path is not under expected '" + ODK_FOLDER_NAME +
            "' Folder (" + path + ") missing file path (nothing under '" +
            ODK_FOLDER_NAME + "'): " + fullpath);
        return null;
      }

      Log.w(t, "File path is not under expected '" + ODK_FOLDER_NAME +
            "' Folder -- remapped " + fullpath + " as: " + path + partialPath);
      return partialPath;
    }
  }

  public static String getAppFolder(String appName) {
    String path = getOdkFolder() + File.separator + appName;
    return path;
  }

  public static String getAndroidObbFolder(String packageName) {
    String path = Environment.getExternalStorageDirectory() + File.separator + "Android"
        + File.separator + "obb" + File.separator + packageName;
    return path;
  }


  // 1st level folders 
  
  public static String getConfigFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + CONFIG_FOLDER_NAME;
    return path;
  }

  public static String getDataFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + DATA_FOLDER_NAME;
    return path;
  }

  public static String getOutputFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + OUTPUT_FOLDER_NAME;
    return path;
  }

  public static String getSystemFolder(String appName) {
    String path = getAppFolder(appName) + File.separator + SYSTEM_FOLDER_NAME;
    return path;
  }

  //////////////////////////////////////////////////////////
  // Everything under config folder
  
  public static String getAssetsFolder(String appName) {
    String path = getConfigFolder(appName) + File.separator + ASSETS_FOLDER_NAME;
    return path;
  }

  public static String getAssetsCsvFolder(String appName) {
    String assetsFolder = getAssetsFolder(appName);
    String result = assetsFolder + File.separator + CSV_FOLDER_NAME;
    return result;
  }
  
  public static String getAssetsCsvInstancesFolder(String appName, String tableId) {
    String assetsCsvFolder = getAssetsCsvFolder(appName);
    String result = assetsCsvFolder +
        File.separator + tableId + File.separator + INSTANCES_FOLDER_NAME;
    return result;
  }

  public static String getAssetsCsvInstanceFolder(String appName, String tableId, String instanceId) {
    String assetsCsvInstancesFolder = getAssetsCsvInstancesFolder(appName, tableId);
    String result = assetsCsvInstancesFolder +
        File.separator + safeInstanceIdFolderName(instanceId);
    return result;
  }
  
  /**
   * Get the path to the tables initialization file for the given app.
   * @param appName
   * @return
   */
  public static String getTablesInitializationFile(String appName) {
    String assetsFolder = getAssetsFolder(appName);
    String result = assetsFolder + File.separator + ODK_TABLES_INIT_FILENAME;
    return result;
  }
  
  /**
   * Get the path to the user-defined home screen file.
   * @param appName
   * @return
   */
  public static String getTablesHomeScreenFile(String appName) {
    String assetsFolder = getAssetsFolder(appName);
    String result = assetsFolder + File.separator + ODK_TABLES_HOME_SCREEN_FILE_NAME;
    return result;
  }
  
  public static String getTablesFolder(String appName) {
    String path = getConfigFolder(appName) + File.separator + TABLES_FOLDER_NAME;
    return path;
  }
  
  public static String getTablesFolder(String appName, String tableId) {
    String path;
    if (tableId == null || tableId.length() == 0) {
      throw new IllegalArgumentException("getTablesFolder: tableId is null or the empty string!");
    } else {
      if ( !tableId.matches("^\\p{L}\\p{M}*(\\p{L}\\p{M}*|\\p{Nd}|_)+$") ) {
        throw new IllegalArgumentException(
            "getFormFolder: tableId does not begin with a letter and contain only letters, digits or underscores!");
      }
      if ( FormsColumns.COMMON_BASE_FORM_ID.equals(tableId) ) {
        path = getAssetsFolder(appName) + File.separator + FormsColumns.COMMON_BASE_FORM_ID;
      } else {
        path = getTablesFolder(appName) + File.separator + tableId;
      }
    }
    File f = new File(path);
    f.mkdirs();
    return f.getAbsolutePath();
  }

  // files under that
  
  public static String getTableDefinitionCsvFile(String appName, String tableId) {
    return getTablesFolder(appName, tableId) + File.separator + DEFINITION_CSV;
  }

  public static String getTablePropertiesCsvFile(String appName, String tableId) {
    return getTablesFolder(appName, tableId) + File.separator + PROPERTIES_CSV;
  }

  public static String getFormsFolder(String appName, String tableId) {
    String path = getTablesFolder(appName, tableId) + File.separator + FORMS_FOLDER_NAME;
    return path;
  }

  public static String getFormFolder(String appName, String tableId, String formId) {
    if ( FormsColumns.COMMON_BASE_FORM_ID.equals(tableId) ) {
      if ( !FormsColumns.COMMON_BASE_FORM_ID.equals(formId) ) {
        throw new IllegalStateException(FormsColumns.COMMON_BASE_FORM_ID + " can only have " + FormsColumns.COMMON_BASE_FORM_ID + " for a formId");
      }
    }

    if (formId == null || formId.length() == 0) {
      throw new IllegalArgumentException("getFormFolder: formId is null or the empty string!");
    } else {
      if ( !formId.matches("^\\p{L}\\p{M}*(\\p{L}\\p{M}*|\\p{Nd}|_)+$") ) {
        throw new IllegalArgumentException(
            "getFormFolder: formId does not begin with a letter and contain only letters, digits or underscores!");
      }
      String path = getFormsFolder(appName, tableId) + File.separator + formId;
      return path;
    }
  }

  /////////////////////////////////////////////////////////
  // Everything under data folder

  public static String getTablesInitializationCompleteMarkerFile(String appName) {
    String result = getDataFolder(appName) + File.separator + ODK_TABLES_INIT_FILENAME;
    return result;
  }

  public static String getAppCacheFolder(String appName) {
    String path = getDataFolder(appName) + File.separator + APP_CACHE_FOLDER_NAME;
    return path;
  }

  public static String getGeoCacheFolder(String appName) {
    String path = getDataFolder(appName) + File.separator + GEO_CACHE_FOLDER_NAME;
    return path;
  }

  public static String getWebDbFolder(String appName) {
    String path = getDataFolder(appName) + File.separator + WEB_DB_FOLDER_NAME;
    return path;
  }

  private static String getTableDataFolder(String appName) {
    String path = getDataFolder(appName) + File.separator + TABLES_FOLDER_NAME;
    return path;
  }
  
  public static String getInstancesFolder(String appName, String tableId) {
    String path;
    path = getTableDataFolder(appName) + File.separator +
        tableId + File.separator + INSTANCES_FOLDER_NAME;

    File f = new File(path);
    f.mkdirs();
    return f.getAbsolutePath();
  }

  private static String safeInstanceIdFolderName(String instanceId) {
    if (instanceId == null || instanceId.length() == 0) {
      throw new IllegalArgumentException("getInstanceFolder: instanceId is null or the empty string!");
    } else {
      String instanceFolder = instanceId.replaceAll("(\\p{P}|\\p{Z})", "_");
      return instanceFolder;
    }
  }
  
  public static String getInstanceFolder(String appName, String tableId, String instanceId) {
    String path;
    String instanceFolder = safeInstanceIdFolderName(instanceId);

    path = getInstancesFolder(appName, tableId) + File.separator + instanceFolder;

    File f = new File(path);
    f.mkdirs();
    return f.getAbsolutePath();
  }

  public static File getRowpathFile( String appName, String tableId, String instanceId, String rowpathUri ) {
    // clean up the value...
    if ( rowpathUri.startsWith("/") ) {
      rowpathUri = rowpathUri.substring(1);
    }
    String instanceFolder = 
        ODKFileUtils.getInstanceFolder(appName, tableId, instanceId);
    String instanceUri = ODKFileUtils.asUriFragment(appName, new File(instanceFolder));
    String fileUri;
    if ( rowpathUri.startsWith(instanceUri) ) {
      // legacy construction
      WebLogger.getLogger(appName).e(t,
          "table [" + tableId + "] contains old-style rowpath constructs!");
      fileUri = rowpathUri;
    } else {
      fileUri = instanceUri + "/" + rowpathUri;
    }
    File theFile = ODKFileUtils.getAsFile(appName, fileUri);
    return theFile;
  }
  
  public static String asRowpathUri( String appName, String tableId, String instanceId, File rowFile ) {
    String instanceFolder =
        ODKFileUtils.getInstanceFolder(appName, tableId, instanceId);
    String instanceUri = ODKFileUtils.asUriFragment(appName, new File(instanceFolder));
    String rowpathUri = ODKFileUtils.asUriFragment(appName, rowFile);
    if ( !rowpathUri.startsWith(instanceUri) ) {
      throw new IllegalArgumentException("asRowpathUri -- rowFile is not in a valid rowpath location!");
    }
    String relativeUri = rowpathUri.substring(instanceUri.length());
    if ( relativeUri.startsWith("/") ) {
      relativeUri = relativeUri.substring(1);
    }
    return relativeUri;
  }
  
  ///////////////////////////////////////////////
  // Everything under output folder

  public static String getLoggingFolder(String appName) {
    String path = getOutputFolder(appName) + File.separator + LOGGING_FOLDER_NAME;
    return path;
  }

  public static String getTablesDebugObjectFolder(String appName) {
    String outputFolder = getOutputFolder(appName);
    String result = outputFolder + File.separator + DEBUG_FOLDER_NAME;
    return result;
  }

  public static String getOutputCsvFolder(String appName) {
    String outputFolder = getOutputFolder(appName);
    String result = outputFolder + File.separator + CSV_FOLDER_NAME;
    return result;
  }
  
  public static String getOutputCsvInstanceFolder(String appName, String tableId, String instanceId) {
    String csvOutputFolder = getOutputCsvFolder(appName);
    String result = csvOutputFolder +
        File.separator + tableId + File.separator + INSTANCES_FOLDER_NAME +
        File.separator + safeInstanceIdFolderName(instanceId);
    return result;
  }

  public static String getOutputTableCsvFile(String appName, String tableId, String fileQualifier) {
    return getOutputCsvFolder(appName) + File.separator + tableId +
        ((fileQualifier != null && fileQualifier.length() != 0) ? ("." + fileQualifier) : "") + ".csv";
  }

  public static String getOutputTableDefinitionCsvFile(String appName, String tableId, String fileQualifier) {
    return getOutputCsvFolder(appName) + File.separator + tableId +
        ((fileQualifier != null && fileQualifier.length() != 0) ? ("." + fileQualifier) : "") + "." + DEFINITION_CSV;
  }

  public static String getOutputTablePropertiesCsvFile(String appName, String tableId, String fileQualifier) {
    return getOutputCsvFolder(appName) + File.separator + tableId +
        ((fileQualifier != null && fileQualifier.length() != 0) ? ("." + fileQualifier) : "") + "." + PROPERTIES_CSV;
  }


  ////////////////////////////////////////
  // Everything under system folder
  
  public static String getPendingDeletionTablesFolder(String appName) {
    String path = getSystemFolder(appName) + File.separator + STALE_TABLES_FOLDER_NAME;
    return path;
  }

  public static String getPendingInsertionTablesFolder(String appName) {
    String path = getSystemFolder(appName) + File.separator + PENDING_TABLES_FOLDER_NAME;
    return path;
  }

  // 4th level config tables tableId folder

  // 3rd level output

  public static boolean isPathUnderAppName(String appName, File path) {

    File parentDir = new File(getAppFolder(appName));

    while (path != null && !path.equals(parentDir)) {
      path = path.getParentFile();
    }

    return (path != null);
  }

  public static String extractAppNameFromPath(File path) {

    if ( path == null ) {
      return null;
    }

    File parent = path.getParentFile();
    File odkDir = new File(getOdkFolder());
    while (parent != null && !parent.equals(odkDir)) {
      path = parent;
      parent = path.getParentFile();
    }

    if ( parent == null ) {
      return null;
    } else {
      return path.getName();
    }
  }

  /**
   * Returns the relative path beginning after the getAppFolder(appName) directory.
   * The relative path does not start or end with a '/'
   *
   * @param appName
   * @param fileUnderAppName
   * @return
   */
  public static String asRelativePath(String appName, File fileUnderAppName) {
    // convert fileUnderAppName to a relative path such that if
    // we just append it to the AppFolder, we have a full path.
    File parentDir = new File(getAppFolder(appName));

    ArrayList<String> pathElements = new ArrayList<String>();

    File f = fileUnderAppName;
    while (f != null && !f.equals(parentDir)) {
      pathElements.add(f.getName());
      f = f.getParentFile();
    }

    if (f == null) {
      throw new IllegalArgumentException("file is not located under this appName (" + appName + ")!");
    }

    StringBuilder b = new StringBuilder();
    for (int i = pathElements.size() - 1; i >= 0; --i) {
      String element = pathElements.get(i);
      b.append(element);
      if ( i != 0 ) {
        b.append(File.separator);
      }
    }
    return b.toString();

  }

  public static String asUriFragment(String appName, File fileUnderAppName) {
    String relativePath = asRelativePath( appName, fileUnderAppName);
    String separatorString;
    if ( File.separatorChar == '\\') {
      // Windows Robolectric
      separatorString = File.separator + File.separator;
    } else {
      separatorString = File.separator;
    }
    String[] segments = relativePath.split(separatorString);
    StringBuilder b = new StringBuilder();
    boolean first = true;
    for ( String s : segments ) {
      if ( !first ) {
        b.append("/"); // uris have forward slashes
      }
      first = false;
      b.append(s);
    }
    return b.toString();
  }

  /**
   * Reconstructs the full path from the appName and uriFragment
   *
   * @param appName
   * @param uriFragment
   * @return
   */
  public static File getAsFile(String appName, String uriFragment) {
    // forward slash always...
    if ( uriFragment == null || uriFragment.length() == 0 ) {
      throw new IllegalArgumentException("Not a valid uriFragment: " +
          appName + "/" + uriFragment +
          " application or subdirectory not specified.");
    }

    File f = fromAppPath(appName);
    if (f == null || !f.exists() || !f.isDirectory()) {
      throw new IllegalArgumentException("Not a valid uriFragment: " +
            appName + "/" + uriFragment + " invalid application.");
    }

    String[] segments = uriFragment.split("/");
    for ( int i = 0 ; i < segments.length ; ++i ) {
      String s = segments[i];
      f = new File(f, s);
    }
    return f;
  }

  /**
   * Convert a relative path into an application filename
   *
   * @param appName
   * @param relativePath
   * @return
   */
  public static File asAppFile(String appName, String relativePath) {
    return new File(getAppFolder(appName) + File.separator + relativePath);
  }

  public static File asConfigFile(String appName, String relativePath) {
    return new File(getConfigFolder(appName) + File.separator + relativePath);
  }

  public static String asConfigRelativePath(String appName, File fileUnderAppConfigName) {
    String relativePath = asRelativePath(appName, fileUnderAppConfigName);
    if ( !relativePath.startsWith(CONFIG_FOLDER_NAME + File.separator) ) {
      throw new IllegalArgumentException("File is not located under config folder");
    }
    relativePath = relativePath.substring(CONFIG_FOLDER_NAME.length()+File.separator.length());
    if ( relativePath.contains(File.separator + "..")) {
      throw new IllegalArgumentException("File contains " + File.separator + "..");
    }
    return relativePath;
  }

  /**
   * The formPath is relative to the framework directory and is passed into
   * the WebKit to specify the form to display.
   *
   * @param appName
   * @param formDefFile
   * @return
   */
  public static String getRelativeFormPath(String appName, File formDefFile) {

    // compute FORM_PATH...
    // we need to do this relative to the AppFolder, as the
    // common index.html is under the ./system folder.

    String relativePath = asRelativePath(appName, formDefFile.getParentFile());
    // adjust for relative path from ./system...
    relativePath = ".." + File.separator + relativePath + File.separator;
    return relativePath;
  }

  /**
   * Used as the baseUrl in the webserver.
   * 
   * @return e.g., ../system
   */
  public static String getRelativeSystemPath() {
    return "../" + ODKFileUtils.SYSTEM_FOLDER_NAME;
  }

  public static byte[] getFileAsBytes(String appName, File file) {
    byte[] bytes = null;
    InputStream is = null;
    try {
      is = new FileInputStream(file);

      // Get the size of the file
      long length = file.length();
      if (length > Integer.MAX_VALUE) {
        WebLogger.getLogger(appName).e(t, "File " + file.getName() + "is too large");
        return null;
      }

      // Create the byte array to hold the data
      bytes = new byte[(int) length];

      // Read in the bytes
      int offset = 0;
      int read = 0;
      try {
        while (offset < bytes.length && read >= 0) {
          read = is.read(bytes, offset, bytes.length - offset);
          offset += read;
        }
      } catch (IOException e) {
        WebLogger.getLogger(appName).e(t, "Cannot read " + file.getName());
        e.printStackTrace();
        return null;
      }

      // Ensure all the bytes have been read in
      if (offset < bytes.length) {
        try {
          throw new IOException("Could not completely read file " + file.getName());
        } catch (IOException e) {
          WebLogger.getLogger(appName).printStackTrace(e);
          return null;
        }
      }

      return bytes;

    } catch (FileNotFoundException e) {
      WebLogger.getLogger(appName).e(t, "Cannot find " + file.getName());
      WebLogger.getLogger(appName).printStackTrace(e);
      return null;

    } finally {
      // Close the input stream
      try {
        is.close();
      } catch (IOException e) {
        WebLogger.getLogger(appName).e(t, "Cannot close input stream for " + file.getName());
        WebLogger.getLogger(appName).printStackTrace(e);
        return null;
      }
    }
  }

  public static String getMd5Hash(String appName, File file) {
    return MD5_COLON_PREFIX + getNakedMd5Hash(appName, file);
  }

  /**
   * Recursively traverse the directory to find the most recently modified
   * file within it.
   *
   * @param formDir
   * @return lastModifiedDate of the most recently modified file.
   */
  public static long getMostRecentlyModifiedDate(File formDir) {
    long lastModifiedDate = formDir.lastModified();
    Iterator<File> allFiles = FileUtils.iterateFiles(formDir, null, true);
    while (allFiles.hasNext()) {
      File f = allFiles.next();
      if (f.lastModified() > lastModifiedDate) {
        lastModifiedDate = f.lastModified();
      }
    }
    return lastModifiedDate;
  }

  public static String getNakedMd5Hash(String appName, File file) {
    try {
      // CTS (6/15/2010) : stream file through digest instead of handing
      // it the byte[]
      MessageDigest md = MessageDigest.getInstance("MD5");
      int chunkSize = 8192;

      byte[] chunk = new byte[chunkSize];

      // Get the size of the file
      long lLength = file.length();

      if (lLength > Integer.MAX_VALUE) {
        WebLogger.getLogger(appName).e(t, "File " + file.getName() + "is too large");
        return null;
      }

      int length = (int) lLength;

      InputStream is = null;
      is = new FileInputStream(file);

      int l = 0;
      for (l = 0; l + chunkSize < length; l += chunkSize) {
        is.read(chunk, 0, chunkSize);
        md.update(chunk, 0, chunkSize);
      }

      int remaining = length - l;
      if (remaining > 0) {
        is.read(chunk, 0, remaining);
        md.update(chunk, 0, remaining);
      }
      byte[] messageDigest = md.digest();

      BigInteger number = new BigInteger(1, messageDigest);
      String md5 = number.toString(16);
      while (md5.length() < 32)
        md5 = "0" + md5;
      is.close();
      return md5;

    } catch (NoSuchAlgorithmException e) {
      WebLogger.getLogger(appName).e("MD5", e.getMessage());
      return null;

    } catch (FileNotFoundException e) {
      WebLogger.getLogger(appName).e("No Cache File", e.getMessage());
      return null;
    } catch (IOException e) {
      WebLogger.getLogger(appName).e("Problem reading from file", e.getMessage());
      return null;
    }

  }

  public static String getNakedMd5Hash(String appName, String contents) {
    try {
      // CTS (6/15/2010) : stream file through digest instead of handing
      // it the byte[]
      MessageDigest md = MessageDigest.getInstance("MD5");
      int chunkSize = 256;

      byte[] chunk = new byte[chunkSize];

      // Get the size of the file
      long lLength = contents.length();

      if (lLength > Integer.MAX_VALUE) {
        WebLogger.getLogger(appName).e(t, "Contents is too large");
        return null;
      }

      int length = (int) lLength;

      InputStream is = null;
      is = new ByteArrayInputStream(contents.getBytes(CharEncoding.UTF_8));

      int l = 0;
      for (l = 0; l + chunkSize < length; l += chunkSize) {
        is.read(chunk, 0, chunkSize);
        md.update(chunk, 0, chunkSize);
      }

      int remaining = length - l;
      if (remaining > 0) {
        is.read(chunk, 0, remaining);
        md.update(chunk, 0, remaining);
      }
      byte[] messageDigest = md.digest();

      BigInteger number = new BigInteger(1, messageDigest);
      String md5 = number.toString(16);
      while (md5.length() < 32)
        md5 = "0" + md5;
      is.close();
      return md5;

    } catch (NoSuchAlgorithmException e) {
      WebLogger.getLogger(appName).e("MD5", e.getMessage());
      return null;

    } catch (FileNotFoundException e) {
      WebLogger.getLogger(appName).e("No Cache File", e.getMessage());
      return null;
    } catch (IOException e) {
      WebLogger.getLogger(appName).e("Problem reading from file", e.getMessage());
      return null;
    }

  }

  public static Bitmap getBitmapScaledToDisplay(String appName, File f, int screenHeight, int screenWidth) {
    // Determine image size of f
    BitmapFactory.Options o = new BitmapFactory.Options();
    o.inJustDecodeBounds = true;
    BitmapFactory.decodeFile(f.getAbsolutePath(), o);

    int heightScale = o.outHeight / screenHeight;
    int widthScale = o.outWidth / screenWidth;

    // Powers of 2 work faster, sometimes, according to the doc.
    // We're just doing closest size that still fills the screen.
    int scale = Math.max(widthScale, heightScale);

    // get bitmap with scale ( < 1 is the same as 1)
    BitmapFactory.Options options = new BitmapFactory.Options();
    options.inSampleSize = scale;
    Bitmap b = BitmapFactory.decodeFile(f.getAbsolutePath(), options);
    if (b != null) {
      WebLogger.getLogger(appName).i(t,
          "Screen is " + screenHeight + "x" + screenWidth + ".  Image has been scaled down by "
              + scale + " to " + b.getHeight() + "x" + b.getWidth());
    }
    return b;
  }

  public static String getXMLText(Node n, boolean trim) {
    NodeList nl = n.getChildNodes();
    return (nl.getLength() == 0 ? null : getXMLText(nl, 0, trim));
  }

  /**
   * reads all subsequent text nodes and returns the combined string needed
   * because escape sequences are parsed into consecutive text nodes e.g.
   * "abc&amp;123" --> (abc)(&)(123)
   **/
  private static String getXMLText(NodeList nl, int i, boolean trim) {
    StringBuffer strBuff = null;

    String text = nl.item(i).getTextContent();
    if (text == null)
      return null;

    for (i++; i < nl.getLength() && nl.item(i).getNodeType() == Node.TEXT_NODE; i++) {
      if (strBuff == null)
        strBuff = new StringBuffer(text);

      strBuff.append(nl.item(i).getTextContent());
    }
    if (strBuff != null)
      text = strBuff.toString();

    if (trim)
      text = text.trim();

    return text;
  }
}
