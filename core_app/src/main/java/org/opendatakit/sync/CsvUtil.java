/*
 * Copyright (C) 2012 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.sync;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.aggregate.odktables.rest.RFC4180CsvReader;
import org.opendatakit.aggregate.odktables.rest.RFC4180CsvWriter;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.application.AppAwareApplication;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.ColumnList;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.Row;
import org.opendatakit.common.android.provider.ColumnDefinitionsColumns;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.provider.KeyValueStoreColumns;
import org.opendatakit.common.android.utilities.*;
import org.opendatakit.database.service.KeyValueStoreEntry;
import org.opendatakit.database.service.OdkDbHandle;

import android.content.ContentValues;
import android.os.RemoteException;
import org.opendatakit.database.service.OdkDbInterface;

/**
 * Various utilities for importing/exporting tables from/to CSV.
 *
 * @author sudar.sam@gmail.com
 * @author unknown
 *
 */
public class CsvUtil {

  public interface ExportListener {

    public void exportComplete(boolean outcome);
  };

  public interface ImportListener {
    public void updateProgressDetail(String progressDetailString);

    public void importComplete(boolean outcome);
  }

  private static final String TAG = CsvUtil.class.getSimpleName();

  private final AppAwareApplication context;
  private final String appName;
  private final OdkDbInterface database;

  public CsvUtil(AppAwareApplication context, String appName, OdkDbInterface database) {
    this.context = context;
    this.appName = appName;
    this.database = database;
  }
  
  private OdkDbInterface getDatabase() {
    return database;
  }

  // ===========================================================================================
  // EXPORT
  // ===========================================================================================

  /**
   * Export the given tableId. Exports three csv files to the output/csv
   * directory under the appName:
   * <ul>
   * <li>tableid.fileQualifier.csv - data table</li>
   * <li>tableid.fileQualifier.definition.csv - data table column definition</li>
   * <li>tableid.fileQualifier.properties.csv - key-value store of this table</li>
   * </ul>
   * If fileQualifier is null or an empty string, then it emits to
   * <ul>
   * <li>tableid.csv - data table</li>
   * <li>tableid.definition.csv - data table column definition</li>
   * <li>tableid.properties.csv - key-value store of this table</li>
   * </ul>
    *
    * @param exportListener
    * @param db
    * @param tableId
    * @param orderedDefns
    * @param fileQualifier
    * @return
    * @throws RemoteException
    */
  public boolean exportSeparable(ExportListener exportListener, OdkDbHandle db, String tableId,
      OrderedColumns orderedDefns, String fileQualifier) throws RemoteException {
    // building array of columns to select and header row for output file
    // then we are including all the metadata columns.
    ArrayList<String> columns = new ArrayList<String>();

    WebLogger.getLogger(appName).i(
        TAG,
        "exportSeparable: tableId: " + tableId + " fileQualifier: "
            + ((fileQualifier == null) ? "<null>" : fileQualifier));

    // put the user-relevant metadata columns in leftmost columns
    columns.add(DataTableColumns.ID);
    columns.add(DataTableColumns.FORM_ID);
    columns.add(DataTableColumns.LOCALE);
    columns.add(DataTableColumns.SAVEPOINT_TYPE);
    columns.add(DataTableColumns.SAVEPOINT_TIMESTAMP);
    columns.add(DataTableColumns.SAVEPOINT_CREATOR);

    // add the data columns
    for (ColumnDefinition cd : orderedDefns.getColumnDefinitions()) {
      if (cd.isUnitOfRetention()) {
        columns.add(cd.getElementKey());
      }
    }

    // And now add all remaining export columns
    String[] exportColumns = getDatabase().getExportColumns();
    for (String colName : exportColumns) {
      if (columns.contains(colName)) {
        continue;
      }
      columns.add(colName);
    }
    
    File tableInstancesFolder = new File(ODKFileUtils.getInstancesFolder(appName, tableId));
    HashSet<File> instancesWithData = new HashSet<File>();
    if ( tableInstancesFolder.exists() && tableInstancesFolder.isDirectory() ) {
      File[] subDirectories = tableInstancesFolder.listFiles(new FileFilter() {

        @Override
        public boolean accept(File pathname) {
          return pathname.isDirectory() && (pathname.list().length != 0);
        }});
      instancesWithData.addAll(Arrays.asList(subDirectories));
    }

    OutputStreamWriter output = null;
    try {
      // both files go under the output/csv directory...
      File outputCsv = new File(ODKFileUtils.getOutputTableCsvFile(appName, tableId, fileQualifier));
      outputCsv.mkdirs();

      // emit properties files
      File definitionCsv = new File(ODKFileUtils.getOutputTableDefinitionCsvFile(appName, tableId,
          fileQualifier));
      File propertiesCsv = new File(ODKFileUtils.getOutputTablePropertiesCsvFile(appName, tableId,
          fileQualifier));

      if (!writePropertiesCsv(db, tableId, orderedDefns, definitionCsv, propertiesCsv)) {
        return false;
      }

      // getting data
      String whereString = DataTableColumns.SAVEPOINT_TYPE + " IS NOT NULL AND ("
          + DataTableColumns.CONFLICT_TYPE + " IS NULL OR " + DataTableColumns.CONFLICT_TYPE
          + " = " + Integer.toString(ConflictType.LOCAL_UPDATED_UPDATED_VALUES) + ")";

      String[] emptyArray = {};
      UserTable table = getDatabase().rawSqlQuery(appName, db, tableId, orderedDefns,
          whereString, emptyArray, emptyArray, null, null, null);

      // emit data table...
      File file = new File(outputCsv, tableId
          + ((fileQualifier != null && fileQualifier.length() != 0) ? ("." + fileQualifier) : "")
          + ".csv");
      FileOutputStream out = new FileOutputStream(file);
      output = new OutputStreamWriter(out, CharEncoding.UTF_8);
      RFC4180CsvWriter cw = new RFC4180CsvWriter(output);
      // don't have to worry about quotes in elementKeys...
      cw.writeNext(columns.toArray(new String[columns.size()]));
      String[] row = new String[columns.size()];
      for (int i = 0; i < table.getNumberOfRows(); i++) {
        Row dataRow = table.getRowAtIndex(i);
        for (int j = 0; j < columns.size(); ++j) {
          row[j] = dataRow.getRawDataOrMetadataByElementKey(columns.get(j));
          ;
        }
        cw.writeNext(row);
        /**
         * Copy all attachment files into the output directory tree.
         * Don't worry about whether they are referenced in the current
         * row. This is a simplification (and biases toward preserving 
         * data).
         */
        String instanceId = dataRow.getRowId();
        File tableInstanceFolder = new File(ODKFileUtils.getInstanceFolder(appName, tableId, instanceId));
        if ( instancesWithData.contains(tableInstanceFolder) ) {
          File outputInstanceFolder = new File(ODKFileUtils.getOutputCsvInstanceFolder(appName, tableId, instanceId));
          outputInstanceFolder.mkdirs();
          FileUtils.copyDirectory(tableInstanceFolder, outputInstanceFolder);
          instancesWithData.remove(tableInstanceFolder);
        }

      }
      cw.flush();
      cw.close();

      return true;
    } catch (IOException e) {
      return false;
    } finally {
      try {
        output.close();
      } catch (IOException e) {
      }
    }
  }

  /**
   * Writes the definition and properties files for the given tableId. This is
   * written to:
   * <ul>
   * <li>tables/tableId/definition.csv - data table column definition</li>
   * <li>tables/tableId/properties.csv - key-value store of this table</li>
   * </ul>
   * The definition.csv file contains the schema definition. md5hash of it
   * corresponds to the former schemaETag.
   *
   * The properties.csv file contains the table-level metadata (key-value
   * store). The md5hash of it corresponds to the propertiesETag.
   *
   * For use by the sync mechanism.
    *
    * @param db
    * @param tableId
    * @param orderedDefns
    * @return
    * @throws RemoteException
    */
  public boolean writePropertiesCsv(OdkDbHandle db, String tableId,
      OrderedColumns orderedDefns) throws RemoteException {
    File definitionCsv = new File(ODKFileUtils.getTableDefinitionCsvFile(appName, tableId));
    File propertiesCsv = new File(ODKFileUtils.getTablePropertiesCsvFile(appName, tableId));
    return writePropertiesCsv(db, tableId, orderedDefns, definitionCsv, propertiesCsv);
  }

  /**
   * Common routine to write the definition and properties files.
    *
    * @param db
    * @param tableId
    * @param orderedDefns
    * @param definitionCsv
    * @param propertiesCsv
    * @return
    * @throws RemoteException
    */
  private boolean writePropertiesCsv(OdkDbHandle db, String tableId,
      OrderedColumns orderedDefns, File definitionCsv, File propertiesCsv) throws RemoteException {
    WebLogger.getLogger(appName).i(TAG, "writePropertiesCsv: tableId: " + tableId);

    // writing metadata
    FileOutputStream out;
    RFC4180CsvWriter cw;
    OutputStreamWriter output = null;
    try {
      // emit definition.csv table...
      out = new FileOutputStream(definitionCsv);
      output = new OutputStreamWriter(out, CharEncoding.UTF_8);
      cw = new RFC4180CsvWriter(output);

      // Emit ColumnDefinitions

      ArrayList<String> colDefHeaders = new ArrayList<String>();
      colDefHeaders.add(ColumnDefinitionsColumns.ELEMENT_KEY);
      colDefHeaders.add(ColumnDefinitionsColumns.ELEMENT_NAME);
      colDefHeaders.add(ColumnDefinitionsColumns.ELEMENT_TYPE);
      colDefHeaders.add(ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS);

      cw.writeNext(colDefHeaders.toArray(new String[colDefHeaders.size()]));
      String[] colDefRow = new String[colDefHeaders.size()];

      /**
       * Since the md5Hash of the file identifies identical schemas, ensure that
       * the list of columns is in alphabetical order.
       */
      for (ColumnDefinition cd : orderedDefns.getColumnDefinitions()) {
        colDefRow[0] = cd.getElementKey();
        colDefRow[1] = cd.getElementName();
        colDefRow[2] = cd.getElementType();
        colDefRow[3] = cd.getListChildElementKeys();
        cw.writeNext(colDefRow);
      }

      cw.flush();
      cw.close();

      // emit properties.csv...
      out = new FileOutputStream(propertiesCsv);
      output = new OutputStreamWriter(out, CharEncoding.UTF_8);
      cw = new RFC4180CsvWriter(output);

      // Emit KeyValueStore

      ArrayList<String> kvsHeaders = new ArrayList<String>();
      kvsHeaders.add(KeyValueStoreColumns.PARTITION);
      kvsHeaders.add(KeyValueStoreColumns.ASPECT);
      kvsHeaders.add(KeyValueStoreColumns.KEY);
      kvsHeaders.add(KeyValueStoreColumns.VALUE_TYPE);
      kvsHeaders.add(KeyValueStoreColumns.VALUE);

      /**
       * Since the md5Hash of the file identifies identical properties, ensure
       * that the list of KVS entries is in alphabetical order.
       */
      List<KeyValueStoreEntry> kvsEntries = getDatabase().getDBTableMetadata(appName, db, tableId,
          null, null, null);
      Collections.sort(kvsEntries, new Comparator<KeyValueStoreEntry>() {

        @Override
        public int compare(KeyValueStoreEntry lhs, KeyValueStoreEntry rhs) {
          int outcome;
          if (lhs.partition == null && rhs.partition == null) {
            outcome = 0;
          } else if (lhs.partition == null) {
            return -1;
          } else if (rhs.partition == null) {
            return 1;
          } else {
            outcome = lhs.partition.compareTo(rhs.partition);
          }
          if (outcome != 0)
            return outcome;
          if (lhs.aspect == null && rhs.aspect == null) {
            outcome = 0;
          } else if (lhs.aspect == null) {
            return -1;
          } else if (rhs.aspect == null) {
            return 1;
          } else {
            outcome = lhs.aspect.compareTo(rhs.aspect);
          }
          if (outcome != 0)
            return outcome;
          if (lhs.key == null && rhs.key == null) {
            outcome = 0;
          } else if (lhs.key == null) {
            return -1;
          } else if (rhs.key == null) {
            return 1;
          } else {
            outcome = lhs.key.compareTo(rhs.key);
          }
          return outcome;
        }
      });

      cw.writeNext(kvsHeaders.toArray(new String[kvsHeaders.size()]));
      String[] kvsRow = new String[kvsHeaders.size()];
      for (int i = 0; i < kvsEntries.size(); i++) {
        KeyValueStoreEntry entry = kvsEntries.get(i);

        // replace all the choiceList entries with their choiceListJSON
        if ( entry.partition.equals(KeyValueStoreConstants.PARTITION_COLUMN) &&
            entry.key.equals(KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST) ) {
          // exported type is an array -- the choiceListJSON
          entry.type = ElementDataType.array.name();
          if ((entry.value != null) && (entry.value.trim().length() != 0)) {
            String choiceListJSON = getDatabase().getChoiceList(appName, db, entry.value);
            entry.value = choiceListJSON;
          } else {
            entry.value = null;
          }
        }

        kvsRow[0] = entry.partition;
        kvsRow[1] = entry.aspect;
        kvsRow[2] = entry.key;
        kvsRow[3] = entry.type;
        kvsRow[4] = entry.value;
        cw.writeNext(kvsRow);
      }
      cw.flush();
      cw.close();

      return true;
    } catch (IOException e) {
      return false;
    } finally {
      try {
        output.close();
      } catch (IOException e) {
      }
    }
  }

  private int countUpToLastNonNullElement(String[] row) {
    for (int i = row.length - 1; i >= 0; --i) {
      if (row[i] != null) {
        return (i + 1);
      }
    }
    return 0;
  }

  /**
   * Update tableId from
   * <ul>
   * <li>tables/tableId/properties.csv</li>
   * <li>tables/tableId/definition.csv</li>
   * </ul>
   *
   * This will either create a table, or verify that the table structure matches
   * that defined in the csv. It will then override all the KVS entries with
   * those present in the file.
   *
   * @param importListener
   * @param tableId
   * @throws IOException
   * @throws RemoteException 
   */
  public synchronized void updateTablePropertiesFromCsv(ImportListener importListener, String tableId)
      throws IOException, RemoteException {

    WebLogger.getLogger(appName).i(TAG, "updateTablePropertiesFromCsv: tableId: " + tableId);

    List<Column> columns = new ArrayList<Column>();
    List<KeyValueStoreEntry> kvsEntries = new ArrayList<KeyValueStoreEntry>();

    // reading data
    File file = null;
    FileInputStream in = null;
    InputStreamReader input = null;
    RFC4180CsvReader cr = null;
    try {
      file = new File(ODKFileUtils.getTableDefinitionCsvFile(appName, tableId));
      in = new FileInputStream(file);
      input = new InputStreamReader(in, CharEncoding.UTF_8);
      cr = new RFC4180CsvReader(input);

      String[] row;

      // Read ColumnDefinitions
      // get the column headers
      String[] colHeaders = cr.readNext();
      int colHeadersLength = countUpToLastNonNullElement(colHeaders);
      // get the first row
      row = cr.readNext();
      while (row != null && countUpToLastNonNullElement(row) != 0) {

        String elementKeyStr = null;
        String elementNameStr = null;
        String elementTypeStr = null;
        String listChildElementKeysStr = null;
        int rowLength = countUpToLastNonNullElement(row);
        for (int i = 0; i < rowLength; ++i) {
          if (i >= colHeadersLength) {
            throw new IllegalStateException("data beyond header row of ColumnDefinitions table");
          }
          if (ColumnDefinitionsColumns.ELEMENT_KEY.equals(colHeaders[i])) {
            elementKeyStr = row[i];
          }
          if (ColumnDefinitionsColumns.ELEMENT_NAME.equals(colHeaders[i])) {
            elementNameStr = row[i];
          }
          if (ColumnDefinitionsColumns.ELEMENT_TYPE.equals(colHeaders[i])) {
            elementTypeStr = row[i];
          }
          if (ColumnDefinitionsColumns.LIST_CHILD_ELEMENT_KEYS.equals(colHeaders[i])) {
            listChildElementKeysStr = row[i];
          }
        }

        if (elementKeyStr == null || elementTypeStr == null) {
          throw new IllegalStateException("ElementKey and ElementType must be specified");
        }

        columns.add(new Column(elementKeyStr, elementNameStr, elementTypeStr,
            listChildElementKeysStr));

        // get next row or blank to end...
        row = cr.readNext();
      }

      cr.close();
      try {
        input.close();
      } catch (IOException e) {
      }
      try {
        in.close();
      } catch (IOException e) {
      }

      file = new File(ODKFileUtils.getTablePropertiesCsvFile(appName, tableId));
      in = new FileInputStream(file);
      input = new InputStreamReader(in, CharEncoding.UTF_8);
      cr = new RFC4180CsvReader(input);
      // Read KeyValueStore
      // read the column headers
      String[] kvsHeaders = cr.readNext();
      // read the first row
      row = cr.readNext();
      while (row != null && countUpToLastNonNullElement(row) != 0) {
        String partition = null;
        String aspect = null;
        String key = null;
        String type = null;
        String value = null;
        int rowLength = countUpToLastNonNullElement(row);
        for (int i = 0; i < rowLength; ++i) {
          if (KeyValueStoreColumns.PARTITION.equals(kvsHeaders[i])) {
            partition = row[i];
          }
          if (KeyValueStoreColumns.ASPECT.equals(kvsHeaders[i])) {
            aspect = row[i];
          }
          if (KeyValueStoreColumns.KEY.equals(kvsHeaders[i])) {
            key = row[i];
          }
          if (KeyValueStoreColumns.VALUE_TYPE.equals(kvsHeaders[i])) {
            type = row[i];
          }
          if (KeyValueStoreColumns.VALUE.equals(kvsHeaders[i])) {
            value = row[i];
          }
        }
        KeyValueStoreEntry kvsEntry = KeyValueStoreUtils
            .buildEntry(tableId, partition, aspect, key, ElementDataType.valueOf(type), value);
        kvsEntries.add(kvsEntry);
        // get next row or blank to end...
        row = cr.readNext();
      }
      cr.close();
      try {
        input.close();
      } catch (IOException e) {
      }
      try {
        in.close();
      } catch (IOException e) {
      }

    } finally {
      try {
        if (input != null) {
          input.close();
        }
      } catch (IOException e) {
      }
    }

    OdkDbHandle db = null;
    try {

      db = getDatabase().openDatabase(appName);

      // Go through the KVS list and replace all the choiceList entries with their choiceListId
      for ( KeyValueStoreEntry entry : kvsEntries ) {
        if ( entry.partition.equals(KeyValueStoreConstants.PARTITION_COLUMN) &&
             entry.key.equals(KeyValueStoreConstants.COLUMN_DISPLAY_CHOICES_LIST) ) {
          // stored type is a string -- the choiceListId
          entry.type = ElementDataType.string.name();
          if ((entry.value != null) && (entry.value.trim().length() != 0)) {
            String choiceListId = getDatabase().setChoiceList(appName, db, entry.value);
            entry.value = choiceListId;
          } else {
            entry.value = null;
          }
        }
      }

      ColumnList cols = new ColumnList(columns);
      getDatabase().createOrOpenDBTableWithColumnsAndProperties(appName, db, tableId, cols, kvsEntries, true);

    } finally {
      if (db != null) {
        getDatabase().closeDatabase(appName, db);
      }
    }
  }

  /**
   * Imports data from a csv file with elementKey headings. This csv file is
   * assumed to be under:
   * <ul>
   * <li>config/assets/csv/tableId.fileQualifier.csv</li>
   * </ul>
   * If the table does not exist, it attempts to create it using the schema and
   * metadata located here:
   * <ul>
   * <li>tables/tableId/definition.csv - data table definition</li>
   * <li>tables/tableId/properties.csv - key-value store</li>
   * </ul>
   *
   * @param importListener
   * @param tableId
   * @param fileQualifier
   * @param createIfNotPresent
   *          -- true if we should try to create the table.
   * @return
   * @throws RemoteException 
   */
  public boolean importSeparable(ImportListener importListener, String tableId,
      String fileQualifier, boolean createIfNotPresent) throws RemoteException {

    OdkDbHandle db = null;
    try {
      db = getDatabase().openDatabase(appName);
      if (!getDatabase().hasTableId(appName, db, tableId)) {
        if (createIfNotPresent) {
          updateTablePropertiesFromCsv(importListener, tableId);
          if (!getDatabase().hasTableId(appName, db, tableId)) {
            return false;
          }
        } else {
          return false;
        }
      }

      OrderedColumns orderedDefns = getDatabase().getUserDefinedColumns(appName, db, tableId);

      WebLogger.getLogger(appName).i(
          TAG,
          "importSeparable: tableId: " + tableId + " fileQualifier: "
              + ((fileQualifier == null) ? "<null>" : fileQualifier));

      // reading data
      InputStreamReader input = null;
      try {
        
        File assetsCsvInstances = new File(ODKFileUtils.getAssetsCsvInstancesFolder(appName, tableId));
        HashSet<File> instancesHavingData = new HashSet<File>();
        if ( assetsCsvInstances.exists() && assetsCsvInstances.isDirectory() ) {
          File[] subDirectories = assetsCsvInstances.listFiles(new FileFilter() {

            @Override
            public boolean accept(File pathname) {
              return pathname.isDirectory() && (pathname.list().length != 0);
            }});
          instancesHavingData.addAll(Arrays.asList(subDirectories));
        }
        
        // both files are read from config/assets/csv directory...
        File assetsCsv = new File(ODKFileUtils.getAssetsCsvFolder(appName));

        // read data table...
        File file = new File(assetsCsv, tableId
            + ((fileQualifier != null && fileQualifier.length() != 0) ? ("." + fileQualifier) : "")
            + ".csv");
        FileInputStream in = new FileInputStream(file);
        input = new InputStreamReader(in, CharEncoding.UTF_8);
        RFC4180CsvReader cr = new RFC4180CsvReader(input);
        // don't have to worry about quotes in elementKeys...
        String[] columnsInFile = cr.readNext();
        int columnsInFileLength = countUpToLastNonNullElement(columnsInFile);

        String v_id;
        String v_form_id;
        String v_locale;
        String v_savepoint_type;
        String v_savepoint_creator;
        String v_savepoint_timestamp;
        String v_row_etag;
        String v_filter_type;
        String v_filter_value;

        Map<String, String> valueMap = new HashMap<String, String>();

        int rowCount = 0;
        String[] row;
        for (;;) {
          row = cr.readNext();
          rowCount++;
          if (rowCount % 5 == 0) {
            importListener.updateProgressDetail("Row " + rowCount);
          }
          if (row == null || countUpToLastNonNullElement(row) == 0) {
            break;
          }
          int rowLength = countUpToLastNonNullElement(row);

          // default values for metadata columns if not provided
          v_id = UUID.randomUUID().toString();
          v_form_id = null;
          v_locale = ODKCursorUtils.DEFAULT_LOCALE;
          v_savepoint_type = SavepointTypeManipulator.complete();
          v_savepoint_creator = ODKCursorUtils.DEFAULT_CREATOR;
          v_savepoint_timestamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
          v_row_etag = null;
          v_filter_type = null;
          v_filter_value = null;
          // clear value map
          valueMap.clear();

          boolean foundId = false;
          for (int i = 0; i < columnsInFileLength; ++i) {
            if (i > rowLength)
              break;
            String column = columnsInFile[i];
            String tmp = row[i];
            if (DataTableColumns.ID.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                foundId = true;
                v_id = tmp;
              }
              continue;
            }
            if (DataTableColumns.FORM_ID.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_form_id = tmp;
              }
              continue;
            }
            if (DataTableColumns.LOCALE.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_locale = tmp;
              }
              continue;
            }
            if (DataTableColumns.SAVEPOINT_TYPE.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_savepoint_type = tmp;
              }
              continue;
            }
            if (DataTableColumns.SAVEPOINT_CREATOR.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_savepoint_creator = tmp;
              }
              continue;
            }
            if (DataTableColumns.SAVEPOINT_TIMESTAMP.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_savepoint_timestamp = tmp;
              }
              continue;
            }
            if (DataTableColumns.ROW_ETAG.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_row_etag = tmp;
              }
              continue;
            }
            if (DataTableColumns.FILTER_TYPE.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_filter_type = tmp;
              }
              continue;
            }
            if (DataTableColumns.FILTER_VALUE.equals(column)) {
              if (tmp != null && tmp.length() != 0) {
                v_filter_value = tmp;
              }
              continue;
            }
            try {
              orderedDefns.find(column);
              valueMap.put(column, tmp);
            } catch (IllegalArgumentException e) {
              // this is OK --
              // the csv contains an extra column
            }
          }

          // TODO: should resolve this properly when we have conflict rows and
          // uncommitted edits. For now, we just add our csv import to those,
          // rather
          // than resolve the problems.
          UserTable table = getDatabase().getRowsWithId(appName, db,
              tableId, orderedDefns, v_id);
          if (table.getNumberOfRows() > 1) {
            throw new IllegalStateException(
                "There are either checkpoint or conflict rows in the destination table");
          }

          SyncState syncState = null;
          if (foundId && table.getNumberOfRows() == 1) {
            String syncStateStr = table.getRowAtIndex(0).getRawDataOrMetadataByElementKey(
                DataTableColumns.SYNC_STATE);
            if (syncStateStr == null) {
              throw new IllegalStateException("Unexpected null syncState value");
            }
            syncState = SyncState.valueOf(syncStateStr);
          }

          /**
           * Insertion will set the SYNC_STATE to new_row.
           *
           * If the table is sync'd to the server, this will cause one sync
           * interaction with the server to confirm that the server also has
           * this record.
           *
           * If a record with this same rowId already exists, if it is in an
           * new_row sync state, we update it here. Otherwise, if there were any
           * local changes, we leave the row unchanged.
           */
          if (syncState != null) {

            ContentValues cv = new ContentValues();
            if (v_id != null) {
              cv.put(DataTableColumns.ID, v_id);
            }
            for (String column : valueMap.keySet()) {
              if (column != null) {
                cv.put(column, valueMap.get(column));
              }
            }

            // The admin columns get added here
            cv.put(DataTableColumns.FORM_ID, v_form_id);
            cv.put(DataTableColumns.LOCALE, v_locale);
            cv.put(DataTableColumns.SAVEPOINT_TYPE, v_savepoint_type);
            cv.put(DataTableColumns.SAVEPOINT_TIMESTAMP, v_savepoint_timestamp);
            cv.put(DataTableColumns.SAVEPOINT_CREATOR, v_savepoint_creator);
            cv.put(DataTableColumns.ROW_ETAG, v_row_etag);
            cv.put(DataTableColumns.FILTER_TYPE, v_filter_type);
            cv.put(DataTableColumns.FILTER_VALUE, v_filter_value);

            cv.put(DataTableColumns.SYNC_STATE, SyncState.new_row.name());

            if (syncState == SyncState.new_row) {
              // we do the actual update here
              getDatabase().updateRowWithId(appName, db, tableId, orderedDefns,
                  cv, v_id);
            }
            // otherwise, do NOT update the row.

          } else {
            ContentValues cv = new ContentValues();
            for (String column : valueMap.keySet()) {
              if (column != null) {
                cv.put(column, valueMap.get(column));
              }
            }

            if (v_id == null) {
              v_id = ODKDataUtils.genUUID();
            }

            // The admin columns get added here
            cv.put(DataTableColumns.FORM_ID, v_form_id);
            cv.put(DataTableColumns.LOCALE, v_locale);
            cv.put(DataTableColumns.SAVEPOINT_TYPE, v_savepoint_type);
            cv.put(DataTableColumns.SAVEPOINT_TIMESTAMP, v_savepoint_timestamp);
            cv.put(DataTableColumns.SAVEPOINT_CREATOR, v_savepoint_creator);
            cv.put(DataTableColumns.ROW_ETAG, v_row_etag);
            cv.put(DataTableColumns.FILTER_TYPE, v_filter_type);
            cv.put(DataTableColumns.FILTER_VALUE, v_filter_value);

            cv.put(DataTableColumns.ID, v_id);

            getDatabase().insertRowWithId(appName, db, tableId, orderedDefns,
                cv, v_id);
          }
          
          /**
           * Copy all attachment files into the destination row.
           * Don't worry about whether they are present in the current
           * row. This is a simplification.
           */
          File assetsInstanceFolder = new File(ODKFileUtils.getAssetsCsvInstanceFolder(appName, tableId, v_id));
          if ( instancesHavingData.contains(assetsInstanceFolder) ) {
            File tableInstanceFolder = new File(ODKFileUtils.getInstanceFolder(appName, tableId, v_id));
            tableInstanceFolder.mkdirs();
            FileUtils.copyDirectory(assetsInstanceFolder, tableInstanceFolder);
            instancesHavingData.remove(assetsInstanceFolder);
          }

        }
        cr.close();
        return true;
      } catch (IOException e) {
        return false;
      } finally {
        try {
          input.close();
        } catch (IOException e) {
        }
      }
    } catch (IOException e) {
      return false;
    } finally {
      if (db != null) {
        getDatabase().closeDatabase(appName, db);
      }
    }
  }

}
