/*
 * Copyright (C) 2012-2014 University of Washington
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
package org.opendatakit.common.android.utilities;

/**
 * 
 * @author sudar.sam@gmail.com
 *
 */
public final class LocalKeyValueStoreConstants {
  
  public static class Graph {
    public static final String PARTITION = "GraphDisplayActivity";
    public static final String PARTITION_VIEWS = PARTITION + ".views";
    public static final String KEY_GRAPH_VIEW_NAME = "nameOfGraphView";
    public static final String KEY_GRAPH_TYPE = "graphtype";
  }
  
  public static final class Map {
    public static final String PARTITION = "TableMapFragment";
    /** The key to grab which column is being used for latitude. */
    public static final String KEY_MAP_LAT_COL = "keyMapLatCol";
    /** The key to grab which column is being used for longitude. */
    public static final String KEY_MAP_LONG_COL = "keyMapLongCol";
    /** The key to grab which file is being used for the list view. */
    public static final String KEY_FILENAME = "keyFilename";
  }
  
  public static final class Spreadsheet {
    public static final String KEY_COLUMN_WIDTH =
        "SpreadsheetView.columnWidth";
    public static final String PARTITION = "SpreadsheetView";
  }

  /**
   * Constants needed to use the key value store with list views.
   * @author sudar.sam@gmail.com
   *
   */
  public static final class ListViews {
    /**
     * The general partition in which table-wide ListDisplayActivity information
     * is stored. An example might be the current list view for a table.
     */
    public static final String PARTITION = "ListDisplayActivity";
    /**
     * The partition under which actual individual view information is stored. For
     * instance if a user added a list view named "Doctor", the partition would be
     * KVS_PARTITION_VIEWS, and all the keys relating to this view would fall
     * within this partition and a particular aspect. (Perhaps the name "Doctor"?)
     */
    public static final String PARTITION_VIEWS = PARTITION + ".views";
    /**
     * This key holds the filename associated with the view.
     */
    public static final String KEY_FILENAME = "filename";
    /**
     * This key holds the name of the list view. In the default aspect the idea is
     * that this will then give the value of the aspect for which the default list
     * view is set.
     * <p>
     * E.g. partition=KVS_PARTITION, aspect=KVS_ASPECT_DEFAULT,
     * key="KEY_LIST_VIEW_NAME", value="My Custom List View" would mean that
     * "My Custom List View" was an aspect under the KVS_PARTITION_VIEWS partition
     * that had the information regarding a custom list view.
     */
    public static final String KEY_LIST_VIEW_NAME = "nameOfListView";
  };
  
  public static final class Tables {

    /** The file name for the list view that has been set on the table. */
    public static final String KEY_LIST_VIEW_FILE_NAME = "listViewFileName";

    /** The file name for the detail view that has been set on the table. */
    public static final String KEY_DETAIL_VIEW_FILE_NAME = "detailViewFileName";

    /** The file name for the list view that is displayed in the map. */
    public static final String KEY_MAP_LIST_VIEW_FILE_NAME = "mapListViewFileName";

  }

}
