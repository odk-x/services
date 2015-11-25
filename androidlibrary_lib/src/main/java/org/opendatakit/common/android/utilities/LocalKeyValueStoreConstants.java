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

  public static final class TableColorRules {
    public static final String PARTITION = "TableColorRuleGroup";
    // Aspect: Default
    // KEYs

    // The row color rule
    public static final String KEY_COLOR_RULES_ROW = "TableColorRuleGroup.ruleList";

    // The status column color rule
    public static final String KEY_COLOR_RULES_STATUS_COLUMN = "StatusColumn.ruleList";

    // NOTE: The other metadata columns do not support color rules

    // Type: array
  }

  public static final class ColumnColorRules {
    public static final String PARTITION = "ColumnColorRuleGroup";
    // Aspect: elementKey of column
    // KEYs
    public static final String KEY_COLOR_RULES_COLUMN = "ColumnColorRuleGroup.ruleList";
    // Type: array
  }

  public static final class Map {
    public static final String PARTITION = "TableMapFragment";
    // Aspect should be the view name eventually (e.g., "Map List View 1")
    // Aspect: KeyValueStoreConstants.ASPECT_DEFAULT

    /** NOTE: The filename is under the Tables subclass */

    /** The key to grab which column is being used for latitude. */
    public static final String KEY_MAP_LAT_COL = "keyMapLatCol";

    /** The key to grab which column is being used for longitude. */
    public static final String KEY_MAP_LONG_COL = "keyMapLongCol";

    /** The key for the type of color rule to use on the map. */
    public static final String KEY_COLOR_RULE_TYPE = "keyColorRuleType";
    /**
     * The key for, if the color rule is based off of a column, the column to use.
     */
    public static final String KEY_COLOR_RULE_COLUMN = "keyColorRuleColumn";

    /** The constant if we want no color rules. */
    public static final String COLOR_TYPE_NONE = "None";
    /** The constant if we want the color rules based off of the table. */
    public static final String COLOR_TYPE_TABLE = "Table Color Rules";
    /** The constant if we want the color rules based off of the status column. */
    public static final String COLOR_TYPE_STATUS = "Status Column Color Rules";
    /** The constant if we want the color rules based off of a column. */
    public static final String COLOR_TYPE_COLUMN = "Selectable Column Color Rules";

    // anything for detail view?
  }
  
  public static final class Spreadsheet {
    public static final String PARTITION = "SpreadsheetView";
    // Aspect: elementKey of column
    // KEYs
    public static final String KEY_COLUMN_WIDTH = "SpreadsheetView.columnWidth";
    public static final String KEY_FONT_SIZE = "fontSize";

    public static final int DEFAULT_COL_WIDTH = 125;
    public static final int MAX_COL_WIDTH = 1000;
  }

  public static final class Tables {
    /** The default view type for this table */
    public static final String TABLE_DEFAULT_VIEW_TYPE = "defaultViewType";

    /** The file name for the list view that has been set on the table. */
    public static final String KEY_LIST_VIEW_FILE_NAME = "listViewFileName";

    /** The file name for the detail view that has been set on the table. */
    public static final String KEY_DETAIL_VIEW_FILE_NAME = "detailViewFileName";

    /** The file name for the list view that is displayed in the map. */
    public static final String KEY_MAP_LIST_VIEW_FILE_NAME = "mapListViewFileName";

  }

}
