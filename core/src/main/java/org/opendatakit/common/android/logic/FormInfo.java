/*
 * Copyright (C) 2012-2013 University of Washington
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

package org.opendatakit.common.android.logic;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.opendatakit.androidlibrary.R;
import org.opendatakit.common.android.provider.FormsColumns;
import org.opendatakit.common.android.utilities.ODKCursorUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.content.Context;
import android.database.Cursor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * Class to hold information about a form. This holds the data fields that are
 * available in the Forms database as well as, if requested, the parsed formDef
 * object (via Jackson).
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class FormInfo {

  private static final String FORMDEF_TITLE_ELEMENT = "title";
  private static final String FORMDEF_DISPLAY_ELEMENT = "display";
  private static final String FORMDEF_SURVEY_SETTINGS = "survey";
  private static final String FORMDEF_SETTINGS_SUBSECTION = "settings";
  private static final String FORMDEF_SPECIFICATION_SECTION = "specification";

  public final long fileLength;
  public final long lastModificationDate;
  public final String settings;
  public final String formId;
  public final String formVersion;
  public final String appName;
  public final String tableId;
  public final String formTitle;
  public final String displaySubtext;
  public final String defaultLocale; // default locale
  public final String instanceName;  // column containing instance name for display

  // formDef.json file...
  public final File formDefFile;
  // the entire formDef, parsed using Jackson...
  public final HashMap<String, Object> formDef;

  static final String FORMDEF_VALUE = "value";

  static final String FORMDEF_DEFAULT_LOCALE = "_default_locale";

  static final String FORMDEF_INSTANCE_NAME = "instance_name";

  static final String FORMDEF_FORM_TITLE = "form_title";

  static final String FORMDEF_TABLE_ID = "table_id";

  static final String FORMDEF_FORM_VERSION = "form_version";

  static final String FORMDEF_FORM_ID = "form_id";

  /**
   * Return an array of string values. Useful for passing as selectionArgs to
   * SQLite. Or for iterating over and populating a ContentValues array.
   *
   * @param projection
   *          -- array of FormsColumns names to declare values of
   * @return
   */
  public String[] asRowValues(String[] projection) {

    if (projection == null) {
      projection = FormsColumns.formsDataColumnNames;
    }

    String[] ret = new String[projection.length];
    for (int i = 0; i < projection.length; ++i) {
      String s = projection[i];

      if (FormsColumns.DISPLAY_NAME.equals(s)) {
        ret[i] = formTitle;
      } else if (FormsColumns.DISPLAY_SUBTEXT.equals(s)) {
        ret[i] = displaySubtext;
      } else if (FormsColumns.TABLE_ID.equals(s)) {
        ret[i] = tableId;
      } else if (FormsColumns.FORM_ID.equals(s)) {
        ret[i] = formId;
      } else if (FormsColumns.FORM_VERSION.equals(s)) {
        ret[i] = formVersion;
      } else if (FormsColumns.JSON_MD5_HASH.equals(s)) {
        ret[i] = "-placeholder-"; // removed by FormsProvider
      } else if (FormsColumns.DATE.equals(s)) {
        ret[i] = Long.toString(lastModificationDate);
      } else if (FormsColumns.FILE_LENGTH.equals(s)) {
        ret[i] = Long.toString(fileLength);
      } else if (FormsColumns.SETTINGS.equals(s)) {
        ret[i] = settings;
      } else if (FormsColumns.DEFAULT_FORM_LOCALE.equals(s)) {
        ret[i] = defaultLocale;
      } else if (FormsColumns.INSTANCE_NAME.equals(s)) {
        ret[i] = instanceName;
      }
    }
    return ret;
  }

  /**
   * Given a Cursor pointing at a valid Forms database row, extract the values
   * from that cursor. If parseFormDef is true, read and parse the formDef.json
   * file.
   *
   * @param c
   *          -- cursor pointing at a valid Forms database row.
   * @param parseFormDef
   *          -- true if the formDef.json file should be opened.
   */
  @SuppressWarnings("unchecked")
  public FormInfo(String appName, Cursor c, boolean parseFormDef) {
    this.appName = appName;

    lastModificationDate = ODKCursorUtils.getIndexAsType(c, Long.class, c.getColumnIndex(FormsColumns.DATE));
    fileLength = ODKCursorUtils.getIndexAsType(c, Long.class, c.getColumnIndex(FormsColumns.FILE_LENGTH));
    tableId = ODKCursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.TABLE_ID));
    formId = ODKCursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.FORM_ID));
    settings = ODKCursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.SETTINGS));
    formVersion = ODKCursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.FORM_VERSION));
    formTitle = ODKCursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.DISPLAY_NAME));
    displaySubtext = ODKCursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.DISPLAY_SUBTEXT));
    defaultLocale = ODKCursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.DEFAULT_FORM_LOCALE));
    instanceName = ODKCursorUtils.getIndexAsString(c, c.getColumnIndex(FormsColumns.INSTANCE_NAME));

    File formFolder = new File( ODKFileUtils.getFormFolder(appName, tableId, formId) );
    formDefFile = new File( formFolder, ODKFileUtils.FORMDEF_JSON_FILENAME);

    if (parseFormDef && !formDefFile.exists()) {
      throw new IllegalArgumentException("File does not exist! " + formDefFile.getAbsolutePath());
    }

    if (!parseFormDef) {
      formDef = null;
    } else {

      // OK -- parse the formDef file.
      HashMap<String, Object> om = null;
      try {
        om = ODKFileUtils.mapper.readValue(formDefFile, HashMap.class);
      } catch (JsonParseException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      formDef = om;
      if (formDef == null) {
        throw new IllegalArgumentException("File is not a json file! "
            + formDefFile.getAbsolutePath());
      }
    }

  }

  /**
   *
   * @param context
   * @param appName
   * @param formDefFile
   */
  @SuppressWarnings("unchecked")
  public FormInfo(Context c, String appName, File formDefFile) {

    // save the appName
    this.appName = appName;
    // save the File of the formDef...
    this.formDefFile = formDefFile;

    /**
     * IMPORTANT: called for its side-effect
     *  -- throws IllegalArgumentException if file is not under appName
     */ 
    ODKFileUtils.getRelativeFormPath(appName, formDefFile);
    
    // OK -- parse the formDef file.
    HashMap<String, Object> om = null;
    try {
      om = ODKFileUtils.mapper.readValue(formDefFile, HashMap.class);
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    formDef = om;
    if (formDef == null) {
      throw new IllegalArgumentException("File is not a json file! "
          + formDefFile.getAbsolutePath());
    }

    // /////////////////////////////////////////////////
    // TODO: DEPENDENCY ALERT!!!
    // TODO: DEPENDENCY ALERT!!!
    // TODO: DEPENDENCY ALERT!!!
    // TODO: DEPENDENCY ALERT!!!
    // THIS ASSUMES A CERTAIN STRUCTURE FOR THE formDef.json
    // file...
    Map<String, Object> specification = (Map<String, Object>) formDef
            .get(FORMDEF_SPECIFICATION_SECTION);
    if (specification == null) {
        throw new IllegalArgumentException("File is not a formdef json file! No specification element."
            + formDefFile.getAbsolutePath());
      }

    Map<String, Object> settings = (Map<String, Object>) specification
        .get(FORMDEF_SETTINGS_SUBSECTION);
    if (settings == null) {
      throw new IllegalArgumentException("File is not a formdef json file! No settings section inside specification element."
          + formDefFile.getAbsolutePath());
    }
    
    try {
      this.settings = ODKFileUtils.mapper.writeValueAsString(settings);
    } catch (JsonProcessingException ex) {
      ex.printStackTrace();
      throw new IllegalArgumentException("Settings could not be re-serialized!");
    }
    
    Map<String, Object> setting = null;

    setting = (Map<String, Object>) settings.get(FORMDEF_FORM_ID);
    if (setting != null) {
      Object o = setting.get(FORMDEF_VALUE);
      if (o == null || !(o instanceof String)) {
        throw new IllegalArgumentException(
            "formId is not specified or invalid in the formdef json file! "
                + formDefFile.getAbsolutePath());
      }
      formId = (String) o;
    } else {
      throw new IllegalArgumentException("formId is not specified in the formdef json file! "
          + formDefFile.getAbsolutePath());
    }

    // formDef.json should always have a _default_locale entry.
    setting = (Map<String, Object>) settings.get(FORMDEF_DEFAULT_LOCALE);
    if (setting != null) {
      Object o = setting.get(FORMDEF_VALUE);
      if (o instanceof String) {
        defaultLocale = (String) o;
      } else {
        throw new IllegalArgumentException(FORMDEF_DEFAULT_LOCALE + " is invalid in the formdef json file! "
            + formDefFile.getAbsolutePath());
      }
    } else {
      throw new IllegalArgumentException(FORMDEF_DEFAULT_LOCALE + " is invalid in the formdef json file! "
          + formDefFile.getAbsolutePath());
    }

    Map<String, Object> formDefStruct = null;
    setting = (Map<String, Object>) settings.get(FORMDEF_SURVEY_SETTINGS);
    if ( setting != null ) {
      setting = (Map<String, Object>) setting.get(FORMDEF_DISPLAY_ELEMENT);
      if ( setting != null ) {
        Object o = setting.get(FORMDEF_TITLE_ELEMENT);
        if (o == null) {
	      throw new IllegalArgumentException("title is not specified in the display section of the survey settings of the formdef json file! "
	          + formDefFile.getAbsolutePath());
	    }
	    if (o instanceof String) {
	      formTitle = (String) o;
	    } else {
	      try {
	        formDefStruct = (Map<String, Object>) o;

	        if (formDefStruct == null || formDefStruct.size() == 0) {
	          throw new IllegalArgumentException(
	              "title is not specified in the display section of the survey settings of the formdef json file! "
	                  + formDefFile.getAbsolutePath());
	        }

	        // just get the one title string from the file...
	        formTitle = (String) formDefStruct.get(defaultLocale);
	      } catch (ClassCastException e) {
	        e.printStackTrace();
	        throw new IllegalArgumentException("formTitle is invalid in the formdef json file! "
	            + formDefFile.getAbsolutePath());
	      }
	    }
      } else {
	    throw new IllegalArgumentException("display entry is not specified in the survey section of the settings of formdef json file! "
	            + formDefFile.getAbsolutePath());
	  }
	} else {
	    throw new IllegalArgumentException("survey entry is not specified in the settings of formdef json file! "
	            + formDefFile.getAbsolutePath());
	}

    setting = (Map<String, Object>) settings.get(FORMDEF_INSTANCE_NAME);
    if (setting != null) {
      Object o = setting.get(FORMDEF_VALUE);
      if (o == null) {
        instanceName = null;
      } else if (o instanceof String) {
        instanceName = (String) o;
      } else {
        instanceName = o.toString();
      }
    } else {
      instanceName = null;
    }

    setting = (Map<String, Object>) settings.get(FORMDEF_FORM_VERSION);
    if (setting != null) {
      Object o = setting.get(FORMDEF_VALUE);
      if (o == null) {
        formVersion = null;
      } else if (o instanceof String) {
        formVersion = (String) o;
      } else {
        formVersion = o.toString();
      }
    } else {
      formVersion = null;
    }

    setting = (Map<String, Object>) settings.get(FORMDEF_TABLE_ID);
    if (setting != null) {
      Object o = setting.get(FORMDEF_VALUE);
      if (o == null) {
        tableId = formId;
      } else if (o instanceof String) {
        tableId = (String) o;
      } else {
        tableId = o.toString();
      }
    } else {
      tableId = formId;
    }

    lastModificationDate = formDefFile.lastModified();
    fileLength = formDefFile.length();
    
    String ts = new SimpleDateFormat(c.getString(R.string.added_on_date_at_time),
        Locale.getDefault()).format(lastModificationDate);
    displaySubtext = ts;
  }

}