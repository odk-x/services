/*
 * Copyright (C) 2013 University of Washington
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.TreeSet;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Methods for dealing with naming conventions.
 * @author sudar.sam@gmail.com
 *
 */
public class NameUtil {

  private static final String TAG = NameUtil.class.getSimpleName();

  /**
   * Because Android content provider internals do not quote the
   * column field names when constructing SQLite queries, we need
   * to either prevent the user from using SQLite keywords or code
   * up our own mapping code for Android. Rather than bloat our
   * code, we restrict the set of keywords the user can use.
   *
   * To this list, we add our metadata element names. This further
   * simplifies references to these fields, as we can just consider
   * them to be hidden during display and non-modifiable by the
   * UI, but accessible by the end user (though perhaps not mutable).
   *
   * Fortunately, the server code properly quotes all column and
   * table names, so we only have to track the SQLite reserved names
   * and not all MySQL or PostgreSQL reserved names.
   */
  private static final ArrayList<String> reservedNamesSortedList;

  private static final Pattern letterFirstPattern;


  static {
    /**
     * This pattern does not support (?U) (UNICODE_CHARACTER_CLASS)
     */
    letterFirstPattern = Pattern.compile("^\\p{L}\\p{M}*(\\p{L}\\p{M}*|\\p{Nd}|_)*$",
                                            Pattern.UNICODE_CASE);

    ArrayList<String> reservedNames = new ArrayList<String>();

    /**
     * ODK Metadata reserved names
     */
    reservedNames.add("ROW_ETAG");
    reservedNames.add("SYNC_STATE");
    reservedNames.add("CONFLICT_TYPE");
    reservedNames.add("SAVEPOINT_TIMESTAMP");
    reservedNames.add("SAVEPOINT_CREATOR");
    reservedNames.add("SAVEPOINT_TYPE");
    reservedNames.add("FILTER_TYPE");
    reservedNames.add("FILTER_VALUE");
    reservedNames.add("FORM_ID");
    reservedNames.add("LOCALE");

    /**
     * SQLite keywords ( http://www.sqlite.org/lang_keywords.html )
     */
    reservedNames.add("ABORT");
    reservedNames.add("ACTION");
    reservedNames.add("ADD");
    reservedNames.add("AFTER");
    reservedNames.add("ALL");
    reservedNames.add("ALTER");
    reservedNames.add("ANALYZE");
    reservedNames.add("AND");
    reservedNames.add("AS");
    reservedNames.add("ASC");
    reservedNames.add("ATTACH");
    reservedNames.add("AUTOINCREMENT");
    reservedNames.add("BEFORE");
    reservedNames.add("BEGIN");
    reservedNames.add("BETWEEN");
    reservedNames.add("BY");
    reservedNames.add("CASCADE");
    reservedNames.add("CASE");
    reservedNames.add("CAST");
    reservedNames.add("CHECK");
    reservedNames.add("COLLATE");
    reservedNames.add("COLUMN");
    reservedNames.add("COMMIT");
    reservedNames.add("CONFLICT");
    reservedNames.add("CONSTRAINT");
    reservedNames.add("CREATE");
    reservedNames.add("CROSS");
    reservedNames.add("CURRENT_DATE");
    reservedNames.add("CURRENT_TIME");
    reservedNames.add("CURRENT_TIMESTAMP");
    reservedNames.add("DATABASE");
    reservedNames.add("DEFAULT");
    reservedNames.add("DEFERRABLE");
    reservedNames.add("DEFERRED");
    reservedNames.add("DELETE");
    reservedNames.add("DESC");
    reservedNames.add("DETACH");
    reservedNames.add("DISTINCT");
    reservedNames.add("DROP");
    reservedNames.add("EACH");
    reservedNames.add("ELSE");
    reservedNames.add("END");
    reservedNames.add("ESCAPE");
    reservedNames.add("EXCEPT");
    reservedNames.add("EXCLUSIVE");
    reservedNames.add("EXISTS");
    reservedNames.add("EXPLAIN");
    reservedNames.add("FAIL");
    reservedNames.add("FOR");
    reservedNames.add("FOREIGN");
    reservedNames.add("FROM");
    reservedNames.add("FULL");
    reservedNames.add("GLOB");
    reservedNames.add("GROUP");
    reservedNames.add("HAVING");
    reservedNames.add("IF");
    reservedNames.add("IGNORE");
    reservedNames.add("IMMEDIATE");
    reservedNames.add("IN");
    reservedNames.add("INDEX");
    reservedNames.add("INDEXED");
    reservedNames.add("INITIALLY");
    reservedNames.add("INNER");
    reservedNames.add("INSERT");
    reservedNames.add("INSTEAD");
    reservedNames.add("INTERSECT");
    reservedNames.add("INTO");
    reservedNames.add("IS");
    reservedNames.add("ISNULL");
    reservedNames.add("JOIN");
    reservedNames.add("KEY");
    reservedNames.add("LEFT");
    reservedNames.add("LIKE");
    reservedNames.add("LIMIT");
    reservedNames.add("MATCH");
    reservedNames.add("NATURAL");
    reservedNames.add("NO");
    reservedNames.add("NOT");
    reservedNames.add("NOTNULL");
    reservedNames.add("NULL");
    reservedNames.add("OF");
    reservedNames.add("OFFSET");
    reservedNames.add("ON");
    reservedNames.add("OR");
    reservedNames.add("ORDER");
    reservedNames.add("OUTER");
    reservedNames.add("PLAN");
    reservedNames.add("PRAGMA");
    reservedNames.add("PRIMARY");
    reservedNames.add("QUERY");
    reservedNames.add("RAISE");
    reservedNames.add("REFERENCES");
    reservedNames.add("REGEXP");
    reservedNames.add("REINDEX");
    reservedNames.add("RELEASE");
    reservedNames.add("RENAME");
    reservedNames.add("REPLACE");
    reservedNames.add("RESTRICT");
    reservedNames.add("RIGHT");
    reservedNames.add("ROLLBACK");
    reservedNames.add("ROW");
    reservedNames.add("SAVEPOINT");
    reservedNames.add("SELECT");
    reservedNames.add("SET");
    reservedNames.add("TABLE");
    reservedNames.add("TEMP");
    reservedNames.add("TEMPORARY");
    reservedNames.add("THEN");
    reservedNames.add("TO");
    reservedNames.add("TRANSACTION");
    reservedNames.add("TRIGGER");
    reservedNames.add("UNION");
    reservedNames.add("UNIQUE");
    reservedNames.add("UPDATE");
    reservedNames.add("USING");
    reservedNames.add("VACUUM");
    reservedNames.add("VALUES");
    reservedNames.add("VIEW");
    reservedNames.add("VIRTUAL");
    reservedNames.add("WHEN");
    reservedNames.add("WHERE");

    Collections.sort(reservedNames);

    reservedNamesSortedList = reservedNames;
  }

  /**
   * Determines whether or not the given name is valid for a user-defined
   * entity in the database. Valid names are determined to not begin with a
   * single underscore, not to begin with a digit, and to contain only unicode
   * appropriate word characters.
   * @param name
   * @return true if valid else false
   */
  public static boolean isValidUserDefinedDatabaseName(String name) {
    boolean matchHit = letterFirstPattern.matcher(name).matches();
    // TODO: uppercase is bad...
    boolean reserveHit = Collections.binarySearch(reservedNamesSortedList,
        name.toUpperCase(Locale.US)) >= 0;
    return (!reserveHit && matchHit);
  }

  public static String constructSimpleDisplayName(String name) {
    String displayName = name.replaceAll("_", " ");
    if ( displayName.startsWith(" ") ) {
      displayName = "_" + displayName;
    }
    if ( displayName.endsWith(" ") ) {
      displayName = displayName + "_";
    }
    return displayName;
  }
  
  public static String normalizeDisplayName(String displayName) {
    if ((displayName.startsWith("\"") && displayName.endsWith("\""))
        || (displayName.startsWith("{") && displayName.endsWith("}"))) {
      return displayName;
    } else {
      try {
        return ODKFileUtils.mapper.writeValueAsString(displayName);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
        throw new IllegalArgumentException("normalizeDisplayName: Invalid displayName " + displayName);
      }
    }
  }
}
