/*
 * Copyright (C) 2011 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
** Modified to support SQLite extensions by the SQLite developers: 
** sqlite-dev@sqlite.org.
*/

package org.sqlite.database.sqlite;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Describes how to configure a database.
 * <p>
 * The purpose of this object is to keep track of all of the little
 * configuration settings that are applied to a database before it
 * is opened.</p><p>
 * Some of these settings should be consistent across all
 * connections opened to the database and in WAL access, should
 * not be changed.</p>
 *
 * @hide
 */
public final class SQLiteDatabaseConfiguration {
    // The pattern we use to strip email addresses from database paths
    // when constructing a label to use in log messages.
    private static final Pattern EMAIL_IN_DB_PATTERN =
            Pattern.compile("[\\w\\.\\-]+@[\\w\\.\\-]+");

  /**
   * Absolute max value that can be set by {@link #setMaxSqlCacheSize(int)}.
   *
   * Each prepared-statement is between 1K - 6K, depending on the complexity of the
   * SQL statement & schema.  A large SQL cache may use a significant amount of memory.
   */
  private static final int MAX_SQL_CACHE_SIZE = 100;

  /**
   * ODK appName
   */
    public final String appName;

    /**
     * The database path.
     */
    public final String path;

    /**
     * The label to use to describe the database when it appears in logs.
     * This is derived from the path but is stripped to remove PII.
     */
    public final String label;

    /**
     * The flags used to open the database.
     */
    public int openFlags;

    /**
     * The maximum size of the prepared statement cache for each database connection.
     * Must be non-negative.
     *
     * Default is 25.
     */
    public int maxSqlCacheSize;

    /**
     * The database locale.
     *
     * Default is the value returned by {@link Locale#getDefault()}.
     */
    private Locale locale;

    /**
     * True if foreign key constraints are enabled.
     *
     * Default is false.
     */
    public boolean foreignKeyConstraintsEnabled;

    /**
     * Creates a database configuration with the required parameters for opening a
     * database and default values for all other parameters.
     *
     * @param path The database path.
     * @param openFlags Open flags for the database
     */
    public SQLiteDatabaseConfiguration(String appName, String path, int openFlags, String label) {
      if (appName == null) {
        throw new IllegalArgumentException("appName must not be null.");
      }

      if (path == null) {
        throw new IllegalArgumentException("path must not be null.");
      }

      this.appName = appName;
      this.path = path;
      if ( label == null ) {
        this.label = path;
      } else {
        this.label = label;
      }
      this.openFlags = openFlags;

      // Set default values for optional parameters.
      maxSqlCacheSize = 25;
      locale = Locale.getDefault();
    }

    /**
     * Creates a database configuration as a copy of another configuration.
     *
     * @param other The other configuration.
     */
    public SQLiteDatabaseConfiguration(SQLiteDatabaseConfiguration other) {
        if (other == null) {
            throw new IllegalArgumentException("other must not be null.");
        }

        this.appName = other.appName;
        this.path = other.path;
        this.label = other.label;
        updateParametersFrom(other);
    }

  /**
   * Sets whether foreign key constraints are enabled for the database.
   * <p>
   * By default, foreign key constraints are not enforced by the database.
   * This method allows an application to enable foreign key constraints.
   * It must be called before the database is opened to ensure that foreign
   * key constraints are enabled for the session.
   * </p><p>
   * When foreign key constraints are disabled, the database does not check whether
   * changes to the database will violate foreign key constraints.  Likewise, when
   * foreign key constraints are disabled, the database will not execute cascade
   * delete or update triggers.  As a result, it is possible for the database
   * state to become inconsistent.  To perform a database integrity check,
   * call SQLiteDatabase.isDatabaseIntegrityOk().
   * </p><p>
   * This method must not be called while a transaction is in progress.
   * </p><p>
   * See also <a href="http://sqlite.org/foreignkeys.html">SQLite Foreign Key Constraints</a>
   * for more details about foreign key constraint support.
   * </p>
   *
   * @param enable True to enable foreign key constraints, false to disable them.
   *
   * @throws IllegalStateException if the are transactions is in progress
   * when this method is called.
   */
  public void setForeignKeyConstraintsEnabled(boolean enable) {
    this.foreignKeyConstraintsEnabled = enable;
  }

  /**
   * Sets the maximum size of the prepared-statement cache for this database.
   * (size of the cache = number of compiled-sql-statements stored in the cache).
   *<p>
   * Maximum cache size can ONLY be increased from its current size (default = 10).
   * If this method is called with smaller size than the current maximum value,
   * then IllegalStateException is thrown.
   *<p>
   * This method is thread-safe.
   *
   * @param cacheSize the size of the cache. can be (0 to {@link #MAX_SQL_CACHE_SIZE})
   * @throws IllegalStateException if input cacheSize > {@link #MAX_SQL_CACHE_SIZE}.
   */
  public void setMaxSqlCacheSize(int cacheSize) {
    if (cacheSize > MAX_SQL_CACHE_SIZE || cacheSize < 0) {
      throw new IllegalStateException(
              "expected value between 0 and " + MAX_SQL_CACHE_SIZE);
    }
    this.maxSqlCacheSize = cacheSize;
  }

  /**
   * Sets the locale for this database.  Does nothing if this database has
   * the NO_LOCALIZED_COLLATORS flag set or was opened read only.
   *
   * @param locale The new locale.  Cannot be null.
   */
  public void setLocale(Locale locale) {
    if (locale == null) {
      throw new IllegalArgumentException("locale must not be null.");
    }
    this.locale = locale;
  }

    /**
     * Updates the non-immutable parameters of this configuration object
     * from the other configuration object.
     *
     * @param other The object from which to copy the parameters.
     */
    private void updateParametersFrom(SQLiteDatabaseConfiguration other) {
        if (other == null) {
            throw new IllegalArgumentException("other must not be null.");
        }
        if (!path.equals(other.path)) {
            throw new IllegalArgumentException("other configuration must refer to "
                    + "the same database.");
        }

        openFlags = other.openFlags;
        maxSqlCacheSize = other.maxSqlCacheSize;
        locale = other.locale;
        foreignKeyConstraintsEnabled = other.foreignKeyConstraintsEnabled;
    }

    private static String stripPathForLogs(String path) {
        if (path.indexOf('@') == -1) {
            return path;
        }
        return EMAIL_IN_DB_PATTERN.matcher(path).replaceAll("XX@YY");
    }
}
