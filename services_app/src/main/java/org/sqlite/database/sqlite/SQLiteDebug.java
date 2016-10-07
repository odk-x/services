/*
 * Copyright (C) 2007 The Android Open Source Project
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

import android.util.Log;

/**
 * Provides debugging info about all SQLite databases running in the current process.
 *
 * Accessed from C++, so we need to leave the public interfaces as-is.
 *
 * {@hide}
 */
public final class SQLiteDebug {

    /**
     * Controls the printing of informational SQL log messages.
     *
     * Enable using "adb shell setprop log.tag.SQLiteLog VERBOSE".
     */
    public static final boolean DEBUG_SQL_LOG =
            Log.isLoggable("SQLiteLog", Log.VERBOSE);

    /**
     * Controls the printing of SQL statements as they are executed.
     *
     * Enable using "adb shell setprop log.tag.SQLiteStatements VERBOSE".
     */
    public static final boolean DEBUG_SQL_STATEMENTS =
            Log.isLoggable("SQLiteStatements", Log.VERBOSE);

    /**
     * Controls the printing of wall-clock time taken to execute SQL statements
     * as they are executed.
     *
     * Enable using "adb shell setprop log.tag.SQLiteTime VERBOSE".
     */
    public static final boolean DEBUG_SQL_TIME =
            Log.isLoggable("SQLiteTime", Log.VERBOSE);

    /**
     * True to enable database performance testing instrumentation.
     * @hide
     */
    public static final boolean DEBUG_LOG_SLOW_QUERIES = false;

    private SQLiteDebug() {
    }

    /**
     * Determines whether a query should be logged.
     *
     * Reads the "db.log.slow_query_threshold" system property, which can be changed
     * by the user at any time.  If the value is zero, then all queries will
     * be considered slow.  If the value does not exist or is negative, then no queries will
     * be considered slow.
     *
     * This value can be changed dynamically while the system is running.
     * For example, "adb shell setprop db.log.slow_query_threshold 200" will
     * log all queries that take 200ms or longer to run.
     * @hide
     */
    public static boolean shouldLogSlowQuery(long elapsedTimeMillis) {
        int slowQueryMillis = 10000;
        return slowQueryMillis >= 0 && elapsedTimeMillis >= slowQueryMillis;
    }
}
