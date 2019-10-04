/*
 * Copyright (C) 2016 University of Washington
 *
 * Extensively modified interface to C++ sqlite codebase
 *
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

import android.annotation.SuppressLint;
import android.os.Build;
import android.os.StatFs;

import org.opendatakit.utilities.ODKFileUtils;

/**
 * Provides access to SQLite functions that affect all database connection,
 * such as memory management.
 *
 * The native code associated with SQLiteGlobal is also sets global configuration options
 * using sqlite3_config() then calls sqlite3_initialize() to ensure that the SQLite
 * library is properly initialized exactly once before any other framework or application
 * code has a chance to run.
 *
 * @hide
 */
public final class SQLiteGlobal {
    private static final String TAG = "SQLiteGlobal";

    private static final Object sLock = new Object();
    private static long sDefaultPageSize = 0L;

    private SQLiteGlobal() {
    }

    /**
     * Gets the default page size to use when creating a database.
     */
    @SuppressLint("NewApi")
    @SuppressWarnings("deprecation")
    public static long getDefaultPageSize() {
        if ( sDefaultPageSize != 0L ) {
            return sDefaultPageSize;
        }

        String path = ODKFileUtils.getOdkFolder();

        synchronized (sLock) {
            if (sDefaultPageSize == 0L) {
                long pageSize = 0L;
                try {
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.JELLY_BEAN) {
                        pageSize = new StatFs(path).getBlockSizeLong();
                    } else {
                        pageSize = new StatFs(path).getBlockSize();
                    }
                } catch ( Throwable t ) {
                    // ignore.
                }
                if ( pageSize < 1024L ) {
                    sDefaultPageSize = 1024L;
                } else {
                    sDefaultPageSize = pageSize;
                }
            }
        }

        return sDefaultPageSize;
    }

    /**
     * Gets the default journal mode when WAL is not in use.
     */
    public static String getDefaultJournalMode() {
        return "delete";
    }

    /**
     * Gets the journal size limit in bytes.
     */
    public static int getJournalSizeLimit() {
        return 3000000;
    }

    /**
     * Gets the default database synchronization mode when WAL is not in use.
     */
    public static String getDefaultSyncMode() {
        return "full";
    }

    /**
     * Gets the database synchronization mode when in WAL mode.
     */
    public static String getWALSyncMode() {
        return "full";
    }

    /**
     * Gets the WAL auto-checkpoint integer in database pages.
     */
    public static int getWALAutoCheckpoint() {
        int value = 1000;
        return Math.max(1, value);
    }

    /**
     * Gets the connection pool size when in WAL mode.
     */
    public static int getWALConnectionPoolSize() {
        int value = 10;
        return Math.max(2, value);
    }
}
