/*
 * Copyright (C) 2007 The Android Open Source Project
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

package org.opendatakit.common.android.database;

import java.io.File;

import org.opendatakit.common.android.utilities.ODKDatabaseImplUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.sqlite.database.sqlite.SQLiteException;


/**
 * We've taken this from Android's SQLiteOpenHelper. However, we can't appropriately lock the
 * database so there may be issues if a thread opens the database read-only and another thread tries
 * to open the database read/write. I don't think this will ever happen in ODK, though. (fingers
 * crossed).
 */

/**
 * A helper class to manage database creation and version management. You create
 * a subclass implementing {@link #onCreate} and {@link #onUpgrade}. This class
 * takes care of opening the database if it exists, creating it if it does not,
 * and upgrading it as necessary.
 * 
 * Transactions are used to make sure the database is always in a sensible
 * state.
 * <p>
 * For an example, see the NotePadProvider class in the NotePad sample
 * application, in the <em>samples/</em> directory of the SDK.
 * </p>
 */
public abstract class ODKSQLiteOpenHelper {
  private static final String TAG = ODKSQLiteOpenHelper.class.getSimpleName();

  /** the appName whose database should be opened */
  private final String mAppName;
  /** database file path (to simplify repeated open calls) */
  private final String mDbFilePath;
  /** the version that the application expects */
  private final int mNewVersion;

  private OdkDatabase mDatabase = null;
  private boolean mIsInitializing = false;

  /**
   * Create a helper object to create and/or open a database. The database is
   * not actually created or opened until one of {@link #getWritableDatabase} is
   * called.
   *
   * @param appName
   *          the appName under which the database exists.
   * @param version
   *          number of the database (starting at 1); if the database is older,
   *          {@link #onUpgrade} will be used to upgrade the database
   */
  public ODKSQLiteOpenHelper(String appName, int version) {
    if (version < 1)
      throw new IllegalArgumentException("Version must be >= 1, was " + version);

    if (appName == null) {
      throw new IllegalStateException("appName must be specified!");
    }

    mAppName = appName;

    File dbFile = new File(ODKFileUtils.getWebDbFolder(appName),
        ODKFileUtils.getNameOfSQLiteDatabase());
    mDbFilePath = dbFile.getAbsolutePath();
    mNewVersion = version;
  }

  /**
   * Return whether or not this is initializing. Only valid on the
   * (appName,appName) database object.
   * 
   * @return
   */
  public boolean isInitializing() {
    return mIsInitializing;
  }

  /**
   * Invoked on the (appName,appName) database object when it is first created
   * to perform any necessary updates to the database schema.
   * 
   * Subsequent calls just open the existing database (and fail if it somehow
   * vanishes).
   */
  protected synchronized void initializeDatabase(String sessionQualifier) {
    if (mDatabase != null) {
      throw new IllegalStateException("initializeDatabase called multiple times!");
    }

    mIsInitializing = true;
    WebLogger.getLogger(mAppName).i(TAG, "initializeDatabase -- initializing database");

    OdkDatabase db = null;
    try {
      db = OdkDatabase.openDatabase(mAppName, mDbFilePath, sessionQualifier);

      int version = db.getVersion();
      if (version != mNewVersion) {
        ODKDatabaseImplUtils.get().beginTransactionNonExclusive(db);
        try {
          if (version == 0) {
            onCreate(db);
          } else {
            onUpgrade(db, version, mNewVersion);
          }
          db.setVersion(mNewVersion);
          db.setTransactionSuccessful();
        } finally {
          db.endTransaction();
        }
      }

    } finally {
      if (db != null) {
        db.close();
      }
    }
    
    mIsInitializing = false;
  }

  /**
   * Open a database that will be used for reading and writing. Once opened
   * successfully, the database is cached, so you can call this method every
   * time you need to write to the database. Make sure to call {@link #close}
   * when you no longer need it.
   * <p>
   * Errors such as bad permissions or a full disk may cause this operation to
   * fail, but future attempts may succeed if the problem is fixed.
   * </p>
   *
   * @throws SQLiteException
   *           if the database cannot be opened for writing
   * @return a read/write database object valid until {@link #close} is called
   */
  protected synchronized OdkDatabase getWritableDatabase(String sesionQualifier) {
    if (mDatabase != null && mDatabase.isOpen()) {
      mDatabase.acquireReference();
      WebLogger.getLogger(mAppName).i(TAG,
          "getWritableDatabase -- obtaining reference to already-open database");
      return mDatabase; // The database is already open for business
    }

    if (mIsInitializing) {
      throw new IllegalStateException("getWritableDatabase called recursively");
    }

    WebLogger.getLogger(mAppName).i(TAG, "getWritableDatabase -- opening database");
    boolean success = false;
    OdkDatabase db = null;
    try {
      db = OdkDatabase.openDatabase(mAppName, mDbFilePath, sesionQualifier);
      success = true;
      return db;
    } finally {
      if (success) {
        if (mDatabase != null) {
          try {
            mDatabase.close();
          } catch (Exception e) {
          }
        }
        mDatabase = db;
      } else {
        if (db != null)
          db.close();
      }
    }
  }

  public String getLastAction() {
    // TODO Auto-generated method stub
    if ( mDatabase != null ) {
      return mDatabase.getLastAction();
    }
    return "-not available-";
  }

  public synchronized void releaseDatabase() {
    while (mDatabase != null && mDatabase.isOpen()) {
      try {
        if (mDatabase.inTransaction()) {
          mDatabase.endTransaction();
        }
        mDatabase.close();
      } catch (SQLiteException e) {
      }
    }
    mDatabase = null;
  }

  /**
   * Called when the database is created for the first time. This is where the
   * creation of tables and the initial population of the tables should happen.
   *
   * @param db
   *          The database.
   */
  protected abstract void onCreate(OdkDatabase db);

  /**
   * Called when the database needs to be upgraded. The implementation should
   * use this method to drop tables, add tables, or do anything else it needs to
   * upgrade to the new schema version.
   * <p>
   * The SQLite ALTER TABLE documentation can be found <a
   * href="http://sqlite.org/lang_altertable.html">here</a>. If you add new
   * columns you can use ALTER TABLE to insert them into a live table. If you
   * rename or remove columns you can use ALTER TABLE to rename the old table,
   * then create the new table and then populate the new table with the contents
   * of the old table.
   *
   * @param db
   *          The database.
   * @param oldVersion
   *          The old database version.
   * @param newVersion
   *          The new database version.
   */
  protected abstract void onUpgrade(OdkDatabase db, int oldVersion, int newVersion);
}
