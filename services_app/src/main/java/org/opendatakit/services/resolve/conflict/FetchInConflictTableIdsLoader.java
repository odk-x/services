/*
 * Copyright (C) 2016 University of Washington
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
package org.opendatakit.services.resolve.conflict;

import android.content.AsyncTaskLoader;
import android.content.Context;

import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.database.service.DbHandle;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Return the list of table_ids that have conflicts or checkpoints-and-conflicts.
 *
 * @author mitchellsundt@gmail.com
 */
public class FetchInConflictTableIdsLoader extends AsyncTaskLoader<ArrayList<String>> {

  private final String mAppName;

  public FetchInConflictTableIdsLoader(Context context, String appName) {
    super(context);
    this.mAppName = appName;
  }

  @Override public ArrayList<String> loadInBackground() {

    OdkConnectionInterface db = null;

    DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

    ArrayList<String> conflictingTableIds = new ArrayList<String>();

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(mAppName, dbHandleName);

      ArrayList<String> tableIds = ODKDatabaseImplUtils.get().getAllTableIds(db);
      for ( String tableId : tableIds ) {
        int status = ODKDatabaseImplUtils.get().getTableHealth(db, tableId);
        if ( status == CursorUtils.TABLE_HEALTH_HAS_CONFLICTS ||
             status == CursorUtils.TABLE_HEALTH_HAS_CHECKPOINTS_AND_CONFLICTS ) {
          conflictingTableIds.add(tableId);
        }
      }
    } catch (Exception e) {
      String msg = e.getLocalizedMessage();
      if (msg == null)
        msg = e.getMessage();
      if (msg == null)
        msg = e.toString();
      msg = "Exception: " + msg;
      WebLogger.getLogger(mAppName).e("FetchInConflictTableIdsLoader",
          mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
      WebLogger.getLogger(mAppName).printStackTrace(e);
      throw new IllegalStateException(msg);
    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }

    return conflictingTableIds;
  }

  @Override protected void onStartLoading() {
    super.onStartLoading();
    forceLoad();
  }
}
