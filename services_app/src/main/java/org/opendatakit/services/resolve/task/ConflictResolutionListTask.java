package org.opendatakit.services.resolve.task;

import android.content.Context;
import android.os.AsyncTask;
import android.widget.ArrayAdapter;
import org.opendatakit.database.service.DbHandle;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.services.R;
import org.opendatakit.services.database.OdkConnectionFactorySingleton;
import org.opendatakit.services.database.OdkConnectionInterface;
import org.opendatakit.services.database.utlities.ODKDatabaseImplUtils;
import org.opendatakit.services.resolve.listener.ResolutionListener;
import org.opendatakit.services.resolve.views.components.ResolveRowEntry;
import org.opendatakit.services.utilities.ActiveUserAndLocale;

import java.util.UUID;

/**
 * @author mitchellsundt@gmail.com
 */
public class ConflictResolutionListTask extends AsyncTask<Void, String, String> {

  /**
   * Holds the currently logged in user and their selected locale
   */
  ActiveUserAndLocale aul;
  /**
   * A string resources for the row resolution status
   */
  String formatStrResolvingRowNofM;
  /**
   * A string resource for when the row has been completely resolved
   */
  String formatStrDone;

  /**
   * Whether to tahe local changes or take the server changes
   */
  boolean mTakeLocal;
  /**
   * The app name
   */
  String mAppName;
  /**
   * The id of the table that needs resolving
   */
  String mTableId;
  /**
   * An adapter between the list view and the entries in the row that need to be resolved
   */
  ArrayAdapter<ResolveRowEntry> mAdapter;
  /**
   * An object to be notified when the row has ben resolved
   */
  ResolutionListener rl;
  /**
   * Changed on progress update, passed to resolutionProgress on the resolution listener
   */
  String mProgress = "";
  /**
   * Set if canceled or on a result, passed to resolutionComplete on the resolution listener
   */
  String mResult = null;

  /**
   * Saves its arguments and pulls string resources from the passed context
   * @param context a context to pull string resources from
   * @param takeLocal whether to take local or server changes
   * @param appName the app name
   */
  public ConflictResolutionListTask(Context context, boolean takeLocal, String appName) {
    super();
    this.mAppName = appName;
    aul = ActiveUserAndLocale.getActiveUserAndLocale(context, mAppName);

    formatStrResolvingRowNofM = context.getString(R.string.resolving_row_n_of_m);
    formatStrDone = context.getString(R.string.done_resolving_rows);
    mTakeLocal = takeLocal;
  }

  @Override
  protected String doInBackground(Void... params) {

    OdkConnectionInterface db = null;

    DbHandle dbHandleName = new DbHandle(UUID.randomUUID().toString());

    StringBuilder exceptions = null;

    try {
      // +1 referenceCount if db is returned (non-null)
      db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
          .getConnection(mAppName, dbHandleName);

      for (int i = 0; i < mAdapter.getCount(); ++i) {
        this.publishProgress(String.format(formatStrResolvingRowNofM, i + 1, mAdapter.getCount()));

        ResolveRowEntry entry = mAdapter.getItem(i);
        try {

          if (entry != null) {
            if (mTakeLocal) {
              // this might fail due to lowered user privileges
              ODKDatabaseImplUtils
                  .resolveServerConflictTakeLocalRowWithId(db, mTableId, entry.rowId,
                      aul.activeUser, aul.rolesList, aul.locale);
            } else {
              // all users can always take the server's changes
              ODKDatabaseImplUtils
                  .resolveServerConflictTakeServerRowWithId(db, mTableId, entry.rowId,
                      aul.activeUser, aul.locale);
            }
          }

        } catch (Exception e) {
          String msg = e.getLocalizedMessage();
          if (msg == null)
            msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          msg = "Exception: " + msg;
          WebLogger.getLogger(mAppName)
              .e("takeAllLocal", mAppName + " " + dbHandleName.getDatabaseHandle() + " " + msg);
          WebLogger.getLogger(mAppName).printStackTrace(e);

          if (exceptions == null) {
            exceptions = new StringBuilder();
          } else {
            exceptions.append("\n");
          }
          exceptions.append(msg);

          // and to be sure, try to release this database connection and create a new one...
          OdkConnectionInterface dbOld = db;
          db = null;

          dbHandleName = new DbHandle(UUID.randomUUID().toString());

          if (dbOld != null) {
            dbOld.releaseReference();
          }

          // +1 referenceCount if db is returned (non-null)
          db = OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
              .getConnection(mAppName, dbHandleName);
        }
      }
      this.publishProgress(formatStrDone);

    } finally {
      if (db != null) {
        // release the reference...
        // this does not necessarily close the db handle
        // or terminate any pending transaction
        db.releaseReference();
      }
    }
    return exceptions != null ? exceptions.toString() : null;
  }

  @Override
  protected void onProgressUpdate(String... progress) {
    synchronized (this) {
      mProgress = progress[0];
      if (rl != null) {
        rl.resolutionProgress(mProgress);
      }
    }
  }

  @Override
  protected void onPostExecute(String result) {
    synchronized (this) {
      mResult = result;
      if (rl != null) {
        rl.resolutionComplete(mResult);
      }
    }
  }

  @Override
  protected void onCancelled(String result) {
    synchronized (this) {
      mResult = result;
      // can be null if cancelled before task executes
      if (rl != null) {
        rl.resolutionComplete(mResult);
      }
    }
  }

  public String getProgress() {
    return mProgress;
  }

  public String getResult() {
    return mResult;
  }

  public void setResolutionListener(ResolutionListener listener) {
    synchronized (this) {
      rl = listener;
    }
  }

  /**
   * Clears the saved listener but only if the passed listener is the current one
   * @param listener the listener to compare to
   */
  public void clearResolutionListener(ResolutionListener listener) {
    synchronized (this) {
      //noinspection ObjectEquality
      if (rl == listener) {
        rl = null;
      }
    }
  }

  public String getAppName() {
    return mAppName;
  }

  public String getTableId() {
    return mTableId;
  }

  public void setTableId(String tableId) {
    synchronized (this) {
      this.mTableId = tableId;
    }
  }

  public ArrayAdapter<ResolveRowEntry> getResolveRowEntryAdapter() {
    return mAdapter;
  }

  public void setResolveRowEntryAdapter(ArrayAdapter<ResolveRowEntry> adapter) {
    synchronized (this) {
      this.mAdapter = adapter;
      this.mProgress = String.format(formatStrResolvingRowNofM, 1, mAdapter.getCount());
    }
  }

}
