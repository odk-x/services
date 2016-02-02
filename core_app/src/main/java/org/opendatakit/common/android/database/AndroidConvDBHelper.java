package org.opendatakit.common.android.database;

import android.content.Context;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteDatabase;

import org.opendatakit.common.android.utilities.ODKFileUtils;

import java.io.File;

/**
 * Created by clarice on 1/27/16.
 */
public class AndroidConvDBHelper extends SQLiteOpenHelper {

  private static AndroidConvDBHelper mInstance = null;

  public static AndroidConvDBHelper getInstance(Context context, String dbFilePath) {
    if (mInstance == null) {
      mInstance = new AndroidConvDBHelper(context.getApplicationContext(), dbFilePath);
    }
    return mInstance;
  }
  public AndroidConvDBHelper(Context context, String dbFilePath) {
    super(context, dbFilePath , null, 1);
  }

  @Override
  public void onCreate(SQLiteDatabase db) {
    // TODO Auto-generated method stub
  }

  @Override
  public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    onCreate(db);
  }
}
