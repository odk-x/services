package org.opendatakit.services.database;

import android.content.Context;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.utilities.StaticStateManipulator;

/**
 * Created by clarice on 1/27/16.
 */
public final class AndroidConvConnectFactory extends OdkConnectionFactoryAbstractClass {
  static {
    OdkConnectionFactorySingleton.set(new AndroidConvConnectFactory());

    // register a state-reset manipulator for 'connectionFactory' field.
    StaticStateManipulator.get().register(50, new StaticStateManipulator.IStaticFieldManipulator() {

      @Override
      public void reset() {
        OdkConnectionFactorySingleton.set(new AndroidConvConnectFactory());
      }

    });
  }

  public static void configure() {
    // just to get the static initialization block (above) to run
  }

  private AndroidConvConnectFactory() {
  }

  @Override
  protected void logInfo(String appName, String message) {
    WebLogger.getLogger(appName).i("AndroidConvConnectFactory", message);
  }

  @Override
  protected void logWarn(String appName, String message) {
    WebLogger.getLogger(appName).w("AndroidConvConnectFactory", message);
  }

  @Override
  protected void logError(String appName, String message) {
    WebLogger.getLogger(appName).e("AndroidConvConnectFactory", message);
  }

  @Override
  protected void printStackTrace(String appName, Throwable e) {
    WebLogger.getLogger(appName).printStackTrace(e);
  }

  @Override
  protected OdkConnectionInterface openDatabase(AppNameSharedStateContainer appNameSharedStateContainer,
                                                String sessionQualifier, Context context) {
    return org.opendatakit.services.database.AndroidConvOdkConnection.openDatabase(appNameSharedStateContainer, sessionQualifier, context);
  }

  /**
   * the database schema version that the application expects
   */
  public static final int mNewVersion = 2;

}