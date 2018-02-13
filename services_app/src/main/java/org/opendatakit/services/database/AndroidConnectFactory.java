/*
 * Copyright (C) 2014 University of Washington
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

package org.opendatakit.services.database;

import org.opendatakit.logging.WebLogger;
import org.opendatakit.utilities.StaticStateManipulator;
import org.opendatakit.utilities.StaticStateManipulator.IStaticFieldManipulator;

public final class AndroidConnectFactory  extends OdkConnectionFactoryAbstractClass {
  private static final String TAG = AndroidConnectFactory.class.getSimpleName();

  static {
    OdkConnectionFactorySingleton.set(new AndroidConnectFactory());

    // register a state-reset manipulator for 'connectionFactory' field.
    StaticStateManipulator.get().register(new IStaticFieldManipulator() {

      @Override
      public void reset() {
        OdkConnectionFactorySingleton.set(new AndroidConnectFactory());
      }

    });
  }

  public static void configure() {
    // just to get the static initialization block (above) to run
  }

  /**
   * @return the database schema version that the application expects
   */
  public static int getDbVersion() {
    return 1;
  }

  private AndroidConnectFactory() {
  }

  @Override
  protected void logInfo(String appName, String message) {
    WebLogger.getLogger(appName).i(TAG, message);
  }

  @Override
  protected void logWarn(String appName, String message) {
    WebLogger.getLogger(appName).w(TAG, message);
  }

  @Override
  protected void logError(String appName, String message) {
    WebLogger.getLogger(appName).e(TAG, message);
  }

  @Override
  protected void printStackTrace(String appName, Throwable e) {
    WebLogger.getLogger(appName).printStackTrace(e);
  }

   @Override
   protected OdkConnectionInterface openDatabase(AppNameSharedStateContainer appNameSharedStateContainer,
       String sessionQualifier) {
      return AndroidOdkConnection.openDatabase(appNameSharedStateContainer,
          sessionQualifier);
   }
}
