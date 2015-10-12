package org.opendatakit.common.android.database;

import android.content.Context;

/**
 * Created by clarice on 9/14/15.
 */
public class OdkConnectionFactorySingleton {

   private static OdkConnectionFactoryInterface connectionFactorySingleton = null;

   public static final OdkConnectionFactoryInterface getOdkConnectionFactoryInterface() {
      if (connectionFactorySingleton == null) {
         throw new IllegalStateException(
             "OdkConnectionFactorySingleton not yet initialized!  If this happens then configure must be called");
      }

      return connectionFactorySingleton;
   }

   protected static final void set(OdkConnectionFactoryInterface factorySingleton) {
      if (connectionFactorySingleton != null) {
         connectionFactorySingleton.releaseAllDatabases();
      }
      connectionFactorySingleton = factorySingleton;
   }

}