package org.opendatakit.services.database;

/**
 * A singleton used to obtain an OdkConnectionFactoryInterface
 *
 * This singleton is referenced throughout the code to access the
 * factory class that provides database connections. This, and the
 * factory interface {@link OdkConnectionFactoryInterface} hide the
 * implementation class of that factory from the rest of the code.
 *
 * Implementations of the OdkConnectionFactoryInterface should
 * invoke the static set(OdkConnectionFactoryInterface) method
 * to register themselves.
 *
 * e.g., see AndroidConnectFactory.configure();
 *
 * @author clarlars@gmail.com
 * @author mitchellsundt@gmail.com
 */
public final class OdkConnectionFactorySingleton {

   private static OdkConnectionFactoryInterface connectionFactorySingleton = null;

   /**
    * Public accessor to retrieve the factory class that provides database connections.
    *
    * @return the registered factory class
    * @throws IllegalStateException if no factory class has been registered.
    */
   public static OdkConnectionFactoryInterface getOdkConnectionFactoryInterface
       () {
      synchronized ( OdkConnectionFactorySingleton.class ) {
         if (connectionFactorySingleton == null) {
            throw new IllegalStateException(
                "OdkConnectionFactorySingleton not yet initialized! If this happens then configure must be called");
         }

         return connectionFactorySingleton;
      }
   }

   /**
    * Called only by the implementor of OdkConnectionFactoryInterface
    * to register itself with this singleton.  If there was a previous
    * factory interface registered, then all of its database connections
    * are closed.
    *
    * @param factorySingleton
    * @throws IllegalArgumentException if factorySingleton is being re-registered.
    */
   public static synchronized void set(OdkConnectionFactoryInterface factorySingleton) {
      OdkConnectionFactoryInterface oldInterface = null;
      synchronized ( OdkConnectionFactorySingleton.class ) {
         oldInterface = connectionFactorySingleton;
         connectionFactorySingleton = factorySingleton;
         // it is an error to re-register
         if ( oldInterface == factorySingleton ) {
            throw new IllegalArgumentException(
                "OdkConnectionFactorySingleton initialized twice with the same factory interface!");
         }
      }

      if ( oldInterface != null ) {
         /**
          * ensure that all connections from the previous factory
          * are released.
          */
         oldInterface.removeAllConnections();
      }
   }

}