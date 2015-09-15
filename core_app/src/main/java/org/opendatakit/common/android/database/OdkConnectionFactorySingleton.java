package org.opendatakit.common.android.database;

/**
 * Created by clarice on 9/14/15.
 */
public abstract class OdkConnectionFactorySingleton implements OdkConnectionFactoryInterface{

    private static OdkConnectionFactorySingleton connectionFactorySingleton = null;

    // CAL: This should probably be renamed once I get things working
    public static OdkConnectionFactorySingleton getOdkConnectionFactorySingleton() {
        if (connectionFactorySingleton == null) {
            throw new IllegalStateException("OdkConenctionFactorySingleton not yet initialized!  If this happens then configure must be called");
        }

        return connectionFactorySingleton.get();
    }

    public static void set(OdkConnectionFactorySingleton factorySingleton) {
        connectionFactorySingleton = factorySingleton;
    }

    public abstract OdkConnectionFactorySingleton get();

}