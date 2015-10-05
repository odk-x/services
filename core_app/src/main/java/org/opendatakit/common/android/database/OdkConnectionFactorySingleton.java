package org.opendatakit.common.android.database;

/**
 * Created by clarice on 9/14/15.
 */
public class OdkConnectionFactorySingleton {

    private static OdkConnectionFactoryInterface connectionFactorySingleton = null;

    public static final OdkConnectionFactoryInterface getOdkConnectionFactoryInterface() {
        if (connectionFactorySingleton == null) {
            throw new IllegalStateException("OdkConenctionFactorySingleton not yet initialized!  If this happens then configure must be called");
        }

        return connectionFactorySingleton;
    }

    protected static final void set(OdkConnectionFactoryInterface factorySingleton) {
        connectionFactorySingleton = factorySingleton;
    }

}