package org.opendatakit.common.android.database;

import android.content.Context;

import org.opendatakit.database.service.OdkDbHandle;

/**
 * Created by clarice on 9/14/15.
 */
public interface OdkConnectionFactoryInterface {

    public static final String GROUP_TYPE_DIVIDER = "--";
    public static final String INTERNAL_TYPE_SUFFIX = "-internal";

    /**
     * This handle is suitable for non-service uses.
     *
     * @return
     */
    public OdkDbHandle generateInternalUseDbHandle();

    /**
     * This handle is suitable for database service use.
     *
     * @return
     */
    public OdkDbHandle generateDatabaseServiceDbHandle();

    public OdkConnectionInterface getConnection(Context context, String appName, OdkDbHandle dbHandleName);

    public void releaseDatabase(Context context, String appName, OdkDbHandle dbHandleName);

    public void releaseAllDatabases(Context context);

    public OdkConnectionInterface getDatabaseGroupInstance(Context context, String appName,
                                                           String sessionGroupQualifier, int instanceQualifier);

    public void releaseDatabaseGroupInstance(Context context, String appName,
                                             String sessionGroupQualifier, int instanceQualifier);

    /**
     * Release any database handles for groups that don't match or match the indicated
     * session group qualifier (depending upon the value of releaseNonMatchingGroupsOnly).
     *
     * Database sessions are group sessions if they contain '--'. Everything up
     * to the last occurrence of '--' in the sessionQualifier is considered the session
     * group qualifier.
     *
     * @param context
     * @param appName
     * @param sessionGroupQualifier
     * @param releaseNonMatchingGroupsOnly
     * @return true if anything was released.
     */
    public boolean releaseDatabaseGroupInstances(Context context, String appName,
                                                 String sessionGroupQualifier, boolean releaseNonMatchingGroupsOnly);

    public boolean releaseAllDatabaseGroupInstances(Context context);

    public boolean releaseAllDatabaseNonGroupNonInternalInstances(Context context);

    public void dumpInfo();

}
