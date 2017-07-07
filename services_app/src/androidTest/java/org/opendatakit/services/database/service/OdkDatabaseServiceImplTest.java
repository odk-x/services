package org.opendatakit.services.database.service;

import org.junit.Before;
import org.junit.Test;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.application.Services;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Niles on 7/6/17.
 */
public class OdkDatabaseServiceImplTest {
  private OdkDatabaseServiceImpl d;
  private PropertiesSingleton props;

  private static String getAppName() {
    return "default";
  }

  @Before
  public void setUp() {
    d = new OdkDatabaseServiceImpl(Services._please_dont_use_getInstance());
    props = CommonToolProperties.get(Services._please_dont_use_getInstance(), getAppName());
    props.clearSettings();
  }

  @Test
  public void testGetActiveUser() throws Exception {
    assertEquals(d.getActiveUser(getAppName()), "anonymous");
    setProp(CommonToolProperties.KEY_AUTHENTICATION_TYPE, props.CREDENTIAL_TYPE_USERNAME_PASSWORD);
    setProp(CommonToolProperties.KEY_USERNAME, "insert username here");
    setProp(CommonToolProperties.KEY_AUTHENTICATED_USER_ID, "user_id_here");
    setProp(CommonToolProperties.KEY_ROLES_LIST, "['list', 'of', 'roles']");
    assertEquals(d.getActiveUser(getAppName()), "user_id_here");
  }

  private Object setProp(String a, String b) throws Exception {
    Map<String, String> m = new HashMap<>();
    m.put(a, b);
    props.setProperties(m);

    return null;
    /*
    Field gpf = props.getClass().getDeclaredField("mGeneralProps");
    gpf.setAccessible(true);
    Properties gp = (Properties) gpf.get(props);
    Field spf = props.getClass().getDeclaredField("mSecureProps");
    spf.setAccessible(true);
    Properties sp = (Properties) spf.get(props);
    Field dpf = props.getClass().getDeclaredField("mDeviceProps");
    dpf.setAccessible(true);
    Properties dp = (Properties) dpf.get(props);
    Method ispm = props.getClass().getDeclaredMethod("isSecureProperty", String.class);
    ispm.setAccessible(true);
    Method idpm = props.getClass().getDeclaredMethod("isDeviceProperty", String.class);
    idpm.setAccessible(true);
    if ((boolean) ispm.invoke(props, a)) {
      return sp.setProperty(a, b);
    } else if ((boolean) idpm.invoke(props, a)) {
      return dp.setProperty(a, b);
    }
    return gp.setProperty(a, b);
    */
  }

  @Test
  public void testGetRolesList() throws Exception {
    assertNull(d.getRolesList(getAppName()));
    setProp(CommonToolProperties.KEY_ROLES_LIST, "['list', 'of', 'roles']");
    assertEquals(d.getRolesList(getAppName()), "['list', 'of', 'roles']");
  }

  @Test
  public void testGetDefaultGroup() throws Exception {
    assertEquals(d.getDefaultGroup(getAppName()),
        props.getProperty(CommonToolProperties.KEY_DEFAULT_GROUP));
  }

  @Test
  public void testGetUsersList() throws Exception {

  }

  @Test
  public void testOpenDatabase() throws Exception {

  }

  @Test
  public void testCloseDatabase() throws Exception {

  }

  @Test
  public void testCreateLocalOnlyTableWithColumns() throws Exception {

  }

  @Test
  public void testDeleteLocalOnlyTable() throws Exception {

  }

  @Test
  public void testInsertLocalOnlyRow() throws Exception {

  }

  @Test
  public void testUpdateLocalOnlyRow() throws Exception {

  }

  @Test
  public void testDeleteLocalOnlyRow() throws Exception {

  }

  @Test
  public void testPrivilegedServerTableSchemaETagChanged() throws Exception {

  }

  @Test
  public void testSetChoiceList() throws Exception {

  }

  @Test
  public void testGetChoiceList() throws Exception {

  }

  @Test
  public void testCreateOrOpenTableWithColumns() throws Exception {

  }

  @Test
  public void testCreateOrOpenTableWithColumnsAndProperties() throws Exception {

  }

  @Test
  public void testDeleteAllCheckpointRowsWithId() throws Exception {

  }

  @Test
  public void testDeleteLastCheckpointRowWithId() throws Exception {

  }

  @Test
  public void testDeleteTableAndAllData() throws Exception {

  }

  @Test
  public void testDeleteTableMetadata() throws Exception {

  }

  @Test
  public void testDeleteRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedDeleteRowWithId() throws Exception {

  }

  @Test
  public void testGetAdminColumns() throws Exception {

  }

  @Test
  public void testGetAllColumnNames() throws Exception {

  }

  @Test
  public void testGetAllTableIds() throws Exception {

  }

  @Test
  public void testGetRowsWithId() throws Exception {

  }

  @Test
  public void testPrivilegedGetRowsWithId() throws Exception {

  }

  @Test
  public void testGetMostRecentRowWithId() throws Exception {

  }

  @Test
  public void testGetTableMetadata() throws Exception {

  }

  @Test
  public void testGetTableMetadataIfChanged() throws Exception {

  }

  @Test
  public void testGetTableHealthStatus() throws Exception {

  }

  @Test
  public void testGetTableHealthStatuses() throws Exception {

  }

  @Test
  public void testGetExportColumns() throws Exception {

  }

  @Test
  public void testGetSyncState() throws Exception {

  }

  @Test
  public void testGetTableDefinitionEntry() throws Exception {

  }

  @Test
  public void testGetUserDefinedColumns() throws Exception {

  }

  @Test
  public void testHasTableId() throws Exception {

  }

  @Test
  public void testInsertCheckpointRowWithId() throws Exception {

  }

  @Test
  public void testInsertRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedInsertRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedPerhapsPlaceRowIntoConflictWithId() throws Exception {

  }

  @Test
  public void testSimpleQuery() throws Exception {

  }

  @Test
  public void testPrivilegedSimpleQuery() throws Exception {

  }

  @Test
  public void testPrivilegedExecute() throws Exception {

  }

  @Test
  public void testReplaceTableMetadata() throws Exception {

  }

  @Test
  public void testReplaceTableMetadataList() throws Exception {

  }

  @Test
  public void testReplaceTableMetadataSubList() throws Exception {

  }

  @Test
  public void testSaveAsIncompleteMostRecentCheckpointRowWithId() throws Exception {

  }

  @Test
  public void testSaveAsCompleteMostRecentCheckpointRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedUpdateTableETags() throws Exception {

  }

  @Test
  public void testPrivilegedUpdateTableLastSyncTime() throws Exception {

  }

  @Test
  public void testUpdateRowWithId() throws Exception {

  }

  @Test
  public void testResolveServerConflictWithDeleteRowWithId() throws Exception {

  }

  @Test
  public void testResolveServerConflictTakeLocalRowWithId() throws Exception {

  }

  @Test
  public void testResolveServerConflictTakeLocalRowPlusServerDeltasWithId() throws Exception {

  }

  @Test
  public void testResolveServerConflictTakeServerRowWithId() throws Exception {

  }

  @Test
  public void testPrivilegedUpdateRowETagAndSyncState() throws Exception {

  }

  @Test
  public void testDeleteAppAndTableLevelManifestSyncETags() throws Exception {

  }

  @Test
  public void testDeleteAllSyncETagsForTableId() throws Exception {

  }

  @Test
  public void testDeleteAllSyncETagsExceptForServer() throws Exception {

  }

  @Test
  public void testDeleteAllSyncETagsUnderServer() throws Exception {

  }

  @Test
  public void testGetFileSyncETag() throws Exception {

  }

  @Test
  public void testGetManifestSyncETag() throws Exception {

  }

  @Test
  public void testUpdateFileSyncETag() throws Exception {

  }

  @Test
  public void testUpdateManifestSyncETag() throws Exception {

  }

}