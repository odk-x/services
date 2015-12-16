package org.opendatakit.sync;

import org.apache.wink.client.ClientWebException;
import org.opendatakit.aggregate.odktables.rest.entity.*;
import org.opendatakit.sync.exceptions.InvalidAuthTokenException;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mitchellsundt@gmail.com
 */
public class AggregateSynchronizer implements Synchronizer {

  private SyncExecutionContext syncExecutionContext;

  public AggregateSynchronizer(SyncExecutionContext syncExecutionContext) {
    this.syncExecutionContext = syncExecutionContext;
  }

  @Override public TableResourceList getTables(String webSafeResumeCursor)
      throws ClientWebException, InvalidAuthTokenException {
    return null;
  }

  @Override public TableDefinitionResource getTableDefinition(String tableDefinitionUri)
      throws ClientWebException, InvalidAuthTokenException {
    return null;
  }

  @Override public TableResource createTable(String tableId, String schemaETag,
      ArrayList<Column> columns) throws ClientWebException, InvalidAuthTokenException {
    return null;
  }

  @Override public void deleteTable(TableResource table)
      throws ClientWebException, InvalidAuthTokenException {

  }

  @Override public ChangeSetList getChangeSets(TableResource tableResource, String dataETag)
      throws ClientWebException, InvalidAuthTokenException {
    return null;
  }

  @Override public RowResourceList getChangeSet(TableResource tableResource, String dataETag,
      boolean activeOnly, String websafeResumeCursor)
      throws ClientWebException, InvalidAuthTokenException {
    return null;
  }

  @Override public RowResourceList getUpdates(TableResource tableResource, String dataETag,
      String websafeResumeCursor) throws ClientWebException, InvalidAuthTokenException {
    return null;
  }

  @Override public RowOutcomeList alterRows(TableResource tableResource,
      List<SyncRow> rowsToInsertUpdateOrDelete)
      throws ClientWebException, InvalidAuthTokenException {
    return null;
  }

  @Override public boolean syncAppLevelFiles(boolean pushLocalFiles,
      String serverReportedAppLevelETag, SynchronizerStatus syncStatus)
      throws ClientWebException, InvalidAuthTokenException {
    return false;
  }

  @Override public void syncTableLevelFiles(String tableId, String serverReportedTableLevelETag,
      OnTablePropertiesChanged onChange, boolean pushLocal, SynchronizerStatus syncStatus)
      throws ClientWebException, InvalidAuthTokenException {

  }

  @Override public boolean getFileAttachments(String instanceFileUri, String tableId,
      SyncRowPending serverRow, boolean deferInstanceAttachments) throws ClientWebException {
    return false;
  }

  @Override public boolean putFileAttachments(String instanceFileUri, String tableId,
      SyncRowPending localRow, boolean deferInstanceAttachments) throws ClientWebException {
    return false;
  }

  @Override public URI constructTableInstanceFileUri(String tableId, String schemaETag) {
    return null;
  }
}
