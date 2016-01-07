/*
 * Copyright (C) 2012 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.sync.service.logic;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.wink.client.ClientWebException;
import org.opendatakit.aggregate.odktables.rest.entity.*;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.data.SyncRow;
import org.opendatakit.sync.service.data.SyncRowPending;
import org.opendatakit.sync.service.exceptions.InvalidAuthTokenException;

/**
 * Synchronizer abstracts synchronization of tables to an external cloud/server.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public interface Synchronizer {

  public interface SynchronizerStatus {
    /**
     * Status of this action.
     *
     * @param state
     * @param textResource
     * @param formatArgVals
     * @param progressPercentage
     *          0..100
     * @param indeterminateProgress
     *          true if progressGrains is N/A
     */
    void updateNotification(SyncProgressState state, int textResource, Object[] formatArgVals,
        Double progressPercentage, boolean indeterminateProgress);
  }

  public interface OnTablePropertiesChanged {
    void onTablePropertiesChanged(String tableId);
  }

  /**
   * Get a list of all tables in the server.
   *
   * @param webSafeResumeCursor null or a non-empty string if we are issuing a resume query
   * @return a list of the table resources on the server
   * @throws ClientWebException
   * @throws InvalidAuthTokenException 
   */
  public TableResourceList getTables(String webSafeResumeCursor) throws ClientWebException,
      InvalidAuthTokenException;

  /**
   * Discover the schema for a table resource.
   *
   * @param tableDefinitionUri
   * @return the table definition
   * @throws ClientWebException
   * @throws InvalidAuthTokenException 
   */
  public TableDefinitionResource getTableDefinition(String tableDefinitionUri) 
      throws ClientWebException, InvalidAuthTokenException;

  /**
   * Assert that a table with the given id and schema exists on the server.
   *
   * @param tableId
   *          the unique identifier of the table
   * @param schemaETag
   *          the current schemaETag for the table
   * @param columns
   *          an array of the columns for this table.
   * @return the TableResource for the table (the server may return different
   *         SyncTag values)
   * @throws ClientWebException
   * @throws InvalidAuthTokenException 
   */
  public TableResource createTable(String tableId, String schemaETag, ArrayList<Column> columns)
      throws ClientWebException, InvalidAuthTokenException;

  /**
   * Delete the table with the given id from the server.
   *
   * @param table
   *          the realizedTable resource to delete
   * @throws ClientWebException
   * @throws InvalidAuthTokenException 
   */
  public void deleteTable(TableResource table) throws ClientWebException, InvalidAuthTokenException;

  /**
   * Retrieve the changeSets applied after the changeSet with the specified dataETag
   *
   * @param tableResource
   * @param dataETag
   * @return
   * @throws ClientWebException
   * @throws InvalidAuthTokenException
   */
  public ChangeSetList getChangeSets(TableResource tableResource, String dataETag) throws ClientWebException, InvalidAuthTokenException;

  /**
   * Retrieve the change set for the indicated dataETag
   *
   * @param tableResource
   * @param dataETag
   * @param activeOnly
   * @param websafeResumeCursor
   * @return
   * @throws ClientWebException
   * @throws InvalidAuthTokenException
   */
  public RowResourceList getChangeSet(TableResource tableResource, String dataETag, boolean activeOnly, String websafeResumeCursor)
      throws ClientWebException, InvalidAuthTokenException;

  /**
   * Retrieve changes in the server state since the last synchronization.
   *
   * @param tableResource
   *          the TableResource from the server for a tableId
   * @param dataETag
   *          tracks the last dataETag successfully pulled into
   *          the local data table. Fetches changes after that dataETag.
   * @param websafeResumeCursor
   *          either null or a value used to resume a prior query.
   *          
   * @return an RowResourceList of the changes on the server since that dataETag.
   * @throws ClientWebException
   * @throws InvalidAuthTokenException 
   */
  public RowResourceList getUpdates(TableResource tableResource, String dataETag, String websafeResumeCursor)
      throws ClientWebException, InvalidAuthTokenException;

  /**
   * Apply inserts, updates and deletes in a collection up to the server.
   * This does not depend upon knowing the current dataETag of the server.
   * The dataETag for the changeSet made by this call is returned to the 
   * caller via the RowOutcomeList.
   * 
   * @param tableResource
   *          the TableResource from the server for a tableId
   * @param rowsToInsertUpdateOrDelete
   * @return
   * @throws ClientWebException
   * @throws InvalidAuthTokenException 
   */
  public RowOutcomeList alterRows(TableResource tableResource,
      List<SyncRow> rowsToInsertUpdateOrDelete) throws ClientWebException, InvalidAuthTokenException;

  /**
   * Synchronizes the app level files. This includes any files that are not
   * associated with a particular table--i.e. those that are not in the
   * directory appid/tables/. It also excludes those files that are in a set of
   * directories that do not sync--appid/metadata, appid/logging, etc.
   *
   * @param pushLocalFiles true if local files should be pushed. Otherwise they are only pulled
   *        down.
   * @param serverReportedAppLevelETag may be null. The server's app-level manifest ETag if known.
   * @param syncStatus
   *          for reporting detailed progress of app-level file sync
   * @return true if successful
   * @throws ClientWebException
   * @throws InvalidAuthTokenException 
   */
  public boolean syncAppLevelFiles(boolean pushLocalFiles, String serverReportedAppLevelETag, SynchronizerStatus syncStatus)
      throws ClientWebException, InvalidAuthTokenException;

  /**
   * Sync only the files associated with the specified table. This does NOT sync
   * any media files associated with individual rows of the table.
   *
   * @param tableId
   * @param serverReportedTableLevelETag may be null. The server's table-level manifest ETag if known.
   * @param onChange
   *          callback if the config/assets/csv/tableId.properties.csv file changes
   * @param pushLocal
   *          true if the local files should be pushed
   * @throws ClientWebException
   * @throws InvalidAuthTokenException 
   */
  public void syncTableLevelFiles(String tableId, String serverReportedTableLevelETag, OnTablePropertiesChanged onChange,
      boolean pushLocal, SynchronizerStatus syncStatus) throws ClientWebException, InvalidAuthTokenException;

  /**
   * Ensure that the file attachments for the indicated row values match between the
   * server and the client. File attachments are immutable on the server -- never updated and
   * never destroyed.
   *
   * @param instanceFileUri
   * @param tableId
   * @param localRow
   * @param attachmentState -- whether to upload, download, both, or neither the attachments
   * @return true if successful
   * @throws ClientWebException
   */
  public boolean syncFileAttachments(String instanceFileUri, String tableId, SyncRowPending
      localRow, SyncAttachmentState attachmentState)
      throws ClientWebException;
  
  /**
   * Use to purge ETags of any instance attachments when server does not remember this schemaETag
   * 
   * @param tableId
   * @param schemaETag
   * @return
   */
  public URI constructTableInstanceFileUri(String tableId, String schemaETag);

}
