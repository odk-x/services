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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import android.os.RemoteException;
import org.opendatakit.aggregate.odktables.rest.entity.*;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncProgressState;
import org.opendatakit.sync.service.data.SyncRow;
import org.opendatakit.sync.service.exceptions.HttpClientWebException;

/**
 * Synchronizer abstracts synchronization of tables to an external cloud/server.
 *
 * This is a lower-level interface with somewhat atomic interactions with the
 * remote server and the database. Higher level interactions are managed in the
 * various Process... classes.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 * @author mitchellsundt@gmail.com
 *
 */
public interface Synchronizer {

  interface SynchronizerStatus {
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

  interface OnTablePropertiesChanged {
    void onTablePropertiesChanged(String tableId);
  }

  /**
   * Verifies that the device's application name is supported by the server.
   *
   * @throws HttpClientWebException
   * @throws IOException
   */
  void verifyServerSupportsAppName() throws HttpClientWebException, IOException;

  /**
   * Get a list of all tables in the server.
   *
   * @param webSafeResumeCursor null or a non-empty string if we are issuing a resume query
   * @return a list of the table resources on the server
   * @throws HttpClientWebException
   * @throws IOException
   */
  TableResourceList getTables(String webSafeResumeCursor) throws HttpClientWebException, IOException;

  /**
   * Discover the schema for a table resource.
   *
   * @param tableDefinitionUri
   * @return the table definition
   * @throws HttpClientWebException
   * @throws IOException
   */
  TableDefinitionResource getTableDefinition(String tableDefinitionUri)
      throws HttpClientWebException, IOException;

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
   * @throws HttpClientWebException
   * @throws IOException
   */
  TableResource createTable(String tableId, String schemaETag, ArrayList<Column> columns)
      throws HttpClientWebException, IOException;

  /**
   * Delete the table with the given id from the server.
   *
   * @param table
   *          the realizedTable resource to delete
   * @throws HttpClientWebException
   * @throws IOException
   */
  void deleteTable(TableResource table) throws HttpClientWebException, IOException;

  /**
   * Retrieve the changeSets applied after the changeSet with the specified dataETag
   *
   * @param tableResource
   * @param dataETag
   * @return
   * @throws HttpClientWebException
   * @throws IOException
   */
  ChangeSetList getChangeSets(TableResource tableResource, String dataETag) throws
      HttpClientWebException, IOException;

  /**
   * Retrieve the change set for the indicated dataETag
   *
   * @param tableResource
   * @param dataETag
   * @param activeOnly
   * @param websafeResumeCursor
   * @return
   * @throws HttpClientWebException
   * @throws IOException
   */
  RowResourceList getChangeSet(TableResource tableResource, String dataETag, boolean activeOnly, String websafeResumeCursor)
      throws HttpClientWebException, IOException;

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
   * @throws HttpClientWebException
   * @throws IOException
   */
  RowResourceList getUpdates(TableResource tableResource, String dataETag, String websafeResumeCursor)
      throws HttpClientWebException, IOException;

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
   * @throws HttpClientWebException
   * @throws IOException
   */
  RowOutcomeList alterRows(TableResource tableResource,
      List<SyncRow> rowsToInsertUpdateOrDelete) throws HttpClientWebException, IOException;

  /**
   * Request the app-level manifest. This uses a NOT_MODIFIED header to detect
   * not-changed status. However, it does not update that value. The caller is
   * expected to update the ETag after they have made the device match the
   * content reported by the server (or vice-versa on a push).
   *
   * @param pushLocalFiles
   * @param serverReportedAppLevelETag
   * @return
   * @throws HttpClientWebException
   * @throws IOException
   */
  FileManifestDocument getAppLevelFileManifest(boolean pushLocalFiles, String serverReportedAppLevelETag)
      throws HttpClientWebException, IOException;

  /**
   * Get the config manifest for a given tableId. This uses a NOT_MODIFIED header to detect
   * not-changed status. However, it does not update that value. The caller is
   * expected to update the ETag after they have made the device match the
   * content reported by the server (or vice-versa on a push).
   *
   * @param tableId
   * @param serverReportedTableLevelETag
   * @param pushLocalFiles
   * @return
   * @throws IOException
   * @throws HttpClientWebException
   */
  FileManifestDocument getTableLevelFileManifest(String tableId, String serverReportedTableLevelETag,
      boolean pushLocalFiles) throws IOException, HttpClientWebException;

  /**
   * Get the manifest for the row attachments for the given tableId and row (instance) Id.
   * Use the attachmentState and localRowAttachmentHash to qualify the ETag to use.
   *
   * This uses a NOT_MODIFIED header to detect
   * not-changed status. However, it does not update that value. The caller is
   * expected to update the ETag after they have made the device match the
   * content reported by the server (or vice-versa on a push).
   *
   * @param serverInstanceFileUri
   * @param tableId
   * @param instanceId
   * @param attachmentState that we are trying to enforce.
   * @param localRowAttachmentHash
   * @return
   * @throws HttpClientWebException
   * @throws IOException
   */
  FileManifestDocument getRowLevelFileManifest(String serverInstanceFileUri,
      String tableId, String instanceId, SyncAttachmentState attachmentState,
      String localRowAttachmentHash) throws
      HttpClientWebException,
      IOException;

  /**
   * Download a file from the given Uri and store it in the destFile.
   *
   * @param destFile
   * @param downloadUrl
   * @throws HttpClientWebException
   * @throws IOException
   */
  void downloadFile(File destFile, URI downloadUrl) throws HttpClientWebException, IOException;

  /**
   * Delete the given config file on the server.
   *
   * @param localFile
   * @throws HttpClientWebException
   * @throws IOException
   */
  void deleteConfigFile(File localFile) throws HttpClientWebException, IOException;

  /**
   *
   * @param localFile
   * @return
   * @throws HttpClientWebException
   * @throws IOException
   */
  void uploadConfigFile(File localFile) throws HttpClientWebException, IOException;

  /**
   *
   * @param file
   * @param instanceFileUri
   * @throws HttpClientWebException
   * @throws IOException
   */
  void uploadInstanceFile(File file, URI instanceFileUri) throws HttpClientWebException, IOException;

  /**
   *
   * @param serverInstanceFileUri
   * @param tableId
   * @param instanceId
   * @param rowpathUri
   * @return
   */
  CommonFileAttachmentTerms createCommonFileAttachmentTerms(String serverInstanceFileUri,
      String tableId, String instanceId, String rowpathUri);

  /**
   *
   * @param batch
   * @param serverInstanceFileUri
   * @param instanceId
   * @param tableId
   * @throws HttpClientWebException
   * @throws IOException
   */
  void uploadInstanceFileBatch(List<CommonFileAttachmentTerms> batch,
      String serverInstanceFileUri, String instanceId, String tableId) throws HttpClientWebException, IOException;

  /**
   *
   * @param filesToDownload
   * @param serverInstanceFileUri
   * @param instanceId
   * @param tableId
   * @throws HttpClientWebException
   * @throws IOException
   */
  void downloadInstanceFileBatch(List<CommonFileAttachmentTerms> filesToDownload,
      String serverInstanceFileUri, String instanceId, String tableId) throws HttpClientWebException, IOException;

  /**
   *
   * @param fileDownloadUri
   * @param tableId
   * @param lastModified
   * @return
   * @throws RemoteException
   */
  String getFileSyncETag(URI
      fileDownloadUri, String tableId, long lastModified) throws RemoteException;

  /**
   * Updates this config file download URI with the indicated ETag
   *
   * @param fileDownloadUri
   * @param tableId
   * @param lastModified
   * @param documentETag
   * @throws RemoteException
   */
  void updateFileSyncETag(URI fileDownloadUri, String tableId, long lastModified, String
      documentETag) throws RemoteException;

  /**
   *
   * @param tableId
   * @return
   * @throws RemoteException
   */
  String getManifestSyncETag(String tableId) throws RemoteException;

  /**
   * Update the manifest content ETag with the indicated value. This should be done
   * AFTER the device matches the content on the server. Until then, the ETag should
   * not be recorded.
   *
   * @param tableId
   * @param documentETag
   * @throws RemoteException
   */
  void updateManifestSyncETag(String tableId, String documentETag) throws
      RemoteException;


  /**
   *
   * @param serverInstanceFileUri
   * @param tableId
   * @param rowId
   * @param attachmentState
   * @param uriFragmentHash
   * @return
   * @throws RemoteException
   */
  String getRowLevelManifestSyncETag(String serverInstanceFileUri, String tableId, String rowId,
      SyncAttachmentState attachmentState, String uriFragmentHash) throws RemoteException;

  /**
   * Update the manifest content ETag with the indicated value. This should be done
   * AFTER the device matches the content on the server. Until then, the ETag should
   * not be recorded.
   *
   * @param serverInstanceFileUri
   * @param tableId
   * @param rowId
   * @param attachmentState
   * @param uriFragmentHash
   * @param documentETag
   * @throws RemoteException
   */
  void updateRowLevelManifestSyncETag(String serverInstanceFileUri, String tableId, String rowId,
      SyncAttachmentState attachmentState, String uriFragmentHash, String documentETag) throws
      RemoteException;

    /**
       * Invoked when the schema of a table has changed or we have never before synced with the server.
       *
       * @param tableId
       * @param newSchemaETag
       * @param oldSchemaETag
       */
  void updateTableSchemaETagAndPurgePotentiallyChangedDocumentETags(String tableId,
      String newSchemaETag, String oldSchemaETag)  throws RemoteException;

}
