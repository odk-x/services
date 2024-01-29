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
package org.opendatakit.services.sync.service.logic;

import org.apache.commons.fileupload.MultipartStream;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.entity.GzipCompressingEntity;
import org.apache.hc.client5.http.entity.mime.ByteArrayBody;
import org.apache.hc.client5.http.entity.mime.FormBodyPartBuilder;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.AppNameList;
import org.opendatakit.aggregate.odktables.rest.entity.ChangeSetList;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.OdkTablesFileManifest;
import org.opendatakit.aggregate.odktables.rest.entity.OdkTablesFileManifestEntry;
import org.opendatakit.aggregate.odktables.rest.entity.PrivilegesInfo;
import org.opendatakit.aggregate.odktables.rest.entity.Row;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.aggregate.odktables.rest.entity.RowList;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.RowResourceList;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinition;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinitionResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResourceList;
import org.opendatakit.aggregate.odktables.rest.entity.UserInfoList;
import org.opendatakit.database.data.ColumnDefinition;
import org.opendatakit.database.data.OrderedColumns;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.provider.DataTableColumns;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.services.sync.service.exceptions.AccessDeniedException;
import org.opendatakit.services.sync.service.exceptions.BadClientConfigException;
import org.opendatakit.services.sync.service.exceptions.ClientDetectedMissingConfigForClientVersionException;
import org.opendatakit.services.sync.service.exceptions.ClientDetectedVersionMismatchedServerResponseException;
import org.opendatakit.services.sync.service.exceptions.HttpClientWebException;
import org.opendatakit.services.sync.service.exceptions.NetworkTransmissionException;
import org.opendatakit.services.sync.service.exceptions.ServerDoesNotRecognizeAppNameException;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.logic.CommonFileAttachmentTerms;
import org.opendatakit.sync.service.logic.FileManifestDocument;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Implementation of {@link Synchronizer} for ODK Aggregate.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class AggregateSynchronizer implements HttpSynchronizer {

  private static final String LOGTAG = AggregateSynchronizer.class.getSimpleName();
  public static final int DEFAULT_BOUNDARY_BUFSIZE = 4096;

  /**
   * Maximum number of bytes to put within one bulk upload/download request for
   * row-level instance files.
   */
  public static final long MAX_BATCH_SIZE = 10485760;


  private SyncExecutionContext sc;
  private HttpRestProtocolWrapper wrapper;
  private final WebLoggerIf log;

  public AggregateSynchronizer(SyncExecutionContext sc) {
    this.sc = sc;
    this.wrapper = new HttpRestProtocolWrapper(sc);
    this.log = WebLogger.getLogger(sc.getAppName());
  }

  @Override
  public URI constructAppLevelFileManifestUri() {
    return wrapper.constructAppLevelFileManifestUri();
  }

  @Override
  public URI constructTableLevelFileManifestUri(String tableId) {
    return wrapper.constructTableLevelFileManifestUri(tableId);
  }

  @Override
  public URI constructRealizedTableIdUri(String tableId, String schemaETag) {
    return wrapper.constructRealizedTableIdUri(tableId, schemaETag);
  }

  @Override
  public URI constructInstanceFileManifestUri(String serverInstanceFileUri, String rowId) {
    return wrapper.constructInstanceFileManifestUri(serverInstanceFileUri, rowId);
  }

  @Override
  public void verifyServerSupportsAppName() throws HttpClientWebException, IOException {

    AppNameList appNameList = null;

    URI uri = wrapper.constructListOfAppNamesUri();
    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;
    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_SC_NOT_FOUND);

      if (response.getCode() == HttpStatus.SC_NOT_FOUND) {
        throw new BadClientConfigException("server does not implement ODK 2.0 REST api",
                request, response);
      }

      String res = wrapper.convertResponseToString(response);

      appNameList = ODKFileUtils.mapper.readValue(res, AppNameList.class);

      if (!appNameList.contains(sc.getAppName())) {
        throw new ServerDoesNotRecognizeAppNameException("server does not recognize this appName",
                request, response);
      }
    } catch ( NetworkTransmissionException e ) {
      if ( e.getCause() != null && e.getCause() instanceof ConnectTimeoutException ) {
        throw new BadClientConfigException("server did not respond. Is the configuration correct?",
                e.getCause(), request, e.getResponse());
      } else {
        throw e;
      }
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public PrivilegesInfo getUserRolesAndDefaultGroup() throws HttpClientWebException,
      IOException {

    URI uri = wrapper.constructListOfUserRolesAndDefaultGroupUri();
    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;

    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_SC_NOT_FOUND);

      if (response.getCode() == HttpStatus.SC_NOT_FOUND) {
        // perhaps an older server (pre-v1.4.11) ?
        return null;
      }

      String res = wrapper.convertResponseToString(response);

      PrivilegesInfo privilegesInfo = ODKFileUtils.mapper.readValue(res, PrivilegesInfo.class);

      return privilegesInfo;

    } catch ( NetworkTransmissionException e ) {
      if (e.getCause() != null && e.getCause() instanceof ConnectTimeoutException) {
        throw new BadClientConfigException("server did not respond. Is the configuration correct?",
                e.getCause(), request, e.getResponse());
      } else {
        throw e;
      }
    } catch ( AccessDeniedException e ) {
      // this must be an anonymousUser
      if (sc.getAuthenticationType().equals(sc.getString(R.string.credential_type_none))) {
        return null;
      }
      throw e;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public   UserInfoList getUsers() throws HttpClientWebException, IOException {

    URI uri = wrapper.constructListOfUsersUri();
    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;

    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_SC_NOT_FOUND);

      if (response.getCode() == HttpStatus.SC_NOT_FOUND) {
        // perhaps an older server (pre-v1.4.11) ?
        return new UserInfoList();
      }

      String res = wrapper.convertResponseToString(response);

      UserInfoList rolesList = ODKFileUtils.mapper.readValue(res, UserInfoList.class);

      return rolesList;

    } catch ( NetworkTransmissionException e ) {
      if (e.getCause() != null && e.getCause() instanceof ConnectTimeoutException) {
        throw new BadClientConfigException("server did not respond. Is the configuration correct?",
            e.getCause(), request, e.getResponse());
      } else {
        throw e;
      }
    } catch ( AccessDeniedException e ) {
      // this must be an anonymousUser
      return new UserInfoList();
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public TableResourceList getTables(String webSafeResumeCursor) throws
      HttpClientWebException, IOException {

    URI uri = wrapper.constructListOfTablesUri(webSafeResumeCursor);
    TableResourceList tableResources = null;
    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;

    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);

      String res = wrapper.convertResponseToString(response);

      tableResources = ODKFileUtils.mapper.readValue(res, TableResourceList.class);

      return tableResources;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public TableResource getTable(String tableId) throws
      HttpClientWebException, IOException {

    URI uri = wrapper.constructTableIdUri(tableId);

    TableResource tableResource = null;
    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;


    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);

      String res = wrapper.convertResponseToString(response);

      tableResource = ODKFileUtils.mapper.readValue(res, TableResource.class);

      return tableResource;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public TableDefinitionResource getTableDefinition(String tableDefinitionUri)
          throws HttpClientWebException, IOException {

    URI uri = URI.create(tableDefinitionUri).normalize();

    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;
    TableDefinitionResource definitionRes = null;

    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);

      String res = wrapper.convertResponseToString(response);

      definitionRes = ODKFileUtils.mapper.readValue(res, TableDefinitionResource.class);

      return definitionRes;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public TableResource createTable(String tableId, String schemaETag, ArrayList<Column> columns)
      throws HttpClientWebException, IOException {

    // build request
    URI uri = wrapper.constructTableIdUri(tableId);
    TableDefinition definition = new TableDefinition(tableId, schemaETag, columns);
    String tableDefinitionJSON = ODKFileUtils.mapper.writeValueAsString(definition);

    // create table
    TableResource resource;

    CloseableHttpResponse response = null;
    HttpPut request = new HttpPut(uri);
    wrapper.buildJsonContentJsonResponseRequest(request);

    HttpEntity entity = new GzipCompressingEntity(new StringEntity(tableDefinitionJSON, Charset.forName("UTF-8")));
    request.setEntity(entity);

    try {
      // TODO: we also need to put up the key value store/properties.
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);

      String res = wrapper.convertResponseToString(response);

      resource = ODKFileUtils.mapper.readValue(res, TableResource.class);
      return resource;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public void deleteTable(TableResource table) throws HttpClientWebException,
          IOException {
    URI uri = URI.create(table.getDefinitionUri()).normalize();

    HttpDelete request = new HttpDelete(uri);
    CloseableHttpResponse response = null;

    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      // TODO: CAL: response should be used?
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public ChangeSetList getChangeSets(TableResource table, String dataETag) throws HttpClientWebException,
          IOException {

    String tableId = table.getTableId();
    // if we have not yet synced, get the changesets from the beginning of time.
    String effectiveDataETag = (table.getDataETag() != null) ? dataETag : null;
    URI uri = wrapper.constructTableDiffChangeSetsUri(table.getDiffUri(), effectiveDataETag);

    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;

    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);
      String res = wrapper.convertResponseToString(response);

      ChangeSetList changeSets = ODKFileUtils.mapper.readValue(res, ChangeSetList.class);

      return changeSets;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  
  @Override
  public RowResourceList getChangeSet(TableResource table, String dataETag, boolean activeOnly, String websafeResumeCursor)
      throws HttpClientWebException, IOException {

    String tableId = table.getTableId();

    if ((table.getDataETag() == null) || dataETag == null) {
      throw new IllegalArgumentException("dataETag cannot be null!");
    }
    URI uri = wrapper.constructTableDiffChangeSetsForDataETagUri(table.getDiffUri(), dataETag,
        activeOnly, websafeResumeCursor);

    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;
    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);

      String res = wrapper.convertResponseToString(response);

      RowResourceList rows = ODKFileUtils.mapper.readValue(res, RowResourceList.class);

      return rows;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public RowResourceList getUpdates(TableResource table, String dataETag,
      String websafeResumeCursor, int fetchLimit)
          throws HttpClientWebException, IOException {

    String tableId = table.getTableId();
    URI uri;

    if ((table.getDataETag() == null) || dataETag == null) {
      uri = wrapper.constructTableDataUri(table.getDataUri(), websafeResumeCursor, fetchLimit);
    } else {
      uri = wrapper.constructTableDataDiffUri(table.getDiffUri(), dataETag, websafeResumeCursor, fetchLimit);
    }

    HttpGet request = new HttpGet(uri);
    CloseableHttpResponse response = null;

    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);

      String res = wrapper.convertResponseToString(response);

      RowResourceList rows = ODKFileUtils.mapper.readValue(res, RowResourceList.class);

      return rows;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public RowOutcomeList pushLocalRows(TableResource resource, OrderedColumns orderedColumns,
      List<org.opendatakit.database.data.TypedRow> rowsToInsertUpdateOrDelete) throws IOException,
      HttpClientWebException {

    ArrayList<Row> rows = new ArrayList<Row>();
    for (org.opendatakit.database.data.TypedRow rowToAlter : rowsToInsertUpdateOrDelete) {

      ArrayList<DataKeyValue> values = new ArrayList<DataKeyValue>();
      for (ColumnDefinition column : orderedColumns.getColumnDefinitions()) {
        if (column.isUnitOfRetention()) {
          String elementKey = column.getElementKey();
          values.add(new DataKeyValue(elementKey, rowToAlter.getStringValueByKey(elementKey)));
        }
      }

      Row row = Row.forUpdate(rowToAlter.getRawStringByKey(DataTableColumns.ID),
          rowToAlter.getRawStringByKey(DataTableColumns.ROW_ETAG),
          rowToAlter.getRawStringByKey(DataTableColumns.FORM_ID),
          rowToAlter.getRawStringByKey(DataTableColumns.LOCALE),
          rowToAlter.getRawStringByKey(DataTableColumns.SAVEPOINT_TYPE),
          rowToAlter.getRawStringByKey(DataTableColumns.SAVEPOINT_TIMESTAMP),
          rowToAlter.getRawStringByKey(DataTableColumns.SAVEPOINT_CREATOR),
          RowFilterScope.asRowFilter(rowToAlter.getRawStringByKey(DataTableColumns.DEFAULT_ACCESS),
              rowToAlter.getRawStringByKey(DataTableColumns.ROW_OWNER), rowToAlter.getRawStringByKey
                  (DataTableColumns.GROUP_READ_ONLY), rowToAlter.getRawStringByKey(DataTableColumns
                  .GROUP_MODIFY), rowToAlter.getRawStringByKey(DataTableColumns.GROUP_PRIVILEGED)),
          values);

      boolean isDeleted = SyncState.deleted.name().equals(
          rowToAlter.getDataByKey(DataTableColumns.SYNC_STATE));
      row.setDeleted(isDeleted);
      rows.add(row);
    }
    RowList rlist = new RowList(rows, resource.getDataETag());

    URI uri = URI.create(resource.getDataUri());
    HttpPut request = new HttpPut(uri);
    CloseableHttpResponse response = null;

    String rowListJSON = ODKFileUtils.mapper.writeValueAsString(rlist);
    HttpEntity entity = new GzipCompressingEntity(new StringEntity(rowListJSON,
        Charset.forName("UTF-8")));


    wrapper.buildJsonContentJsonResponseRequest(request);
    request.setEntity(entity);

    RowOutcomeList outcomes;

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_SC_CONFLICT);
      if ( response.getCode() == HttpStatus.SC_CONFLICT ) {
        return null;
      }
      String res = wrapper.convertResponseToString(response);
      outcomes = ODKFileUtils.mapper.readValue(res, RowOutcomeList.class);
      return outcomes;
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public FileManifestDocument getAppLevelFileManifest(
      String lastKnownLocalAppLevelManifestETag,
      String serverReportedAppLevelETag, boolean pushLocalFiles)
      throws HttpClientWebException, IOException {

    URI fileManifestUri = wrapper.constructAppLevelFileManifestUri();

    HttpGet request = new HttpGet(fileManifestUri);
    wrapper.buildNoContentJsonResponseRequest(request);

    // don't short-circuit manifest if we are pushing local files,
    // as we need to know exactly what is on the server to minimize
    // transmissions of files being pushed up to the server.
    if (!pushLocalFiles && lastKnownLocalAppLevelManifestETag != null) {
      request.addHeader(HttpHeaders.IF_NONE_MATCH, lastKnownLocalAppLevelManifestETag);
      if ( serverReportedAppLevelETag != null &&
          serverReportedAppLevelETag.equals(lastKnownLocalAppLevelManifestETag) ) {
        // no change -- we can skip the request to the server
        return null;
      }
    }

    CloseableHttpResponse response = null;
    List<OdkTablesFileManifestEntry> theList = null;

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_SC_NOT_MODIFIED);

      if (response.getCode() == HttpStatus.SC_NOT_MODIFIED) {
        // signal this by returning null;
        return null;
      }

      // update the manifest ETag record...
      String eTag = response.getFirstHeader(HttpHeaders.ETAG).getValue();

      String res = wrapper.convertResponseToString(response);

      // retrieve the manifest...
      OdkTablesFileManifest manifest;

      manifest = ODKFileUtils.mapper.readValue(res, OdkTablesFileManifest.class);

      if (manifest != null) {
        theList = manifest.getFiles();
      }

      if (theList == null) {
        theList = Collections.emptyList();
      }

      // if the server has no configuration for our client version, then we should
      // fail. It is likely that the user wanted to reset the app server to upload
      // a configuration.
      if (!pushLocalFiles && theList.isEmpty()) {
        throw new ClientDetectedMissingConfigForClientVersionException(
                "server has no configuration for this client version", request, response);
      }

      // and return the list of values...
      return new FileManifestDocument(eTag, theList);
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public FileManifestDocument getTableLevelFileManifest(String tableId,
      String lastKnownLocalTableLevelManifestETag,
      String serverReportedTableLevelETag,
      boolean pushLocalFiles)
      throws IOException,
      HttpClientWebException {

    URI fileManifestUri = wrapper.constructTableLevelFileManifestUri(tableId);

    HttpGet request = new HttpGet(fileManifestUri);
    wrapper.buildNoContentJsonResponseRequest(request);
    CloseableHttpResponse response = null;

    // don't short-circuit manifest if we are pushing local files,
    // as we need to know exactly what is on the server to minimize
    // transmissions of files being pushed up to the server.
    if (!pushLocalFiles && lastKnownLocalTableLevelManifestETag != null) {
      request.addHeader(HttpHeaders.IF_NONE_MATCH, lastKnownLocalTableLevelManifestETag);
      if ( serverReportedTableLevelETag != null &&
          serverReportedTableLevelETag.equals(lastKnownLocalTableLevelManifestETag) ) {
        // no change -- we can skip the request to the server
        return null;
      }
    }

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_SC_NOT_MODIFIED);

      if (response.getCode() == HttpStatus.SC_NOT_MODIFIED) {
        // signal this by returning null;
        return null;
      }

      // retrieve the manifest...
      List<OdkTablesFileManifestEntry> theList = null;

      // update the manifest ETag record...
      Header eTagHdr = response.getFirstHeader(HttpHeaders.ETAG);
      String eTag = eTagHdr.getValue();

      String res = wrapper.convertResponseToString(response);
      OdkTablesFileManifest manifest = ODKFileUtils.mapper.readValue(res, OdkTablesFileManifest.class);

      if (manifest != null) {
        theList = manifest.getFiles();
      }
      if (theList == null) {
        theList = Collections.emptyList();
      }

      // if the server has no configuration for our client version, then we should
      // fail. It is likely that the user wanted to reset the app server to upload
      // a configuration.
      if (!pushLocalFiles && theList.isEmpty()) {
        throw new ClientDetectedMissingConfigForClientVersionException(
                "server has no configuration for table at this client version", request, response);
      }

      // and return the list of values...
      return new FileManifestDocument(eTag, theList);
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public FileManifestDocument getRowLevelFileManifest(String serverInstanceFileUri,
      String tableId, String instanceId, SyncAttachmentState attachmentState,
      String lastKnownLocalRowLevelManifestETag)
      throws HttpClientWebException, IOException {

    URI instanceFileManifestUri =
        wrapper.constructInstanceFileManifestUri(serverInstanceFileUri, instanceId);

    HttpGet request = new HttpGet(instanceFileManifestUri);
    CloseableHttpResponse response = null;
    wrapper.buildNoContentJsonResponseRequest(request);

    if ( lastKnownLocalRowLevelManifestETag != null ) {
      request.addHeader(HttpHeaders.IF_NONE_MATCH, lastKnownLocalRowLevelManifestETag);
    }

    List<OdkTablesFileManifestEntry> theList = null;

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_SC_NOT_MODIFIED);

      if (response.getCode() == HttpStatus.SC_NOT_MODIFIED) {
        // signal this by returning null;
        return null;
      }

      // update the manifest ETag record...
      Header eTagHdr = response.getFirstHeader(HttpHeaders.ETAG);
      String eTag = eTagHdr.getValue();

      // retrieve the manifest...
      String res = wrapper.convertResponseToString(response);
      OdkTablesFileManifest manifest = ODKFileUtils.mapper.readValue(res, OdkTablesFileManifest.class);

      if (manifest != null) {
        theList = manifest.getFiles();
      }

      if (theList == null) {
        theList = Collections.emptyList();
      }
      log.i(LOGTAG, "returning a row-level manifest for " + instanceId);

      // and return the list of values...
      return new FileManifestDocument(eTag, theList);
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  /**
   * Download the file at the given URI to the specified local file.
   *
   * @param destFile
   * @param downloadUrl
   * @throws HttpClientWebException
   * @throws IOException
   */
  @Override
  public void downloadFile(File destFile, URI downloadUrl) throws HttpClientWebException,
      IOException {

    // WiFi network connections can be renegotiated during a large form download
    // sequence.
    // This will cause intermittent download failures. Silently retry once after
    // each
    // failure. Only if there are two consecutive failures, do we abort.
    boolean success = false;
    int attemptCount = 0;
    while (!success && attemptCount++ <= 2) {

      HttpGet request = new HttpGet(downloadUrl);
      // no body content-type and no response content-type requested
      wrapper.buildBasicRequest(request);
      if ( destFile.exists() ) {
        String md5Hash = ODKFileUtils.getMd5Hash(sc.getAppName(), destFile);
        request.addHeader(HttpHeaders.IF_NONE_MATCH, md5Hash);
      }

      CloseableHttpResponse response = null;
      try {
        response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_SC_NOT_MODIFIED);
        int statusCode = response.getCode();

        if (statusCode == HttpStatus.SC_NOT_MODIFIED) {
          log.i(LOGTAG, "downloading " + downloadUrl.toString() + " returns non-modified -- No-Op");
          return;
        }

        File tmp = new File(destFile.getParentFile(), destFile.getName() + ".tmp");
        int totalLen = 0;
        InputStream is = null;
        BufferedOutputStream os = null;
        try {
          // open the InputStream of the (uncompressed) entity body...
          is = response.getEntity().getContent();

          os = new BufferedOutputStream(new FileOutputStream(tmp));

          // write connection to temporary file
          byte buf[] = new byte[8192];
          int len;
          while ((len = is.read(buf, 0, buf.length)) >= 0) {
            if (len != 0) {
              totalLen += len;
              os.write(buf, 0, len);
            }
          }
          is.close();
          is = null;

          os.flush();
          os.close();
          os = null;

          success = tmp.renameTo(destFile);
        } catch (Exception e) {
          // most likely a socket timeout
          e.printStackTrace();
          log.e(LOGTAG,  "downloading " + downloadUrl.toString() + " failed after " + totalLen + " bytes: " + e.toString());
          try {
            // signal to the framework that this socket is hosed.
            // with the various nested streams, this may not work...
            is.reset();
          } catch ( Exception ex ) {
            // ignore
          }
          throw e;
        } finally {
          if (os != null) {
            try {
              os.close();
            } catch (Exception e) {
              // no-op
            }
          }
          if (is != null) {
            try {
              // ensure stream is consumed...
              byte buf[] = new byte[8192];
              while (is.read(buf) >= 0)
                ;
            } catch (Exception e) {
              // no-op
            }
            try {
              is.close();
            } catch (Exception e) {
              // no-op
            }
          }
          if (tmp.exists()) {
            tmp.delete();
          }

          if (response != null) {
            response.close();
          }
        }
      } catch (Exception e) {
        log.printStackTrace(e);
        if (attemptCount != 1) {
          throw e;
        }
      } finally {
        if ( response != null ) {
          EntityUtils.consumeQuietly(response.getEntity());
          response.close();
        }
      }
    }
  }

  @Override
  public void deleteConfigFile(File localFile) throws HttpClientWebException, IOException {
    String pathRelativeToConfigFolder = ODKFileUtils.asConfigRelativePath(sc.getAppName(),
        localFile);
    URI filesUri = wrapper.constructConfigFileUri(pathRelativeToConfigFolder);
    log.i(LOGTAG, "CLARICE:[deleteConfigFile] fileDeleteUri: " + filesUri.toString());

    HttpDelete request = new HttpDelete(filesUri);
    CloseableHttpResponse response = null;
    wrapper.buildNoContentJsonResponseRequest(request);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public void uploadConfigFile(File localFile) throws HttpClientWebException, IOException {
    String pathRelativeToConfigFolder = ODKFileUtils.asConfigRelativePath(sc.getAppName(),
        localFile);
    URI filesUri = wrapper.constructConfigFileUri(pathRelativeToConfigFolder);
    log.i(LOGTAG, "[uploadConfigFile] filePostUri: " + filesUri.toString());
    String ct = HttpRestProtocolWrapper.determineContentType(localFile.getName());
    ContentType contentType = ContentType.create(ct);

    CloseableHttpResponse response = null;
    HttpPost request = new HttpPost(filesUri);
    wrapper.buildSpecifiedContentJsonResponseRequest(contentType, request);

    HttpEntity entity = wrapper.makeHttpEntity(localFile);
    request.setEntity(entity);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_CREATED_SC_ACCEPTED);
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public void uploadInstanceFile(File file, URI instanceFileUri) throws HttpClientWebException,
      IOException
  {
    log.i(LOGTAG, "[uploadInstanceFile] filePostUri: " + instanceFileUri.toString());
    String ct = HttpRestProtocolWrapper.determineContentType(file.getName());
    ContentType contentType = ContentType.create(ct);

    CloseableHttpResponse response = null;
    HttpPost request = new HttpPost(instanceFileUri);
    wrapper.buildSpecifiedContentJsonResponseRequest(contentType, request);

    HttpEntity entity = wrapper.makeHttpEntity(file);
    request.setEntity(entity);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_CREATED);
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public CommonFileAttachmentTerms createCommonFileAttachmentTerms(String serverInstanceFileUri,
                                                                   String tableId, String instanceId, String rowpathUri) {

    File localFile =
        ODKFileUtils.getRowpathFile(sc.getAppName(), tableId, instanceId, rowpathUri);

    // use a cleaned-up rowpathUri in case there are leading slashes, instance paths, etc.
    String cleanRowpathUri = ODKFileUtils.asRowpathUri(sc.getAppName(), tableId, instanceId, localFile);

    URI instanceFileDownloadUri = wrapper.constructInstanceFileUri(serverInstanceFileUri,
        instanceId, cleanRowpathUri);

    CommonFileAttachmentTerms cat = new CommonFileAttachmentTerms();
    cat.rowPathUri = rowpathUri;
    cat.localFile = localFile;
    cat.instanceFileDownloadUri = instanceFileDownloadUri;

    return cat;
  }

  @Override
  public void uploadInstanceFileBatch(List<CommonFileAttachmentTerms> batch,
      String serverInstanceFileUri, String instanceId, String tableId) throws HttpClientWebException, IOException {

    URI instanceFilesUploadUri = wrapper.constructInstanceFileBulkUploadUri(serverInstanceFileUri, instanceId);
    String boundary = "ref" + UUID.randomUUID();

    NameValuePair params = new BasicNameValuePair("boundary", boundary);
    ContentType mt = ContentType.create(ContentType.MULTIPART_FORM_DATA.getMimeType(), params);

    HttpPost request = new HttpPost(instanceFilesUploadUri);
    CloseableHttpResponse response = null;
    wrapper.buildSpecifiedContentJsonResponseRequest(mt, request);

    MultipartEntityBuilder mpEntBuilder = MultipartEntityBuilder.create();

    mpEntBuilder.setBoundary(boundary);

    for (CommonFileAttachmentTerms cat : batch) {
      log.i(LOGTAG, "[uploadFile] filePostUri: " + cat.instanceFileDownloadUri.toString());
      String ct = HttpRestProtocolWrapper.determineContentType(cat.localFile.getName());

      String filename = ODKFileUtils
          .asRowpathUri(sc.getAppName(), tableId, instanceId, cat.localFile);
      filename = filename.replace("\"", "\"\"");

      FormBodyPartBuilder formPartBodyBld = FormBodyPartBuilder.create();
      formPartBodyBld.addField("Content-Disposition", "file;filename=\"" + filename + "\"");
      formPartBodyBld.addField("Content-Type", ct);

      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      InputStream is = null;
      try {
        is = new BufferedInputStream(new FileInputStream(cat.localFile));
        int length = 1024;
        // Transfer bytes from in to out
        byte[] data = new byte[length];
        int len;
        while ((len = is.read(data, 0, length)) >= 0) {
          if (len != 0) {
            bo.write(data, 0, len);
          }
        }
      } finally {
        if ( is != null ) {
          is.close();
        }
      }

      byte[] content = bo.toByteArray();

      ByteArrayBody byteArrayBod = new ByteArrayBody(content, filename);
      formPartBodyBld.setBody(byteArrayBod);
      formPartBodyBld.setName(filename);
      mpEntBuilder.addPart(formPartBodyBld.build());
    }

    HttpEntity mpFormEntity = mpEntBuilder.build();
    request.setEntity(mpFormEntity);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_CREATED);
    } finally {
      if ( response != null ) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public void downloadInstanceFileBatch(List<CommonFileAttachmentTerms> filesToDownload,
      String serverInstanceFileUri, String instanceId, String tableId) throws HttpClientWebException, IOException {
    // boolean downloadedAllFiles = true;

    URI instanceFilesDownloadUri = wrapper.constructInstanceFileBulkDownloadUri(serverInstanceFileUri, instanceId);

    ArrayList<OdkTablesFileManifestEntry> entries = new ArrayList<OdkTablesFileManifestEntry>();
    for (CommonFileAttachmentTerms cat : filesToDownload) {
      OdkTablesFileManifestEntry entry = new OdkTablesFileManifestEntry();
      entry.filename = cat.rowPathUri;
      entries.add(entry);
    }

    OdkTablesFileManifest manifest = new OdkTablesFileManifest();
    manifest.setFiles(entries);

    String boundaryVal = null;
    InputStream inStream = null;
    OutputStream os = null;

    HttpPost request = new HttpPost(instanceFilesDownloadUri);
    CloseableHttpResponse response = null;

    // no body content-type and no response content-type requested
    wrapper.buildBasicRequest(request);
    request.addHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType());

    String fileManifestEntries = ODKFileUtils.mapper.writeValueAsString(manifest);

    HttpEntity entity = new StringEntity(fileManifestEntries,
        Charset.forName("UTF-8"));

    request.setEntity(entity);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);

      HttpEntity et = response.getEntity();
      String str = et.getContentType();

      String [] keyValues= str.split(";");
      for (String kvp : keyValues) {
        if (kvp.contains(HttpRestProtocolWrapper.BOUNDARY + "=" + HttpRestProtocolWrapper.BOUNDARY)) {
          String[] parts = kvp.split("=");
          String nvp_name = parts[0].trim();
          String nvp_value = parts[1].trim();
          if (nvp_name.equals(HttpRestProtocolWrapper.BOUNDARY)) {
            boundaryVal = nvp_value;
            break;
          }
        }
      }

      // Best to return at this point if we can't
      // determine the boundary to parse the multi-part form
      if (boundaryVal == null) {
        throw new ClientDetectedVersionMismatchedServerResponseException(
            "unable to extract boundary parameter", request, response);
      }

      inStream = response.getEntity().getContent();

      byte[] msParam = boundaryVal.getBytes(Charset.forName("UTF-8"));
      MultipartStream multipartStream = new MultipartStream(inStream, msParam, DEFAULT_BOUNDARY_BUFSIZE, null);

      // Parse the request
      boolean nextPart = multipartStream.skipPreamble();
      while (nextPart) {
        String header = multipartStream.readHeaders();
        System.out.println("Headers: " + header);

        String partialPath = wrapper.extractInstanceFileRelativeFilename(header);

        if (partialPath == null) {
          log.e("putAttachments", "Server did not identify the rowPathUri for the file");
          throw new ClientDetectedVersionMismatchedServerResponseException(
              "server did not specify rowPathUri for file", request, response);
        }

        File instFile = ODKFileUtils
            .getRowpathFile(sc.getAppName(), tableId, instanceId, partialPath);

        os = new BufferedOutputStream(new FileOutputStream(instFile));

        multipartStream.readBodyData(os);
        os.flush();
        os.close();

        nextPart = multipartStream.readBoundary();
      }
    } finally {
      if (os != null) {
        try {
          os.close();
        } catch (IOException e) {
          e.printStackTrace();
          System.out.println("batchGetFilesForRow: Download file batches: Error closing output stream");
        }
      }
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public void publishTableSyncStatus(TableResource resource, Map<String, Object> statusMap)
      throws HttpClientWebException, IOException {

    // build request
    URI uri = wrapper.constructRealizedTableIdSyncStatusUri(resource.getTableId(),
        resource.getSchemaETag());
    CloseableHttpResponse response = null;
    HttpPost request = new HttpPost(uri);
    wrapper.buildJsonContentJsonResponseRequest(request);

    // and augment with info about the
    HttpEntity entity = new GzipCompressingEntity(
        new StringEntity(ODKFileUtils.mapper.writeValueAsString(statusMap), Charset.forName("UTF-8")));
    request.setEntity(entity);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);
    } finally {
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }

  @Override
  public void publishDeviceInformation(Map<String, Object> statusMap)
      throws HttpClientWebException, IOException {

    // build request
    URI uri = wrapper.constructDeviceInformationUri();
    CloseableHttpResponse response = null;
    HttpPost request = new HttpPost(uri);
    wrapper.buildJsonContentJsonResponseRequest(request);

    // and augment with info about the
    HttpEntity entity = new GzipCompressingEntity(
        new StringEntity(ODKFileUtils.mapper.writeValueAsString(statusMap), Charset.forName("UTF-8")));
    request.setEntity(entity);

    try {
      response = wrapper.httpClientExecute(request, HttpRestProtocolWrapper.SC_OK_ONLY);
    } finally {
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
        response.close();
      }
    }
  }
}
