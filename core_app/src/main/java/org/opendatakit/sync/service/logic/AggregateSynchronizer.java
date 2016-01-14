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

import java.io.*;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.RuntimeDelegate;

import android.util.Log;
import org.apache.commons.lang3.CharEncoding;
import org.apache.http.HeaderElement;
import org.apache.http.HttpStatus;
import org.apache.http.message.BasicHeaderValueParser;
import org.apache.http.message.HeaderValueParser;
import org.apache.wink.client.ClientConfig;
import org.apache.wink.client.ClientResponse;
import org.apache.wink.client.ClientWebException;
import org.apache.wink.client.EntityType;
import org.apache.wink.client.Resource;
import org.apache.wink.client.RestClient;
import org.apache.wink.client.internal.handlers.GzipHandler;
import org.apache.wink.common.model.multipart.BufferedOutMultiPart;
import org.apache.wink.common.model.multipart.InMultiPart;
import org.apache.wink.common.model.multipart.InPart;
import org.apache.wink.common.model.multipart.OutPart;
import org.opendatakit.aggregate.odktables.rest.ApiConstants;
import org.opendatakit.aggregate.odktables.rest.entity.ChangeSetList;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.OdkTablesFileManifest;
import org.opendatakit.aggregate.odktables.rest.entity.OdkTablesFileManifestEntry;
import org.opendatakit.aggregate.odktables.rest.entity.Row;
import org.opendatakit.aggregate.odktables.rest.entity.RowList;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.RowResourceList;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinition;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinitionResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResourceList;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.android.utilities.WebLoggerIf;
import org.opendatakit.core.R;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.sync.service.SyncAttachmentState;
import org.opendatakit.sync.service.SyncExecutionContext;
import org.opendatakit.sync.service.data.SyncRow;
import org.opendatakit.sync.service.data.SyncRowPending;
import org.opendatakit.sync.service.exceptions.InvalidAuthTokenException;
import org.opendatakit.sync.service.SyncProgressState;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.os.RemoteException;
import org.opendatakit.sync.service.transport.ODKClientApplication;
import org.opendatakit.sync.service.transport.ReAuthSecurityHandler;

/**
 * Implementation of {@link Synchronizer} for ODK Aggregate.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class AggregateSynchronizer implements Synchronizer {

  private static final String LOGTAG = AggregateSynchronizer.class.getSimpleName();
  private static final String TOKEN_INFO = "https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=";
  public static final int CONNECTION_TIMEOUT = 45000;

  public static final long MAX_BATCH_SIZE = 10485760;

  // parameters for queries that could return a lot of data...
  public static final String CURSOR_PARAMETER = "cursor";
  public static final String FETCH_LIMIT = "fetchLimit";

  // parameter for file downloads -- if we want to have it come down as an attachment.
  public static final String PARAM_AS_ATTACHMENT = "as_attachment";

  // parameters for data/diff/query APIs.
  public static final String QUERY_DATA_ETAG = "data_etag";
  public static final String QUERY_SEQUENCE_VALUE = "sequence_value";
  public static final String QUERY_ACTIVE_ONLY = "active_only";
  // parameters for query API
  public static final String QUERY_START_TIME = "startTime";
  public static final String QUERY_END_TIME = "endTime";

  /** Timeout (in ms) we specify for each http request */
  public static final int HTTP_REQUEST_TIMEOUT_MS = 30 * 1000;
  /** Path to the file servlet on the Aggregate server. */

  private static final String FORWARD_SLASH = "/";

  static Map<String, String> mimeMapping;
  static {

    Map<String, String> m = new HashMap<String, String>();
    m.put("jpeg", "image/jpeg");
    m.put("jpg", "image/jpeg");
    m.put("png", "image/png");
    m.put("gif", "image/gif");
    m.put("pbm", "image/x-portable-bitmap");
    m.put("ico", "image/x-icon");
    m.put("bmp", "image/bmp");
    m.put("tiff", "image/tiff");

    m.put("mp2", "audio/mpeg");
    m.put("mp3", "audio/mpeg");
    m.put("wav", "audio/x-wav");

    m.put("asf", "video/x-ms-asf");
    m.put("avi", "video/x-msvideo");
    m.put("mov", "video/quicktime");
    m.put("mpa", "video/mpeg");
    m.put("mpeg", "video/mpeg");
    m.put("mpg", "video/mpeg");
    m.put("mp4", "video/mp4");
    m.put("qt", "video/quicktime");

    m.put("css", "text/css");
    m.put("htm", "text/html");
    m.put("html", "text/html");
    m.put("csv", "text/csv");
    m.put("txt", "text/plain");
    m.put("log", "text/plain");
    m.put("rtf", "application/rtf");
    m.put("pdf", "application/pdf");
    m.put("zip", "application/zip");
    m.put("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    m.put("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
    m.put("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation");
    m.put("xml", "application/xml");
    m.put("js", "application/x-javascript");
    m.put("json", "application/x-javascript");
    mimeMapping = m;

    // Android does not support Runtime delegation. Set it up manually...
    // force this once...
    org.apache.wink.common.internal.runtime.RuntimeDelegateImpl rd = new org.apache.wink.common.internal.runtime.RuntimeDelegateImpl();
    RuntimeDelegate.setInstance(rd);
  }

  private SyncExecutionContext sc;
  private String accessToken;
  private final RestClient tokenRt;
  private final RestClient rt;
  private final Map<String, TableResource> resources;
  /** normalized aggregateUri */
  private final URI baseUri;
  private final WebLoggerIf log;
  // cookie manager 
  private final CookieManager cm;


  private final URI normalizeUri(String aggregateUri, String additionalPathPortion) {
    URI uriBase = URI.create(aggregateUri).normalize();
    String term = uriBase.getPath();
    if (term.endsWith(FORWARD_SLASH)) {
      if (additionalPathPortion.startsWith(FORWARD_SLASH)) {
        term = term.substring(0, term.length() - 1);
      }
    } else if (!additionalPathPortion.startsWith(FORWARD_SLASH)) {
      term = term + FORWARD_SLASH;
    }
    term = term + additionalPathPortion;
    URI uri = uriBase.resolve(term).normalize();
    log.d(LOGTAG, "normalizeUri: " + uri.toString());
    return uri;
  }

  private static final String escapeSegment(String segment) {
    return segment;
    // String encoding = CharEncoding.UTF_8;
    // String encodedSegment;
    // try {
    // encodedSegment = URLEncoder.encode(segment, encoding)
    // .replaceAll("\\+", "%20")
    // .replaceAll("\\%21", "!")
    // .replaceAll("\\%27", "'")
    // .replaceAll("\\%28", "(")
    // .replaceAll("\\%29", ")")
    // .replaceAll("\\%7E", "~");
    //
    // } catch (UnsupportedEncodingException e) {
    // log.printStackTrace(e);
    // throw new IllegalStateException("Should be able to encode with " +
    // encoding);
    // }
    // return encodedSegment;
  }

  /**
   * Format a file path to be pushed up to aggregate. Essentially escapes the
   * string as for an html url, but leaves forward slashes. The path must begin
   * with a forward slash, as if starting at the root directory.
   * 
   * @return a properly escaped url, with forward slashes remaining.
   */
  private String uriEncodeSegments(String path) {
    String[] parts = path.split("/");
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < parts.length; ++i) {
      if (i != 0) {
        b.append("/");
      }
      b.append(escapeSegment(parts[i]));
    }
    String escaped = b.toString();
    return escaped;
  }

  private String getTablesUriFragment() {
    /**
     * Path to the tables servlet (the one that manages table definitions) on
     * the Aggregate server.
     */
    return "/odktables/" + escapeSegment(sc.getAppName()) + "/tables/";
  }

  private String getManifestUriFragment() {
    /**
     * Path to the tables servlet (the one that manages table definitions) on
     * the Aggregate server.
     */
    return "/odktables/" + escapeSegment(sc.getAppName()) + "/manifest/"
        + escapeSegment(sc.getOdkClientApiVersion()) + "/";
  }

  /**
   * Get the URI for the file servlet on the Aggregate server located at
   * aggregateUri.
    *
    * @return
    */
  private String getFilePathURI() {
    return "/odktables/" + escapeSegment(sc.getAppName()) + "/files/"
        + escapeSegment(sc.getOdkClientApiVersion()) + "/";
  }
  /**
   * Simple Resource for all server interactions.
   * 
   * @param uri
   * @return
   * @throws InvalidAuthTokenException 
   */
  private Resource buildBasicResource(URI uri) throws InvalidAuthTokenException {

    Resource rsc = rt.resource(uri);

    // report our locale... (not currently used by server)
    rsc.acceptLanguage(Locale.getDefault());

    // set the access token...
    rsc.header(ApiConstants.ACCEPT_CONTENT_ENCODING_HEADER, ApiConstants.GZIP_CONTENT_ENCODING);
    rsc.header(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER, ApiConstants.OPEN_DATA_KIT_VERSION);
    rsc.header(HttpHeaders.USER_AGENT, sc.getUserAgent());
    GregorianCalendar g = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
    Date now = new Date();
    g.setTime(now);
    SimpleDateFormat formatter = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss zz", Locale.US);
    formatter.setCalendar(g);
    rsc.header(ApiConstants.DATE_HEADER, formatter.format(now));

    if (accessToken != null && baseUri != null) {
      if (uri.getHost().equals(baseUri.getHost()) && uri.getPort() == baseUri.getPort()) {
        rsc.header("Authorization", "Bearer " + accessToken);
      }
    }
    
    return rsc;
  }

  private Resource buildResource(URI uri, MediaType contentType) throws InvalidAuthTokenException {

    Resource rsc = buildBasicResource(uri);

    rsc.contentType(contentType);

    // set our preferred response media type to json using quality parameters
    Map<String, String> mediaTypeParams;
    // we really like JSON
    mediaTypeParams = new HashMap<String, String>();
    mediaTypeParams.put("q", "1.0");
    MediaType json = new MediaType(MediaType.APPLICATION_JSON_TYPE.getType(),
        MediaType.APPLICATION_JSON_TYPE.getSubtype(), mediaTypeParams);
    // don't really want plaintext...
    mediaTypeParams = new HashMap<String, String>();
    mediaTypeParams.put("charset", CharEncoding.UTF_8.toLowerCase(Locale.ENGLISH));
    mediaTypeParams.put("q", "0.4");
    MediaType tplainUtf8 = new MediaType(MediaType.TEXT_PLAIN_TYPE.getType(),
        MediaType.TEXT_PLAIN_TYPE.getSubtype(), mediaTypeParams);

    // accept either json or plain text (no XML to device)
    rsc.accept(json, tplainUtf8);

    // set the response entity character set to CharEncoding.UTF_8
    rsc.header("Accept-Charset", CharEncoding.UTF_8);
    
    return rsc;
  }

  /**
   * Simple Resource for file download.
   * 
   * @param uri
   * @return
   * @throws InvalidAuthTokenException 
   */
  private Resource buildFileDownloadResource(URI uri) throws InvalidAuthTokenException {

    Resource rsc = buildBasicResource(uri);
    
    return rsc;
  }

  private Resource buildResource(URI uri) throws InvalidAuthTokenException {

    // select our preferred protocol...
    MediaType protocolType = MediaType.APPLICATION_JSON_TYPE;
    
    Resource rsc = buildResource(uri, protocolType);
    
    return rsc;
  }

  public AggregateSynchronizer(SyncExecutionContext sc) throws
      InvalidAuthTokenException {
    this.sc = sc;
    this.log = WebLogger.getLogger(sc.getAppName());
    log.e(LOGTAG, "AggregateUri:" + sc.getAggregateUri());
    this.baseUri = normalizeUri(sc.getAggregateUri(), "/");
    log.e(LOGTAG, "baseUri:" + baseUri);

    // This is technically not correct, as we should really have a global
    // that we manage for this... If there are two or more service threads
    // running, we could forget other session cookies. But, by creating a 
    // new cookie manager here, we ensure that we don't have any stale 
    // session cookies at the start of each sync.
    
    cm = new CookieManager();
    CookieHandler.setDefault(cm);
    
    // now everything should work...
    ClientConfig cc;

    cc = new ClientConfig();
    cc.setLoadWinkApplications(false);
    cc.applications(new ODKClientApplication());
    cc.handlers(new GzipHandler(), new ReAuthSecurityHandler(this));
    cc.connectTimeout(CONNECTION_TIMEOUT);
    cc.readTimeout(2 * CONNECTION_TIMEOUT);
    cc.followRedirects(true);

    this.rt = new RestClient(cc);

    cc = new ClientConfig();
    cc.setLoadWinkApplications(false);
    cc.applications(new ODKClientApplication());
    cc.connectTimeout(CONNECTION_TIMEOUT);
    cc.readTimeout(2 * CONNECTION_TIMEOUT);
    cc.followRedirects(true);

    this.tokenRt = new RestClient(cc);

    this.resources = new HashMap<String, TableResource>();

    String accessToken = sc.getAccessToken();
    checkAccessToken(accessToken);
    this.accessToken = accessToken;
  }

  private final static String authString = "oauth2:https://www.googleapis.com/auth/userinfo.email";

  public String updateAccessToken() throws InvalidAuthTokenException {
    try {
      AccountManager accountManager = sc.getAccountManager();
      Account account = sc.getAccount();
      this.accessToken = accountManager.blockingGetAuthToken(account, authString, true);
      return accessToken;
    } catch (Exception e) {
      e.printStackTrace();
      throw new InvalidAuthTokenException("unable to update access token -- please re-authorize");
    }
  }
  
  private void checkAccessToken(String accessToken) throws InvalidAuthTokenException {
    try {
      @SuppressWarnings("unused")
      Object responseEntity = tokenRt.resource(
          TOKEN_INFO + URLEncoder.encode(accessToken, ApiConstants.UTF8_ENCODE)).get(Object.class);
    } catch (ClientWebException e) {
      log.e(LOGTAG, "HttpClientErrorException in checkAccessToken");
      Object o = null;
      try {
        String entityBody = e.getResponse().getEntity(String.class);
        o = ODKFileUtils.mapper.readValue(entityBody, Object.class);
      } catch (Exception e1) {
        log.printStackTrace(e1);
        throw new InvalidAuthTokenException(
            "Unable to parse response from auth token verification (" + e.toString() + ")", e);
      }
      if (o != null && o instanceof Map) {
        @SuppressWarnings("rawtypes")
        Map m = (Map) o;
        if (m.containsKey("error")) {
          throw new InvalidAuthTokenException("Invalid auth token (" + m.get("error").toString()
              + "): " + accessToken, e);
        } else {
          throw new InvalidAuthTokenException("Unknown response from auth token verification ("
              + e.toString() + ")", e);
        }
      }
    } catch (Exception e) {
      log.e(LOGTAG, "HttpClientErrorException in checkAccessToken");
      log.printStackTrace(e);
      throw new InvalidAuthTokenException("Invalid auth token (): " + accessToken, e);
    }
  }

  @Override
  public TableResourceList getTables(String webSafeResumeCursor) throws ClientWebException, InvalidAuthTokenException {

    TableResourceList tableResources;
    try {
      String tableFrag = getTablesUriFragment();
      tableFrag = tableFrag.substring(0, tableFrag.length() - 1);
      URI uri = normalizeUri(sc.getAggregateUri(), tableFrag);
      if ( webSafeResumeCursor == null ) {
        tableResources = buildResource(uri).get(TableResourceList.class);
      } else {
        tableResources = buildResource(uri).queryParam(CURSOR_PARAMETER, webSafeResumeCursor)
            .get(TableResourceList.class);
      }
    } catch (ClientWebException e) {
      log.e(LOGTAG, "Exception while requesting list of tables from server: " + e.toString());
      throw e;
    }

    return tableResources;
  }

  @Override
  public TableDefinitionResource getTableDefinition(String tableDefinitionUri)
      throws ClientWebException, InvalidAuthTokenException {
    URI uri = URI.create(tableDefinitionUri).normalize();
    TableDefinitionResource definitionRes = buildResource(uri).get(TableDefinitionResource.class);
    return definitionRes;
  }

  @Override
  public URI constructTableInstanceFileUri(String tableId, String schemaETag) {
    // e.g., https://msundt-test.appspot.com:443/odktables/tables/tables/Milk_bank/ref/uuid:bb26cdaf-9ccf-4a4f-8e28-c114fe30358a/attachments
    URI instanceFileUri = normalizeUri(sc.getAggregateUri(), getTablesUriFragment() + tableId + "/ref/" + schemaETag + "/attachments");
    return instanceFileUri;
  }
  
  @Override
  public TableResource createTable(String tableId, String schemaETag, ArrayList<Column> columns)
      throws ClientWebException, InvalidAuthTokenException {

    // build request
    URI uri = normalizeUri(sc.getAggregateUri(), getTablesUriFragment() + tableId);
    TableDefinition definition = new TableDefinition(tableId, schemaETag, columns);
    // create table
    TableResource resource;
    try {
      // TODO: we also need to put up the key value store/properties.
      resource = buildResource(uri).put(new EntityType<TableResource>() {
      }, definition);
    } catch (ClientWebException e) {
      log.e(LOGTAG,
          "ResourceAccessException in createTable: " + tableId + " exception: " + e.toString());
      throw e;
    }

    // save resource
    this.resources.put(resource.getTableId(), resource);
    return resource;
  }

  @Override
  public void deleteTable(TableResource table) throws ClientWebException, InvalidAuthTokenException {
    URI uri = URI.create(table.getDefinitionUri()).normalize();
    buildResource(uri).delete();
    this.resources.remove(table.getTableId());
  }
  
  @Override
  public ChangeSetList getChangeSets(TableResource table, String dataETag) throws ClientWebException, InvalidAuthTokenException {

    String tableId = table.getTableId();
    URI uri;
    Resource resource;
    uri = normalizeUri(table.getDiffUri(), "/changeSets");
    resource = buildResource(uri);
    if ((table.getDataETag() != null) && dataETag != null) {
      resource = buildResource(uri).queryParam(QUERY_DATA_ETAG, dataETag);
    }
    
    ChangeSetList changeSets;
    try {
      changeSets = resource.get(ChangeSetList.class);
      return changeSets;
    } catch (ClientWebException e) {
      log.e(LOGTAG, "Exception while requesting list of changeSets from server: " + tableId
          + " exception: " + e.toString());
      throw e;
    }
  }
  
  @Override
  public RowResourceList getChangeSet(TableResource table, String dataETag, boolean activeOnly, String websafeResumeCursor)
      throws ClientWebException, InvalidAuthTokenException {

    String tableId = table.getTableId();
    URI uri;
    Resource resource;
    if ((table.getDataETag() == null) || dataETag == null) {
      throw new IllegalArgumentException("dataETag cannot be null!");
    }
    
    uri = normalizeUri(table.getDiffUri(), "/changeSets/" + dataETag);
    resource = buildResource(uri);
    
    if ( activeOnly ) {
      resource = resource.queryParam(QUERY_ACTIVE_ONLY, "true");
    }

    // and apply the cursor...
    if ( websafeResumeCursor != null ) {
      resource = resource.queryParam(CURSOR_PARAMETER, websafeResumeCursor);
    }
    
    RowResourceList rows;
    try {
      rows = resource.get(RowResourceList.class);
      return rows;
    } catch (ClientWebException e) {
      log.e(LOGTAG, "Exception while requesting changeSet rows from server: " + tableId
          + " exception: " + e.toString());
      throw e;
    }
  }


  @Override
  public RowResourceList getUpdates(TableResource table, String dataETag, String websafeResumeCursor)
      throws ClientWebException, InvalidAuthTokenException {

    String tableId = table.getTableId();
    URI uri;
    Resource resource;
    if ((table.getDataETag() == null) || dataETag == null) {
      uri = URI.create(table.getDataUri());
      resource = buildResource(uri);
    } else {
      uri = URI.create(table.getDiffUri());
      resource = buildResource(uri).queryParam(QUERY_DATA_ETAG, dataETag);
    }
    // and apply the cursor...
    if ( websafeResumeCursor != null ) {
      resource = resource.queryParam(CURSOR_PARAMETER, websafeResumeCursor);
    }
    RowResourceList rows;
    try {
      rows = resource.get(RowResourceList.class);
      return rows;
    } catch (ClientWebException e) {
      log.e(LOGTAG, "Exception while requesting list of rows from server: " + tableId
          + " exception: " + e.toString());
      throw e;
    }
  }

  @Override
  public RowOutcomeList alterRows(TableResource resource,
      List<SyncRow> rowsToInsertUpdateOrDelete) throws ClientWebException, InvalidAuthTokenException {

    ArrayList<Row> rows = new ArrayList<Row>();
    for (SyncRow rowToAlter : rowsToInsertUpdateOrDelete) {
      Row row = Row.forUpdate(rowToAlter.getRowId(), rowToAlter.getRowETag(),
          rowToAlter.getFormId(), rowToAlter.getLocale(),
          rowToAlter.getSavepointType(), rowToAlter.getSavepointTimestamp(),
          rowToAlter.getSavepointCreator(), rowToAlter.getFilterScope(),
          rowToAlter.getValues());
      row.setDeleted(rowToAlter.isDeleted());
      rows.add(row);
    }
    RowList rlist = new RowList(rows, resource.getDataETag());

    URI uri = URI.create(resource.getDataUri());
    RowOutcomeList outcomes;
    try {
      outcomes = buildResource(uri).put(new EntityType<RowOutcomeList>() {
      }, rlist);
    } catch (ClientWebException e) {
      log.e(LOGTAG,
          "Exception while updating rows on server: " + resource.getTableId() + " exception: " + e.toString());
      throw e;
    }
    return outcomes;
  }

  private List<String> getAppLevelFiles() {
    File baseFolder = new File(ODKFileUtils.getAppFolder(sc.getAppName()));

    // Return an empty list of the folder doesn't exist or is not a directory
    if (!baseFolder.exists()) {
      return new ArrayList<String>();
    } else if (!baseFolder.isDirectory()) {
      log.e(LOGTAG, "[getAppLevelFiles] application folder is not a directory: " +
          baseFolder.getAbsolutePath());
      return new ArrayList<String>();
    }

    baseFolder = new File(ODKFileUtils.getConfigFolder(sc.getAppName()));
    // Return an empty list of the folder doesn't exist or is not a directory
    if (!baseFolder.exists()) {
      return new ArrayList<String>();
    } else if (!baseFolder.isDirectory()) {
      log.e(LOGTAG, "[getAppLevelFiles] config folder is not a directory: " +
          baseFolder.getAbsolutePath());
      return new ArrayList<String>();
    }

    LinkedList<File> unexploredDirs = new LinkedList<File>();
    List<String> relativePaths = new ArrayList<String>();
    
    unexploredDirs.add(baseFolder);

    boolean haveFilteredTablesDir = false;
    boolean haveFilteredAssetsCsvDir = false;
    boolean haveFilteredTableInitFile = false;
    
    while (!unexploredDirs.isEmpty()) {
      File exploring = unexploredDirs.removeFirst();
      File[] files = exploring.listFiles();
      for (File f : files) {
        if (f.isDirectory()) {

          // ignore the config/tables dir
          if ( !haveFilteredTablesDir ) {
            File tablesDir = new File(ODKFileUtils.getTablesFolder(sc.getAppName()));
            if ( f.equals(tablesDir) ) {
              haveFilteredTablesDir = true;
              continue;
            }
          }
          // ignore the config/assets/csv dir
          if ( !haveFilteredAssetsCsvDir ) {
            File csvDir = new File(ODKFileUtils.getAssetsCsvFolder(sc.getAppName()));
            if ( f.equals(csvDir) ) {
              haveFilteredAssetsCsvDir = true;
              continue;
            }
          }

          // we'll need to explore it
          unexploredDirs.add(f);
        } else {

          // ignore the config/assets/tables.init file -- never sync'd to server...
          if ( !haveFilteredTableInitFile ) {
            File tablesInitFile = new File(ODKFileUtils.getTablesInitializationFile(sc.getAppName()));
            if ( f.equals(tablesInitFile) ) {
              haveFilteredTableInitFile = true;
              continue;
            }
          }

          // we'll add it to our list of files.
          relativePaths.add(ODKFileUtils.asRelativePath(sc.getAppName(), f));
        }
      }
    }

    return relativePaths; 
  }

  private static List<String> filterInTableIdFiles(List<String> relativePaths, String tableId) {
    List<String> newList = new ArrayList<String>();
    for (String relativePath : relativePaths) {
      if (relativePath.startsWith("config/assets/csv/")) {
        // by convention, the files here begin with their identifying tableId
        String[] parts = relativePath.split("/");
        if (parts.length >= 4) {
          String[] nameElements = parts[3].split("\\.");
          if (nameElements[0].equals(tableId)) {
            newList.add(relativePath);
          }
        }
      }
    }
    return newList;
  }

  /**
   * Get all the files under the given folder, excluding those directories that
   * are the concatenation of folder and a member of excluding. If the member of
   * excluding is a directory, none of its children will be reported.
   * <p>
   * If the baseFolder doesn't exist it returns an empty list.
   * <p>
   * If the baseFolder exists but is not a directory, logs an error and returns an
   * empty list.
   * 
   * @param baseFolder
   * @param excludingNamedItemsUnderFolder
   *          can be null--nothing will be excluded. Should be relative to the
   *          given folder.
   * @return list of app-relative paths of the files and directories that were found.
   */
  private List<String> getAllFilesUnderFolder(File baseFolder,
      final Set<String> excludingNamedItemsUnderFolder) {
    String appName = ODKFileUtils.extractAppNameFromPath(baseFolder);

    // Return an empty list of the folder doesn't exist or is not a directory
    if (!baseFolder.exists()) {
      return new ArrayList<String>();
    } else if (!baseFolder.isDirectory()) {
      log.e(LOGTAG, "[getAllFilesUnderFolder] folder is not a directory: " + baseFolder.getAbsolutePath());
      return new ArrayList<String>();
    }

    // construct the set of starting directories and files to process
    File[] partials = baseFolder.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        if (excludingNamedItemsUnderFolder == null) {
          return true;
        } else {
          return !excludingNamedItemsUnderFolder.contains(pathname.getName());
        }
      }
    });

    if (partials == null) {
      return Collections.emptyList();
    }

    LinkedList<File> unexploredDirs = new LinkedList<File>();
    List<File> nondirFiles = new ArrayList<File>();

    // copy the starting set into a queue of unexploredDirs
    // and a list of files to be sync'd
    for (int i = 0; i < partials.length; ++i) {
      if (partials[i].isDirectory()) {
        unexploredDirs.add(partials[i]);
      } else {
        nondirFiles.add(partials[i]);
      }
    }

    while (!unexploredDirs.isEmpty()) {
      File exploring = unexploredDirs.removeFirst();
      File[] files = exploring.listFiles();
      for (File f : files) {
        if (f.isDirectory()) {
          // we'll need to explore it
          unexploredDirs.add(f);
        } else {
          // we'll add it to our list of files.
          nondirFiles.add(f);
        }
      }
    }

    List<String> relativePaths = new ArrayList<String>();
    // we want the relative path, so drop the necessary bets.
    for (File f : nondirFiles) {
      // +1 to exclude the separator.
      relativePaths.add(ODKFileUtils.asRelativePath(sc.getAppName(), f));
    }
    return relativePaths;
  }

  @Override
  public boolean syncAppLevelFiles(boolean pushLocalFiles, String serverReportedAppLevelETag, SynchronizerStatus syncStatus)
      throws ClientWebException, InvalidAuthTokenException {
    // Get the app-level files on the server.
    syncStatus.updateNotification(SyncProgressState.APP_FILES, R.string
            .sync_getting_app_level_manifest,
        null, 1.0, false);
    List<OdkTablesFileManifestEntry> manifest = getAppLevelFileManifest(pushLocalFiles, serverReportedAppLevelETag);

    if (manifest == null) {
      log.i(LOGTAG, "no change in app-level manifest -- skipping!");
      // short-circuited -- no change in manifest
      syncStatus.updateNotification(SyncProgressState.APP_FILES,
          R.string.sync_getting_app_level_manifest, null, 100.0, false);
      return true;
    }

    // Get the app-level files on our device.
    List<String> relativePathsOnDevice = getAppLevelFiles();

    boolean success = true;
    double stepSize = 100.0 / (1 + relativePathsOnDevice.size() + manifest.size());
    int stepCount = 1;

    if (pushLocalFiles) {
      // if we are pushing, we want to push the local files that are different
      // up to the server, then remove the files on the server that are not
      // in the local set.
      List<File> serverFilesToDelete = new ArrayList<File>();

      for (OdkTablesFileManifestEntry entry : manifest) {
        File localFile = ODKFileUtils.asConfigFile(sc.getAppName(), entry.filename);
        if (!localFile.exists() || !localFile.isFile()) {
          // we need to delete this file from the server.
          serverFilesToDelete.add(localFile);
        } else if (ODKFileUtils.getMd5Hash(sc.getAppName(), localFile).equals(entry.md5hash)) {
          // we are ok -- no need to upload or delete
          relativePathsOnDevice.remove(ODKFileUtils.asRelativePath(sc.getAppName(), localFile));
        }
      }

      // this is the actual step size when we are pushing...
      stepSize = 100.0 / (1 + relativePathsOnDevice.size() + serverFilesToDelete.size());

      for (String relativePath : relativePathsOnDevice) {

        syncStatus.updateNotification(SyncProgressState.APP_FILES, R.string
                .sync_uploading_local_file,
            new Object[] { relativePath }, stepCount * stepSize, false);

        File localFile = ODKFileUtils.asAppFile(sc.getAppName(), relativePath);
        if (!uploadConfigFile(localFile)) {
          success = false;
          log.e(LOGTAG, "Unable to upload file to server: " + relativePath);
        }

        ++stepCount;
      }

      for (File localFile : serverFilesToDelete) {

        String relativePath = ODKFileUtils.asRelativePath(sc.getAppName(), localFile);
        syncStatus.updateNotification(SyncProgressState.APP_FILES,
            R.string.sync_deleting_file_on_server,
            new Object[] { relativePath }, stepCount * stepSize, false);

        if (!deleteConfigFile(localFile)) {
          success = false;
          log.e(LOGTAG, "Unable to delete file on server: " + relativePath);
        }

        ++stepCount;
      }
    } else {
      // if we are pulling, we want to pull the server files that are different
      // down from the server, then remove the local files that are not present
      // on the server.

      for (OdkTablesFileManifestEntry entry : manifest) {
        File localFile = ODKFileUtils.asConfigFile(sc.getAppName(), entry.filename);
        String relativePath = ODKFileUtils.asRelativePath(sc.getAppName(), localFile);

        syncStatus.updateNotification(SyncProgressState.APP_FILES,
            R.string.sync_verifying_local_file,
            new Object[] { relativePath }, stepCount * stepSize, false);

        // make sure our copy is current
        compareAndDownloadConfigFile(null, entry, localFile);
        // remove it from the set of app-level files we found before the sync
        relativePathsOnDevice.remove(relativePath);

        // this is the corrected step size based upon matching files
        stepSize = 100.0 / (1 + relativePathsOnDevice.size() + manifest.size());

        ++stepCount;
      }

      for (String relativePath : relativePathsOnDevice) {

        syncStatus.updateNotification(SyncProgressState.APP_FILES,
            R.string.sync_deleting_local_file,
            new Object[] { relativePath }, stepCount * stepSize, false);

        // and remove any remaining files, as these do not match anything on
        // the server.
        File localFile = ODKFileUtils.asAppFile(sc.getAppName(), relativePath);
        if (!localFile.delete()) {
          success = false;
          log.e(LOGTAG, "Unable to delete " + localFile.getAbsolutePath());
        }

        ++stepCount;
      }
    }

    return success;
  }

  private String determineContentType(String fileName) {
    int ext = fileName.lastIndexOf('.');
    if (ext == -1) {
      return "application/octet-stream";
    }
    String type = fileName.substring(ext + 1);
    String mimeType = mimeMapping.get(type);
    if (mimeType == null) {
      return "application/octet-stream";
    }
    return mimeType;
  }

  @Override
  public void syncTableLevelFiles(String tableId, String serverReportedTableLevelETag, OnTablePropertiesChanged onChange,
      boolean pushLocalFiles, SynchronizerStatus syncStatus) throws ClientWebException, InvalidAuthTokenException {

    syncStatus.updateNotification(SyncProgressState.TABLE_FILES,
        R.string.sync_getting_table_manifest,
        new Object[] { tableId }, 1.0, false);

    // get the table files on the server
    List<OdkTablesFileManifestEntry> manifest = getTableLevelFileManifest(tableId,
        serverReportedTableLevelETag, pushLocalFiles);

    if (manifest == null) {
      log.i(LOGTAG, "no change in table manifest -- skipping!");
      // short-circuit because our files should match those on the server
      syncStatus.updateNotification(SyncProgressState.TABLE_FILES,
          R.string.sync_getting_table_manifest,
          new Object[] { tableId }, 100.0, false);

      return;
    }
    String tableIdDefinitionFile = ODKFileUtils.asRelativePath(sc.getAppName(),
        new File(ODKFileUtils.getTableDefinitionCsvFile(sc.getAppName(), tableId)));

    String tableIdPropertiesFile = ODKFileUtils.asRelativePath(sc.getAppName(),
        new File(ODKFileUtils.getTablePropertiesCsvFile(sc.getAppName(), tableId)));

    boolean tablePropertiesChanged = false;

    // Get any config/assets/csv files that begin with tableId
    Set<String> dirsToExclude = new HashSet<String>();
    File assetsCsvFolder = new File(ODKFileUtils.getAssetsCsvFolder(sc.getAppName()));
    List<String> relativePathsToTableIdAssetsCsvOnDevice = getAllFilesUnderFolder(assetsCsvFolder,
        dirsToExclude);
    relativePathsToTableIdAssetsCsvOnDevice = filterInTableIdFiles(
        relativePathsToTableIdAssetsCsvOnDevice, tableId);

    // instance directory is now under the data tree, so we don't have to worry about it...
    File tableFolder = new File(ODKFileUtils.getTablesFolder(sc.getAppName(), tableId));
    List<String> relativePathsOnDevice = getAllFilesUnderFolder(tableFolder, dirsToExclude);

    // mix in the assets files for this tableId, if any...
    relativePathsOnDevice.addAll(relativePathsToTableIdAssetsCsvOnDevice);

    double stepSize = 100.0 / (1 + relativePathsOnDevice.size() + manifest.size());
    int stepCount = 1;

    if (pushLocalFiles) {
      // if we are pushing, we want to push the local files that are different
      // up to the server, then remove the files on the server that are not
      // in the local set.
      List<File> serverFilesToDelete = new ArrayList<File>();

      for (OdkTablesFileManifestEntry entry : manifest) {
        File localFile = ODKFileUtils.asConfigFile(sc.getAppName(), entry.filename);
        if (!localFile.exists() || !localFile.isFile()) {
          // we need to delete this file from the server.
          serverFilesToDelete.add(localFile);
        } else if (ODKFileUtils.getMd5Hash(sc.getAppName(), localFile).equals(entry.md5hash)) {
          // we are ok -- no need to upload or delete
          relativePathsOnDevice.remove(ODKFileUtils.asRelativePath(sc.getAppName(), localFile));
        }
      }

      // this is the actual step size when we are pushing...
      stepSize = 100.0 / (1 + relativePathsOnDevice.size() + serverFilesToDelete.size());

      boolean success = true;
      for (String relativePath : relativePathsOnDevice) {

        syncStatus.updateNotification(SyncProgressState.TABLE_FILES,
            R.string.sync_uploading_local_file,
            new Object[] { relativePath }, stepCount * stepSize, false);

        File localFile = ODKFileUtils.asAppFile(sc.getAppName(), relativePath);
        if (!uploadConfigFile(localFile)) {
          success = false;
          log.e(LOGTAG, "Unable to upload file to server: " + relativePath);
        }

        ++stepCount;
      }

      for (File localFile : serverFilesToDelete) {

        String relativePath = ODKFileUtils.asRelativePath(sc.getAppName(), localFile);
        syncStatus.updateNotification(SyncProgressState.TABLE_FILES,
            R.string.sync_deleting_file_on_server,
            new Object[] { relativePath }, stepCount * stepSize, false);

        if (!deleteConfigFile(localFile)) {
          success = false;
          log.e(LOGTAG, "Unable to delete file on server: " + relativePath);
        }

        ++stepCount;
      }

      if (!success) {
        log.i(LOGTAG, "unable to delete one or more files!");
      }

    } else {
      // if we are pulling, we want to pull the server files that are different
      // down from the server, then remove the local files that are not present
      // on the server.

      for (OdkTablesFileManifestEntry entry : manifest) {
        File localFile = ODKFileUtils.asConfigFile(sc.getAppName(), entry.filename);
        String relativePath = ODKFileUtils.asRelativePath(sc.getAppName(), localFile);

        syncStatus.updateNotification(SyncProgressState.TABLE_FILES,
            R.string.sync_verifying_local_file,
            new Object[] { relativePath }, stepCount * stepSize, false);

        // make sure our copy is current
        boolean outcome = compareAndDownloadConfigFile(tableId, entry, localFile);
        // and if it was the table properties file, remember whether it changed.
        if (relativePath.equals(tableIdPropertiesFile)) {
          tablePropertiesChanged = outcome;
        }
        // remove it from the set of app-level files we found before the sync
        relativePathsOnDevice.remove(relativePath);

        // this is the corrected step size based upon matching files
        stepSize = 100.0 / (1 + relativePathsOnDevice.size() + manifest.size());

        ++stepCount;
      }

      boolean success = true;
      for (String relativePath : relativePathsOnDevice) {

        syncStatus.updateNotification(SyncProgressState.TABLE_FILES,
            R.string.sync_deleting_local_file,
            new Object[] { relativePath }, stepCount * stepSize, false);

        // and remove any remaining files, as these do not match anything on
        // the server.
        File localFile = ODKFileUtils.asAppFile(sc.getAppName(), relativePath);
        if (!localFile.delete()) {
          success = false;
          log.e(LOGTAG, "Unable to delete " + localFile.getAbsolutePath());
        }

        ++stepCount;
      }

      if (tablePropertiesChanged && (onChange != null)) {
        // update this table's KVS values...
        onChange.onTablePropertiesChanged(tableId);
      }

      if (!success) {
        log.i(LOGTAG, "unable to delete one or more files!");
      }

      // should we return our status?
    }
  }

  public List<OdkTablesFileManifestEntry> getAppLevelFileManifest(boolean pushLocalFiles, String serverReportedAppLevelETag)
      throws ClientWebException, InvalidAuthTokenException {

    URI fileManifestUri = normalizeUri(sc.getAggregateUri(), getManifestUriFragment());
    String eTag = null;
    try {
      eTag = getManifestSyncETag(fileManifestUri, null);
    } catch (RemoteException e) {
      log.printStackTrace(e);
      log.e(LOGTAG, "database access error (ignoring)");
    }
    Resource rsc = buildResource(fileManifestUri);
    // don't short-circuit manifest if we are pushing local files,
    // as we need to know exactly what is on the server to minimize
    // transmissions of files being pushed up to the server.
    if (!pushLocalFiles && eTag != null) {
      rsc.header(HttpHeaders.IF_NONE_MATCH, eTag);
      if ( serverReportedAppLevelETag != null && serverReportedAppLevelETag.equals(eTag) ) {
        // no change -- we can skip the request to the server
        return null;
      }
    }
    ClientResponse rsp = rsc.get();
    if (rsp.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
      // signal this by returning null;
      return null;
    }
    if (rsp.getStatusCode() < 200 || rsp.getStatusCode() >= 300) {
      throw new ClientWebException(null, rsp);
    }
    if (!rsp.getHeaders().containsKey(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER) ) {
      throw new ClientWebException(null, rsp);
    }

    // retrieve the manifest...
    OdkTablesFileManifest manifest;
    manifest = rsp.getEntity(OdkTablesFileManifest.class);
    List<OdkTablesFileManifestEntry> theList = null;
    if (manifest != null) {
      theList = manifest.getFiles();
    }
    if (theList == null) {
      theList = Collections.emptyList();
    }
    // update the manifest ETag record...
    eTag = rsp.getHeaders().getFirst(HttpHeaders.ETAG);
    try {
      updateManifestSyncETag(fileManifestUri, null, eTag);
    } catch (RemoteException e) {
      log.printStackTrace(e);
      log.e(LOGTAG, "database access error (ignoring)");
    }
    // and return the list of values...
    return theList;
  }

  public List<OdkTablesFileManifestEntry> getTableLevelFileManifest(String tableId, String serverReportedTableLevelETag,
      boolean pushLocalFiles) throws ClientWebException, InvalidAuthTokenException {

    URI fileManifestUri = normalizeUri(sc.getAggregateUri(), getManifestUriFragment() + tableId);
    String eTag = null;
    try {
      eTag = getManifestSyncETag(fileManifestUri, tableId);
    } catch (RemoteException e) {
      log.printStackTrace(e);
      log.e(LOGTAG, "database access error (ignoring)");
    }
    Resource rsc = buildResource(fileManifestUri);
    // don't short-circuit manifest if we are pushing local files,
    // as we need to know exactly what is on the server to minimize
    // transmissions of files being pushed up to the server.
    if (!pushLocalFiles && eTag != null) {
      rsc.header(HttpHeaders.IF_NONE_MATCH, eTag);
      if ( serverReportedTableLevelETag != null && serverReportedTableLevelETag.equals(eTag) ) {
        // no change -- we can skip the request to the server
        return null;
      }
    }
    ClientResponse rsp = rsc.get();
    if (rsp.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
      // signal this by returning null;
      return null;
    }
    if (rsp.getStatusCode() < 200 || rsp.getStatusCode() >= 300) {
      throw new ClientWebException(null, rsp);
    }
    if (!rsp.getHeaders().containsKey(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER) ) {
      throw new ClientWebException(null, rsp);
    }

    // retrieve the manifest...
    OdkTablesFileManifest manifest;
    manifest = rsp.getEntity(OdkTablesFileManifest.class);
    List<OdkTablesFileManifestEntry> theList = null;
    if (manifest != null) {
      theList = manifest.getFiles();
    }
    if (theList == null) {
      theList = Collections.emptyList();
    }
    // update the manifest ETag record...
    eTag = rsp.getHeaders().getFirst(HttpHeaders.ETAG);
    try {
      updateManifestSyncETag(fileManifestUri, tableId, eTag);
    } catch (RemoteException e) {
      log.printStackTrace(e);
      log.e(LOGTAG, "database access error (ignoring)");
    }
    // and return the list of values...
    return theList;
  }

  public List<OdkTablesFileManifestEntry> getRowLevelFileManifest(String serverInstanceFileUri,
      String tableId, String instanceId) throws ClientWebException, InvalidAuthTokenException {

    URI instanceFileManifestUri = normalizeUri(serverInstanceFileUri, instanceId + "/manifest");

    String eTag = null;
    try {
      eTag = getManifestSyncETag(instanceFileManifestUri, tableId);
    } catch (RemoteException e) {
      log.printStackTrace(e);
      log.e(LOGTAG, "database access error (ignoring)");
    }
    Resource rsc = buildResource(instanceFileManifestUri);

    /* TODO: Do we need to add pushLocalFiles and uncomment this?
    // don't short-circuit manifest if we are pushing local files,
    // as we need to know exactly what is on the server to minimize
    // transmissions of files being pushed up to the server.
    if (!pushLocalFiles && eTag != null) {
      rsc.header(HttpHeaders.IF_NONE_MATCH, eTag);
      if ( serverReportedTableLevelETag != null && serverReportedTableLevelETag.equals(eTag) ) {
        // no change -- we can skip the request to the server
        return null;
      }
    }
    */
    ClientResponse rsp = rsc.get();
    if (rsp.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
      // signal this by returning null;
      return null;
    }
    if (rsp.getStatusCode() < 200 || rsp.getStatusCode() >= 300) {
      throw new ClientWebException(null, rsp);
    }
    if (!rsp.getHeaders().containsKey(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER) ) {
      throw new ClientWebException(null, rsp);
    }

    // retrieve the manifest...
    OdkTablesFileManifest manifest;
    manifest = rsp.getEntity(OdkTablesFileManifest.class);
    List<OdkTablesFileManifestEntry> theList = null;
    if (manifest != null) {
      theList = manifest.getFiles();
    }
    if (theList == null) {
      theList = Collections.emptyList();
    }
    // update the manifest ETag record...
    eTag = rsp.getHeaders().getFirst(HttpHeaders.ETAG);
    try {
      updateManifestSyncETag(instanceFileManifestUri, tableId, eTag);
    } catch (RemoteException e) {
      log.printStackTrace(e);
      log.e(LOGTAG, "database access error (ignoring)");
    }
    // and return the list of values...
    return theList;


  }

  private boolean deleteConfigFile(File localFile) throws ClientWebException,
      InvalidAuthTokenException {
    String pathRelativeToConfigFolder = ODKFileUtils.asConfigRelativePath(sc.getAppName(),
        localFile);
    String escapedPath = uriEncodeSegments(pathRelativeToConfigFolder);
    URI filesUri = normalizeUri(sc.getAggregateUri(), getFilePathURI() + escapedPath);
    log.i(LOGTAG, "[deleteConfigFile] fileDeleteUri: " + filesUri.toString());
    buildResource(filesUri).delete();
    // TODO: verify whether or not this worked.
    return true;
  }

  private boolean uploadConfigFile(File localFile) throws
      InvalidAuthTokenException {
    String pathRelativeToConfigFolder = ODKFileUtils.asConfigRelativePath(sc.getAppName(),
        localFile);
    String escapedPath = uriEncodeSegments(pathRelativeToConfigFolder);
    URI filesUri = normalizeUri(sc.getAggregateUri(), getFilePathURI() + escapedPath);
    log.i(LOGTAG, "[uploadConfigFile] filePostUri: " + filesUri.toString());
    String ct = determineContentType(localFile.getName());
    MediaType contentType = MediaType.valueOf(ct);
    ClientResponse response = buildResource(filesUri, contentType).post(localFile);
    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
      return false;
    }
    if ( !response.getHeaders().containsKey(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER)) {
      return false;
    }
    return true;
  }

  private boolean uploadInstanceFile(File file, URI instanceFileUri) throws InvalidAuthTokenException {
    log.i(LOGTAG, "[uploadFile] filePostUri: " + instanceFileUri.toString());
    String ct = determineContentType(file.getName());
    MediaType contentType = MediaType.valueOf(ct);
    ClientResponse response = buildResource(instanceFileUri, contentType).post(file);
    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
      return false;
    }
    if ( !response.getHeaders().containsKey(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER)) {
      return false;
    }
    return true;
  }

  /**
   * Get the URI to which to post in order to upload the file.
   *
   * @param pathRelativeToAppFolder
   * @return
   */
  public URI getFilePostUri(String pathRelativeToAppFolder) {
    String escapedPath = uriEncodeSegments(pathRelativeToAppFolder);
    URI filesUri = normalizeUri(sc.getAggregateUri(), getFilePathURI() + escapedPath);
    return filesUri;
  }

  /**
   *
   * @param entry
   * @return
   */
  private boolean compareAndDownloadConfigFile(String tableId, OdkTablesFileManifestEntry entry,
      File localFile) {
    String basePath = ODKFileUtils.getAppFolder(sc.getAppName());

    // if the file is a placeholder on the server, then don't do anything...
    if (entry.contentLength == 0) {
      return false;
    }
    // now we need to look through the manifest and see where the files are
    // supposed to be stored. Make sure you don't return a bad string.
    if (entry.filename == null || entry.filename.equals("")) {
      log.i(LOGTAG, "returned a null or empty filename");
      return false;
    } else {

      URI uri = null;
      URL urlFile = null;
      try {
        log.i(LOGTAG, "[compareAndDownloadConfigFile] downloading at url: " + entry.downloadUrl);
        urlFile = new URL(entry.downloadUrl);
        uri = urlFile.toURI();
      } catch (MalformedURLException e) {
        log.e(LOGTAG, e.toString());
        log.printStackTrace(e);
        return false;
      } catch (URISyntaxException e) {
        log.e(LOGTAG, e.toString());
        log.printStackTrace(e);
        return false;
      }

      // Before we try dl'ing the file, we have to make the folder,
      // b/c otherwise if the folders down to the path have too many non-
      // existent folders, we'll get a FileNotFoundException when we open
      // the FileOutputStream.
      String folderPath = localFile.getParent();
      ODKFileUtils.createFolder(folderPath);
      if (!localFile.exists()) {
        // the file doesn't exist on the system
        // filesToDL.add(localFile);
        try {
          int statusCode = downloadFile(localFile, uri);
          if (statusCode == HttpStatus.SC_OK) {
            updateFileSyncETag(uri, tableId, localFile.lastModified(),
                entry.md5hash);
            return true;
          } else {
            return false;
          }
        } catch (Exception e) {
          log.printStackTrace(e);
          log.e(LOGTAG, "trouble downloading file for first time");
          return false;
        }
      } else {
        boolean hasUpToDateEntry = true;
        String md5hash = null;
        try {
          md5hash = getFileSyncETag(uri, tableId, localFile.lastModified());
        } catch (RemoteException e1) {
          log.printStackTrace(e1);
          log.e(LOGTAG, "database access error (ignoring)");
        }
        if (md5hash == null) {
          // file exists, but no record of what is on the server
          // compute local value
          hasUpToDateEntry = false;
          md5hash = ODKFileUtils.getMd5Hash(sc.getAppName(), localFile);
        }
        // so as it comes down from the manifest, the md5 hash includes a
        // "md5:" prefix. Add that and then check.
        if (!md5hash.equals(entry.md5hash)) {
          hasUpToDateEntry = false;
          // it's not up to date, we need to download it.
          try {
            int statusCode = downloadFile(localFile, uri);
            if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_NOT_MODIFIED) {
              updateFileSyncETag(uri, tableId, localFile.lastModified(),
                  md5hash);
              return true;
            } else {
              return false;
            }
          } catch (Exception e) {
            log.printStackTrace(e);
            // TODO throw correct exception
            log.e(LOGTAG, "trouble downloading new version of existing file");
            return false;
          }
        } else {
          if (!hasUpToDateEntry) {
            try {
              updateFileSyncETag(uri, tableId, localFile.lastModified(), md5hash);
            } catch (RemoteException e) {
              log.printStackTrace(e);
              log.e(LOGTAG, "database access error (ignoring)");
            }
          }
          // no change
          return false;
        }
      }
    }
  }

  /**
   *
   * @param destFile
   * @param downloadUrl
   * @return true if the download was successful
   * @throws Exception
   */
  private int downloadFile(File destFile, URI downloadUrl) throws Exception {

    // WiFi network connections can be renegotiated during a large form download
    // sequence.
    // This will cause intermittent download failures. Silently retry once after
    // each
    // failure. Only if there are two consecutive failures, do we abort.
    boolean success = false;
    int attemptCount = 0;
    while (!success && attemptCount++ <= 2) {

      Resource resource = buildFileDownloadResource(downloadUrl);
      if ( destFile.exists() ) {
        String md5Hash = ODKFileUtils.getMd5Hash(sc.getAppName(), destFile);
        resource.header(HttpHeaders.IF_NONE_MATCH, md5Hash);
      }
      
      ClientResponse response = null;
      try {
        response = resource.get();
        int statusCode = response.getStatusCode();

        if (statusCode != HttpStatus.SC_OK) {
          response.consumeContent();
          if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
            // clear the cookies -- should not be necessary?
            // ss: might just be a collect thing?
          }
          log.w(LOGTAG, "downloading " + downloadUrl.toString() + " returns " + statusCode);
          return statusCode;
        }
        
        if (!response.getHeaders().containsKey(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER)) {
          response.consumeContent();
          log.w(LOGTAG, "downloading " + downloadUrl.toString() + " appears to have been redirected.");
          return 302;
        }
        
        File tmp = new File(destFile.getParentFile(), destFile.getName() + ".tmp");
        int totalLen = 0;
        InputStream is = null;
        BufferedOutputStream os = null;
        try {
          // open the InputStream of the (uncompressed) entity body...
          is = response.getEntity(InputStream.class);
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
          response.consumeContent();
        }
      } catch (Exception e) {
        log.printStackTrace(e);
        if ( response != null ) {
          response.consumeContent();
        }
        if (attemptCount != 1) {
          throw e;
        }
      }
    }
    return HttpStatus.SC_OK;
  }

  private static final class CommonFileAttachmentTerms {
    String rowPathUri;
    File localFile;
    URI instanceFileDownloadUri;
  }

  private CommonFileAttachmentTerms computeCommonFileAttachmentTerms(String serverInstanceFileUri, 
      String tableId, String instanceId, String rowpathUri) {
    
    File localFile = 
        ODKFileUtils.getRowpathFile(sc.getAppName(), tableId, instanceId, rowpathUri);

    // use a cleaned-up rowpathUri in case there are leading slashes, instance paths, etc.
    String cleanRowpathUri = ODKFileUtils.asRowpathUri(sc.getAppName(), tableId, instanceId, localFile);
    
    URI instanceFileDownloadUri = normalizeUri(serverInstanceFileUri, instanceId + "/file/"
        + cleanRowpathUri);

  
    CommonFileAttachmentTerms cat = new CommonFileAttachmentTerms();
    cat.rowPathUri = rowpathUri;
    cat.localFile = localFile;
    cat.instanceFileDownloadUri = instanceFileDownloadUri;
    
    return cat;
  }

  private String getFileSyncETag(URI
      fileDownloadUri, String tableId, long lastModified) throws RemoteException {
    OdkDbHandle db = null;
    try {
      db = sc.getDatabase();
      return sc.getDatabaseService().getFileSyncETag(sc.getAppName(), db,
          fileDownloadUri.toString(), tableId,
          lastModified);
    } finally {
      sc.releaseDatabase(db);
      db = null;
    }
  }

  private void updateFileSyncETag(URI fileDownloadUri, String tableId, long lastModified, String documentETag) throws RemoteException {
    OdkDbHandle db = null;
    try {
      db = sc.getDatabase();
      sc.getDatabaseService().updateFileSyncETag(sc.getAppName(), db, fileDownloadUri.toString(), tableId,
          lastModified, documentETag);
    } finally {
      sc.releaseDatabase(db);
      db = null;
    }
  }

  private String getManifestSyncETag(URI fileDownloadUri, String tableId) throws RemoteException {
    OdkDbHandle db = null;
    try {
      db = sc.getDatabase();
      return sc.getDatabaseService().getManifestSyncETag(sc.getAppName(), db, fileDownloadUri.toString(), tableId);
    } finally {
      sc.releaseDatabase(db);
      db = null;
    }
  }

  private void updateManifestSyncETag(URI fileDownloadUri, String tableId, String documentETag) throws RemoteException {
    OdkDbHandle db = null;
    try {
      db = sc.getDatabase();
      sc.getDatabaseService().updateManifestSyncETag(sc.getAppName(), db, fileDownloadUri.toString(), tableId,
          documentETag);
    } finally {
      sc.releaseDatabase(db);
      db = null;
    }
  }

  @Override
  public boolean syncFileAttachments(String serverInstanceFileUri, String tableId,
      SyncRowPending localRow, SyncAttachmentState attachmentState) throws ClientWebException {

    if (localRow.getUriFragments().isEmpty()) {
      throw new IllegalStateException("should never get here!");
    }

    // If we are not syncing instance files, then return
    if (attachmentState.equals(SyncAttachmentState.NONE)) {
      return false;
    }

    boolean fullySynced = true;
    try {
       // 1) Get this row's instanceId (rowId)
      String instanceId = localRow.getRowId();

      // 2) Get the list of files on the server
      List<OdkTablesFileManifestEntry> entries = getRowLevelFileManifest(serverInstanceFileUri,
          tableId, instanceId);

      // 3) Create a list of files to that need to be uploaded to or downloaded from the server.
      List<String> localRowPathUris = localRow.getUriFragments();
      List<CommonFileAttachmentTerms> filesToUpload = new ArrayList<CommonFileAttachmentTerms>();
      List<CommonFileAttachmentTerms> filesToDownload = new ArrayList<CommonFileAttachmentTerms>();

      // First, iterate over the files that exist on the server, noting any that are missing
      // or changed on either end
      for (OdkTablesFileManifestEntry entry : entries) {

        // Remove files that exist on the server from our list of local files, leaving us with a
        // list of files that only exist locally.
        localRowPathUris.remove(entry.filename);

        CommonFileAttachmentTerms cat = computeCommonFileAttachmentTerms(serverInstanceFileUri,
            tableId, instanceId, entry.filename);

        if (!cat.localFile.exists()) {
          // File exists on the server but not locally; queue it for download
          filesToDownload.add(cat);
          continue;
        }

        // Check if the server and local versions match
        String localMd5 = ODKFileUtils.getMd5Hash(sc.getAppName(), cat.localFile);
        if (entry.md5hash == null || entry.md5hash.length() == 0) {
          // Recorded but not present on server -- upload
          filesToUpload.add(cat);
        } else if (!localMd5.equals(entry.md5hash)) {
          // Found, but it is wrong locally, so we need to pull it
          log.e(LOGTAG, "Put attachments: md5Hash on server does not match local file hash!");
          filesToDownload.add(cat);
          continue;
        } else {
          // Server matches local; don't upload or download
        }
      }

      // Next, account for any files that exist locally but aren't referenced on the server
      for (String rowPathUri : localRowPathUris) {

        CommonFileAttachmentTerms cat = computeCommonFileAttachmentTerms(serverInstanceFileUri,
            tableId, instanceId, rowPathUri);

        if (!cat.localFile.exists()) {
          Log.e(LOGTAG, "Put attachments: row has reference to non-existant file");
          fullySynced = false;
          continue;
        }

        filesToUpload.add(cat);
      }

      // 4) Split that list of files to upload into 10MB batches and upload those to the server
      if (!(attachmentState.equals(SyncAttachmentState.SYNC) ||
          attachmentState.equals(SyncAttachmentState.UPLOAD))) {
        // If we are not set to upload files, then don't.
        fullySynced = false;
      } else if (filesToUpload.isEmpty()) {
        log.i(LOGTAG, "Put attachments: no files to send to server -- they are all synced");
      } else {
        long batchSize = 0;
        List<CommonFileAttachmentTerms> batch = new LinkedList<CommonFileAttachmentTerms>();
        for (CommonFileAttachmentTerms fileAttachment : filesToUpload) {

          // Check if adding the file exceeds the batch limit. If so, upload the current batch and
          // start a new one.
          // Note: If the batch is empty then this is just one giant file and it will get uploaded
          // on the next iteration.
          if (batchSize + fileAttachment.localFile.length() > MAX_BATCH_SIZE && !batch.isEmpty()) {
            fullySynced &= uploadBatch(batch, serverInstanceFileUri, instanceId, tableId);
            batch.clear();
            batchSize = 0;
          }

          batch.add(fileAttachment);
          batchSize += fileAttachment.localFile.length();
        }

        // Upload the final batch
        fullySynced &= uploadBatch(batch, serverInstanceFileUri, instanceId, tableId);

      }

      // 5) Download the files from the server TODO: Batch these calls into 10MB chunks
      if (!(attachmentState.equals(SyncAttachmentState.SYNC) ||
          attachmentState.equals(SyncAttachmentState.DOWNLOAD))) {
      } else if (filesToDownload.isEmpty()){
        log.i(LOGTAG, "Put attachments: no files to fetch from server -- they are all synced");
      } else {
        fullySynced &= downloadFileBatches(filesToDownload, serverInstanceFileUri, instanceId,
            tableId);
      }

      return fullySynced;

    } catch (ClientWebException e) {
      log.e(LOGTAG, "Exception while putting attachment: " + e.toString());
      throw e;
    } catch (Exception e) {
      log.e(LOGTAG, "Exception during sync: " + e.toString());
      return false;
    }

  }

  private boolean uploadBatch(List<CommonFileAttachmentTerms> batch,
      String serverInstanceFileUri, String instanceId, String tableId) throws Exception {
    Resource rsc;
    URI instanceFilesUploadUri = normalizeUri(serverInstanceFileUri, instanceId + "/upload");
    String boundary = "ref" + UUID.randomUUID();

    Map<String, String> params = Collections.singletonMap("boundary", boundary);
    MediaType mt = new MediaType(MediaType.MULTIPART_FORM_DATA_TYPE.getType(), MediaType.MULTIPART_FORM_DATA_TYPE.getSubtype(), params);
    rsc = buildResource(instanceFilesUploadUri, mt);

    BufferedOutMultiPart mpOut = new BufferedOutMultiPart();
    mpOut.setBoundary(boundary);

    for (CommonFileAttachmentTerms cat : batch) {
      log.i(LOGTAG, "[uploadFile] filePostUri: " + cat.instanceFileDownloadUri.toString());
      String ct = determineContentType(cat.localFile.getName());

      OutPart part = new OutPart();
      part.setContentType(ct);
      String filename = ODKFileUtils
          .asRowpathUri(sc.getAppName(), tableId, instanceId, cat.localFile);
      filename = filename.replace("\"", "\"\"");
      part.addHeader("Content-Disposition", "file;filename=\"" + filename + "\"");
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
        is.close();
      }
      byte[] content = bo.toByteArray();
      part.setBody(content);
      mpOut.addPart(part);
    }
    ClientResponse response = rsc.post(mpOut);
    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
      return false;
    }
    if (!response.getHeaders().containsKey(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER)) {
      return false;
    }

    return true;
  }

  private boolean downloadFileBatches(List<CommonFileAttachmentTerms> filesToDownload,
      String serverInstanceFileUri, String instanceId, String tableId) throws Exception {
    Resource rsc;
    boolean downloadedAllFiles = true;


    URI instanceFilesDownloadUri = normalizeUri(serverInstanceFileUri, instanceId +
        "/download");

    ArrayList<OdkTablesFileManifestEntry> entries = new ArrayList<OdkTablesFileManifestEntry>();
    for (CommonFileAttachmentTerms cat : filesToDownload) {
      OdkTablesFileManifestEntry entry = new OdkTablesFileManifestEntry();
      entry.filename = cat.rowPathUri;
      entries.add(entry);
    }

    OdkTablesFileManifest manifest = new OdkTablesFileManifest();
    manifest.setFiles(entries);

    rsc = buildBasicResource(instanceFilesDownloadUri);
    rsc.contentType(MediaType.APPLICATION_JSON_TYPE);
    ClientResponse response = rsc.post(manifest);
    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
      return false;
    }

    InMultiPart inMP = response.getEntity(InMultiPart.class);

    // Parse the request
    while (inMP.hasNext()) {
      InPart part = inMP.next();
      MultivaluedMap<String, String> headers = part.getHeaders();
      String disposition = (headers != null) ? headers.getFirst("Content-Disposition") : null;
      if (disposition == null) {
        log.e("putAttachments", "Unable to retrieve ContentDisposition from response part");
        return false;
      }
      String partialPath = null;
      {
        HeaderValueParser parser = new BasicHeaderValueParser();
        HeaderElement[] values = BasicHeaderValueParser.parseElements(disposition, parser);
        for (HeaderElement v : values) {
          if (v.getName().equalsIgnoreCase("file")) {
            partialPath = v.getParameterByName("filename").getValue();
            break;
          }
        }
      }
      if (partialPath == null) {
        log.e("putAttachments", "Server did not identify the rowPathUri for the file");
        return false;
      }

      String contentType = (headers != null) ? headers.getFirst("Content-Type") : null;

      File instFile = ODKFileUtils
          .getRowpathFile(sc.getAppName(), tableId, instanceId, partialPath);
      OutputStream os = null;
      InputStream bi = null;
      try {
        bi = new BufferedInputStream(part.getInputStream());
        os = new BufferedOutputStream(new FileOutputStream(instFile));
        int length = 1024;
        // Transfer bytes from in to out
        byte[] data = new byte[length];
        int len;
        while ((len = bi.read(data, 0, length)) >= 0) {
          if (len != 0) {
            os.write(data, 0, len);
          }
        }
        os.flush();
        os.close();
        os = null;
        bi.close();
        bi = null;
      } catch (IOException e) {
        log.printStackTrace(e);
        log.e(LOGTAG, "Download file batches: Unable to read attachment");
        return false;
      } finally {
        if (bi != null) {
          try {
            bi.close();
          } catch (IOException e) {
            log.printStackTrace(e);
            log.e(LOGTAG, "Download file batches: Error closing input stream");
          }
        }
        if (os != null) {
          try {
            os.close();
          } catch (IOException e) {
            log.printStackTrace(e);
            log.e(LOGTAG, "Download file batches: Error closing output stream");
          }
        }
      }
    }


    return downloadedAllFiles;
  }
}
