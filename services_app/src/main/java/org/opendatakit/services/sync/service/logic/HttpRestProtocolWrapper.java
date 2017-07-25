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

import android.accounts.Account;
import android.accounts.AccountManager;
import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.aggregate.odktables.rest.ApiConstants;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.WebLoggerIf;
import org.opendatakit.httpclientandroidlib.*;
import org.opendatakit.httpclientandroidlib.auth.AuthScope;
import org.opendatakit.httpclientandroidlib.auth.Credentials;
import org.opendatakit.httpclientandroidlib.auth.UsernamePasswordCredentials;
import org.opendatakit.httpclientandroidlib.client.ClientProtocolException;
import org.opendatakit.httpclientandroidlib.client.CookieStore;
import org.opendatakit.httpclientandroidlib.client.CredentialsProvider;
import org.opendatakit.httpclientandroidlib.client.config.AuthSchemes;
import org.opendatakit.httpclientandroidlib.client.config.CookieSpecs;
import org.opendatakit.httpclientandroidlib.client.config.RequestConfig;
import org.opendatakit.httpclientandroidlib.client.entity.GzipCompressingEntity;
import org.opendatakit.httpclientandroidlib.client.methods.*;
import org.opendatakit.httpclientandroidlib.client.protocol.HttpClientContext;
import org.opendatakit.httpclientandroidlib.client.utils.URIBuilder;
import org.opendatakit.httpclientandroidlib.config.SocketConfig;
import org.opendatakit.httpclientandroidlib.entity.ByteArrayEntity;
import org.opendatakit.httpclientandroidlib.entity.ContentType;
import org.opendatakit.httpclientandroidlib.impl.client.BasicCookieStore;
import org.opendatakit.httpclientandroidlib.impl.client.BasicCredentialsProvider;
import org.opendatakit.httpclientandroidlib.impl.client.CloseableHttpClient;
import org.opendatakit.httpclientandroidlib.impl.client.HttpClientBuilder;
import org.opendatakit.httpclientandroidlib.message.BasicNameValuePair;
import org.opendatakit.httpclientandroidlib.protocol.BasicHttpContext;
import org.opendatakit.httpclientandroidlib.protocol.HttpContext;
import org.opendatakit.httpclientandroidlib.util.EntityUtils;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.services.sync.service.exceptions.*;

import java.io.*;
import java.net.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Extraction of the lower-level REST protocol support methods from
 * the AggregateSynchronizer class.
 *
 */
public class HttpRestProtocolWrapper {

  private static final String LOGTAG = HttpRestProtocolWrapper.class.getSimpleName();
  private static final String TOKEN_INFO = "https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=";
  public static final int CONNECTION_TIMEOUT = 60000;

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

  public static final String BOUNDARY = "boundary";
  public static final String multipartFileHeader = "filename=\"";

  private static final String FORWARD_SLASH = "/";

  private CloseableHttpClient httpClient = null;
  private CloseableHttpClient httpAuthClient = null;

  private HttpContext localContext = null;
  private HttpContext localAuthContext = null;

  private CookieStore cookieStore = null;

  private CredentialsProvider credsProvider = null;

  static Map<String, String> mimeMapping;

  static List<Integer> SC_OK_ONLY;
  static List<Integer> SC_OK_SC_NOT_MODIFIED;
  static List<Integer> SC_OK_SC_CONFLICT;
  static List<Integer> SC_OK_SC_NOT_FOUND;
  static List<Integer> SC_CREATED;
  static List<Integer> SC_CREATED_SC_ACCEPTED;

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

    ArrayList<Integer> al;

    al = new ArrayList<Integer>();
    al.add(HttpStatus.SC_OK);
    SC_OK_ONLY = al;

    al = new ArrayList<Integer>();
    al.add(HttpStatus.SC_OK);
    al.add(HttpStatus.SC_NOT_MODIFIED);
    SC_OK_SC_NOT_MODIFIED = al;

    al = new ArrayList<Integer>();
    al.add(HttpStatus.SC_OK);
    al.add(HttpStatus.SC_CONFLICT);
    SC_OK_SC_CONFLICT = al;

    al = new ArrayList<Integer>();
    al.add(HttpStatus.SC_OK);
    al.add(HttpStatus.SC_NOT_FOUND);
    SC_OK_SC_NOT_FOUND = al;

    al = new ArrayList<Integer>();
    al.add(HttpStatus.SC_CREATED);
    SC_CREATED = al;

    al = new ArrayList<Integer>();
    al.add(HttpStatus.SC_CREATED);
    al.add(HttpStatus.SC_ACCEPTED);
    SC_CREATED_SC_ACCEPTED = al;
  }

  private SyncExecutionContext sc;
  private String accessToken;
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

  private static String escapeSegment(String segment) {
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

  public URI constructListOfAppNamesUri() {
    URI uri = normalizeUri(sc.getAggregateUri(), "/odktables/");
    return uri;
  }

  public URI constructListOfUserRolesAndDefaultGroupUri() {
    URI uri = normalizeUri(sc.getAggregateUri(),
        "/odktables/" + escapeSegment(sc.getAppName()) + "/privilegesInfo");
    return uri;
  }

  public URI constructListOfUsersUri() {
    URI uri = normalizeUri(sc.getAggregateUri(),
        "/odktables/" + escapeSegment(sc.getAppName()) + "/usersInfo");
    return uri;
  }

  public URI constructDeviceInformationUri() {
    URI uri = normalizeUri(sc.getAggregateUri(),
        "/odktables/" + escapeSegment(sc.getAppName()) + "/installationInfo");
    return uri;
  }

  /**
   * Uri that will return the list of tables on the server.
   *
   * @return
   */
  public URI constructListOfTablesUri(String webSafeResumeCursor) {
    String tableFrag = getTablesUriFragment();
    tableFrag = tableFrag.substring(0, tableFrag.length() - 1);
    URI uri = normalizeUri(sc.getAggregateUri(), tableFrag);

    if (webSafeResumeCursor != null) {
      try {
        uri = new URIBuilder(uri.toString())
            .addParameter(HttpRestProtocolWrapper.CURSOR_PARAMETER, webSafeResumeCursor)
            .build();
      } catch (URISyntaxException e) {
        log.printStackTrace(e);
        throw new IllegalStateException("this should never happen");
      }
    }

    return uri;
  }

  /**
   * Get the URI to use to get the list of all app-level config files
   *
   * @return
   */
  public URI constructAppLevelFileManifestUri() {
    URI uri = normalizeUri(sc.getAggregateUri(), getManifestUriFragment());
    return uri;
  }

  /**
   * Get the URI to use to get the list of config files for a specific tableId
   *
   * @param tableId
   * @return
   */
  public URI constructTableLevelFileManifestUri(String tableId) {
    URI uri = normalizeUri(sc.getAggregateUri(), getManifestUriFragment() + tableId);
    return uri;
  }

  /**
   * Get the URI to which to get or post a config file.
   *
   * @param pathRelativeToConfigFolder
   * @return
   */
  public URI constructConfigFileUri(String pathRelativeToConfigFolder) {
    String escapedPath = uriEncodeSegments(pathRelativeToConfigFolder);
    URI uri = normalizeUri(sc.getAggregateUri(), getFilePathURI() + escapedPath);
    return uri;
  }

  /**
   * Uri that will return information about the data table for a particular tableId.
   *
   * @param tableId
   * @return
   */
  public URI constructTableIdUri(String tableId) {
    URI uri = normalizeUri(sc.getAggregateUri(), getTablesUriFragment() + tableId);
    return uri;
  }

  public URI constructRealizedTableIdUri(String tableId, String schemaETag) {
    URI uri = normalizeUri(sc.getAggregateUri(), getTablesUriFragment() + tableId + "/ref/" + schemaETag);
    return uri;
  }

  public URI constructRealizedTableIdSyncStatusUri(String tableId, String schemaETag) {
    URI uri = normalizeUri(sc.getAggregateUri(), getTablesUriFragment() + tableId + "/ref/" +
        schemaETag + "/installationStatus");
    return uri;
  }

  public URI constructTableDiffChangeSetsUri(String tableIdDiffUri, String fromDataETag) {
    URI uri = normalizeUri(tableIdDiffUri, "/changeSets");

    if (fromDataETag != null) {
      try {
        uri = new URIBuilder(uri.toString())
            .addParameter(HttpRestProtocolWrapper.QUERY_DATA_ETAG, fromDataETag)
            .build();
      } catch (URISyntaxException e) {
        log.printStackTrace(e);
        throw new IllegalStateException("should never be possible");
      }
    }

    return uri;
  }

  public URI constructTableDiffChangeSetsForDataETagUri(String tableIdDiffUri, String dataETag,
      boolean activeOnly, String websafeResumeCursor) {
    URI uri = normalizeUri(tableIdDiffUri, "/changeSets/" + dataETag);

    try {
      if (activeOnly) {
        uri = new URIBuilder(uri.toString())
            .addParameter(HttpRestProtocolWrapper.QUERY_DATA_ETAG, "true").build();
      }

      // and apply the cursor...
      if (websafeResumeCursor != null) {
        uri = new URIBuilder(uri.toString())
            .addParameter(HttpRestProtocolWrapper.CURSOR_PARAMETER, websafeResumeCursor).build();
      }
    } catch (URISyntaxException e) {
      log.printStackTrace(e);
      throw new IllegalStateException("should never be possible");
    }

    return uri;
  }

  public URI constructTableDataUri(String tableIdDataUri, String websafeResumeCursor,
      int fetchLimit) {
    URI uri = URI.create(tableIdDataUri);

    try {
      // apply the fetchLimit
      uri = new URIBuilder(uri.toString())
          .addParameter(HttpRestProtocolWrapper.FETCH_LIMIT, Integer.toString(fetchLimit)).build();

      if (websafeResumeCursor != null) {
        // and apply the cursor...
        uri = new URIBuilder(uri.toString())
            .addParameter(HttpRestProtocolWrapper.CURSOR_PARAMETER, websafeResumeCursor)
                .build();
      }
    } catch (URISyntaxException e) {
      log.printStackTrace(e);
      throw new IllegalStateException("should never be possible");
    }

    return uri;
  }

  public URI constructTableDataDiffUri(String tableIdDiffUri, String dataETag, String websafeResumeCursor,
      int fetchLimit) {

    URI uri = URI.create(tableIdDiffUri);

    try {
      uri = new URIBuilder(uri.toString())
          .addParameter(HttpRestProtocolWrapper.QUERY_DATA_ETAG, dataETag)
          .addParameter(HttpRestProtocolWrapper.FETCH_LIMIT, Integer.toString(fetchLimit)).build();

      // and apply the cursor...
      if (websafeResumeCursor != null) {
        uri = new URIBuilder(uri.toString())
            .addParameter(HttpRestProtocolWrapper.CURSOR_PARAMETER, websafeResumeCursor).build();
      }
    } catch (URISyntaxException e) {
      log.printStackTrace(e);
      throw new IllegalStateException("should never be possible");
    }

    return uri;
  }

  /**
   * Construct the row-level (instanceId) attachment file manifest
   *
   * @param tableIdInstanceFileServiceUri
   * @param instanceId
   * @return
   */
  public URI constructInstanceFileManifestUri(String tableIdInstanceFileServiceUri, String instanceId) {
    URI uri = normalizeUri(tableIdInstanceFileServiceUri, instanceId + "/manifest");
    return uri;
  }

  /**
   * Construct the row-level (instanceId) attachment file bulk upload uri
   *
   * @param tableIdInstanceFileServiceUri
   * @param instanceId
   * @return
   */
  public URI constructInstanceFileBulkUploadUri(String tableIdInstanceFileServiceUri, String instanceId) {
    URI uri = normalizeUri(tableIdInstanceFileServiceUri, instanceId + "/upload");
    return uri;
  }

  /**
   * Construct the row-level (instanceId) attachment file bulk download uri
   *
   * @param tableIdInstanceFileServiceUri
   * @param instanceId
   * @return
   */
  public URI constructInstanceFileBulkDownloadUri(String tableIdInstanceFileServiceUri, String
      instanceId) {
    URI uri = normalizeUri(tableIdInstanceFileServiceUri, instanceId + "/download");
    return uri;
  }

  /**
   * Construct the row-level (instanceId) attachment uri for a specific file
   *
   * @param tableIdInstanceFileServiceUri
   * @param instanceId
   * @param cleanRowpathUri
   * @return
   */
  public URI constructInstanceFileUri(String tableIdInstanceFileServiceUri, String instanceId, String cleanRowpathUri) {
    URI uri = normalizeUri(tableIdInstanceFileServiceUri, instanceId + "/file/" + cleanRowpathUri);
    return uri;
  }

  /**
   * Simple Request for all server interactions.
   *
   * @param uri
   * @param request
   * @return
   */
  public void buildBasicRequest(URI uri, HttpRequestBase request) {

    if (uri == null) {
      throw new IllegalArgumentException("buildBasicRequest: URI cannot be null");
    }

    if (request == null) {
      throw new IllegalArgumentException("buildBasicRequest: HttpRequest cannot be null");
    }

    String agg_uri = uri.toString();
    log.i(LOGTAG, "buildBasicRequest: agg_uri is " + agg_uri);

    request.setURI(uri);

    // report our locale... (not currently used by server)
    request.addHeader("Accept-Language", Locale.getDefault().getLanguage());
    request.addHeader(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER, ApiConstants.OPEN_DATA_KIT_VERSION);
    request.addHeader(ApiConstants.OPEN_DATA_KIT_INSTALLATION_HEADER, sc.getInstallationId());
    request.addHeader(ApiConstants.ACCEPT_CONTENT_ENCODING_HEADER, ApiConstants.GZIP_CONTENT_ENCODING);
    request.addHeader(HttpHeaders.USER_AGENT, sc.getUserAgent());

    GregorianCalendar g = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
    Date now = new Date();
    g.setTime(now);
    SimpleDateFormat formatter = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss zz", Locale.US);
    formatter.setCalendar(g);
    request.addHeader(ApiConstants.DATE_HEADER, formatter.format(now));
  }

  /**
   * Request to receive a JSON response. Unspecified content type
   *
   * @param uri
   * @param request
   */
  public void buildBasicJsonResponseRequest(URI uri, HttpRequestBase request) {

    buildBasicRequest(uri, request);

    // set our preferred response media type to json using quality parameters
    NameValuePair param1 = (new BasicNameValuePair("q", "1.0"));
    ContentType json = ContentType.create(ContentType.APPLICATION_JSON.getMimeType(), param1);

    // don't really want plaintext...
    NameValuePair param2 = new BasicNameValuePair("charset", CharEncoding.UTF_8.toLowerCase(Locale.ENGLISH));
    NameValuePair param3 = new BasicNameValuePair("q", "0.4");

    ContentType tplainUtf8 = ContentType.create(ContentType.TEXT_PLAIN.getMimeType(), param2, param3);

    // accept either json or plain text (no XML to device)
    request.addHeader("accept", json.toString());

    request.addHeader("accept", tplainUtf8.toString());

    // set the response entity character set to CharEncoding.UTF_8
    request.addHeader("Accept-Charset", CharEncoding.UTF_8);
  }


  /**
   * Request to send a specified content-type body and receive a JSON response
   *
   * @param uri
   * @param contentType
   * @param request
   */
  public void buildSpecifiedContentJsonResponseRequest(URI uri, ContentType contentType,
      HttpRequestBase request) {

    buildBasicJsonResponseRequest(uri, request);

    if ( contentType != null ) {
      request.addHeader("content-type", contentType.toString());
    }
  }

  /**
   * Request to send a no-content-body and receive a JSON response
   *
   * @param uri
   * @param request
   */
  public void buildNoContentJsonResponseRequest(URI uri, HttpRequestBase request) {

    if ( request.getMethod().equals(HttpPost.METHOD_NAME) ||
        request.getMethod().equals(HttpPut.METHOD_NAME) ) {
      throw new IllegalArgumentException("No content type specified on a POST or PUT request!");
    }
    buildBasicJsonResponseRequest(uri, request);
  }

  /**
   * Request to send a JSON content body and receive a JSON response
   *
   * @param uri
   * @param request
   */
  public void buildJsonContentJsonResponseRequest(URI uri, HttpRequestBase request) {

    // select our preferred protocol...
    ContentType protocolType = ContentType.APPLICATION_JSON;

    buildSpecifiedContentJsonResponseRequest(uri, protocolType, request);
  }


  public HttpRestProtocolWrapper(SyncExecutionContext sc) throws
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

    // HttpClient for auth tokens
    localAuthContext = new BasicHttpContext();

    SocketConfig socketAuthConfig = SocketConfig.copy(SocketConfig.DEFAULT).setSoTimeout(2 * CONNECTION_TIMEOUT).build();

    RequestConfig requestAuthConfig = RequestConfig.copy(RequestConfig.DEFAULT)
            .setConnectTimeout(CONNECTION_TIMEOUT)
            // support authenticating
            .setAuthenticationEnabled(true)
            // support redirecting to handle http: => https: transition
            .setRedirectsEnabled(true)
            // max redirects is set to 4
            .setMaxRedirects(4)
            .setCircularRedirectsAllowed(true)
            //.setTargetPreferredAuthSchemes(targetPreferredAuthSchemes)
            .setCookieSpec(CookieSpecs.DEFAULT)
            .build();

    httpAuthClient = HttpClientBuilder.create()
            .setDefaultSocketConfig(socketAuthConfig)
            .setDefaultRequestConfig(requestAuthConfig).build();

    // Context
    // context holds authentication state machine, so it cannot be
    // shared across independent activities.
    localContext = new BasicHttpContext();

    cookieStore = new BasicCookieStore();
    credsProvider = new BasicCredentialsProvider();

    URI destination = normalizeUri(sc.getAggregateUri(), "/");
    String host = destination.getHost();
    String authenticationType = sc.getAuthenticationType();

    if ( sc.getString(R.string.credential_type_google_account)
        .equals(authenticationType)) {

      String accessToken = sc.getAccessToken();
      checkAccessToken(accessToken);
      this.accessToken = accessToken;


    } else if ( sc.getString(R.string.credential_type_username_password)
        .equals(authenticationType)) {
      String username = sc.getUsername();
      String password = sc.getPassword();

      List<AuthScope> asList = new ArrayList<AuthScope>();
      {
        AuthScope a;
        // allow digest auth on any port...
        // TODO switch this to digest
        a = new AuthScope(host, -1, null, AuthSchemes.DIGEST);
        asList.add(a);
        if ( destination.getScheme().equals("https")) {
          // and allow basic auth on https connections...
          a = new AuthScope(host, destination.getPort(), null, AuthSchemes.BASIC);
          asList.add(a);
        }
        // this might be disabled in production builds...
        if ( sc.getAllowUnsafeAuthentication() ) {
          log.e(LOGTAG, "Enabling Unsafe Authentication!");
          a = new AuthScope(host, -1, null, AuthSchemes.BASIC);
          asList.add(a);
        }

      }

      // add username
      if (username != null && username.trim().length() != 0) {
        log.i(LOGTAG, "adding credential for host: " + host + " username:" + username);
        Credentials c = new UsernamePasswordCredentials(username, password);

        for (AuthScope a : asList) {
          credsProvider.setCredentials(a, c);
        }
      }
    }

    localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
    localContext.setAttribute(HttpClientContext.CREDS_PROVIDER, credsProvider);

    SocketConfig socketConfig = SocketConfig.copy(SocketConfig.DEFAULT).setSoTimeout(2 * CONNECTION_TIMEOUT).build();

    // if possible, bias toward digest auth (may not be in 4.0 beta 2)
    List<String> targetPreferredAuthSchemes = new ArrayList<String>();
    targetPreferredAuthSchemes.add(AuthSchemes.DIGEST);
    targetPreferredAuthSchemes.add(AuthSchemes.BASIC);

    RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
            .setConnectTimeout(CONNECTION_TIMEOUT)
            // support authenticating
            .setAuthenticationEnabled(true)
            // support redirecting to handle http: => https: transition
            .setRedirectsEnabled(true)
            // max redirects is set to 4
            .setMaxRedirects(4)
            .setCircularRedirectsAllowed(true)
            .setTargetPreferredAuthSchemes(targetPreferredAuthSchemes)
            .setCookieSpec(CookieSpecs.DEFAULT)
            .build();

    httpClient = HttpClientBuilder.create()
            .setDefaultSocketConfig(socketConfig)
            .setDefaultRequestConfig(requestConfig).build();

  }

  private final static String authString = "oauth2:https://www.googleapis.com/auth/userinfo.email";

  private String updateAccessToken() throws InvalidAuthTokenException {
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

    CloseableHttpResponse response = null;
    try {
      HttpGet request = new HttpGet();
      String tokenStr =  TOKEN_INFO + URLEncoder.encode(accessToken, ApiConstants.UTF8_ENCODE);
      URI tokenUri = new URI(tokenStr);
      request.setURI(tokenUri);

      if (localAuthContext != null) {
        response = httpAuthClient.execute(request, localAuthContext);
      } else {
        response = httpAuthClient.execute(request);
      }
    } catch (Exception e) {
      log.e(LOGTAG, "HttpClientErrorException in checkAccessToken");
      log.printStackTrace(e);
      throw new InvalidAuthTokenException("Invalid auth token (): " + accessToken, e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.e(LOGTAG, "checkAccessToken: error when trying to close response");
        log.printStackTrace(e);
      }
    }
  }

  public String convertResponseToString(CloseableHttpResponse response) throws IOException {

    if (response == null) {
      throw new IllegalArgumentException("Can't convert null response to string!!");
    }

    try {
      BufferedReader rd = new BufferedReader(
          new InputStreamReader(response.getEntity().getContent(), Charset.forName("UTF-8")));

      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String res = strLine.toString();

      return res;
    } finally {
      response.close();
    }
  }

  public CloseableHttpResponse httpClientExecute(HttpRequestBase request, List<Integer>
      handledReturnCodes) throws HttpClientWebException {

    CloseableHttpResponse response = null;
    String authenticationType = sc.getAuthenticationType();

    boolean isGoogleAccount = false;
    if ( sc.getString(R.string.credential_type_google_account)
        .equals(authenticationType)) {

      isGoogleAccount = true;
      request.addHeader("Authorization", "Bearer " + accessToken);
    }

    // we set success to true when we return the response.
    // When we exit the outer try, if success is false,
    // consume any response entity and close the response.
    boolean success = false;
    try {
      try {
        if (localContext != null) {
          response = httpClient.execute(request, localContext);
        } else {
          response = httpClient.execute(request);
        }

        if (isGoogleAccount && response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
          request.removeHeaders("Authorization");
          updateAccessToken();
          request.addHeader("Authorization", "Bearer " + accessToken);

          // re-issue the request with new access token
          if (localContext != null) {
            response = httpClient.execute(request, localContext);
          } else {
            response = httpClient.execute(request);
          }
        }
      } catch (MalformedURLException e) {
        log.e(LOGTAG, "Bad client config -- malformed URL");
        log.printStackTrace(e);
        // bad client config
        throw new BadClientConfigException("malformed URL", e, request, response);
      } catch (UnknownHostException e) {
        log.e(LOGTAG, "Bad client config -- Unknown host");
        log.printStackTrace(e);
        // bad client config
        throw new BadClientConfigException("Unknown Host", e, request, response);
      } catch (ClientProtocolException e) {
        log.e(LOGTAG, "Bad request construction - " + e.toString());
        log.printStackTrace(e);
        // bad request construction
        throw new ServerDetectedVersionMismatchedClientRequestException("Bad request construction - " + e.toString(), e,
                request, response);
      } catch (UnknownServiceException e) {
        log.e(LOGTAG, "Bad request construction - " + e.toString());
        log.printStackTrace(e);
        // bad request construction
        throw new ServerDetectedVersionMismatchedClientRequestException("Bad request construction - " + e.toString(), e,
                request, response);
      } catch (InvalidAuthTokenException e) {
        log.e(LOGTAG, "updating of Google access token failed");
        log.printStackTrace(e);
        // problem interacting with Google to update Auth token.
        // this should be treated as an authentication failure
        throw new AccessDeniedReauthException("updating of Google access token failed", e,
                request, response);
      } catch (Exception e) {
        log.e(LOGTAG, "Network failure - " + e.toString());
        log.printStackTrace(e);
        // network transmission or SSL or other comm failure
        throw new NetworkTransmissionException("Network failure - " + e.toString(), e,
                request, response);
      }

      // TODO: For now we have to check for 401 Unauthorized before we check the headers because
      // Spring will spit out a 401 with a bad username/password before it even touches our code
      int statusCode = response.getStatusLine().getStatusCode();
      String errorText = "Unexpected server response statusCode: " + Integer.toString(statusCode);
      if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
        log.e(LOGTAG, errorText);
        // server rejected our request -- mismatched client and server implementations
        throw new AccessDeniedException(errorText, request, response);
      }

      // if we do not find our header in the response, then this is most likely a
      // wifi network login screen.
      Header[] odkHeaders = response.getHeaders(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER);
      if (odkHeaders == null || odkHeaders.length == 0) {
        throw new NotOpenDataKitServerException(request, response);
      }

      if (handledReturnCodes.contains(statusCode)) {
        success = true;
        return response;
      }

      if (statusCode >= 200 && statusCode < 300) {
        log.e(LOGTAG, errorText);
        // server returned an unexpected success response --
        // mismatched client and server implementations
        throw new ClientDetectedVersionMismatchedServerResponseException(errorText,
                request, response);
      }

      if (statusCode >= 400 && statusCode < 500) {
        log.e(LOGTAG, errorText);
        // server rejected our request -- mismatched client and server implementations
        throw new ServerDetectedVersionMismatchedClientRequestException(errorText,
                request, response);
      }

      if (statusCode >= 500 && statusCode < 600) {
        log.e(LOGTAG, errorText);
        // internal error within the server -- admin should check server logs
        throw new InternalServerFailureException(errorText,
                request, response);
      }

      log.e(LOGTAG, errorText);
      // some sort of 300 (or impossible) response that we were
      // not expecting and don't know how to handle
      throw new UnexpectedServerRedirectionStatusCodeException(errorText,
              request, response);
    } finally {
      if ( response != null && !success ) {
        EntityUtils.consumeQuietly(response.getEntity());
        try {
          response.close();
        } catch (IOException e) {
          log.e(LOGTAG, "failed to close response");
          log.printStackTrace(e);
        }
      }
    }
  }

  public String determineContentType(String fileName) {
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

  public String extractInstanceFileRelativeFilename(String header) {
    // Get the file name
    int firstIndex = header.indexOf(multipartFileHeader);
    if ( firstIndex == -1 ) {
      return null;
    }
    firstIndex += multipartFileHeader.length();
    int lastIndex = header.lastIndexOf("\"");
    String partialPath = header.substring(firstIndex, lastIndex);
    return partialPath;
  }

  public HttpEntity makeHttpEntity(File localFile) throws IOException {
    if (localFile == null) {
      throw new IllegalArgumentException("makeHttpEntity: localFile cannot be null");
    }

    int size = (int) localFile.length();
    byte[] bytes = new byte[size];
    try {
      BufferedInputStream buf = new BufferedInputStream(new FileInputStream(localFile));
      buf.read(bytes, 0, bytes.length);
      buf.close();
    } catch (IOException ioe) {
      log.e(LOGTAG, "makeHttpEntity: exception while tyring to read file");
      log.printStackTrace(ioe);
      throw ioe;
    }

    return new GzipCompressingEntity(new ByteArrayEntity(bytes));
    //return new ByteArrayEntity(bytes);
  }

}
