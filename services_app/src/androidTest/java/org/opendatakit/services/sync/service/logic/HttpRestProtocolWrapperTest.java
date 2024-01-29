package org.opendatakit.services.sync.service.logic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import android.Manifest;
import android.content.Context;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.GrantPermissionRule;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.net.URIBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opendatakit.aggregate.odktables.rest.ApiConstants;
import org.opendatakit.database.service.ODKServiceTestRule;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.services.sync.service.SyncProgressTracker;
import org.opendatakit.services.sync.service.exceptions.NotOpenDataKitServerException;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.utilities.ODKFileUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Niles on 7/3/17.
 */
public class HttpRestProtocolWrapperTest {
  private static final Pattern PATTERN_1 = Pattern.compile("%3F", Pattern.LITERAL);
  private static final Pattern PATTERN_2 = Pattern.compile("%3D", Pattern.LITERAL);
  private static final Pattern PATTERN_3 = Pattern.compile("%26", Pattern.LITERAL);
  private SyncExecutionContext sc;
  private HttpRestProtocolWrapper h;

  private URI uri;

  private String appName = "REST-Test";


  @Rule
  public final ODKServiceTestRule mServiceRule = new ODKServiceTestRule();

  @Rule
  public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);

  @Rule
  public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

  public String makePlainUri(String path) throws Exception {
    URIBuilder b = new URIBuilder(uri);
    b.setPath(path);
    String tmp = b.build().toString();
    tmp = PATTERN_1.matcher(tmp).replaceAll(Matcher.quoteReplacement("?"));
    tmp = PATTERN_2.matcher(tmp).replaceAll(Matcher.quoteReplacement("="));
    tmp = PATTERN_3.matcher(tmp).replaceAll(Matcher.quoteReplacement("&"));
    return tmp;
  }

  public String makeOdkxUri(String path) throws Exception {
    return makePlainUri("/odktables/"+ appName + path);
  }

  public String makeOdkxManifestUri(String table) throws Exception {
    return makeOdkxUri("/manifest/" + sc.getOdkClientApiVersion() + "/" + table);
  }

  public String makeOdkxFilesUri(String filePath) throws Exception {
    return makeOdkxUri("/files/" + sc.getOdkClientApiVersion() + "/" + filePath);
  }
  public String makeOdkxTablesUri(String table) throws Exception {
    return makeOdkxUri("/tables/" + table);
  }


  private void checkJsonRequest(HttpUriRequestBase req) throws Exception {
    checkGenericRequest(req);
    for (Header header : req.getHeaders("accept")) {
      switch (header.getValue()) {
        case "application/json; q=1.0":
        case "text/plain; charset=utf-8; q=0.4":
          break;
        default:
          throw new Exception("Bad accept header: " + header.getValue());
      }
    }
  }

  @Before
  public void setUp() throws Throwable {
    Context underTestContext = InstrumentationRegistry.getInstrumentation().getTargetContext();
    String uriStr = underTestContext.getString(R.string.sync_default_server_url);
    uri = new URI(uriStr);

    SyncProgressTracker syncProg = new SyncProgressTracker(underTestContext,
            new GlobalSyncNotificationManagerStub(), appName);
    SyncOverallResult syncRes = new SyncOverallResult();

    sc =  new SyncExecutionContext(underTestContext, "version_here", appName, syncProg, syncRes);
    h = new HttpRestProtocolWrapper(sc);
    sc.setSynchronizer(new AggregateSynchronizer(sc));
  }

  @Test
  public void testConstructListOfAppNamesUri() throws Exception {
    assertEquals(h.constructListOfAppNamesUri().toString(), makePlainUri("/odktables/"));
  }

  @Test
  public void testConstructListOfUserRolesAndDefaultGroupUri() throws Exception {
    String testStr = h.constructListOfUserRolesAndDefaultGroupUri().toString();
    String verifyStr = makeOdkxUri("/privilegesInfo");
    assertEquals(testStr,verifyStr);
  }

  @Test
  public void testConstructListOfUsersUri() throws Exception {
    assertEquals(h.constructListOfUsersUri().toString(), makeOdkxUri("/usersInfo"));
  }

  @Test
  public void testConstructDeviceInformationUri() throws Exception {
    assertEquals(h.constructDeviceInformationUri().toString(),
        makeOdkxUri("/installationInfo"));
  }

  @Test
  public void testConstructListOfTablesUri() throws Exception {
    String testStr = h.constructListOfTablesUri(null).toString();
    String verifyStr = makeOdkxUri("/tables");
    assertEquals(testStr, verifyStr);

    testStr = h.constructListOfTablesUri("webSafeResumeCursor").toString();
    verifyStr = makeOdkxUri("/tables?cursor=webSafeResumeCursor");
    assertEquals(testStr, verifyStr);
  }

  @Test
  public void testConstructAppLevelFileManifestUri() throws Exception {
    assertEquals(h.constructAppLevelFileManifestUri().toString(),
            makeOdkxManifestUri(""));

  }

  @Test
  public void testConstructTableLevelFileManifestUri() throws Exception {
    String testTableName = "Tea_houses";
    String testUri = h.constructTableLevelFileManifestUri(testTableName).toString();
    String verifyUri = makeOdkxManifestUri(testTableName);
    assertEquals(testUri,verifyUri);

  }

  @Test
  public void testConstructConfigFileUri() throws Exception {
    String testStr = "tables/Tea_houses/forms/Tea_houses/formDef.json";
    String testUri = h.constructConfigFileUri(testStr).toString();
    String verfiyUri = makeOdkxFilesUri(testStr);
    assertEquals(testUri, verfiyUri);
  }

  @Test
  public void testConstructTableIdUri() throws Exception {
    String testStr = h.constructTableIdUri("Tea_houses").toString();
    String verifyStr = makeOdkxTablesUri("Tea_houses");
    assertEquals(testStr, verifyStr);
  }

  @Test
  public void testConstructRealizedTableIdUri() throws Exception {
    assertEquals(h.constructRealizedTableIdUri("Tea_houses", "etag_here").toString(),
        makeOdkxUri("/tables/Tea_houses/ref/etag_here"));

  }

  @Test
  public void testConstructRealizedTableIdSyncStatusUri() throws Exception {
    assertEquals(h.constructRealizedTableIdSyncStatusUri("Tea_houses", "etag_here").toString(),
        makeOdkxUri("/tables/Tea_houses/ref/etag_here/installationStatus"));

  }

  @Test
  public void testConstructTableDiffChangeSetsUri() throws Exception {
    assertEquals(
        h.constructTableDiffChangeSetsUri(makeOdkxUri("/tableDiffUri/Tea_houses"), "etag").toString(),
            makeOdkxUri("/tableDiffUri/Tea_houses/changeSets?data_etag=etag"));
    assertEquals(
        h.constructTableDiffChangeSetsUri(makeOdkxUri("/tableDiffUri/Tea_houses"), "etag").toString(),
            makeOdkxUri("/tableDiffUri/Tea_houses/changeSets?data_etag=etag"));
  }

  @Test
  public void testConstructTableDiffChangeSetsForDataETagUri() throws Exception {
    String testStr = h.constructTableDiffChangeSetsForDataETagUri(makeOdkxUri("/tableDiffUri/Tea_houses"), "etag",
            false, "websafe_resume_cursor").toString();
    String verifyStr = makeOdkxUri("/tableDiffUri/Tea_houses/changeSets/etag?cursor=websafe_resume_cursor");

    assertEquals(testStr, verifyStr);

    testStr = h.constructTableDiffChangeSetsForDataETagUri(makeOdkxUri("/tableDiffUri/Tea_houses"), "etag",
            true, "websafe_resume_cursor").toString();
    verifyStr = makeOdkxUri(
            "/tableDiffUri/Tea_houses/changeSets/etag?data_etag=true&cursor"
                    + "=websafe_resume_cursor");
    assertEquals(testStr, verifyStr);

    testStr = h.constructTableDiffChangeSetsForDataETagUri(makeOdkxUri("/tableDiffUri/Tea_houses"), null,
            false, "websafe_resume_cursor").toString();
    verifyStr = makeOdkxUri("/tableDiffUri/Tea_houses/changeSets/null?cursor=websafe_resume_cursor");
    assertEquals(testStr, verifyStr);

    testStr = h.constructTableDiffChangeSetsForDataETagUri(makeOdkxUri("/tableDiffUri/Tea_houses"), null,
            true, "websafe_resume_cursor").toString();
    verifyStr = makeOdkxUri(
            "/tableDiffUri/Tea_houses/changeSets/null?data_etag=true&cursor=websafe_resume_cursor");
    assertEquals(testStr, verifyStr);

  }

  @Test
  public void testConstructTableDataUri() throws Exception {
    String testStr = h.constructTableDataUri(makeOdkxUri("/tableDiffUri/Tea_houses"), "cursor", 100).toString();
    String verifyStr = makeOdkxUri("/tableDiffUri/Tea_houses?fetchLimit=100&cursor=cursor");
    assertEquals(testStr, verifyStr);
    testStr = h.constructTableDataUri(makeOdkxUri("/tableDiffUri/Tea_houses"), null, 100).toString();
    verifyStr = makeOdkxUri("/tableDiffUri/Tea_houses?fetchLimit=100");
    assertEquals(testStr, verifyStr);
  }

  @Test
 public void testConstructTableDataDiffUri() throws Exception {
    String testStr = h.constructTableDataDiffUri(makePlainUri("/tableDiffUri/Tea_houses"), "etag", "cursor", 100)
            .toString();
    String verifyStr = makePlainUri("/tableDiffUri/Tea_houses?data_etag=etag&fetchLimit=100&cursor=cursor");
    assertEquals(testStr, verifyStr);

    testStr = h.constructTableDataDiffUri(makePlainUri("/tableDiffUri/Tea_houses"), null, "cursor", 100)
            .toString();
    verifyStr = makePlainUri("/tableDiffUri/Tea_houses?data_etag&fetchLimit=100&cursor=cursor");

    assertEquals(testStr, verifyStr);

    testStr = h.constructTableDataDiffUri(makePlainUri("/tableDiffUri/Tea_houses"), "etag", null, 100)
            .toString();
    verifyStr = makePlainUri("/tableDiffUri/Tea_houses?data_etag=etag&fetchLimit=100");

    assertEquals(testStr, verifyStr);
  }

  @Test
  public void testConstructInstanceFileManifestUri() throws Exception {
    String testStr = h.constructInstanceFileManifestUri(makeOdkxUri("/tableIdInstanceFileServiceUri/Tea_houses"),
            "instance_id").toString();
    String verifyStr = makeOdkxUri("/tableIdInstanceFileServiceUri/Tea_houses/instance_id/manifest");
    assertEquals(testStr, verifyStr);
  }

  @Test
  public void testConstructInstanceFileBulkUploadUri() throws Exception {
    assertEquals(
        h.constructInstanceFileBulkUploadUri(makeOdkxUri("tableIdInstanceFileServiceUri/Tea_houses"),
            "instanceId").toString(),
        makeOdkxUri("tableIdInstanceFileServiceUri/Tea_houses/instanceId/upload"));
  }

  @Test
  public void testConstructInstanceFileBulkDownloadUri() throws Exception {
    assertEquals(
        h.constructInstanceFileBulkDownloadUri(makeOdkxUri("tableIdInstanceFileServiceUri/Tea_houses"),
            "instanceId").toString(),
        makeOdkxUri("tableIdInstanceFileServiceUri/Tea_houses/instanceId/download"));

  }

  @Test
  public void testConstructInstanceFileUri() throws Exception {
    assertEquals(h.constructInstanceFileUri(makeOdkxUri("tableIdInstanceFileServiceUri/Tea_houses"),
        "instanceId", "rowPath").toString(),
        makeOdkxUri("tableIdInstanceFileServiceUri/Tea_houses/instanceId/file/rowPath"));

  }

  private void checkGenericRequest(HttpUriRequestBase req) throws URISyntaxException {
    String reqUri = req.getUri().toString();
    String calcUri = h.constructListOfAppNamesUri().toString();
    assertEquals(reqUri, calcUri);
    assertEquals(req.getFirstHeader(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER).getValue(),
        ApiConstants.OPEN_DATA_KIT_VERSION);
    assertEquals(req.getFirstHeader(ApiConstants.OPEN_DATA_KIT_INSTALLATION_HEADER).getValue(),
        sc.getInstallationId());
    assertEquals(req.getFirstHeader(ApiConstants.ACCEPT_CONTENT_ENCODING_HEADER).getValue(),
        ApiConstants.GZIP_CONTENT_ENCODING);
    assertEquals(req.getFirstHeader(HttpHeaders.USER_AGENT).getValue(), sc.getUserAgent());
  }

  @Test
  public void testBuildBasicRequest() throws URISyntaxException {
    HttpUriRequestBase req = new HttpGet(h.constructListOfAppNamesUri());
    h.buildBasicRequest(req);
    checkGenericRequest(req);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildBasicRequestWithNullRequest()  {
    h.buildBasicRequest(null);
  }

  @Test
  public void testBuildBasicJsonResponseRequest() throws Exception {
    HttpUriRequestBase req = new HttpGet(h.constructListOfAppNamesUri());
    h.buildBasicJsonResponseRequest(req);
    checkJsonRequest(req);
  }

  @Test
  public void testBuildSpecifiedContentJsonResponseRequest() throws Exception {
    HttpUriRequestBase req = new HttpGet(h.constructListOfAppNamesUri());
    h.buildSpecifiedContentJsonResponseRequest(
        ContentType.APPLICATION_FORM_URLENCODED, req);
    assertEquals(req.getFirstHeader("content-type").getValue(),
        ContentType.APPLICATION_FORM_URLENCODED.toString());
    checkJsonRequest(req);
  }

  @Test
  public void testBuildSpecifiedContentJsonResponseRequestWithNullContentType() throws Exception {
    HttpUriRequestBase req = new HttpGet(h.constructListOfAppNamesUri());
    h.buildSpecifiedContentJsonResponseRequest(null, req);
    assertNull(req.getFirstHeader("content-type"));
    checkJsonRequest(req);

  }

  @Test
  public void testBuildNoContentJsonResponseRequestOnGet() throws Exception {
    HttpUriRequestBase req = new HttpGet(h.constructListOfAppNamesUri());
    h.buildNoContentJsonResponseRequest(req);
    checkJsonRequest(req);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildNoContentJsonResponseRequestOnPost() throws Exception {
    HttpUriRequestBase req = new HttpPost(h.constructListOfAppNamesUri());
    h.buildNoContentJsonResponseRequest(req);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildNoContentJsonResponseRequestOnPut() throws Exception {
    HttpUriRequestBase req = new HttpPut(h.constructListOfAppNamesUri());
    h.buildNoContentJsonResponseRequest(req);
  }

  @Test
  public void testBuildJsonContentJsonResponseRequest() throws Exception {
    HttpUriRequestBase req = new HttpGet(h.constructListOfAppNamesUri());
    h.buildJsonContentJsonResponseRequest(req);
    assertEquals(req.getFirstHeader("content-type").getValue(),
        ContentType.APPLICATION_JSON.toString());
    checkJsonRequest(req);
  }

  //@Test
  @Test(expected = NotOpenDataKitServerException.class)
  public void testConvertResponseToStringAndHttpClientExecute() throws Exception {
    HttpUriRequestBase req = new HttpGet("https://gstatic.com/generate_204");
    CloseableHttpResponse response = h.httpClientExecute(req, Collections.singletonList(204));
    assertEquals(HttpRestProtocolWrapper.convertResponseToString(response), "");
    req = new HttpGet("https://raw.githubusercontent.com/does_not_exist");
    response = h.httpClientExecute(req, Collections.singletonList(400));
    assertEquals(HttpRestProtocolWrapper.convertResponseToString(response), "400: Invalid request");
    req = new HttpGet(
        "https://raw.githubusercontent.com/nilesr/emulator/tree/master/does_not_exist");
    response = h.httpClientExecute(req, Collections.singletonList(404));
    assertEquals(HttpRestProtocolWrapper.convertResponseToString(response), "404: Not Found");
  }

  //@Test(expected = ServerDetectedVersionMismatchedClientRequestException.class)
  @Test(expected = NotOpenDataKitServerException.class)
  public void testHttpClientExecuteNotHandled() throws Exception {
    HttpUriRequestBase req = new HttpGet(
        "https://raw.githubusercontent.com/nilesr/emulator/tree/master/does_not_exist");
    h.httpClientExecute(req, Collections.singletonList(200));
  }

  //@Test(expected = ServerDetectedVersionMismatchedClientRequestException.class)
  @Test(expected = NotOpenDataKitServerException.class)
  public void testHttpClientExecute200NotHandled() throws Exception {
    // should return 200, but we only handle 404, so should throw an exception
    HttpUriRequestBase req = new HttpGet("http://myip.is/");
    h.httpClientExecute(req, Collections.singletonList(404));
  }

  @Test
  public void testDetermineContentType() throws Exception {
    assertEquals(HttpRestProtocolWrapper.determineContentType(
        ODKFileUtils.getFormFolder("default", "Tea_houses", "Tea_houses") + "/"
            + ODKFileUtils.FORMDEF_JSON_FILENAME), "application/x-javascript");
    assertEquals(HttpRestProtocolWrapper.determineContentType("file with no extension"), "application/octet-stream");
    assertEquals(HttpRestProtocolWrapper.determineContentType("something.tiff"), "image/tiff");
  }

  @Test
  public void testExtractInstanceFileRelativeFilename() throws Exception {
    assertEquals(
        HttpRestProtocolWrapper.extractInstanceFileRelativeFilename("something filename=\"test\"..."), "test");
    assertNull(HttpRestProtocolWrapper.extractInstanceFileRelativeFilename("something filename=bad format"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMakeHttpEntityNullArgument() throws Exception {
    h.makeHttpEntity(null);
  }

}