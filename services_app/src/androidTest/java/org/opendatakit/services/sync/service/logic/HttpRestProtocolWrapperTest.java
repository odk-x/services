package org.opendatakit.services.sync.service.logic;

import android.support.test.runner.AndroidJUnit4;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.aggregate.odktables.rest.ApiConstants;
import org.opendatakit.httpclientandroidlib.Header;
import org.opendatakit.httpclientandroidlib.HttpEntity;
import org.opendatakit.httpclientandroidlib.HttpHeaders;
import org.opendatakit.httpclientandroidlib.client.entity.GzipCompressingEntity;
import org.opendatakit.httpclientandroidlib.client.methods.*;
import org.opendatakit.httpclientandroidlib.client.utils.URIBuilder;
import org.opendatakit.httpclientandroidlib.entity.ByteArrayEntity;
import org.opendatakit.httpclientandroidlib.entity.ContentType;
import org.opendatakit.httpclientandroidlib.entity.HttpEntityWrapper;
import org.opendatakit.services.application.Services;
import org.opendatakit.services.sync.service.SyncExecutionContext;
import org.opendatakit.services.sync.service.exceptions.NotOpenDataKitServerException;
import org.opendatakit.sync.service.SyncNotification;
import org.opendatakit.sync.service.SyncOverallResult;
import org.opendatakit.utilities.ODKFileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Created by Niles on 7/3/17.
 */
@RunWith(AndroidJUnit4.class)
public class HttpRestProtocolWrapperTest {
  private static final Pattern PATTERN = Pattern.compile("%3F", Pattern.LITERAL);
  private SyncExecutionContext sc;
  private HttpRestProtocolWrapper h;

  public static String makeUri(String path) throws Exception {
    URIBuilder b = new URIBuilder();
    b.setScheme("https");
    b.setHost("open-data-kit.appspot.com");
    b.setPath(path);
    return PATTERN.matcher(b.build().toString()).replaceAll(Matcher.quoteReplacement("?"));
  }

  private void checkJsonRequest(HttpRequestBase req) throws Exception {
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
    sc = new SyncExecutionContext(Services._please_dont_use_getInstance(), "version_here",
        "default", new SyncNotification(Services._please_dont_use_getInstance(), "default"),
        new SyncOverallResult());
    h = new HttpRestProtocolWrapper(sc);
  }

  @Test
  public void testConstructListOfAppNamesUri() throws Exception {
    assertEquals(h.constructListOfAppNamesUri().toString(), makeUri("/odktables/"));
  }

  @Test
  public void testConstructListOfUserRolesAndDefaultGroupUri() throws Exception {
    assertEquals(h.constructListOfUserRolesAndDefaultGroupUri().toString(),
        makeUri("/odktables/default/privilegesInfo"));

  }

  @Test
  public void testConstructListOfUsersUri() throws Exception {
    assertEquals(h.constructListOfUsersUri().toString(), makeUri("/odktables/default/usersInfo"));
  }

  @Test
  public void testConstructDeviceInformationUri() throws Exception {
    assertEquals(h.constructDeviceInformationUri().toString(),
        makeUri("/odktables/default/installationInfo"));
  }

  @Test
  public void testConstructListOfTablesUri() throws Exception {
    assertEquals(h.constructListOfTablesUri(null).toString(), makeUri("/odktables/default/tables"));
    assertEquals(h.constructListOfTablesUri("webSafeResumeCursor").toString(),
        makeUri("/odktables/default/tables?cursor=webSafeResumeCursor"));
  }

  @Test
  public void testConstructAppLevelFileManifestUri() throws Exception {
    assertEquals(h.constructAppLevelFileManifestUri().toString(),
        makeUri("/odktables/default/manifest/version_he/"));

  }

  @Test
  public void testConstructTableLevelFileManifestUri() throws Exception {
    assertEquals(h.constructTableLevelFileManifestUri("Tea_houses").toString(),
        makeUri("/odktables/default/manifest/version_he/Tea_houses"));

  }

  @Test
  public void testConstructConfigFileUri() throws Exception {
    assertEquals(
        h.constructConfigFileUri("tables/Tea_houses/forms/Tea_houses/formDef.json").toString(),
        makeUri("/odktables/default/files/version_he/tables/Tea_houses/forms/Tea_houses/formDef"
            + ".json"));

  }

  @Test
  public void testConstructTableIdUri() throws Exception {
    assertEquals(h.constructTableIdUri("Tea_houses").toString(),
        makeUri("/odktables/default/tables/Tea_houses"));

  }

  @Test
  public void testConstructRealizedTableIdUri() throws Exception {
    assertEquals(h.constructRealizedTableIdUri("Tea_houses", "etag_here").toString(),
        makeUri("/odktables/default/tables/Tea_houses/ref/etag_here"));

  }

  @Test
  public void testConstructRealizedTableIdSyncStatusUri() throws Exception {
    assertEquals(h.constructRealizedTableIdSyncStatusUri("Tea_houses", "etag_here").toString(),
        makeUri("/odktables/default/tables/Tea_houses/ref/etag_here/installationStatus"));

  }

  @Test
  public void testConstructTableDiffChangeSetsUri() throws Exception {
    assertEquals(
        h.constructTableDiffChangeSetsUri(makeUri("/tableDiffUri/Tea_houses"), "etag").toString(),
        makeUri("/tableDiffUri/Tea_houses/changeSets?data_etag=etag"));
    assertEquals(
        h.constructTableDiffChangeSetsUri(makeUri("/tableDiffUri/Tea_houses"), "etag").toString(),
        makeUri("/tableDiffUri/Tea_houses/changeSets?data_etag=etag"));
  }

  @Test
  public void testConstructTableDiffChangeSetsForDataETagUri() throws Exception {
    assertEquals(
        h.constructTableDiffChangeSetsForDataETagUri(makeUri("/tableDiffUri/Tea_houses"), "etag",
            false, "websafe_resume_cursor").toString(),
        makeUri("/tableDiffUri/Tea_houses/changeSets/etag?cursor=websafe_resume_cursor"));
    assertEquals(
        h.constructTableDiffChangeSetsForDataETagUri(makeUri("/tableDiffUri/Tea_houses"), "etag",
            true, "websafe_resume_cursor").toString(), makeUri(
            "/tableDiffUri/Tea_houses/changeSets/etag?data_etag=true&cursor"
                + "=websafe_resume_cursor"));
    assertEquals(
        h.constructTableDiffChangeSetsForDataETagUri(makeUri("/tableDiffUri/Tea_houses"), null,
            false, "websafe_resume_cursor").toString(),
        makeUri("/tableDiffUri/Tea_houses/changeSets/null?cursor=websafe_resume_cursor"));
    assertEquals(
        h.constructTableDiffChangeSetsForDataETagUri(makeUri("/tableDiffUri/Tea_houses"), null,
            true, "websafe_resume_cursor").toString(), makeUri(
            "/tableDiffUri/Tea_houses/changeSets/null?data_etag=true&cursor=websafe_resume_cursor"));
  }

  @Test
  public void testConstructTableDataUri() throws Exception {
    assertEquals(
        h.constructTableDataUri(makeUri("/tableDiffUri/Tea_houses"), "cursor", 100).toString(),
        makeUri("/tableDiffUri/Tea_houses?fetchLimit=100&cursor=cursor"));
    assertEquals(h.constructTableDataUri(makeUri("/tableDiffUri/Tea_houses"), null, 100).toString(),
        makeUri("/tableDiffUri/Tea_houses?fetchLimit=100"));
  }

  @Test
  public void testConstructTableDataDiffUri() throws Exception {
    assertEquals(
        h.constructTableDataDiffUri(makeUri("/tableDiffUri/Tea_houses"), "etag", "cursor", 100)
            .toString(),
        makeUri("/tableDiffUri/Tea_houses?data_etag=etag&fetchLimit=100&cursor=cursor"));
    assertEquals(
        h.constructTableDataDiffUri(makeUri("/tableDiffUri/Tea_houses"), null, "cursor", 100)
            .toString(),
        makeUri("/tableDiffUri/Tea_houses?data_etag&fetchLimit=100&cursor=cursor"));
    assertEquals(h.constructTableDataDiffUri(makeUri("/tableDiffUri/Tea_houses"), "etag", null, 100)
        .toString(), makeUri("/tableDiffUri/Tea_houses?data_etag=etag&fetchLimit=100"));
  }

  @Test
  public void testConstructInstanceFileManifestUri() throws Exception {
    assertEquals(
        h.constructInstanceFileManifestUri(makeUri("/tableIdInstanceFileServiceUri/Tea_houses"),
            "instance_id").toString(),
        makeUri("/tableIdInstanceFileServiceUri/Tea_houses/instance_id/manifest"));

  }

  @Test
  public void testConstructInstanceFileBulkUploadUri() throws Exception {
    assertEquals(
        h.constructInstanceFileBulkUploadUri(makeUri("/tableIdInstanceFileServiceUri/Tea_houses"),
            "instanceId").toString(),
        makeUri("/tableIdInstanceFileServiceUri/Tea_houses/instanceId/upload"));
  }

  @Test
  public void testConstructInstanceFileBulkDownloadUri() throws Exception {
    assertEquals(
        h.constructInstanceFileBulkDownloadUri(makeUri("/tableIdInstanceFileServiceUri/Tea_houses"),
            "instanceId").toString(),
        makeUri("/tableIdInstanceFileServiceUri/Tea_houses/instanceId/download"));

  }

  @Test
  public void testConstructInstanceFileUri() throws Exception {
    assertEquals(h.constructInstanceFileUri(makeUri("/tableIdInstanceFileServiceUri/Tea_houses"),
        "instanceId", "rowPath").toString(),
        makeUri("/tableIdInstanceFileServiceUri/Tea_houses/instanceId/file/rowPath"));

  }

  private void checkGenericRequest(HttpRequestBase req) {
    assertEquals(req.getURI().toString(), h.constructListOfAppNamesUri().toString());
    assertEquals(req.getFirstHeader(ApiConstants.OPEN_DATA_KIT_VERSION_HEADER).getValue(),
        ApiConstants.OPEN_DATA_KIT_VERSION);
    assertEquals(req.getFirstHeader(ApiConstants.OPEN_DATA_KIT_INSTALLATION_HEADER).getValue(),
        sc.getInstallationId());
    assertEquals(req.getFirstHeader(ApiConstants.ACCEPT_CONTENT_ENCODING_HEADER).getValue(),
        ApiConstants.GZIP_CONTENT_ENCODING);
    assertEquals(req.getFirstHeader(HttpHeaders.USER_AGENT).getValue(), sc.getUserAgent());
  }

  @Test
  public void testBuildBasicRequest() throws Exception {
    HttpRequestBase req = new HttpGet();
    h.buildBasicRequest(h.constructListOfAppNamesUri(), req);
    checkGenericRequest(req);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildBasicRequestWithNullRequest() throws Exception {
    h.buildBasicRequest(h.constructListOfUsersUri(), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildBasicRequestWithNullUri() throws Exception {
    HttpRequestBase req = new HttpGet();
    h.buildBasicRequest(null, req);
  }

  @Test
  public void testBuildBasicJsonResponseRequest() throws Exception {
    HttpRequestBase req = new HttpGet();
    h.buildBasicJsonResponseRequest(h.constructListOfAppNamesUri(), req);
    checkJsonRequest(req);
  }

  @Test
  public void testBuildSpecifiedContentJsonResponseRequest() throws Exception {
    HttpRequestBase req = new HttpGet();
    h.buildSpecifiedContentJsonResponseRequest(h.constructListOfAppNamesUri(),
        ContentType.APPLICATION_FORM_URLENCODED, req);
    assertEquals(req.getFirstHeader("content-type").getValue(),
        ContentType.APPLICATION_FORM_URLENCODED.toString());
    checkJsonRequest(req);
  }

  @Test
  public void testBuildSpecifiedContentJsonResponseRequestWithNullContentType() throws Exception {
    HttpRequestBase req = new HttpGet();
    h.buildSpecifiedContentJsonResponseRequest(h.constructListOfAppNamesUri(), null, req);
    assertNull(req.getFirstHeader("content-type"));
    checkJsonRequest(req);

  }

  @Test
  public void testBuildNoContentJsonResponseRequestOnGet() throws Exception {
    HttpRequestBase req = new HttpGet();
    h.buildNoContentJsonResponseRequest(h.constructListOfAppNamesUri(), req);
    checkJsonRequest(req);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildNoContentJsonResponseRequestOnPost() throws Exception {
    HttpRequestBase req = new HttpPost();
    h.buildNoContentJsonResponseRequest(h.constructListOfAppNamesUri(), req);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildNoContentJsonResponseRequestOnPut() throws Exception {
    HttpRequestBase req = new HttpPut();
    h.buildNoContentJsonResponseRequest(h.constructListOfAppNamesUri(), req);
  }

  @Test
  public void testBuildJsonContentJsonResponseRequest() throws Exception {
    HttpRequestBase req = new HttpGet();
    h.buildJsonContentJsonResponseRequest(h.constructListOfAppNamesUri(), req);
    assertEquals(req.getFirstHeader("content-type").getValue(),
        ContentType.APPLICATION_JSON.toString());
    checkJsonRequest(req);
  }

  //@Test
  @Test(expected = NotOpenDataKitServerException.class)
  public void testConvertResponseToStringAndHttpClientExecute() throws Exception {
    HttpRequestBase req = new HttpGet("https://gstatic.com/generate_204");
    CloseableHttpResponse response = h.httpClientExecute(req, Collections.singletonList(204));
    assertEquals(h.convertResponseToString(response), "");
    req = new HttpGet("https://raw.githubusercontent.com/does_not_exist");
    response = h.httpClientExecute(req, Collections.singletonList(400));
    assertEquals(h.convertResponseToString(response), "400: Invalid request");
    req = new HttpGet(
        "https://raw.githubusercontent.com/nilesr/emulator/tree/master/does_not_exist");
    response = h.httpClientExecute(req, Collections.singletonList(404));
    assertEquals(h.convertResponseToString(response), "404: Not Found");
  }

  //@Test(expected = ServerDetectedVersionMismatchedClientRequestException.class)
  @Test(expected = NotOpenDataKitServerException.class)
  public void testHttpClientExecuteNotHandled() throws Exception {
    HttpRequestBase req = new HttpGet(
        "https://raw.githubusercontent.com/nilesr/emulator/tree/master/does_not_exist");
    h.httpClientExecute(req, Collections.singletonList(200));
  }

  //@Test(expected = ServerDetectedVersionMismatchedClientRequestException.class)
  @Test(expected = NotOpenDataKitServerException.class)
  public void testHttpClientExecute200NotHandled() throws Exception {
    // should return 200, but we only handle 404, so should throw an exception
    HttpRequestBase req = new HttpGet("http://myip.is/");
    h.httpClientExecute(req, Collections.singletonList(404));
  }

  @Test
  public void testDetermineContentType() throws Exception {
    assertEquals(h.determineContentType(
        ODKFileUtils.getFormFolder("default", "Tea_houses", "Tea_houses") + "/"
            + ODKFileUtils.FORMDEF_JSON_FILENAME), "application/x-javascript");
    assertEquals(h.determineContentType("file with no extension"), "application/octet-stream");
    assertEquals(h.determineContentType("something.tiff"), "image/tiff");
  }

  @Test
  public void testExtractInstanceFileRelativeFilename() throws Exception {
    assertEquals(
        h.extractInstanceFileRelativeFilename("something filename=\"test\"..."), "test");
    assertNull(h.extractInstanceFileRelativeFilename("something filename=bad format"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMakeHttpEntityNullArgument() throws Exception {
    h.makeHttpEntity(null);
  }

  @Test
  public void testMakeHttpEntity() throws Exception {
    File file = new File(ODKFileUtils.getFormFolder("default", "Tea_houses", "Tea_houses") + "/"
        + ODKFileUtils.FORMDEF_JSON_FILENAME);
    HttpEntity res = h.makeHttpEntity(file);
    HttpEntityWrapper e = (HttpEntityWrapper) res;
    // I swear to god there is no easier way to do this, e.getContent() is useless
    Field f = HttpEntityWrapper.class.getDeclaredField("wrappedEntity");
    f.setAccessible(true);
    ByteArrayEntity bae = (ByteArrayEntity) f.get(e);
    f = ByteArrayEntity.class.getDeclaredField("b");
    f.setAccessible(true);
    byte[] bytes = (byte[]) f.get(bae);
    //noinspection IOResourceOpenedButNotSafelyClosed if it throws an exception, let the test fail
    InputStream is = new FileInputStream(file);
    byte[] realBytes = new byte[bytes.length];
    assertNotEquals(0, is.read(realBytes));
    assertArrayEquals(bytes, realBytes);
    is.close();
  }

}