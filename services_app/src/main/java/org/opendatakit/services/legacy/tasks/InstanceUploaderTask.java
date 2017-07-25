/*
 * Copyright (C) 2009-2013 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.services.legacy.tasks;

import android.app.Application;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;

import android.provider.BaseColumns;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringEscapeUtils;
import org.opendatakit.database.utilities.CursorUtils;
import org.opendatakit.httpclientandroidlib.Header;
import org.opendatakit.httpclientandroidlib.HttpResponse;
import org.opendatakit.httpclientandroidlib.auth.AuthScope;
import org.opendatakit.httpclientandroidlib.auth.Credentials;
import org.opendatakit.httpclientandroidlib.auth.UsernamePasswordCredentials;
import org.opendatakit.httpclientandroidlib.client.ClientProtocolException;
import org.opendatakit.httpclientandroidlib.client.CookieStore;
import org.opendatakit.httpclientandroidlib.client.CredentialsProvider;
import org.opendatakit.httpclientandroidlib.client.HttpClient;
import org.opendatakit.httpclientandroidlib.client.config.AuthSchemes;
import org.opendatakit.httpclientandroidlib.client.methods.HttpHead;
import org.opendatakit.httpclientandroidlib.client.methods.HttpPost;
import org.opendatakit.httpclientandroidlib.client.protocol.HttpClientContext;
import org.opendatakit.httpclientandroidlib.conn.ConnectTimeoutException;
import org.opendatakit.httpclientandroidlib.conn.HttpHostConnectException;
import org.opendatakit.httpclientandroidlib.entity.ContentType;
import org.opendatakit.httpclientandroidlib.entity.mime.MultipartEntityBuilder;
import org.opendatakit.httpclientandroidlib.impl.client.BasicCookieStore;
import org.opendatakit.httpclientandroidlib.impl.client.BasicCredentialsProvider;
import org.opendatakit.httpclientandroidlib.impl.client.CloseableHttpClient;
import org.opendatakit.httpclientandroidlib.protocol.HttpContext;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.provider.InstanceColumns;
import org.opendatakit.provider.InstanceProviderAPI;
import org.opendatakit.provider.ProviderConsts;
import org.opendatakit.services.R;
import org.opendatakit.services.legacy.listeners.InstanceUploaderListener;
import org.opendatakit.services.legacy.logic.InstanceUploadOutcome;
import org.opendatakit.services.legacy.utilities.WebUtils;
import org.opendatakit.utilities.FileSet;
import org.opendatakit.utilities.FileSet.MimeFile;
import org.opendatakit.utilities.LocalizationUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Background task for uploading completed forms.
 *
 * @author Carl Hartung (carlhartung@gmail.com)
 */
public class InstanceUploaderTask extends AsyncTask<String, Integer, InstanceUploadOutcome> {

  private static final String TAG = InstanceUploaderTask.class.getSimpleName();
  private static final String fail = "Error: ";

  private Application appContext;
  private InstanceUploaderListener mStateListener;

  private InstanceUploadOutcome mOutcome = new InstanceUploadOutcome();

  private InstanceUploadOutcome mResultOutcome = new InstanceUploadOutcome();

  private final String appName;
  private final String uploadTableId;

  public InstanceUploaderTask(String appName, String uploadTableId) {
    super();
    this.appName = appName;
    this.uploadTableId = uploadTableId;
  }

  /**
   * Uploads to urlString the submission identified by id with filepath of
   * instance
   *
   * @param urlString
   *          destination URL
   * @param toUpdate
   * @param id
   *          -- _ID in the InstanceColumns table.
   * @param submissionInstanceId
   * @param instanceFiles
   * @param httpclient
   *          - client connection
   * @param localContext
   *          - context (e.g., credentials, cookies) for client connection
   * @param uriRemap
   *          - mapping of Uris to avoid redirects on subsequent invocations
   * @return false if credentials are required and we should terminate
   *         immediately.
   */
  private boolean uploadOneSubmission(String urlString, Uri toUpdate, String id,
      String submissionInstanceId, FileSet instanceFiles, HttpClient httpclient,
      HttpContext localContext, Map<URI, URI> uriRemap) {

    ContentValues cv = new ContentValues();
    cv.put(InstanceColumns.SUBMISSION_INSTANCE_ID, submissionInstanceId);
    URI u = null;
    try {
      URL url = new URL(URLDecoder.decode(urlString, CharEncoding.UTF_8));
      u = url.toURI();
    } catch (MalformedURLException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      mOutcome.mResults.put(id,
          fail + "invalid url: " + urlString + " :: details: " + e.getMessage());
      cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
      appContext.getContentResolver().update(toUpdate, cv, null, null);
      return true;
    } catch (URISyntaxException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      mOutcome.mResults.put(id,
          fail + "invalid uri: " + urlString + " :: details: " + e.getMessage());
      cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
      appContext.getContentResolver().update(toUpdate, cv, null, null);
      return true;
    } catch (UnsupportedEncodingException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      mOutcome.mResults.put(id,
          fail + "invalid url: " + urlString + " :: details: " + e.getMessage());
      cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
      appContext.getContentResolver().update(toUpdate, cv, null, null);
      return true;
    }

    // NOTE: ODK Survey assumes you are interfacing with an
    // OpenRosa-compliant server

    if (uriRemap.containsKey(u)) {
      // we already issued a head request and got a response,
      // so we know the proper URL to send the submission to
      // and the proper scheme. We also know that it was an
      // OpenRosa compliant server.
      u = uriRemap.get(u);
    } else {
      // we need to issue a head request
      HttpHead httpHead = WebUtils.get().createOpenRosaHttpHead(u);

      // prepare response
      HttpResponse response = null;
      try {
        response = httpclient.execute(httpHead, localContext);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 401) {
          WebUtils.get().discardEntityBytes(response);
          // we need authentication, so stop and return what we've
          // done so far.
          mOutcome.mAuthRequestingServer = u;
          return false;
        } else if (statusCode == 204) {
          Header[] locations = response.getHeaders("Location");
          WebUtils.get().discardEntityBytes(response);
          if (locations != null && locations.length == 1) {
            try {
              URL url = new URL(URLDecoder.decode(locations[0].getValue(), CharEncoding.UTF_8));
              URI uNew = url.toURI();
              if (u.getHost().equalsIgnoreCase(uNew.getHost())) {
                // trust the server to tell us a new location
                // ... and possibly to use https instead.
                uriRemap.put(u, uNew);
                u = uNew;
              } else {
                // Don't follow a redirection attempt to a
                // different host.
                // We can't tell if this is a spoof or not.
                mOutcome.mResults.put(id, fail
                    + "Unexpected redirection attempt to a different host: " + uNew.toString());
                cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
                appContext.getContentResolver().update(toUpdate, cv, null, null);
                return true;
              }
            } catch (Exception e) {
              WebLogger.getLogger(appName).printStackTrace(e);
              mOutcome.mResults.put(id, fail + urlString + " " + e.getMessage());
              cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
              appContext.getContentResolver().update(toUpdate, cv, null, null);
              return true;
            }
          }
        } else {
          // may be a server that does not handle
          WebUtils.get().discardEntityBytes(response);

          WebLogger.getLogger(appName).w(TAG, "Status code on Head request: " + statusCode);
          if (statusCode >= 200 && statusCode <= 299) {
            mOutcome.mResults
                .put(
                    id,
                    fail
                        + "Invalid status code on Head request.  If you have a web proxy, you may need to login to your network. ");
            cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
            appContext.getContentResolver().update(toUpdate, cv, null, null);
            return true;
          }
        }
      } catch (ClientProtocolException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        WebLogger.getLogger(appName).e(TAG, e.getMessage());
        mOutcome.mResults.put(id, fail + "Client Protocol Exception");
        cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
        appContext.getContentResolver().update(toUpdate, cv, null, null);
        return true;
      } catch (ConnectTimeoutException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        WebLogger.getLogger(appName).e(TAG, e.getMessage());
        mOutcome.mResults.put(id, fail + "Connection Timeout");
        cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
        appContext.getContentResolver().update(toUpdate, cv, null, null);
        return true;
      } catch (UnknownHostException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        mOutcome.mResults.put(id, fail + e.getMessage() + " :: Network Connection Failed");
        WebLogger.getLogger(appName).e(TAG, e.getMessage());
        cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
        appContext.getContentResolver().update(toUpdate, cv, null, null);
        return true;
      } catch (SocketTimeoutException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        WebLogger.getLogger(appName).e(TAG, e.getMessage());
        mOutcome.mResults.put(id, fail + "Connection Timeout");
        cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
        appContext.getContentResolver().update(toUpdate, cv, null, null);
        return true;
      } catch (HttpHostConnectException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        WebLogger.getLogger(appName).e(TAG, e.toString());
        mOutcome.mResults.put(id, fail + "Network Connection Refused");
        cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
        appContext.getContentResolver().update(toUpdate, cv, null, null);
        return true;
      } catch (Exception e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        mOutcome.mResults.put(id, fail + "Generic Exception");
        WebLogger.getLogger(appName).e(TAG, e.getMessage());
        cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
        appContext.getContentResolver().update(toUpdate, cv, null, null);
        return true;
      }
    }

    // At this point, we may have updated the uri to use https.
    // This occurs only if the Location header keeps the host name
    // the same. If it specifies a different host name, we error
    // out.
    //
    // And we may have set authentication cookies in our
    // cookiestore (referenced by localContext) that will enable
    // authenticated publication to the server.
    //
    // get instance file
    File instanceFile = instanceFiles.instanceFile;

    if (!instanceFile.exists()) {
      mOutcome.mResults.put(id, fail + "instance XML file does not exist!");
      cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
      appContext.getContentResolver().update(toUpdate, cv, null, null);
      return true;
    }

    List<MimeFile> files = instanceFiles.attachmentFiles;
    boolean first = true;
    int j = 0;
    int lastJ;
    while (j < files.size() || first) {
      lastJ = j;
      first = false;

      HttpPost httppost = WebUtils.get().createOpenRosaHttpPost(u);

      long byteCount = 0L;

      // mime post
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();

      // add the submission file first...
      builder.addBinaryBody("xml_submission_file", instanceFile,
          ContentType.TEXT_XML.withCharset(Charset.forName(CharEncoding.UTF_8)), instanceFile.getName());
      WebLogger.getLogger(appName).i(TAG, "added xml_submission_file: " + instanceFile.getName());
      byteCount += instanceFile.length();

      for (; j < files.size(); j++) {
        MimeFile mf = files.get(j);
        File f = mf.file;
        String contentType = mf.contentType;

        builder.addBinaryBody(f.getName(), f, ContentType.create(contentType), f.getName());
        byteCount += f.length();
        WebLogger.getLogger(appName).i(TAG, "added " + contentType + " file " + f.getName());

        // we've added at least one attachment to the request...
        if (j + 1 < files.size()) {
          long nextFileLength = (files.get(j + 1).file.length());
          if ((j - lastJ + 1 > 100) || (byteCount + nextFileLength > 10000000L)) {
            // the next file would exceed the 10MB threshold...
            WebLogger.getLogger(appName).i(TAG, "Extremely long post is being split into multiple posts");
            try {
              builder.addTextBody("*isIncomplete*", "yes",
                  ContentType.TEXT_PLAIN.withCharset(Charset.forName((CharEncoding.UTF_8))));
            } catch (Exception e) {
              WebLogger.getLogger(appName).printStackTrace(e); // never happens...
            }
            ++j; // advance over the last attachment added...
            break;
          }
        }
      }

      httppost.setEntity(builder.build());

      // prepare response and return uploaded
      HttpResponse response = null;
      try {
        response = httpclient.execute(httppost, localContext);
        int responseCode = response.getStatusLine().getStatusCode();
        WebUtils.get().discardEntityBytes(response);

        WebLogger.getLogger(appName).i(TAG, "Response code:" + responseCode);
        // verify that the response was a 201 or 202.
        // If it wasn't, the submission has failed.
        if (responseCode != 201 && responseCode != 202) {
          if (responseCode == 200) {
            mOutcome.mResults.put(id, fail + "Network login failure? Again?");
          } else {
            mOutcome.mResults.put(id, fail + response.getStatusLine().getReasonPhrase() + " ("
                + responseCode + ") at " + urlString);
          }
          cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
          appContext.getContentResolver().update(toUpdate, cv, null, null);
          return true;
        }
      } catch (Exception e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        mOutcome.mResults.put(id, fail + "Generic Exception. " + e.getMessage());
        cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMISSION_FAILED);
        appContext.getContentResolver().update(toUpdate, cv, null, null);
        return true;
      }
    }

    // if it got here, it must have worked
    mOutcome.mResults.put(id, appContext.getString(R.string.success));
    cv.put(InstanceColumns.XML_PUBLISH_STATUS, InstanceColumns.STATUS_SUBMITTED);
    appContext.getContentResolver().update(toUpdate, cv, null, null);
    return true;
  }

  /**
   * Write's the data to the sdcard, and updates the instances content provider.
   * In theory we don't have to write to disk, and this is where you'd add other
   * methods.
   *
   * @param instanceId
   * @param submissionInstanceId
   * @return
   * @throws JsonParseException
   * @throws JsonMappingException
   * @throws IOException
   */
  private FileSet constructSubmissionFiles(String instanceId, String submissionInstanceId)
      throws JsonParseException, JsonMappingException, IOException {

    Uri manifest = Uri.parse(ProviderConsts.XML_SUBMISSION_URL_PREFIX + "/"
        + URLEncoder.encode(appName, CharEncoding.UTF_8) + "/"
        + URLEncoder.encode(uploadTableId, CharEncoding.UTF_8) + "/"
        + URLEncoder.encode(instanceId, CharEncoding.UTF_8) + "/"
        + URLEncoder.encode(submissionInstanceId, CharEncoding.UTF_8));

    InputStream is = appContext.getContentResolver().openInputStream(manifest);

    FileSet f = FileSet.parse(appName, is);
    return f;
  }

  /**
   * Construct the list of scopes (port + authProtocol) for a given host.
   * @param host
   * @return
   */
  private List<AuthScope> buildAuthScopes(String host) {
    List<AuthScope> asList = new ArrayList<AuthScope>();

    AuthScope a;
    // allow digest auth on any port...
    a = new AuthScope(host, -1, null, AuthSchemes.BASIC);
    asList.add(a);
    // and allow basic auth on the standard TLS/SSL ports...
    a = new AuthScope(host, 443, null, AuthSchemes.BASIC);
    asList.add(a);
    a = new AuthScope(host, 8443, null, AuthSchemes.BASIC);
    asList.add(a);

    return asList;
  }

  /**
   * Remove all credentials for accessing the specified host and, if the
   * username is not null or blank then add a (username, password) credential
   * for accessing this host.
   *
   * @param credsProvider
   * @param username
   * @param password
   * @param host
   */
  private synchronized void registerUsernamePassword(
      CredentialsProvider credsProvider,
      String username, String password, String host) {

    List<AuthScope> asList = buildAuthScopes(host);

    // ensure that this is the only authentication available for this host...
    WebLogger.getLogger(appName).i(TAG, "clearHostCredentials: " + host);
    for (AuthScope a : asList) {
      credsProvider.setCredentials(a, null);
    }

    // add username
    if (username != null && username.trim().length() != 0) {
      WebLogger.getLogger(appName).i(TAG, "adding credential for host: " + host + " username:" + username);
      Credentials c = new UsernamePasswordCredentials(username, password);

      for (AuthScope a : asList) {
        credsProvider.setCredentials(a, c);
      }
    }
  }

  // TODO: This method is like 350 lines long, down from 400.
  // still. ridiculous. make it smaller.
  @Override
  protected InstanceUploadOutcome doInBackground(String... toUpload) {
    mOutcome = new InstanceUploadOutcome();
    mOutcome.mResults = new HashMap<String, String>();
    mOutcome.mAuthRequestingServer = null;

    PropertiesSingleton props = CommonToolProperties.get(appContext, appName);

    String urlString = null;
    urlString = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);
    urlString = urlString + "/submission";
    URI u = null;
    try {
      URL url = new URL(URLDecoder.decode(urlString, CharEncoding.UTF_8));
      u = url.toURI();
    } catch (MalformedURLException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      mOutcome.mResults.put("unknown",
          fail + "invalid url: " + urlString + " :: details: " + e.getMessage());
      return mOutcome;
    } catch (URISyntaxException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      mOutcome.mResults.put("unknown",
          fail + "invalid uri: " + urlString + " :: details: " + e.getMessage());
      return mOutcome;
    } catch (UnsupportedEncodingException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      mOutcome.mResults.put("unknown",
          fail + "invalid url: " + urlString + " :: details: " + e.getMessage());
      return mOutcome;
    }

    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    CookieStore cookieStore = new BasicCookieStore();

    String authType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
    if ( authType.equals(appContext.getString(R.string.credential_type_username_password))) {
      String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
      String password = props.getProperty(CommonToolProperties.KEY_PASSWORD);
      registerUsernamePassword(credsProvider, username, password, u.getHost());
    } else if (authType.equals(appContext.getString(R.string.credential_type_google_account))) {
      throw new IllegalStateException("legacy doesn't support Google Authentication");
    }

    // context holds authentication state machine, so it cannot be
    // shared across independent activities.
    HttpClientContext localContext = HttpClientContext.create();

    localContext.setCookieStore(cookieStore);
    localContext.setCredentialsProvider(credsProvider);

    CloseableHttpClient httpclient = WebUtils.createHttpClient(WebUtils.CONNECTION_TIMEOUT);

    Map<URI, URI> uriRemap = new HashMap<URI, URI>();

    for (int i = 0; i < toUpload.length; ++i) {
      if (isCancelled()) {
        return mOutcome;
      }
      publishProgress(i + 1, toUpload.length);

      Uri toUpdate = Uri.withAppendedPath(InstanceProviderAPI.CONTENT_URI, appName + "/"
          + uploadTableId + "/" + StringEscapeUtils.escapeHtml4(toUpload[i]));
      Cursor c = null;
      try {
        c = appContext.getContentResolver().query(toUpdate, null, null, null, null);
        if (c.getCount() == 1 && c.moveToFirst()) {

          String id = CursorUtils.getIndexAsString(c,
              c.getColumnIndex(BaseColumns._ID));
          String dataTableInstanceId = CursorUtils.getIndexAsString(c,
              c.getColumnIndex(InstanceColumns.DATA_INSTANCE_ID));
          String lastOutcome = CursorUtils.getIndexAsString(c,
              c.getColumnIndex(InstanceColumns.XML_PUBLISH_STATUS));
          String submissionInstanceId = LocalizationUtils.genUUID();
          // submissions always get a new legacy instance id UNLESS the last
          // submission failed,
          // in which case we retry the submission using the legacy instance id
          // associated with
          // that failure. This supports resumption of sends of forms with many
          // attachments.
          if (lastOutcome != null && lastOutcome.equals(InstanceColumns.STATUS_SUBMISSION_FAILED)) {
            String lastId = CursorUtils.getIndexAsString(c,
                c.getColumnIndex(InstanceColumns.SUBMISSION_INSTANCE_ID));
            if (lastId != null) {
              submissionInstanceId = lastId;
            }
          }
          c.close();

          FileSet instanceFiles;
          try {
            instanceFiles = constructSubmissionFiles(dataTableInstanceId, submissionInstanceId);
            // NOTE: /submission must not be translated! It is
            // the well-known path on the server.

            if (!uploadOneSubmission(urlString, toUpdate, id, submissionInstanceId, instanceFiles,
                httpclient, localContext, uriRemap)) {
              return mOutcome; // get credentials...
            }
          } catch (JsonParseException e) {
            WebLogger.getLogger(appName).printStackTrace(e);
            mOutcome.mResults.put(id, fail + "unable to obtain manifest: " + dataTableInstanceId
                + " :: details: " + e.toString());
          } catch (JsonMappingException e) {
            WebLogger.getLogger(appName).printStackTrace(e);
            mOutcome.mResults.put(id, fail + "unable to obtain manifest: " + dataTableInstanceId
                + " :: details: " + e.toString());
          } catch (IOException e) {
            WebLogger.getLogger(appName).printStackTrace(e);
            mOutcome.mResults.put(id, fail + "unable to obtain manifest: " + dataTableInstanceId
                + " :: details: " + e.toString());
          }
        } else {
          mOutcome.mResults.put("unknown", fail + "unable to retrieve instance information via: "
              + toUpdate.toString());
        }
      } finally {
        if (c != null && !c.isClosed()) {
          c.close();
        }
      }
    }

    return mOutcome;
  }

  @Override
  protected void onPostExecute(InstanceUploadOutcome result) {
    synchronized (this) {
      mResultOutcome = result;
      if (mStateListener != null) {
        mStateListener.uploadingComplete(mResultOutcome);
      }
    }
  }

  @Override
  protected void onCancelled(InstanceUploadOutcome result) {
    synchronized (this) {
      if (result == null) {
        mResultOutcome = new InstanceUploadOutcome();
        mResultOutcome.mAuthRequestingServer = null;
        mResultOutcome.mResults = new HashMap<String, String>();
        mResultOutcome.mResults.put("unknown", "cancelled");
      } else {
        mResultOutcome = result;
      }
      if (mStateListener != null) {
        mStateListener.uploadingComplete(mResultOutcome);
      }
    }
  }

  @Override
  protected void onProgressUpdate(Integer... values) {
    synchronized (this) {
      if (mStateListener != null) {
        // update progress and total
        mStateListener.progressUpdate(values[0].intValue(), values[1].intValue());
      }
    }
  }

  public InstanceUploadOutcome getResult() {
    return mResultOutcome;
  }

  public void setUploaderListener(InstanceUploaderListener sl) {
    synchronized (this) {
      mStateListener = sl;
    }
  }

  public void setApplication(Application appContext) {
    synchronized (this) {
      this.appContext = appContext;
    }
  }

  public Application getApplication() {
    return appContext;
  }
}
