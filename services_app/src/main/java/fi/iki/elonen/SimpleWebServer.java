package fi.iki.elonen;

/*
 * #%L
 * NanoHttpd-Webserver
 * %%
 * Copyright (C) 2012 - 2015 nanohttpd
 * %%
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the nanohttpd nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

import org.opendatakit.utilities.ODKFileUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import fi.iki.elonen.NanoHTTPD.Response.IStatus;

public class SimpleWebServer extends NanoHTTPD {
  private static final String t = "SimpleWebServer";
  public static final String DEBUG_HTTP_FILE_NAME = "httpDebug.txt";
  /**
   * Default Index file names.
   */
  @SuppressWarnings("serial")
  public static final List<String> INDEX_FILE_NAMES = new ArrayList<String>() {

    {
      add("index.html");
      add("index.htm");
    }
  };


  /**
   * The distribution licence
   */
  private static final String LICENCE;

  static {
    mimeTypes();
    String text;
    try {
      InputStream stream = SimpleWebServer.class.getResourceAsStream("/LICENSE.txt");
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      byte[] buffer = new byte[1024];
      int count;
      while ((count = stream.read(buffer)) >= 0) {
        bytes.write(buffer, 0, count);
      }
      text = bytes.toString("UTF-8");
    } catch (Exception e) {
      text = "unknown";
    }
    LICENCE = text;
  }

  private final boolean quiet;

  private boolean shouldCreateLogFile;

  private final String cors;

  protected List<File> rootDirs;

  public SimpleWebServer(String host, int port, File wwwroot, boolean quiet, String cors) {
    this(host, port, Collections.singletonList(wwwroot), quiet, cors);
  }

  public SimpleWebServer(String host, int port, File wwwroot, boolean quiet) {
    this(host, port, Collections.singletonList(wwwroot), quiet, null);
  }

  public SimpleWebServer(String host, int port, List<File> wwwroots, boolean quiet) {
    this(host, port, wwwroots, quiet, null);
  }

  public SimpleWebServer(String host, int port, List<File> wwwroots, boolean quiet, String cors) {
    super(host, port);
    this.quiet = quiet;
    this.cors = cors;
    this.rootDirs = new ArrayList<File>(wwwroots);

    init();
  }

  @Override
  protected boolean useGzipWhenAccepted(Response r) {
    return super.useGzipWhenAccepted(r) && r.getStatus() != Response.Status.NOT_MODIFIED;
  }

  /**
   * URL-encodes everything between "/"-characters. Encodes spaces as '%20'
   * instead of '+'.
   */
  private String encodeUri(String uri) {
    String newUri = "";
    StringTokenizer st = new StringTokenizer(uri, "/ ", true);
    while (st.hasMoreTokens()) {
      String tok = st.nextToken();
      if ("/".equals(tok)) {
        newUri += "/";
      } else if (" ".equals(tok)) {
        newUri += "%20";
      } else {
        try {
          newUri += URLEncoder.encode(tok, "UTF-8");
        } catch (UnsupportedEncodingException ignored) {
        }
      }
    }
    return newUri;
  }

  private String findIndexFileInDirectory(File directory) {
    for (String fileName : SimpleWebServer.INDEX_FILE_NAMES) {
      File indexFile = new File(directory, fileName);
      if (indexFile.isFile()) {
        return fileName;
      }
    }
    return null;
  }

  protected Response getForbiddenResponse(String s) {
    return newFixedLengthResponse(Response.Status.FORBIDDEN, NanoHTTPD.MIME_PLAINTEXT, "FORBIDDEN: " + s);
  }

  protected Response getInternalErrorResponse(String s) {
    return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, NanoHTTPD.MIME_PLAINTEXT, "INTERNAL ERROR: " + s);
  }

  protected Response getNotFoundResponse() {
    return newFixedLengthResponse(Response.Status.NOT_FOUND, NanoHTTPD.MIME_PLAINTEXT, "Error 404, file not found.");
  }

  /**
   * Used to initialize and customize the server.
   */
  public void init() {
  }

  protected String listDirectory(String uri, File f) {
    String heading = "Directory " + uri;
    StringBuilder msg =
            new StringBuilder("<html><head><title>" + heading + "</title><style><!--\n" + "span.dirname { font-weight: bold; }\n" + "span.filesize { font-size: 75%; }\n"
                    + "// -->\n" + "</style>" + "</head><body><h1>" + heading + "</h1>");

    String up = null;
    if (uri.length() > 1) {
      String u = uri.substring(0, uri.length() - 1);
      int slash = u.lastIndexOf('/');
      if (slash >= 0 && slash < u.length()) {
        up = uri.substring(0, slash + 1);
      }
    }

    List<String> files = Arrays.asList(f.list(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return new File(dir, name).isFile();
      }
    }));
    Collections.sort(files);
    List<String> directories = Arrays.asList(f.list(new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return new File(dir, name).isDirectory();
      }
    }));
    Collections.sort(directories);
    if (up != null || directories.size() + files.size() > 0) {
      msg.append("<ul>");
      if (up != null || directories.size() > 0) {
        msg.append("<section class=\"directories\">");
        if (up != null) {
          msg.append("<li><a rel=\"directory\" href=\"").append(up).append("\"><span class=\"dirname\">..</span></a></li>");
        }
        for (String directory : directories) {
          String dir = directory + "/";
          msg.append("<li><a rel=\"directory\" href=\"").append(encodeUri(uri + dir)).append("\"><span class=\"dirname\">").append(dir).append("</span></a></li>");
        }
        msg.append("</section>");
      }
      if (files.size() > 0) {
        msg.append("<section class=\"files\">");
        for (String file : files) {
          msg.append("<li><a href=\"").append(encodeUri(uri + file)).append("\"><span class=\"filename\">").append(file).append("</span></a>");
          File curFile = new File(f, file);
          long len = curFile.length();
          msg.append("&nbsp;<span class=\"filesize\">(");
          if (len < 1024) {
            msg.append(len).append(" bytes");
          } else if (len < 1024 * 1024) {
            msg.append(len / 1024).append(".").append(len % 1024 / 10 % 100).append(" KB");
          } else {
            msg.append(len / (1024 * 1024)).append(".").append(len % (1024 * 1024) / 10000 % 100).append(" MB");
          }
          msg.append(")</span></li>");
        }
        msg.append("</section>");
      }
      msg.append("</ul>");
    }
    msg.append("</body></html>");
    return msg.toString();
  }

  public static Response newFixedLengthResponse(IStatus status, String mimeType, String message) {
    Response response = NanoHTTPD.newFixedLengthResponse(status, mimeType, message);
    response.addHeader("Accept-Ranges", "bytes");
    return response;
  }

  private Response respond(Map<String, String> headers, IHTTPSession session, String uri) {
    // First let's handle CORS OPTION query
    Response r;
    if (cors != null && Method.OPTIONS.equals(session.getMethod())) {
      r = new NanoHTTPD.Response(Response.Status.OK, MIME_PLAINTEXT, null, 0);
    } else {
      r = defaultRespond(headers, session, uri);
    }

    if (cors != null) {
      r = addCORSHeaders(headers, r, cors);
    }

    // Add what is going over the wire fro
    return r;
  }

  private Response defaultRespond(Map<String, String> headers, IHTTPSession session, String uri) {
    // Remove URL arguments
    uri = uri.trim().replace(File.separatorChar, '/');
    if (uri.indexOf('?') >= 0) {
      uri = uri.substring(0, uri.indexOf('?'));
    }

    // Prohibit getting out of current directory
    if (uri.contains("../")) {
      return getForbiddenResponse("Won't serve ../ for security reasons.");
    }

    // canServeUri functionality
    File f = ODKFileUtils.fileFromUriOnWebServer(uri);
    if (f == null) {
      return getNotFoundResponse();
    }

    if (f.isDirectory() && !uri.endsWith("/")) {
      uri += "/";
      Response res =
              newFixedLengthResponse(Response.Status.REDIRECT, NanoHTTPD.MIME_HTML, "<html><body>Redirected: <a href=\"" + uri + "\">" + uri + "</a></body></html>");
      res.addHeader("Location", uri);
      return res;
    }

    if (f.isDirectory()) {
      // First look for index files (index.html, index.htm, etc) and if
      // none found, list the directory if readable.
      String indexFile = findIndexFileInDirectory(f);
      if (indexFile == null) {
        if (f.canRead()) {
          // No index file, list the directory if it is readable
          return newFixedLengthResponse(Response.Status.OK, NanoHTTPD.MIME_HTML, listDirectory(uri, f));
        } else {
          return getForbiddenResponse("No directory listing.");
        }
      } else {
        return respond(headers, session, uri + indexFile);
      }
    }
    String mimeTypeForFile = getMimeTypeForFile(uri);

    Response response = serveFile(uri, headers, f, mimeTypeForFile);

    return response != null ? response : getNotFoundResponse();
  }

  @Override
  public Response serve(IHTTPSession session) {
    Map<String, String> header = session.getHeaders();
    Map<String, String> parms = session.getParms();
    String uri = session.getUri();

    // Changed from original version to be able to use the
    // WebLogger - have to get the appName first
    String appName = ODKFileUtils.getOdkDefaultAppName();
    if (uri.startsWith("/")) {
      int nextSlash = uri.indexOf('/', 1);

      if (nextSlash == -1) {
        // this is likely the /favicon.ico request
        //
        // use the referer to retrieve the appname
        // adjust uri to be /appname/config/uri
        //
        // i.e., yielding /default/config/favicon.ico
        //
        String referer = header.get("referer");
        if (referer == null) {
          return getNotFoundResponse();
        }
        int idx = referer.indexOf("//");
        idx = referer.indexOf('/', idx + 2);
        nextSlash = referer.indexOf('/', idx + 1);
        appName = referer.substring(idx + 1, nextSlash);
        // adjust the uri to be under the config directory of the referer
        File file = new File(ODKFileUtils.getConfigFolder(appName), uri.substring(1));
        // we expect a leading slash...
        uri = "/" + appName + "/" + ODKFileUtils.asUriFragment(appName, file);
        nextSlash = uri.indexOf('/', 1);
      }
      appName = uri.substring(1, nextSlash);

      // Given the app name check if the output file to begin
      // Check if there is a debug file that would enable logging
      String debugOutputDir = ODKFileUtils.getTablesDebugObjectFolder(appName);
      File httpDebugFile = new File(debugOutputDir + File.separator + DEBUG_HTTP_FILE_NAME);
      if (httpDebugFile.exists()) {
        this.setEnableLog(appName, true);
      }
    }


    // Make sure we won't die of an exception later
    File root = new File(ODKFileUtils.getOdkFolder());
    try {
      ODKFileUtils.verifyExternalStorageAvailability();
      if ( !root.exists() || !root.isDirectory()) {
        return getInternalErrorResponse("given path is not a directory (" + root.getAbsolutePath() + ").");
      }
    } catch ( Exception e) {
      return getInternalErrorResponse("exception " + e.toString() + " accessing directory (" + root.getAbsolutePath() + ").");
    }

    Response res = respond(Collections.unmodifiableMap(header), session, uri);
    res.setOdkAppName(appName);
    res.setRequestHeaders(header);
    res.setResponseUri(uri);
    return res;
  }

  /**
   * Serves file from homeDir and its' subdirectories (only). Uses only URI,
   * ignores all headers and HTTP parameters.
   */
  Response serveFile(String uri, Map<String, String> header, File file, String mime) {
    Response res;
    try {
      // Calculate etag
      String etag = Integer.toHexString((file.getAbsolutePath() + file.lastModified() + "" + file.length()).hashCode());

      // Support (simple) skipping:
      long startFrom = 0;
      long endAt = -1;
      String range = header.get("range");
      if (range != null) {
        if (range.startsWith("bytes=")) {
          range = range.substring("bytes=".length());
          int minus = range.indexOf('-');
          try {
            if (minus > 0) {
              startFrom = Long.parseLong(range.substring(0, minus));
              endAt = Long.parseLong(range.substring(minus + 1));
            }
          } catch (NumberFormatException ignored) {
          }
        }
      }

      // get if-range header. If present, it must match etag or else we
      // should ignore the range request
      String ifRange = header.get("if-range");
      boolean headerIfRangeMissingOrMatching = (ifRange == null || etag.equals(ifRange));

      String ifNoneMatch = header.get("if-none-match");
      boolean headerIfNoneMatchPresentAndMatching = ifNoneMatch != null && ("*".equals(ifNoneMatch) || ifNoneMatch.equals(etag));

      // Change return code and add Content-Range header when skipping is
      // requested
      long fileLen = file.length();

      if (headerIfRangeMissingOrMatching && range != null && startFrom >= 0 && startFrom < fileLen) {
        // range request that matches current etag
        // and the startFrom of the range is satisfiable
        if (headerIfNoneMatchPresentAndMatching) {
          // range request that matches current etag
          // and the startFrom of the range is satisfiable
          // would return range from file
          // respond with not-modified
          res = newFixedLengthResponse(Response.Status.NOT_MODIFIED, mime, "");
          res.addHeader("ETag", etag);
        } else {
          if (endAt < 0) {
            endAt = fileLen - 1;
          }
          long newLen = endAt - startFrom + 1;
          if (newLen < 0) {
            newLen = 0;
          }

          FileInputStream fis = new FileInputStream(file);
          fis.skip(startFrom);

          res = newFixedLengthResponse(Response.Status.PARTIAL_CONTENT, mime, fis, newLen);
          res.addHeader("Accept-Ranges", "bytes");
          res.addHeader("Content-Length", "" + newLen);
          res.addHeader("Content-Range", "bytes " + startFrom + "-" + endAt + "/" + fileLen);
          res.addHeader("ETag", etag);
        }
      } else {

        if (headerIfRangeMissingOrMatching && range != null && startFrom >= fileLen) {
          // return the size of the file
          // 4xx responses are not trumped by if-none-match
          res = newFixedLengthResponse(Response.Status.RANGE_NOT_SATISFIABLE, NanoHTTPD.MIME_PLAINTEXT, "");
          res.addHeader("Content-Range", "bytes */" + fileLen);
          res.addHeader("ETag", etag);
        } else if (range == null && headerIfNoneMatchPresentAndMatching) {
          // full-file-fetch request
          // would return entire file
          // respond with not-modified
          res = newFixedLengthResponse(Response.Status.NOT_MODIFIED, mime, "");
          res.addHeader("ETag", etag);
        } else if (!headerIfRangeMissingOrMatching && headerIfNoneMatchPresentAndMatching) {
          // range request that doesn't match current etag
          // would return entire (different) file
          // respond with not-modified

          res = newFixedLengthResponse(Response.Status.NOT_MODIFIED, mime, "");
          res.addHeader("ETag", etag);
        } else {
          // supply the file
          res = newFixedFileResponse(file, mime);
          res.addHeader("Content-Length", "" + fileLen);
          res.addHeader("ETag", etag);
        }
      }
    } catch (IOException ioe) {
      res = getForbiddenResponse("Reading file failed.");
    }

    return res;
  }

  private Response newFixedFileResponse(File file, String mime) throws FileNotFoundException {
    Response res;
    res = newFixedLengthResponse(Response.Status.OK, mime, new FileInputStream(file), (int) file.length());
    res.addHeader("Accept-Ranges", "bytes");
    return res;
  }

  protected Response addCORSHeaders(Map<String, String> queryHeaders, Response resp, String cors) {
    resp.addHeader("Access-Control-Allow-Origin", cors);
    resp.addHeader("Access-Control-Allow-Headers", calculateAllowHeaders(queryHeaders));
    resp.addHeader("Access-Control-Allow-Credentials", "true");
    resp.addHeader("Access-Control-Allow-Methods", ALLOWED_METHODS);
    resp.addHeader("Access-Control-Max-Age", "" + MAX_AGE);

    return resp;
  }

  private String calculateAllowHeaders(Map<String, String> queryHeaders) {
    // here we should use the given asked headers
    // but NanoHttpd uses a Map whereas it is possible for requester to send
    // several time the same header
    // let's just use default values for this version
    return System.getProperty(ACCESS_CONTROL_ALLOW_HEADER_PROPERTY_NAME, DEFAULT_ALLOWED_HEADERS);
  }

  private final static String ALLOWED_METHODS = "GET, POST, PUT, DELETE, OPTIONS, HEAD";

  private final static int MAX_AGE = 42 * 60 * 60;

  // explicitly relax visibility to package for tests purposes
  final static String DEFAULT_ALLOWED_HEADERS = "origin,accept,content-type";

  public final static String ACCESS_CONTROL_ALLOW_HEADER_PROPERTY_NAME = "AccessControlAllowHeader";
}
