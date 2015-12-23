/*
 * Copyright (C) 2012-2013 University of Washington
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

package org.opendatakit.common.android.utilities;

import android.os.FileObserver;
import android.util.Log;
import org.apache.commons.lang3.CharEncoding;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Implementation of WebLoggerIf for android that emits logs to the
 * LOGGING_PATH and recycles them as needed.
 * Useful to separate out ODK log entries from the overall logging stream,
 * especially on heavily logged 4.x systems.
 *
 * @author mitchellsundt@gmail.com
 */
public class WebLoggerImpl implements WebLoggerIf {
  private static final long FLUSH_INTERVAL = 12000L; // 5 times a minute
  private static final long MILLISECONDS_DAY = 86400000L;

  private static final int ASSERT = 1;
  private static final int VERBOSE = 2;
  private static final int DEBUG = 3;
  private static final int INFO = 4;
  private static final int WARN = 5;
  private static final int ERROR = 6;
  private static final int SUCCESS = 7;
  private static final int TIP = 8;

  private static final int MIN_LOG_LEVEL_TO_SPEW = 3;
  private static final String DATE_FORMAT = "yyyy-MM-dd_HH";
  private static final String LOG_LINE_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

  /**
   * Instance variables
   */

  // appName under which to write log
  private final String appName;
  // dateStamp (filename) of opened stream
  private String dateStamp = null;
  // opened stream
  private OutputStreamWriter logFile = null;
  // the last time we flushed our output stream
  private long lastFlush = 0L;

  // date formatter
  private SimpleDateFormat restrictedFileDateFormatter =
      new SimpleDateFormat(DATE_FORMAT, Locale.US);

  public String getFormattedFileDateNow() {
    // SimpleDateFormat is not thread safe...
    synchronized (restrictedFileDateFormatter) {
      // the format() return is a String overlaying a common buffer.
      // the string content can change unexpectedly. Access it only
      // within this synchronized section.
      String value = restrictedFileDateFormatter.format(new Date());
      return new String(value);
    }

  }

  private SimpleDateFormat restrictedLogLineDateFormatter =
      new SimpleDateFormat(LOG_LINE_DATE_FORMAT, Locale.US);

  public String getFormattedLogLineDateNow() {
    // SimpleDateFormat is not thread safe...
    synchronized (restrictedLogLineDateFormatter) {
      // the format() return is a String overlaying a common buffer.
      // the string content can change unexpectedly. Access it only
      // within this synchronized section.
      String value = restrictedLogLineDateFormatter.format(new Date());
      return new String(value);
    }
  }


  private class LoggingFileObserver extends FileObserver {

    public LoggingFileObserver(String path) {
      super(path, FileObserver.DELETE_SELF | FileObserver.MOVE_SELF);
    }

    @Override
    public void onEvent(int event, String path) {
      if (WebLoggerImpl.this.logFile != null) {
        try {
          if (event == FileObserver.MOVE_SELF) {
            WebLoggerImpl.this.logFile.flush();
          }
          WebLoggerImpl.this.logFile.close();
        } catch (IOException e) {
          e.printStackTrace();
          Log.w("WebLogger", "detected delete or move of logging directory -- shutting down");
        }
        WebLoggerImpl.this.logFile = null;
      }
    }
  }

  private LoggingFileObserver loggingObserver = null;

  WebLoggerImpl(String appName) {
    this.appName = appName;
  }

  public synchronized void close() {
    if (logFile != null) {
      OutputStreamWriter writer = logFile;
      logFile = null;
      String loggingPath = ODKFileUtils.getLoggingFolder(appName);
      File loggingDirectory = new File(loggingPath);
      if (!loggingDirectory.exists()) {
        Log.e("WebLogger", "Logging directory does not exist -- special handling under " + appName);
        try {
          writer.close();
        } catch (IOException e) {
        }
        loggingDirectory.delete();
      } else {
        try {
          writer.flush();
          writer.close();
        } catch (IOException e) {
          Log.e("WebLogger", "Unable to flush and close " + appName + " WebLogger");
        }
      }
    }
  }

  public synchronized void staleFileScan(long now) {
    try {
      // ensure we have the directories created...
      ODKFileUtils.verifyExternalStorageAvailability();
      ODKFileUtils.assertDirectoryStructure(appName);
      // scan for stale logs...
      String loggingPath = ODKFileUtils.getLoggingFolder(appName);
      final long distantPast = now - 30L * MILLISECONDS_DAY; // thirty days
      // ago...
      File loggingDirectory = new File(loggingPath);
      if (!loggingDirectory.exists()) {
        if (!loggingDirectory.mkdirs()) {
          Log.e("WebLogger", "Unable to create logging directory");
          return;
        }
      }

      if (!loggingDirectory.isDirectory()) {
        Log.e("WebLogger", "Logging Directory exists but is not a directory!");
        return;
      }

      File[] stale = loggingDirectory.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return (pathname.lastModified() < distantPast);
        }
      });

      if (stale != null) {
        for (File f : stale) {
          f.delete();
        }
      }
    } catch (Exception e) {
      // no exceptions are claimed, but since we can mount/unmount
      // the SDCard, there might be an external storage unavailable
      // exception that would otherwise percolate up.
      e.printStackTrace();
    }
  }

  private synchronized void log(String logMsg) throws IOException {
    String curDateStamp = getFormattedFileDateNow();
    if (logFile == null || dateStamp == null || !curDateStamp.equals(dateStamp)) {
      // the file we should log to has changed.
      // or has not yet been opened.

      if (logFile != null) {
        // close existing writer...
        OutputStreamWriter writer = logFile;
        logFile = null;
        try {
          writer.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      // ensure we have the directories created...
      ODKFileUtils.verifyExternalStorageAvailability();
      ODKFileUtils.assertDirectoryStructure(appName);
      String loggingPath = ODKFileUtils.getLoggingFolder(appName);
      File loggingDirectory = new File(loggingPath);
      if (!loggingDirectory.exists()) {
        if (!loggingDirectory.mkdirs()) {
          Log.e("WebLogger", "Unable to create logging directory");
          return;
        }
      }

      if (!loggingDirectory.isDirectory()) {
        Log.e("WebLogger", "Logging Directory exists but is not a directory!");
        return;
      }

      if ( loggingObserver == null ) {
        loggingObserver = new LoggingFileObserver(loggingDirectory.getAbsolutePath());
      }

      File f = new File(loggingDirectory, curDateStamp + ".log");
      try {
        FileOutputStream fo = new FileOutputStream(f, true);
        logFile = new OutputStreamWriter(new BufferedOutputStream(fo), CharEncoding.UTF_8);
        dateStamp = curDateStamp;
        // if we see a lot of these being logged, we have a problem
        logFile.write("---- starting ----\n");
      } catch (Exception e) {
        e.printStackTrace();
        Log.e("WebLogger", "Unexpected exception while opening logging file: " + e.toString());
        try {
          if (logFile != null) {
            logFile.close();
          }
        } catch (Exception ex) {
          // ignore
        }
        logFile = null;
        return;
      }
    }

    if (logFile != null) {
      logFile.write(logMsg + "\n");
    }

    if (lastFlush + WebLoggerImpl.FLUSH_INTERVAL < System.currentTimeMillis()) {
      // log when we are explicitly flushing, just to have a record of that in
      // the log
      logFile.write("---- flushing ----\n");
      logFile.flush();
      lastFlush = System.currentTimeMillis();
    }
  }

  public void log(int severity, String t, String logMsg) {
    try {
      if (MIN_LOG_LEVEL_TO_SPEW > severity) {
        // we are suppressing this level of logging
        return;
      }

      String androidLogLine = logMsg;

      // insert timestamp to help with time tracking
      String curLogLineStamp = getFormattedLogLineDateNow();
      if ( !logMsg.startsWith(curLogLineStamp.substring(0, 16))) {
        logMsg = curLogLineStamp + " " + logMsg;
      } else {
        androidLogLine = (logMsg.length() > curLogLineStamp.length()) ?
            logMsg.substring(curLogLineStamp.length()) : logMsg;
      }
      if ( androidLogLine.length() > 128 ) {
        androidLogLine = androidLogLine.substring(0,125) + "...";
      }

      String androidTag = t;
      int periodIdx = t.lastIndexOf('.');
      if ( t.length() > 26 && periodIdx != -1 ) {
        androidTag = t.substring(periodIdx+1);
      }

      // do logcat logging...
      if (severity == ERROR) {
        Log.e(androidTag, androidLogLine);
      } else if (severity == WARN) {
        Log.w(androidTag, androidLogLine);
      } else if (severity == INFO || severity == SUCCESS || severity == TIP) {
        Log.i(androidTag, androidLogLine);
      } else if (severity == DEBUG){
        Log.d(androidTag, androidLogLine);
      } else {
        Log.v(androidTag, androidLogLine);
      }

      // and compose the log to the file...
      switch (severity) {
      case ASSERT:
        logMsg = "A/" + t + ": " + logMsg;
        break;
      case DEBUG:
        logMsg = "D/" + t + ": " + logMsg;
        break;
      case ERROR:
        logMsg = "E/" + t + ": " + logMsg;
        break;
      case INFO:
        logMsg = "I/" + t + ": " + logMsg;
        break;
      case SUCCESS:
        logMsg = "S/" + t + ": " + logMsg;
        break;
      case VERBOSE:
        logMsg = "V/" + t + ": " + logMsg;
        break;
      case TIP:
        logMsg = "T/" + t + ": " + logMsg;
        break;
      case WARN:
        logMsg = "W/" + t + ": " + logMsg;
        break;
      default:
        Log.d(t, logMsg);
        logMsg = "?/" + t + ": " + logMsg;
        break;
      }
      log(logMsg);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void a(String t, String logMsg) {
    log(ASSERT, t, logMsg);
  }

  public void t(String t, String logMsg) {
    log(TIP, t, logMsg);
  }

  public void v(String t, String logMsg) {
    log(VERBOSE, t, logMsg);
  }

  public void d(String t, String logMsg) {
    log(DEBUG, t, logMsg);
  }

  public void i(String t, String logMsg) {
    log(INFO, t, logMsg);
  }

  public void w(String t, String logMsg) {
    log(WARN, t, logMsg);
  }

  public void e(String t, String logMsg) {
    log(ERROR, t, logMsg);
  }

  public void s(String t, String logMsg) {
    log(SUCCESS, t, logMsg);
  }

  public void printStackTrace(Throwable e) {
    e.printStackTrace();
    ByteArrayOutputStream ba = new ByteArrayOutputStream();
    PrintStream w;
    try {
      w = new PrintStream(ba, false, "UTF-8");
      e.printStackTrace(w);
      w.flush();
      w.close();
      log(ba.toString("UTF-8"));
    } catch (UnsupportedEncodingException e1) {
      // error if it ever occurs
      throw new IllegalStateException("unable to specify UTF-8 Charset!");
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }

}
