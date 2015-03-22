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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;

import android.os.FileObserver;
import android.util.Log;

/**
 * Logger that emits logs to the LOGGING_PATH and recycles them as needed.
 * Useful to separate out ODK log entries from the overall logging stream,
 * especially on heavily logged 4.x systems.
 *
 * @author mitchellsundt@gmail.com
 */
public class WebLogger {
  private static final long MILLISECONDS_DAY = 86400000L;
  private static final long FLUSH_INTERVAL = 12000L; // 5 times a minute

  private static final int ASSERT = 1;
  private static final int VERBOSE = 2;
  private static final int DEBUG = 3;
  private static final int INFO = 4;
  private static final int WARN = 5;
  private static final int ERROR = 6;
  private static final int SUCCESS = 7;
  private static final int TIP = 8;

  private static final int LOG_INFO_LEVEL = 1;

  private static long lastStaleScan = 0L;
  private static Map<String, WebLogger> loggers = new HashMap<String, WebLogger>();

  static {
    // register a state-reset manipulator for 'loggers' field.
    StaticStateManipulator.get().register(99, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        closeAll();
      }

    });
  }

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

  private class LoggingFileObserver extends FileObserver {

    public LoggingFileObserver(String path) {
      super(path, FileObserver.DELETE_SELF | FileObserver.MOVE_SELF);
    }

    @Override
    public void onEvent(int event, String path) {
      if (WebLogger.this.logFile != null) {
        try {
          if (event == FileObserver.MOVE_SELF) {
            WebLogger.this.logFile.flush();
          }
          WebLogger.this.logFile.close();
        } catch (IOException e) {
          e.printStackTrace();
          Log.w("WebLogger", "detected delete or move of logging directory -- shutting down");          
        }
        WebLogger.this.logFile = null;
      }
    }
  }

  private LoggingFileObserver loggingObserver = null;

  private static class ThreadLogger extends ThreadLocal<String> {

    @Override
    protected String initialValue() {
      return null;
    }

  }

  private static ThreadLogger contextLogger = new ThreadLogger();

  public static synchronized void closeAll() {
    for (WebLogger l : loggers.values()) {
      l.close();
    }
    loggers.clear();
  }

  public static WebLogger getContextLogger() {
    String appNameOfThread = contextLogger.get();
    if (appNameOfThread != null) {
      return getLogger(appNameOfThread);
    }
    return null;
  }

  public synchronized static WebLogger getLogger(String appName) {
    WebLogger logger = loggers.get(appName);
    if (logger == null) {
      logger = new WebLogger(appName);
      loggers.put(appName, logger);
    }

    contextLogger.set(appName);

    long now = System.currentTimeMillis();
    if (lastStaleScan + MILLISECONDS_DAY < now) {
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
            return logger;
          }
        }

        if (!loggingDirectory.isDirectory()) {
          Log.e("WebLogger", "Logging Directory exists but is not a directory!");
          return logger;
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
      } finally {
        // whether or not we failed, record that we did the scan.
        lastStaleScan = now;
      }
    }
    return logger;
  }

  private WebLogger(String appName) {
    this.appName = appName;
  }

  private synchronized void close() {
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

  private synchronized void log(String logMsg) throws IOException {
    String curDateStamp = (new SimpleDateFormat("yyyy-MM-dd_HH", Locale.ENGLISH))
        .format(new Date());
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

    if (lastFlush + WebLogger.FLUSH_INTERVAL < System.currentTimeMillis()) {
      // log when we are explicitly flushing, just to have a record of that in
      // the log
      logFile.write("---- flushing ----\n");
      logFile.flush();
      lastFlush = System.currentTimeMillis();
    }
  }

  public void log(int severity, String t, String logMsg) {
    try {
      // do logcat logging...
      if (severity == ERROR) {
        Log.e(t, logMsg);
      } else if (severity == WARN) {
        Log.w(t, logMsg);
      } else if (LOG_INFO_LEVEL >= severity) {
        Log.i(t, logMsg);
      } else {
        Log.d(t, logMsg);
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

  public void s(String t, String logMsg) {
    log(SUCCESS, t, logMsg);
  }

}
