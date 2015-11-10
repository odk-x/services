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

import java.util.HashMap;
import java.util.Map;

import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;

/**
 * Logger that emits logs to the LOGGING_PATH and recycles them as needed.
 * Useful to separate out ODK log entries from the overall logging stream,
 * especially on heavily logged 4.x systems.
 *
 * @author mitchellsundt@gmail.com
 */
public class WebLogger {
  private static final long MILLISECONDS_DAY = 86400000L;

  private static long lastStaleScan = 0L;
  private static Map<String, WebLoggerIf> loggers = new HashMap<String, WebLoggerIf>();

  private static WebLoggerFactoryIf webLoggerFactory;

  static {
    webLoggerFactory = new WebLoggerFactoryImpl();

    // register a state-reset manipulator for 'loggers' field.
    StaticStateManipulator.get().register(99, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        closeAll();
      }

    });
  }

  public static void setFactory(WebLoggerFactoryIf webLoggerFactoryImpl) {
    webLoggerFactory = webLoggerFactoryImpl;
  }

  private static class ThreadLogger extends ThreadLocal<String> {

    @Override
    protected String initialValue() {
      return null;
    }

  }

  private static ThreadLogger contextLogger = new ThreadLogger();

  public static synchronized void closeAll() {
    for (WebLoggerIf l : loggers.values()) {
      l.close();
    }
    loggers.clear();
    // and forget any thread-local associations for the loggers
    contextLogger = new ThreadLogger();
  }

  public static WebLoggerIf getContextLogger() {
    String appNameOfThread = contextLogger.get();
    if (appNameOfThread != null) {
      return getLogger(appNameOfThread);
    }
    return null;
  }

  public synchronized static WebLoggerIf getLogger(String appName) {
    WebLoggerIf logger = loggers.get(appName);
    if (logger == null) {
      logger = webLoggerFactory.createWebLogger(appName);
      loggers.put(appName, logger);
    }

    contextLogger.set(appName);

    long now = System.currentTimeMillis();
    if (lastStaleScan + MILLISECONDS_DAY < now) {
      try {
        logger.staleFileScan(now);
      } finally {
        // whether or not we failed, record that we did the scan.
        lastStaleScan = now;
      }
    }
    return logger;
  }
}
