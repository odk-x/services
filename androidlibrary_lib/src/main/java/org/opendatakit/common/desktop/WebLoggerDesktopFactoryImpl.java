/*
 * Copyright (C) 2015 University of Washington
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

package org.opendatakit.common.desktop;

import java.util.logging.Level;
import java.util.logging.Logger;

import android.annotation.SuppressLint;
import org.opendatakit.common.android.utilities.WebLoggerFactoryIf;
import org.opendatakit.common.android.utilities.WebLoggerIf;

/**
 * Replacement implementation for desktop deoployments (e.g., app-designer) and
 * when running JUnit4 tests on the desktop.
 *
 * @author mitchellsundt@gmail.com
 */
@SuppressLint("NewApi")
public class WebLoggerDesktopFactoryImpl implements WebLoggerFactoryIf {

  public class WebLoggerDesktopImpl implements WebLoggerIf {

    private String appName;

    public WebLoggerDesktopImpl(String appName) {
      this.appName = appName;
    }

    public void staleFileScan(long now) {
     // no-op
    }

    public void close() {
      // no-op
    }

    public void log(int severity, String t, String logMsg) {
      Logger.getGlobal().log(Level.INFO, t, "N:" + severity + "/" + logMsg);
    }

    public void a(String t, String logMsg) {
      Logger.getGlobal().log(Level.FINEST, t, logMsg);
    }

    public void t(String t, String logMsg) {
      Logger.getGlobal().log(Level.FINER, t, "Trace/" + logMsg);
    }

    public void v(String t, String logMsg) {
      Logger.getGlobal().log(Level.FINER, t, "Verbose/" + logMsg);
    }

    public void d(String t, String logMsg) {
      Logger.getGlobal().log(Level.FINE, t, logMsg);
    }

    public void i(String t, String logMsg) {
      Logger.getGlobal().log(Level.INFO, t, logMsg);
    }

    public void w(String t, String logMsg) {
      Logger.getGlobal().log(Level.WARNING, t, logMsg);
    }

    public void e(String t, String logMsg) {
      Logger.getGlobal().log(Level.SEVERE, t, logMsg);
    }

    public void s(String t, String logMsg) {
      Logger.getGlobal().log(Level.INFO, t, "Success/" + logMsg);
    }

    public void printStackTrace(Throwable e) {
      Logger.getGlobal().throwing("unknown", "unknown", e);
    }

  }

  public synchronized WebLoggerIf createWebLogger(String appName) {
    WebLoggerIf logger = new WebLoggerDesktopImpl(appName);
    return logger;
  }

}
