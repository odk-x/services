/*
 * Copyright (C) 2011-2013 University of Washington
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

package org.opendatakit.services.utilities;

import org.apache.commons.lang3.CharEncoding;
import org.opendatakit.logging.WebLogger;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Wrapper class for accessing Base64 functionality. This allows API Level 7
 * deployment of ODK Survey while enabling API Level 8 and higher phone to
 * support encryption.
 *
 * @author mitchellsundt@gmail.com
 */
public class Base64Wrapper {

  private static final int FLAGS = 2;// NO_WRAP
  private final String appName;
  private Class<?> base64 = null;
  private static final String TAG = Base64Wrapper.class.getSimpleName();

  public Base64Wrapper(String appName) throws ClassNotFoundException {
    this.appName = appName;
    base64 = this.getClass().getClassLoader().loadClass("android.util.Base64");
  }

  /**
   * Encodes the given byte array to a base64 encoded string
   * @param ba the bytes to encode
   * @return the bytes encoded as base64 and put in a string
   */
  public String encodeToString(byte[] ba) {
    Class<?>[] argClassList = new Class[] { byte[].class, int.class };
    try {

      Method m = base64.getDeclaredMethod("encode", argClassList);
      Object[] argList = new Object[] { ba, FLAGS };
      Object o = m.invoke(null, argList);
      byte[] outArray = (byte[]) o;
      return new String(outArray, CharEncoding.UTF_8);
    } catch (SecurityException | NoSuchMethodException | IllegalAccessException | UnsupportedEncodingException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new IllegalArgumentException(e.toString(), e);
    } catch (InvocationTargetException e) {
      WebLogger.getLogger(appName).e(TAG, "Unexpected base64 error");
      WebLogger.getLogger(appName).printStackTrace(e.getCause());
      throw new IllegalArgumentException(e.getCause().toString(), e);
    }
  }

  public byte[] decode(String base64String) {
    Class<?>[] argClassList = new Class[] { String.class, int.class };
    Object o;
    try {
      Method m = base64.getDeclaredMethod("decode", argClassList);
      Object[] argList = new Object[] { base64String, FLAGS };
      o = m.invoke(null, argList);
    } catch (SecurityException | NoSuchMethodException | IllegalAccessException e) {
      WebLogger.getLogger(appName).printStackTrace(e);
      throw new IllegalArgumentException(e.toString(), e);
    } catch (InvocationTargetException e) {
      WebLogger.getLogger(appName).e(TAG, "Unexpected base64 error");
      WebLogger.getLogger(appName).printStackTrace(e);
      WebLogger.getLogger(appName).printStackTrace(e.getCause());
      throw new IllegalArgumentException(e.toString(), e);
    }
    return (byte[]) o;
  }
}
