/*
 * Copyright (C) 2014 University of Washington
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
package org.opendatakit.common.android.utilities;

/**
 * Simple conversion utilities to interpret stored values.
 *
 * @author mitchellsundt@gmail.com
 *
 */
public class DataHelper {

  public static boolean intToBool(int i) {
    return i != 0;
  }

  public static int boolToInt(boolean b) {
    return b ? 1 : 0;
  }

  public static boolean stringToBool(String bool) {
    return (bool == null) ? true : bool.equalsIgnoreCase("true");
  }

}
