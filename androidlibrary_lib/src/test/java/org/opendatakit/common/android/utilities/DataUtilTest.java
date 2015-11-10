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

package org.opendatakit.common.android.utilities;

import java.util.Locale;
import java.util.TimeZone;

import org.junit.*;
import static org.junit.Assert.*;

import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.desktop.WebLoggerDesktopFactoryImpl;

public class DataUtilTest {

  @BeforeClass
  public static void oneTimeSetUp() throws Exception {
    StaticStateManipulator.get().reset();
    WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
  }

  @Test
  public void testDateInterpretation() {
    TimeZone tz = TimeZone.getTimeZone(TimeZone.getAvailableIDs()[0]);
    DataUtil util = new DataUtil(Locale.US, tz);
    
    String value = util.validifyDateValue("3/4/2015");
    
    String expected = "2015-03-04T";
    assertEquals(expected, value.substring(0,expected.length()));
  }

}
