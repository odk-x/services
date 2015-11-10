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

import org.opendatakit.common.android.utilities.UTMConverter;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.desktop.WebLoggerDesktopFactoryImpl;

public class UTMConverterTest {

  @BeforeClass
  public static void oneTimeSetUp() throws Exception {
    StaticStateManipulator.get().reset();
    WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
  }

  private boolean nearlySame(double a, double b) {
    if ( a == b ) return true;
    double big = (Math.abs(a) > Math.abs(b)) ? a : b;
    double diffFromBig = (Math.abs(a) > Math.abs(b)) ? (a-b) : (b-a);
    double scaledDiff = Math.abs( diffFromBig / big);

    // same if differ by less than 0.1%
    return (scaledDiff < 0.001);
  }

  @Test
  public void testUTMConversion1() {

    // values taken from http://www.uwgb.edu/dutchs/usefuldata/ConvertUTMNoOZ.HTM
    double[] results = UTMConverter.parseUTM(500000.0, 8881585.8, 38, false);
    double[] latLong = { 80.0, 45.0 };

    assertTrue(nearlySame(latLong[0], results[0]));
    assertTrue(nearlySame(latLong[1], results[1]));
  }


  @Test
  public void testUTMConversion2() {

    // values taken from http://www.uwgb.edu/dutchs/usefuldata/ConvertUTMNoOZ.HTM
    double[] results = UTMConverter.parseUTM(500000.0, 8881585.8, 23, false);
    double[] latLong = { 80.0, -45.0 };

    assertTrue(nearlySame(latLong[0], results[0]));
    assertTrue(nearlySame(latLong[1], results[1]));
  }

  @Test
  public void testUTMConversion3() {

    // values taken from http://www.uwgb.edu/dutchs/usefuldata/ConvertUTMNoOZ.HTM
    double[] results = UTMConverter.parseUTM(500000.0, 8894587.5, 23, true);
    double[] latLong = { -10.0, -45.0 };

    assertTrue(nearlySame(latLong[0], results[0]));
    assertTrue(nearlySame(latLong[1], results[1]));
  }

  @Test
  public void testUTMConversion4() {

    // values taken from http://www.uwgb.edu/dutchs/usefuldata/ConvertUTMNoOZ.HTM
    double[] results = UTMConverter.parseUTM(500000.0, 8894587.5, 38, true);
    double[] latLong = { -10.0, 45.0 };

    assertTrue(nearlySame(latLong[0], results[0]));
    assertTrue(nearlySame(latLong[1], results[1]));
  }
}
