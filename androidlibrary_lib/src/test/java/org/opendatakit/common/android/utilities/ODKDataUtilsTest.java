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
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.*;
import static org.junit.Assert.*;

import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.desktop.WebLoggerDesktopFactoryImpl;

public class ODKDataUtilsTest {

  @BeforeClass
  public static void oneTimeSetUp() throws Exception {
    StaticStateManipulator.get().reset();
    WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
  }

  @Test
  public void testHackedName() {
    assertEquals("aname", ODKDataUtils.getLocalizedDisplayName(
        NameUtil.normalizeDisplayName(NameUtil.constructSimpleDisplayName("aname"))));
    assertEquals("a name", ODKDataUtils.getLocalizedDisplayName(
        NameUtil.normalizeDisplayName(NameUtil.constructSimpleDisplayName("a_name"))));
    assertEquals("_ an am e", ODKDataUtils.getLocalizedDisplayName(
        NameUtil.normalizeDisplayName(NameUtil.constructSimpleDisplayName("_an_am_e"))));
    assertEquals("an ame _", ODKDataUtils.getLocalizedDisplayName(
        NameUtil.normalizeDisplayName(NameUtil.constructSimpleDisplayName("an_ame_"))));
  }

  @Test
  public void testNormalizeDisplayName() throws JsonProcessingException {
    Map<String,Object> langMap = new TreeMap<String,Object>();
    langMap.put("en_US", "This is a test");
    langMap.put("en_GB", "Test is This");
    langMap.put("en", "Huh Test");
    langMap.put("fr", "Je suis");
    langMap.put("default", "No way!");
    String value = ODKFileUtils.mapper.writeValueAsString(langMap);

    String match;

    Locale.setDefault(Locale.US);
    match = ODKDataUtils.getLocalizedDisplayName(value);
    assertEquals("This is a test", match);

    Locale.setDefault(Locale.UK);
    match = ODKDataUtils.getLocalizedDisplayName(value);
    assertEquals("Test is This", match);

    Locale.setDefault(Locale.CANADA);
    match = ODKDataUtils.getLocalizedDisplayName(value);
    assertEquals("Huh Test", match);

    Locale.setDefault(Locale.CANADA_FRENCH);
    match = ODKDataUtils.getLocalizedDisplayName(value);
    assertEquals("Je suis", match);

    Locale.setDefault(Locale.GERMANY);
    match = ODKDataUtils.getLocalizedDisplayName(value);
    assertEquals("No way!", match);

    Locale.setDefault(Locale.US);
  }

  @Test
  public void testNormalizeDisplayName2() {
    Map<String,Object> langMap = new TreeMap<String,Object>();
    langMap.put("en_US", "This is a test");
    langMap.put("en_GB", "Test is This");
    langMap.put("en", "Huh Test");
    langMap.put("fr", "Je suis");
    langMap.put("default", "No way!");

    String match;

    Locale.setDefault(Locale.US);
    match = ODKDataUtils.getLocalizedDisplayName(langMap);
    assertEquals("This is a test", match);

    Locale.setDefault(Locale.UK);
    match = ODKDataUtils.getLocalizedDisplayName(langMap);
    assertEquals("Test is This", match);

    Locale.setDefault(Locale.CANADA);
    match = ODKDataUtils.getLocalizedDisplayName(langMap);
    assertEquals("Huh Test", match);

    Locale.setDefault(Locale.CANADA_FRENCH);
    match = ODKDataUtils.getLocalizedDisplayName(langMap);
    assertEquals("Je suis", match);

    Locale.setDefault(Locale.GERMANY);
    match = ODKDataUtils.getLocalizedDisplayName(langMap);
    assertEquals("No way!", match);

    Locale.setDefault(Locale.US);
  }
}
