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

package org.opendatakit.common.android.data;

import java.util.Locale;
import java.util.TimeZone;

import android.graphics.Color;
import org.junit.*;
import static org.junit.Assert.*;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.common.android.data.ColorRule;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.desktop.WebLoggerDesktopFactoryImpl;

public class ColorRuleTest {

   @BeforeClass
   public static void oneTimeSetUp() throws Exception {
      StaticStateManipulator.get().reset();
      WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
   }

   @Test
   public void testColorRule() {
      ColorRule cr1 = new ColorRule("myElement", ColorRule.RuleType.EQUAL, "5", Color.BLUE, Color
          .WHITE);
      ColorRule cr2 = new ColorRule("myElement", ColorRule.RuleType.EQUAL, "5", Color.BLUE, Color
          .WHITE);

      assertTrue(cr1.equalsWithoutId(cr2));
      assertTrue(cr2.equalsWithoutId(cr1));

      assertEquals(Color.WHITE, cr1.getBackground());
      assertEquals(Color.BLUE, cr1.getForeground());
      assertEquals(ColorRule.RuleType.EQUAL, cr1.getOperator());
      assertEquals("myElement", cr1.getColumnElementKey());
      assertEquals("5", cr1.getVal());

      String crs1 = cr1.toString();
      String crs2 = cr2.toString();
      assertEquals(crs1.substring(crs1.indexOf(',')), crs2.substring((crs2.indexOf(','))));

      assertNotEquals(cr1.getRuleId(), cr2.getRuleId());
      assertNotEquals(cr1, cr2);

      cr2.setVal("6");
      assertFalse(cr1.equalsWithoutId(cr2));
      assertEquals("6", cr2.getVal());
      cr1.setVal("6");

      assertTrue(cr1.equalsWithoutId(cr2));

      cr2.setBackground(Color.GREEN);
      assertFalse(cr1.equalsWithoutId(cr2));
      assertEquals(Color.GREEN, cr2.getBackground());
      cr1.setBackground(Color.GREEN);

      assertTrue(cr1.equalsWithoutId(cr2));

      cr2.setForeground(Color.RED);
      assertFalse(cr1.equalsWithoutId(cr2));
      assertEquals(Color.RED, cr2.getForeground());
      cr1.setForeground(Color.RED);

      assertTrue(cr1.equalsWithoutId(cr2));

      cr2.setOperator(ColorRule.RuleType.GREATER_THAN);
      assertFalse(cr1.equalsWithoutId(cr2));
      assertEquals(ColorRule.RuleType.GREATER_THAN, cr2.getOperator());
      cr1.setOperator(ColorRule.RuleType.GREATER_THAN);

      assertTrue(cr1.equalsWithoutId(cr2));

      cr2.setColumnElementKey("fredColumn");
      assertFalse(cr1.equalsWithoutId(cr2));
      assertEquals("fredColumn", cr2.getColumnElementKey());
      cr1.setColumnElementKey("fredColumn");

      assertTrue(cr1.equalsWithoutId(cr2));
   }

}
