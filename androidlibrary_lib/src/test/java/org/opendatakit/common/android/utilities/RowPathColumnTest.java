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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.*;
import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.utilities.RowPathColumnUtil;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.desktop.WebLoggerDesktopFactoryImpl;

public class RowPathColumnTest {

  @BeforeClass
  public static void oneTimeSetUp() throws Exception {
     StaticStateManipulator.get().reset();
     WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
  }

  @Test
  public void testRowPathColumnExtractionOneRecord() throws JsonParseException,
      JsonMappingException,
      IOException {

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("myField1", "myField1",
        ElementDataType.string.name(), null));
    columns.add(new Column("myField2", "myField2",
        ElementDataType.integer.name(), null));
    columns.add(new Column("myField3", "myField3",
        ElementDataType.number.name(), null));
    ArrayList<String> subElements = new ArrayList<String>();
    subElements.add("myField4_uriFragment");
    subElements.add("myField4_contentType");
    columns.add(new Column("myField4", "myField4",
        ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements)));
    columns.add(new Column("myField4_uriFragment", "uriFragment",
        ElementDataType.rowpath.name(), null));
    columns.add(
        new Column("myField4_contentType", "contentType", ElementDataType.string.name(), null));

    OrderedColumns orderedColumns = new OrderedColumns("myApp", "mytable", columns);

    List<ColumnDefinition> cols = RowPathColumnUtil.get().getUriColumnDefinitions(orderedColumns);

    assertNotNull(cols);
    int size = cols.size();
    assertEquals(size, 1);
    ColumnDefinition defn = cols.get(0);
    assertEquals(defn.getElementKey(), "myField4");
  }


  @Test
  public void testRowPathColumnExtractionTwoRecords() throws JsonParseException,
      JsonMappingException,
      IOException {

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("myField1", "myField1",
        ElementDataType.string.name(), null));
    columns.add(new Column("myField2", "myField2",
        ElementDataType.integer.name(), null));
    columns.add(new Column("myField3", "myField3",
        ElementDataType.number.name(), null));
    ArrayList<String> subElements1 = new ArrayList<String>();
    subElements1.add("myField4_uriFragment");
    subElements1.add("myField4_contentType");
    columns.add(new Column("myField4", "myField4",
        ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements1)));
    columns.add(new Column("myField4_uriFragment", "uriFragment",
        ElementDataType.rowpath.name(), null));
    columns.add(
        new Column("myField4_contentType", "contentType", ElementDataType.string.name(), null));
    ArrayList<String> subElements2 = new ArrayList<String>();
    subElements2.add("myField5_uriFragment");
    subElements2.add("myField5_contentType");
    columns.add(new Column("myField5", "myField5",
        ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements2)));
    columns.add(new Column("myField5_uriFragment", "uriFragment",
        ElementDataType.rowpath.name(), null));
    columns.add(
        new Column("myField5_contentType", "contentType", ElementDataType.string.name(), null));

    OrderedColumns orderedColumns = new OrderedColumns("myApp", "mytable", columns);

    List<ColumnDefinition> cols = RowPathColumnUtil.get().getUriColumnDefinitions(
        orderedColumns);

    assertNotNull(cols);
    int size = cols.size();
    assertEquals(size, 2);
    ColumnDefinition defn1 = cols.get(0);
    assertEquals(defn1.getElementKey(), "myField4");
    ColumnDefinition defn2 = cols.get(1);
    assertEquals(defn2.getElementKey(), "myField5");
  }

   @Test
   public void testRowPathColumnExtractionSecondRecordWrongFragType() throws
       JsonParseException,
       JsonMappingException,
       IOException {

      List<Column> columns = new ArrayList<Column>();
      columns.add(new Column("myField1", "myField1",
          ElementDataType.string.name(), null));
      columns.add(new Column("myField2", "myField2",
          ElementDataType.integer.name(), null));
      columns.add(new Column("myField3", "myField3",
          ElementDataType.number.name(), null));
      ArrayList<String> subElements1 = new ArrayList<String>();
      subElements1.add("myField4_uriFragment");
      subElements1.add("myField4_contentType");
      columns.add(new Column("myField4", "myField4",
          ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements1)));
      columns.add(new Column("myField4_uriFragment", "uriFragment",
          ElementDataType.rowpath.name(), null));
      columns.add(
          new Column("myField4_contentType", "contentType", ElementDataType.string.name(), null));
      ArrayList<String> subElements2 = new ArrayList<String>();
      subElements2.add("myField5_uriFragment");
      subElements2.add("myField5_contentType");
      columns.add(new Column("myField5", "myField5",
          ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements2)));
      columns.add(new Column("myField5_uriFragment", "uriFragment",
          ElementDataType.string.name(), null));
      columns.add(
          new Column("myField5_contentType", "contentType", ElementDataType.string.name(), null));

      OrderedColumns orderedColumns = new OrderedColumns("myApp", "mytable", columns);

      List<ColumnDefinition> cols = RowPathColumnUtil.get().getUriColumnDefinitions(orderedColumns);

      assertNotNull(cols);
      int size = cols.size();
      assertEquals(size, 1);
      ColumnDefinition defn1 = cols.get(0);
      assertEquals(defn1.getElementKey(), "myField4");
   }

   @Test
   public void testRowPathColumnExtractionSecondRecordWrongContentType() throws
       JsonParseException,
       JsonMappingException,
       IOException {

      List<Column> columns = new ArrayList<Column>();
      columns.add(new Column("myField1", "myField1",
          ElementDataType.string.name(), null));
      columns.add(new Column("myField2", "myField2",
          ElementDataType.integer.name(), null));
      columns.add(new Column("myField3", "myField3",
          ElementDataType.number.name(), null));
      ArrayList<String> subElements1 = new ArrayList<String>();
      subElements1.add("myField4_uriFragment");
      subElements1.add("myField4_contentType");
      columns.add(new Column("myField4", "myField4",
          ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements1)));
      columns.add(new Column("myField4_uriFragment", "uriFragment",
          ElementDataType.rowpath.name(), null));
      columns.add(
          new Column("myField4_contentType", "contentType", ElementDataType.string.name(), null));
      ArrayList<String> subElements2 = new ArrayList<String>();
      subElements2.add("myField5_uriFragment");
      subElements2.add("myField5_contentType");
      columns.add(new Column("myField5", "myField5",
          ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements2)));
      columns.add(new Column("myField5_uriFragment", "uriFragment",
          ElementDataType.rowpath.name(), null));
      columns.add(
          new Column("myField5_contentType", "contentType", ElementDataType.integer.name(), null));

      OrderedColumns orderedColumns = new OrderedColumns("myApp", "mytable", columns);

      List<ColumnDefinition> cols = RowPathColumnUtil.get().getUriColumnDefinitions(orderedColumns);

      assertNotNull(cols);
      int size = cols.size();
      assertEquals(size, 1);
      ColumnDefinition defn1 = cols.get(0);
      assertEquals(defn1.getElementKey(), "myField4");
   }

  @Test
  public void testRowPathColumnExtractionNoParentSharedNamesWrongTypes() throws JsonParseException,
      JsonMappingException,
      IOException {

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("uriFragment", "uriFragment",
        ElementDataType.string.name(), null));
    columns.add(new Column("contentType", "contentType",
        ElementDataType.integer.name(), null));
    columns.add(new Column("myField3", "myField3",
        ElementDataType.number.name(), null));
    ArrayList<String> subElements = new ArrayList<String>();
    subElements.add("myField4_uriFragment");
    subElements.add("myField4_contentType");
    columns.add(new Column("myField4", "myField4",
        ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements)));
    columns.add(new Column("myField4_uriFragment", "uriFragment",
        ElementDataType.rowpath.name(), null));
    columns.add(
        new Column("myField4_contentType", "contentType", ElementDataType.string.name(), null));

    OrderedColumns orderedColumns = new OrderedColumns("myApp", "mytable", columns);

    List<ColumnDefinition> cols = RowPathColumnUtil.get().getUriColumnDefinitions(
        orderedColumns);

    assertNotNull(cols);
    int size = cols.size();
    assertEquals(size, 1);
    ColumnDefinition defn = cols.get(0);
    assertEquals(defn.getElementKey(), "myField4");
  }

  @Test
  public void testRowPathColumnExtractionNoParentSharedNamesRightTypes() throws JsonParseException,
      JsonMappingException,
      IOException {

    List<Column> columns = new ArrayList<Column>();
    columns.add(new Column("contentType", "contentType",
        ElementDataType.string.name(), null));
    columns.add(new Column("uriFragment", "uriFragment",
        ElementDataType.rowpath.name(), null));
    columns.add(new Column("myField2", "myField2",
        ElementDataType.integer.name(), null));
    columns.add(new Column("myField3", "myField3",
        ElementDataType.number.name(), null));
    ArrayList<String> subElements = new ArrayList<String>();
    subElements.add("myField4_uriFragment");
    subElements.add("myField4_contentType");
    columns.add(new Column("myField4", "myField4",
        ElementDataType.object.name(), ODKFileUtils.mapper.writeValueAsString(subElements)));
    columns.add(new Column("myField4_uriFragment", "uriFragment",
        ElementDataType.rowpath.name(), null));
    columns.add(
        new Column("myField4_contentType", "contentType", ElementDataType.string.name(), null));

    OrderedColumns orderedColumns = new OrderedColumns("myApp", "mytable", columns);

    List<ColumnDefinition> cols = RowPathColumnUtil.get().getUriColumnDefinitions(
        orderedColumns);

    assertNotNull(cols);
    int size = cols.size();
    assertEquals(size, 1);
    ColumnDefinition defn = cols.get(0);
    assertEquals(defn.getElementKey(), "myField4");
  }
}
