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

import java.util.List;

import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;

public class GeoColumnUtil {

  private static GeoColumnUtil geoColumnUtil = new GeoColumnUtil();
  
  static {
    // register a state-reset manipulator for 'geoColumnUtil' field.
    StaticStateManipulator.get().register(50, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        geoColumnUtil = new GeoColumnUtil();
      }
      
    });
  }

  public static GeoColumnUtil get() {
    return geoColumnUtil;
  }

  /**
   * For mocking -- supply a mocked object.
   * 
   * @param util
   */
  public static void set(GeoColumnUtil util) {
    geoColumnUtil = util;
  }

  protected GeoColumnUtil() {
  }

  public boolean isLatitudeColumnDefinition(List<ColumnDefinition> geoPointList, ColumnDefinition cd) {
    if (!cd.isUnitOfRetention()) {
      return false;
    }

    ElementDataType type = cd.getType().getDataType();
    if (!(type == ElementDataType.number || type == ElementDataType.integer)) {
      return false;
    }

    ColumnDefinition cdParent = cd.getParent();

    if (cdParent != null && geoPointList.contains(cdParent)
        && cd.getElementName().equals("latitude")) {
      return true;
    }

    if (endsWithIgnoreCase(cd.getElementName(), "latitude")) {
      return true;
    }

    return false;
  }

  public boolean isLongitudeColumnDefinition(List<ColumnDefinition> geoPointList,
      ColumnDefinition cd) {
    if (!cd.isUnitOfRetention()) {
      return false;
    }

    ElementDataType type = cd.getType().getDataType();
    if (!(type == ElementDataType.number || type == ElementDataType.integer))
      return false;

    ColumnDefinition cdParent = cd.getParent();

    if (cdParent != null && geoPointList.contains(cdParent)
        && cd.getElementName().equals("longitude")) {
      return true;
    }

    if (endsWithIgnoreCase(cd.getElementName(), "longitude")) {
      return true;
    }

    return false;
  }

  private boolean endsWithIgnoreCase(String text, String ending) {
    if (text.equalsIgnoreCase(ending)) {
      return true;
    }
    int spidx = text.lastIndexOf(' ');
    int usidx = text.lastIndexOf('_');
    int idx = Math.max(spidx, usidx);
    if (idx == -1) {
      return false;
    }
    return text.substring(idx + 1).equalsIgnoreCase(ending);
  }

}
