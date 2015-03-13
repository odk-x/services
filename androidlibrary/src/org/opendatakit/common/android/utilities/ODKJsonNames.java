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
 * These are tags used by the sensor framework to define data
 * tables into which results are published.
 * 
 * Pulled out of WorkerThread and ODKSensorManager
 * 
 * @author mitchellsundt@gmail.com
 *
 */
public class ODKJsonNames {
  public static final String jsonTableStr = "table";
  public static final String jsonTableIdStr = "tableId";
  public static final String jsonElementKeyStr = "elementKey";
  public static final String jsonElementNameStr = "elementName";
  public static final String jsonElementTypeStr = "elementType";
  public static final String jsonListChildElementKeysStr = "listChildElementKeys";
  public static final String jsonColumnsStr = "columns";
}
