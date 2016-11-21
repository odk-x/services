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
package org.opendatakit.services.resolve.views.components;

/**
 * Represents a column that is in conflict--i.e. the contents differ between the
 * server and local versions or in the original row and most recent checkpoint.
 *
 * @author sudar.sam@gmail.com
 *
 */
public final class ConflictColumn {
  private final int position;
  private final String title;
  private final String elementKey;
  // most recent checkpoint or the local update
  private final String localRawValue;
  private final String localDisplayValue;
  // original row or the server update
  private final String serverRawValue;
  private final String serverDisplayValue;

  public ConflictColumn(int position, String title, String elementKey, String localRawValue,
      String localDisplayValue, String serverRawValue, String serverDisplayValue) {
    this.position = position;
    this.title = title;
    this.elementKey = elementKey;
    this.localRawValue = localRawValue;
    this.localDisplayValue = localDisplayValue;
    this.serverRawValue = serverRawValue;
    this.serverDisplayValue = serverDisplayValue;
  }

  public int getPosition() {
    return this.position;
  }

  public String getTitle() {
    return this.title;
  }

  public String getElementKey() {
    return this.elementKey;
  }

  public String getLocalRawValue() {
    return this.localRawValue;
  }

  public String getServerRawValue() {
    return this.serverRawValue;
  }

  public String getLocalDisplayValue() {
    return this.localDisplayValue;
  }

  public String getServerDisplayValue() {
    return this.serverDisplayValue;
  }

}