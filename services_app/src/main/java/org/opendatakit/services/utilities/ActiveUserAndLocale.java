/*
 * Copyright (C) 2016 University of Washington
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

import android.content.Context;

import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;

/**
 * @author mitchellsundt@gmail.com
 */
public class ActiveUserAndLocale {
  public final String activeUser;
  public final String rolesList;
  public final String locale;

  private ActiveUserAndLocale(String activeUser, String rolesList, String locale) {
    this.activeUser = activeUser;
    this.rolesList = rolesList;
    this.locale = locale;
  }

  public static ActiveUserAndLocale getActiveUserAndLocale(Context context, String appName) {

    PropertiesSingleton props = CommonToolProperties.get(context, appName);

    String activeUser = ODKServicesPropertyUtils.getActiveUser(props);

    return new ActiveUserAndLocale(activeUser,
        props.getProperty(CommonToolProperties.KEY_ROLES_LIST),
        props.getUserSelectedDefaultLocale());
  }
}
