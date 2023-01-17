/*
 * Copyright (C) 2017 University of Washington
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

import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;

import java.util.HashMap;
import java.util.Map;

/**
 * Isolates the interactions with the secure properties within ODK Services
 * so as to make it more obvious to others that the way to access these properties
 * from outside of ODK Services is via the database API.
 *
 * @author mitchellsundt@gmail.com
 */

public class ODKServicesPropertyUtils {

   public static void clearActiveUser(PropertiesSingleton props) {
      Map<String,String> properties = new HashMap<String,String>();
      properties.put(CommonToolProperties.KEY_ROLES_LIST, "");
      properties.put(CommonToolProperties.KEY_DEFAULT_GROUP, "");
      properties.put(CommonToolProperties.KEY_USERS_LIST, "");

      properties.put(CommonToolProperties.KEY_USERNAME, "");
      properties.put(CommonToolProperties.KEY_PASSWORD, "");

      properties.put(CommonToolProperties.KEY_IS_USER_AUTHENTICATED,"false");
      properties.put(CommonToolProperties.KEY_CURRENT_USER_STATE, UserState.LOGGED_OUT.name());

      props.setProperties(properties);
   }

   public static String getActiveUser(PropertiesSingleton props) {
      String activeUserName = null;
      String authType = props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE);
      if (authType.equals(props.CREDENTIAL_TYPE_NONE)) {
         activeUserName = CommonToolProperties.ANONYMOUS_USER;
      } else if (authType.equals(props.CREDENTIAL_TYPE_USERNAME_PASSWORD)) {
         String name = props.getProperty(CommonToolProperties.KEY_USERNAME);
         String user_id = props.getProperty(CommonToolProperties.KEY_AUTHENTICATED_USER_ID);
         String roles = props.getProperty(CommonToolProperties.KEY_ROLES_LIST);
         if (name != null && name.length() != 0 &&
             user_id != null && user_id.length() != 0 &&
             roles != null && roles.length() != 0) {
            activeUserName = user_id;
         } else {
            activeUserName = CommonToolProperties.ANONYMOUS_USER;
         }
      } else {
         throw new IllegalStateException("unexpected authentication type!");
      }
      return activeUserName;
   }

}
