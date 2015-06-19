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
package org.opendatakit.sync.service;

import org.opendatakit.sync.service.SyncStatus;
import org.opendatakit.sync.service.SyncProgressState;

interface OdkSyncServiceInterface {

	SyncStatus getSyncStatus(in String appName);
	
	boolean synchronize(in String appName, in boolean deferInstanceAttachments);
	
	boolean push(in String appName);
	
	SyncProgressState getSyncProgress(in String appName);
	
	String getSyncUpdateMessage(in String appName);
}
