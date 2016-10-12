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

package org.opendatakit.services.sync.service.logic;

import org.opendatakit.aggregate.odktables.rest.entity.OdkTablesFileManifestEntry;

import java.util.List;

/**
 * Holds the file manifest list and the eTag of that document. Allows the fetching of the
 * list to be separated from the updating of the fetch eTag, eliminating a possible failure\
 * mode where the sync force closes before all the files are processed and which could (previously)
 * have caused the device to consider itself successfully sync'd but might be missing one or
 * more configuration files.
 *
 * @author mitchellsundt@gmail.com
 */
public class FileManifestDocument {

  public final String eTag;
  public final List<OdkTablesFileManifestEntry> entries;

  public FileManifestDocument(String eTag, List<OdkTablesFileManifestEntry> entries ) {
    this.eTag = eTag;
    this.entries = entries;
  }
}
