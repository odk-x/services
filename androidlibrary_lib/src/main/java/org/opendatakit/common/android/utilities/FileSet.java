/*
 * Copyright (C) 2012-2013 University of Washington
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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.type.CollectionType;
import org.opendatakit.common.android.utilities.ODKFileUtils;

import android.content.Context;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * Holds the files required for a submission to the ODK Aggregate legacy
 * interface
 *
 * @author mitchellsundt@gmail.com
 */
public class FileSet {
  private static final String APPLICATION_XML = "application/xml";
  private static final String URI_FRAGMENT = "uriFragment";
  private static final String CONTENT_TYPE = "contentType";
  public File instanceFile = null;
  public final String appName;

  public static final class MimeFile {
    public File file;
    public String contentType;
  };

  public ArrayList<MimeFile> attachmentFiles = new ArrayList<MimeFile>();

  public FileSet(String appName) {
    this.appName = appName;
  }

  public void addAttachmentFile(File file, String contentType) {
    MimeFile f = new MimeFile();
    f.file = file;
    f.contentType = contentType;
    attachmentFiles.add(f);
  }

  public String serializeUriFragmentList(Context context) throws JsonGenerationException, JsonMappingException, IOException {
    ArrayList<HashMap<String, String>> str = new ArrayList<HashMap<String, String>>();

    HashMap<String, String> map;
    map = new HashMap<String, String>();
    map.put(URI_FRAGMENT, ODKFileUtils.asUriFragment(appName, instanceFile));
    map.put(CONTENT_TYPE, APPLICATION_XML);
    str.add(map);
    for (MimeFile f : attachmentFiles) {
      map = new HashMap<String, String>();
      map.put(URI_FRAGMENT, ODKFileUtils.asUriFragment(appName, f.file));
      map.put(CONTENT_TYPE, f.contentType);
      str.add(map);
    }

    String serializedString = ODKFileUtils.mapper.writeValueAsString(str);
    return serializedString;
  }

  public static final FileSet parse(Context context, String appName, InputStream src) throws JsonParseException,
      JsonMappingException, IOException {
    CollectionType javaType =
        ODKFileUtils.mapper.getTypeFactory().constructCollectionType(ArrayList.class, Map.class);
    ArrayList<Map> mapArrayList = null;
    try {
      mapArrayList = ODKFileUtils.mapper.readValue(src, javaType);
    } catch (JsonParseException e) {
      WebLogger.getLogger(appName).e("FileSet",
          "parse: problem parsing json list entry from the fileSet");
      WebLogger.getLogger(appName).printStackTrace(e);
    } catch (JsonMappingException e) {
      WebLogger.getLogger(appName).e("FileSet",
          "parse: problem mapping json list entry from the fileSet");
      WebLogger.getLogger(appName).printStackTrace(e);
    } catch (IOException e) {
      WebLogger.getLogger(appName).e("FileSet",
          "parse: i/o problem with json for list entry from the fileSet");
      WebLogger.getLogger(appName).printStackTrace(e);
    }

    FileSet fs = new FileSet(appName);
    String instanceRelativePath = (String) mapArrayList.get(0).get(URI_FRAGMENT);
    fs.instanceFile = ODKFileUtils.getAsFile(appName, instanceRelativePath);
    for (int i = 1; i < mapArrayList.size(); ++i) {
      String relativePath = (String) mapArrayList.get(i).get(URI_FRAGMENT);
      String contentType = (String) mapArrayList.get(i).get(CONTENT_TYPE);
      MimeFile f = new MimeFile();
      f.file = ODKFileUtils.getAsFile(appName, relativePath);
      f.contentType = contentType;
      fs.attachmentFiles.add(f);
    }
    return fs;
  }

}