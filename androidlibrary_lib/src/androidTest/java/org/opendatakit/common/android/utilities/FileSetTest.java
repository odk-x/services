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

import android.test.AndroidTestCase;

import com.fasterxml.jackson.core.JsonGenerationException;
import org.opendatakit.common.android.utilities.StaticStateManipulator;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.common.desktop.WebLoggerDesktopFactoryImpl;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class FileSetTest extends AndroidTestCase {
  private static final String APP_NAME = "fileSetTest";
  private static final String TABLE_ID_1 = "myTableId_1";
  private static final String TABLE_ID_2 = "myTableId_2";
  private static final String INSTANCE_ID_1 = "myInstanceId_1";
  private static final String INSTANCE_ID_2 = "myInstanceId_2";
  private static final String INSTANCE_FILENAME = "submission.xml";
  private static final String FILENAME_1 = "foo.jpg";
  private static final String FILENAME_2 = "bar.wav";

  private static final String MIME_1 = "image/jpg";
  private static final String MIME_2 = "audio/wav";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    StaticStateManipulator.get().reset();
    WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
  }

  public void testFileSetSerialization() throws IOException {
    FileSet fileSet = new FileSet("fileSetTest");

    String firstDir = ODKFileUtils.getInstanceFolder(APP_NAME, TABLE_ID_1, INSTANCE_ID_1);
    File instanceFilename = new File(firstDir, INSTANCE_FILENAME);
    File attachment1 = new File(firstDir, FILENAME_1);
    File attachment2 = new File(firstDir, FILENAME_2);

    fileSet.instanceFile = instanceFilename;
    fileSet.addAttachmentFile(attachment1, MIME_1);
    fileSet.addAttachmentFile(attachment2, MIME_2);

    String value = fileSet.serializeUriFragmentList(getContext());

    ByteArrayInputStream bis = new ByteArrayInputStream(value.getBytes(Charset.forName("UTF-8")));
    FileSet outSet = FileSet.parse(getContext(), APP_NAME, bis);

    assertEquals( fileSet.instanceFile, outSet.instanceFile);
    assertEquals( fileSet.attachmentFiles.size(), outSet.attachmentFiles.size());
    assertEquals( fileSet.attachmentFiles.get(0).contentType,
        outSet.attachmentFiles.get(0).contentType);
    assertEquals( fileSet.attachmentFiles.get(0).file, outSet.attachmentFiles.get(0).file);
    assertEquals( fileSet.attachmentFiles.get(1).contentType,
        outSet.attachmentFiles.get(1).contentType);
    assertEquals( fileSet.attachmentFiles.get(1).file, outSet.attachmentFiles.get(1).file);
  }
}
