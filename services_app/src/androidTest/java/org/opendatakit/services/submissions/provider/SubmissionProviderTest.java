package org.opendatakit.services.submissions.provider;

import android.net.Uri;

import org.junit.Before;
import org.junit.Test;

/**
 * Created by Niles on 6/29/17.
 */

public class SubmissionProviderTest {
  private SubmissionProvider p;

  @Test
  public void testOnCreate() throws Exception {
    p.onCreate(); // should not throw an exception
  }

  @Test
  public void testOpenFile() throws Exception {
    // TODO not testing because it appears entirely unused
  }

  @Test
  public void testDelete() throws Exception {
    // TODO not testing because the method isn't implemented
  }

  @Test
  public void testGetType() throws Exception {
    // TODO not testing because the method isn't implemented
  }

  @Test
  public void testInsert() throws Exception {
    // TODO not testing because the method isn't implemented

  }

  @Test
  public void testQuery() throws Exception {
    // TODO not testing because the method isn't implemented

  }

  @Test
  public void testUpdate() throws Exception {
    // TODO not testing because the method isn't implemented
  }

  @Before
  public void setUp() {
    p = new SubmissionProvider();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOpenFileBadMode() throws Throwable {
    p.openFile(new Uri.Builder().authority("unused").build(), "w");
  }

}
