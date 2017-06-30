package org.opendatakit.services.submissions.provider;

import android.net.Uri;
import android.support.test.runner.AndroidJUnit4;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by Niles on 6/29/17.
 */

@RunWith(AndroidJUnit4.class)
public class SubmissionProviderTest {
  @Before
  public void setUp() {

  }

  @Test(expected = IllegalArgumentException.class)
  public void testOpenFileBadMode() throws Throwable {
    new SubmissionProvider().openFile(new Uri.Builder().authority("unused").build(), "w");
  }

}
