package org.opendatakit.utilities.test;

import android.support.test.runner.AndroidJUnit4;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.services.utilities.Base64Wrapper;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(AndroidJUnit4.class)
public class Base64WrapperTest {

  private static Base64Wrapper wrapper;

  @Before
  public void setUp() throws Throwable {
    wrapper = new Base64Wrapper("some app name");
  }


  @Test
  public void testEncode() throws Throwable {
    String encoded = wrapper
        .encodeToString(new byte[] { (byte) 'a', (byte) 'b', (byte) 'c', (byte) '\n' });
    assertEquals(encoded, "YWJjCg==");
  }

  @Test
  public void testDecode() throws Throwable {
    byte[] decoded = wrapper.decode("YWJjCg==");
    assertArrayEquals(decoded, new byte[] {97, 98, 99, 10}); // "abc\n"
  }
}
