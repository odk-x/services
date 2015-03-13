package org.opendatakit.common.android.utilities;

import java.util.Locale;
import java.util.TimeZone;

import android.test.AndroidTestCase;

public class DataUtilTest extends AndroidTestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }
  
  public void testDateInterpretation() {
    TimeZone tz = TimeZone.getTimeZone(TimeZone.getAvailableIDs()[0]);
    DataUtil util = new DataUtil(Locale.US, tz);
    
    String value = util.validifyDateValue("3/4/2015");
    
    String expected = "2015-03-04T";
    assertEquals(expected, value.substring(0,expected.length()));
  }

}
