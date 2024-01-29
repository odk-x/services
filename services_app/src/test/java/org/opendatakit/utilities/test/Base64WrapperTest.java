package org.opendatakit.utilities.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.opendatakit.services.utilities.Base64Wrapper;

@RunWith(JUnit4.class)
public class Base64WrapperTest {

    @Test
    public void testWrapperCreation() throws ClassNotFoundException {
        Base64Wrapper wrapper = new Base64Wrapper("unittestTMP");
    }

}
