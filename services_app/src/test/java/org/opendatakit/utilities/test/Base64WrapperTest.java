package org.opendatakit.utilities.test;

import junit.framework.TestCase;

import org.junit.Test;
import org.opendatakit.services.utilities.Base64Wrapper;

public class Base64WrapperTest extends TestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Test(expected=ClassNotFoundException.class)
    public void testWrapperCreation() {
        try {
            Base64Wrapper wrapper = new Base64Wrapper("unittestTMP");
        } catch (ClassNotFoundException e) {

        }
    }
}
