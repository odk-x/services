package org.opendatakit.database.service;

import android.support.test.runner.AndroidJUnit4;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertTrue;

@RunWith(AndroidJUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicTest {

    @Test
    public void testFailureUsedToVerifyBuildsCatchWhenSet() {
        //assertTrue(false);
        assertTrue(true);
    }


}
