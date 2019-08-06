package org.opendatakit.database.service;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertTrue;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicTest {

    @Test
    public void testFailureUsedToVerifyBuildsCatchWhenSet() {
        //assertTrue(false);
        assertTrue(true);
    }


}
