package org.opendatakit;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.intent.Intents;
import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.junit.Before;

public abstract class BaseUITest<T extends Activity> {
    protected final static String APP_NAME = "testAppName";
    protected final static String TEST_SERVER_URL = "https://testUrl.com";
    protected final static String USER_NAME = "testUser";
    protected ActivityScenario<T> activityScenario;

    @Before
    public void setUp() {
        activityScenario = ActivityScenario.launch(getLaunchIntent());
        setUpPostLaunch();
        Intents.init();
    }

    protected abstract void setUpPostLaunch();
    protected abstract Intent getLaunchIntent();

    @After
    public void tearDown() throws Exception {
        if (activityScenario != null) activityScenario.close();
        Intents.release();
    }

    protected Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }
}
