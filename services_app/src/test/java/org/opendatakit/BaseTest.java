package org.opendatakit;

import android.content.Context;
import android.widget.ArrayAdapter;

import androidx.test.platform.app.InstrumentationRegistry;

import org.opendatakit.services.resolve.listener.ResolutionListener;
import org.opendatakit.services.resolve.views.components.ResolveRowEntry;

public abstract class BaseTest {

    protected final static String APP_NAME = "testAppName";
    protected final static String TABLE_ID = "11";
    protected final static String RESOLVING_ROWS = "Resolving row 1 of 0";
    protected final static String USER_NAME = "testUser";
    protected final static String TEST_SERVER_URL = "https://testUrl.com";
    protected final static String LAST_SYNC_TIME = "2022-03-19 12:30:02";
    protected final static String TEST_VERIFY_TYPE = "server";

    protected ResolutionListener resolutionListener;
    protected ArrayAdapter<ResolveRowEntry> mAdapter;
    protected String mProgress;

    protected Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }
}
