package org.opendatakit.activites.SyncActivity;

import static com.google.common.truth.Truth.assertThat;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.intent.Intents;
import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.SyncActivity;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;

import java.util.Map;

public abstract class BaseSyncActivity {

    protected static ActivityScenario<SyncActivity> activityScenario;
    protected static final String TEST_SERVER_URL= "https://testUrl.com";

    @After
    public void clearTestEnvironment() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(
                    activity.getString(org.opendatakit.androidlibrary.R.string.default_sync_server_url)
            );
            assertThat(serverProperties).isNotNull();
            serverProperties.put(CommonToolProperties.KEY_FIRST_LAUNCH,"true");
            serverProperties.put(CommonToolProperties.KEY_SYNC_ATTACHMENT_STATE, null);
            props.setProperties(serverProperties);
        });
        activityScenario.close();
        Intents.release();
    }

    protected Activity getActivity() {
        final Activity[] activity1 = new Activity[1];
        activityScenario.onActivity(activity -> activity1[0] =activity);
        return activity1[0];
    }

    protected Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }
}