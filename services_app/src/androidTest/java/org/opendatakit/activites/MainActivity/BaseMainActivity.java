package org.opendatakit.activites.MainActivity;

import static com.google.common.truth.Truth.assertThat;

import android.content.Context;

import androidx.test.core.app.ActivityScenario;
import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;

import java.util.Map;

public abstract class BaseMainActivity {
    protected static ActivityScenario<MainActivity> activityScenario;
    protected static final String TEST_SERVER_URL= "https://testUrl.com";

    @After
    public void clearTestEnvironment() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(
                    activity.getString(org.opendatakit.androidlibrary.R.string.default_sync_server_url)
            );
            assertThat(serverProperties).isNotNull();
            serverProperties.put(CommonToolProperties.KEY_FIRST_LAUNCH, "true");
            props.setProperties(serverProperties);
        });
    }

    protected Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

}