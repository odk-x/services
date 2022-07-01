package org.opendatakit.activites.VerifyServerSettingsActivity;

import static com.google.common.truth.Truth.assertThat;

import android.content.Context;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.intent.Intents;
import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;

import java.util.Map;

public abstract class BaseVerifyServerSettingActivity {
    protected static final String TEST_SERVER_URL = "https://testUrl.com";

    protected static ActivityScenario<VerifyServerSettingsActivity> activityScenario;

    @After
    public void clearTestEnvironment() {
        activityScenario.close();
        Intents.release();
    }

    protected Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

}
