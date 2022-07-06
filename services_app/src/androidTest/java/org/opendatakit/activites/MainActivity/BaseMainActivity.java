package org.opendatakit.activites.MainActivity;

import static com.google.common.truth.Truth.assertThat;

import android.content.Context;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.intent.Intents;
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
    protected static final String SERVER_URL = "https://tables-demo.odk-x.org";

    @After
    public void clearTestEnvironment() {
      activityScenario.close();
      Intents.release();
    }

    protected Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

}