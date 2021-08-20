package org.opendatakit.activites.MainActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.isNotEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;

import androidx.test.espresso.ViewInteraction;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.intent.matcher.IntentMatchers;
import androidx.test.ext.junit.rules.ActivityScenarioRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.services.sync.actions.fragments.UpdateServerSettingsFragment;

import java.util.Collections;
import java.util.Map;

public class GeneralStateTest {

    @Rule
    public ActivityScenarioRule<MainActivity> mainActivityScenarioRule = new ActivityScenarioRule<>(MainActivity.class);

    private final String TEST_SERVER_URL = "https://testUrl.com";

    @Before
    public void setUp() {
        onView(withId(android.R.id.button2)).perform(ViewActions.click());
        mainActivityScenarioRule.getScenario().onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(TEST_SERVER_URL);
            assertThat(serverProperties).isNotNull();
            props.setProperties(serverProperties);

            activity.updateViewModelWithProps();
        });
    }

    @Test
    public void checkFirstStartupTest() {
        mainActivityScenarioRule.getScenario().onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            assertThat(props).isNotNull();

            props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FIRST_LAUNCH, "true"));
            activity.recreate();
        });

        onView(withId(android.R.id.button1)).perform(ViewActions.click());

        onView(withId(R.id.inputServerUrl)).check(matches(isDisplayed()));
        onView(withId(R.id.inputTextServerUrl)).check(matches(withText(TEST_SERVER_URL)));
    }

    @Test
    public void checkDefaultValuesTest() {
        onView(withId(R.id.tvServerUrlMain)).check(matches(withText(TEST_SERVER_URL)));
    }

    @Test
    public void checkToolbarVerifyBtnClick() {
        Intents.init();
        onView(withId(R.id.action_verify_server_settings)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(VerifyServerSettingsActivity.class.getName()));
        Intents.release();
    }

    @Test
    public void checkToolbarSettingsBtnClick() {
        Intents.init();
        onView(withId(R.id.action_settings)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(AppPropertiesActivity.class.getName()));
        Intents.release();
    }

    @Test
    public void checkDrawerSettingsBtnClick() {
        Intents.init();
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_settings)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(AppPropertiesActivity.class.getName()));
        Intents.release();
    }

    @Test
    public void checkDrawerAboutUsBtnClick() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());

        ViewInteraction btnAboutUs = onView(withId(R.id.drawer_about_us));
        btnAboutUs.check(matches(isEnabled()));

        btnAboutUs.perform(ViewActions.click());

        onView(withId(org.opendatakit.androidlibrary.R.id.versionText)).check(matches(isDisplayed()));
        btnAboutUs.check(matches(isNotEnabled()));
    }

    @After
    public void clearTestEnvironment() {
        mainActivityScenarioRule.getScenario().onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            assertThat(props).isNotNull();

            Map<String, String> serverProperties = UpdateServerSettingsFragment.getUpdateUrlProperties(
                    activity.getString(org.opendatakit.androidlibrary.R.string.default_sync_server_url)
            );
            assertThat(serverProperties).isNotNull();
            serverProperties.put(CommonToolProperties.KEY_FIRST_LAUNCH,"true");
            props.setProperties(serverProperties);
        });
    }

}
