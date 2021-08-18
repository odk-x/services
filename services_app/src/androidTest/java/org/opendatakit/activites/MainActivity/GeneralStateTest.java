package org.opendatakit.activites.MainActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.isNotEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import android.content.Context;

import androidx.test.espresso.ViewInteraction;
import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.intent.matcher.IntentMatchers;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.runner.AndroidJUnit4;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;
import org.opendatakit.utilities.StaticStateManipulator;

@RunWith(AndroidJUnit4.class)
public class GeneralStateTest {

    private static final String APP_NAME = "MainActivityTest";

    @Rule
    public ActivityScenarioRule<MainActivity> mainActivityScenarioRule = new ActivityScenarioRule<>(MainActivity.class);

    @Before
    public void setUp() {
        StaticStateManipulator.get().reset();

//        onView(withId(android.R.id.button2)).perform(ViewActions.click());
    }

    @Test
    public void checkToolbarVerifyBtnClick(){
        Intents.init();

        onView(withId(R.id.action_verify_server_settings))
                .perform(ViewActions.click());

        Intents.intended(IntentMatchers.hasComponent(VerifyServerSettingsActivity.class.getName()));

        Intents.release();
    }

    @Test
    public void checkToolbarSettingsBtnClick(){
        Intents.init();

        onView(withId(R.id.action_settings))
                .perform(ViewActions.click());

        Intents.intended(IntentMatchers.hasComponent(AppPropertiesActivity.class.getName()));

        Intents.release();
    }

    @Test
    public void checkDrawerSettingsBtnClick(){
        Intents.init();

        onView(withId(R.id.btnDrawerOpen))
                .perform(ViewActions.click());

        onView(withId(R.id.drawer_settings))
                .perform(ViewActions.click());

        Intents.intended(IntentMatchers.hasComponent(AppPropertiesActivity.class.getName()));

        Intents.release();
    }

    @Test
    public void checkDrawerAboutUsBtnClick(){
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());

        ViewInteraction btnAboutUs = onView(withId(R.id.drawer_about_us));
        btnAboutUs.check(matches(isEnabled()));

        btnAboutUs.perform(ViewActions.click());

        onView(withId(org.opendatakit.androidlibrary.R.id.versionText)).check(matches(isDisplayed()));
        btnAboutUs.check(matches(isNotEnabled()));
    }

}
