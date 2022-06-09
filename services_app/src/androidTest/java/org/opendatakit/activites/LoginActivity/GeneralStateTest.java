package org.opendatakit.activites.LoginActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.doesNotExist;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;

import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.intent.Intents;
import androidx.test.espresso.intent.matcher.IntentMatchers;
import androidx.test.espresso.matcher.RootMatchers;

import org.junit.Test;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;

import java.util.Collections;

public class GeneralStateTest extends BaseLoginActivity {
    @Test
    public void verifyValuesTest() {
        onView(withId(R.id.tvTitleLogin)).check(matches(withText(getContext().getString(R.string.drawer_sign_in_button_text))));
        onView(withId(R.id.btnAnonymousSignInLogin)).check(matches(withText(R.string.anonymous_user)));
        onView(withId(R.id.btnUserSignInLogin)).check(matches(withText(R.string.authenticated_user)));
        onView(withId(R.id.btnAnonymousSignInLogin)).check(matches(isEnabled()));
        onView(withId(R.id.btnUserSignInLogin)).check(matches(isEnabled()));
    }

    @Test
    public void verifyVisibilityTest() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_update_credentials)).check(doesNotExist());
        onView(withId(R.id.drawer_switch_sign_in_type)).check(doesNotExist());
    }

    @Test
    public void checkFirstStartupTest() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = CommonToolProperties.get(activity, activity.getAppName());
            assertThat(props).isNotNull();

            props.setProperties(Collections.singletonMap(CommonToolProperties.KEY_FIRST_LAUNCH, "true"));
            activity.recreate();
        });

        onView(withId(android.R.id.button1)).inRoot(RootMatchers.isDialog()).perform(ViewActions.click());

        onView(withId(R.id.inputServerUrl)).check(matches(isDisplayed()));
        onView(withId(R.id.inputTextServerUrl)).check(matches(withText(TEST_SERVER_URL)));
    }

    @Test
    public void checkDrawerServerLoginTest() {
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_server_login)).perform(ViewActions.click());

        onView(withId(R.id.inputServerUrl)).check(matches(isDisplayed()));
        onView(withId(R.id.inputTextServerUrl)).check(matches(withText(TEST_SERVER_URL)));
    }

    @Test
    public void checkToolbarSettingsButtonClick() {
        Intents.init();
        onView(withId(R.id.action_settings)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(AppPropertiesActivity.class.getName()));
        Intents.release();
    }

    @Test
    public void checkDrawerSettingsClick() {
        Intents.init();
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.drawer_settings)).perform(ViewActions.click());
        Intents.intended(IntentMatchers.hasComponent(AppPropertiesActivity.class.getName()));
        Intents.release();
    }
}
