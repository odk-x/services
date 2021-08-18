package org.opendatakit.activites.MainActivity;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.assertion.ViewAssertions.doesNotExist;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withEffectiveVisibility;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;

import android.content.Context;

import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.matcher.ViewMatchers;
import androidx.test.ext.junit.rules.ActivityScenarioRule;
import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.MainActivity;
import org.opendatakit.services.R;
import org.opendatakit.utilities.StaticStateManipulator;

public class AnonymousStateTest {

    private static final String APP_NAME = "MainActivityTest";

    @Rule
    public ActivityScenarioRule<MainActivity> mainActivityScenarioRule = new ActivityScenarioRule<>(MainActivity.class);

    @Before
    public void setUp() {
        StaticStateManipulator.get().reset();
    }

    @Test
    public void verifyVisibilityTest() {
        onView(withId(R.id.btnSignInMain)).perform(ViewActions.click());
        onView(withId(R.id.btnAnonymousSignInLogin)).perform(ViewActions.click());
        onView(withId(android.R.id.button2)).perform(ViewActions.click());

        onView(withId(R.id.action_sync)).check(matches(isDisplayed()));

        onView(withId(R.id.drawer_resolve_conflict)).check(matches(isDisplayed()));
        onView(withId(R.id.drawer_switch_sign_in_type)).check(matches(isDisplayed()));
        onView(withId(R.id.drawer_update_credentials)).check(doesNotExist());

        onView(withId(R.id.tvUsernameMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
        onView(withId(R.id.tvLastSyncTimeMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.VISIBLE)));
        onView(withId(R.id.btnSignInMain)).check(matches(withEffectiveVisibility(ViewMatchers.Visibility.GONE)));
    }

    @Test
    public void verifyValuesTest() {
        onView(withId(R.id.tvUserStateMain))
                .check(matches(withText(getContext().getString(R.string.anonymous_user))));

        onView(withId(R.id.btnDrawerLogin))
                .check(matches(withText(getContext().getString(R.string.drawer_sign_out_button_text))));
    }

    @After
    public void clearProperties(){
        onView(withId(R.id.btnDrawerOpen)).perform(ViewActions.click());
        onView(withId(R.id.btnDrawerLogin)).perform(ViewActions.click());
    }

    private Context getContext(){
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

    private PropertiesSingleton getProps(){
        return CommonToolProperties.get(getContext(), APP_NAME);
    }

}
