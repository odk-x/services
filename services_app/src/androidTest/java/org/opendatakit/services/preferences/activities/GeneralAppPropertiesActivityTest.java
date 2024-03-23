package org.opendatakit.services.preferences.activities;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.action.ViewActions.scrollTo;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.contrib.RecyclerViewActions.actionOnItemAtPosition;
import static androidx.test.espresso.intent.Intents.intended;
import static androidx.test.espresso.intent.matcher.IntentMatchers.hasComponent;
import static androidx.test.espresso.matcher.RootMatchers.isDialog;
import static androidx.test.espresso.matcher.ViewMatchers.hasDescendant;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.opendatakit.utilities.ViewMatchers.childAtPosition;

import android.content.Intent;

import androidx.test.espresso.contrib.RecyclerViewActions;

import org.junit.Ignore;
import org.junit.Test;
import org.opendatakit.BaseUITest;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;

public class GeneralAppPropertiesActivityTest extends BaseUITest<AppPropertiesActivity> {

    @Override
    protected void setUpPostLaunch() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();
        });
        onView(withId(R.id.app_properties_content)).check(matches(isDisplayed()));

    }


    @Test
    public void whenOpenDocumentationScreenIsClicked_launchUrl() {
        onView(withId(androidx.preference.R.id.recycler_view)).perform(actionOnItemAtPosition(7, scrollTo()))
                .check(matches(atPosition(7, hasDescendant(withText(R.string.user_documentation)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 7),
                isDisplayed())).check(matches(withText(R.string.visit_user_docs)));
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(7,
                        click()));
    }


    @Test
    public void checkIfServerSettingScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view)).perform(actionOnItemAtPosition(1, scrollTo()))
                .check(matches(atPosition(1, hasDescendant(withText(R.string.server)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 1),
                isDisplayed())).check(matches(withText(R.string.server_settings_summary)));
    }


    @Test
    public void checkIfDeviceSettingScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view)).perform(actionOnItemAtPosition(4, scrollTo()))
                .check(matches(atPosition(4, hasDescendant(withText(R.string.preferences)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 4),
                isDisplayed())).check(matches(withText(R.string.configure_device_settings)));
    }


    @Test
    public void checkIfTableSpecificSettingScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view)).perform(actionOnItemAtPosition(5, scrollTo()))
                .check(matches(atPosition(5, hasDescendant(withText(R.string.odkx_tables)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 5),
                isDisplayed())).check(matches(withText(R.string.tool_tables_settings_summary)));
    }


    @Test
    public void checkIfEnableUserRestrictionScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view)).perform(actionOnItemAtPosition(3, scrollTo()))
                .check(matches(atPosition(3, hasDescendant(withText(R.string.user_restrictions)))));

        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 3),
                isDisplayed())).check(matches(withText(R.string.enable_user_restrictions)));
    }


    @Test
    public void whenResetConfigurationScreenIsClicked_launchResetConfigurationDialog() {
        onView(withId(androidx.preference.R.id.recycler_view)).perform(actionOnItemAtPosition(6, scrollTo()))
                .check(matches(atPosition(6, hasDescendant(withText(R.string.clear_settings)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 6),
                isDisplayed())).check(matches(withText(R.string.reset_configuration)));

        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(6,
                        click()));
        onView(withText(R.string.reset_settings))
                .inRoot(isDialog())
                .check(matches(isDisplayed()));

        onView(allOf(withId(android.R.id.button1), withText("OK"))).perform(click());
    }


    @Test
    public void checkIfVerifyUserPermissionScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view)).perform(actionOnItemAtPosition(2, scrollTo()))
                .check(matches(atPosition(2, hasDescendant(withText(R.string.verify_server_settings_header)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(R.string.click_to_verify_server_settings)));
    }

    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), AppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        return intent;
    }

}
