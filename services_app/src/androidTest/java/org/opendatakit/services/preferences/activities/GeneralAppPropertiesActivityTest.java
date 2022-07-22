package org.opendatakit.services.preferences.activities;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
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
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(0, hasDescendant(withText(R.string.opendatakit_website)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 0),
                isDisplayed())).check(matches(withText(R.string.click_to_web)));
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(0,
                        click()));
    }


    @Test
    public void checkIfServerSettingScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(1, hasDescendant(withText(R.string.server)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 1),
                isDisplayed())).check(matches(withText(R.string.server_settings_summary)));
    }


    @Test
    public void checkIfDeviceSettingScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(2, hasDescendant(withText(R.string.device)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(R.string.device_settings_summary)));
    }

    @Test
    public void checkIfTableSpecificSettingScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(3, hasDescendant(withText(R.string.tool_tables_settings)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 3),
                isDisplayed())).check(matches(withText(R.string.tool_tables_settings_summary)));
    }

    @Test
    public void checkIfEnableUserRestrictionScreen_isVisible() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(4, hasDescendant(withText(R.string.enable_admin_password)))));

        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 4),
                isDisplayed())).check(matches(withText(R.string.admin_password_disabled)));
    }

    @Test
    public void whenResetConfigurationScreenIsClicked_launchResetConfigurationDialog() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(5, hasDescendant(withText(R.string.clear_configuration_settings)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 5),
                isDisplayed())).check(matches(withText(R.string.click_to_clear_settings)));

        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(5,
                        click()));
        onView(withText(R.string.reset_settings))
                .inRoot(isDialog())
                .check(matches(isDisplayed()));

        onView(allOf(withId(android.R.id.button1), withText("OK"))).perform(click());
    }

    @Test
    public void whenVerifyUserPermissionScreenIsClicked_launchVerifyServerSettingsActivity() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(6, hasDescendant(withText(R.string.verify_server_settings_start)))));
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 6),
                isDisplayed())).check(matches(withText(R.string.click_to_verify_server_settings)));
    }

    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), AppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        return intent;
    }

}
