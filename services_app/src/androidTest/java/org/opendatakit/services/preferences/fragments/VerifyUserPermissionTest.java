package org.opendatakit.services.preferences.fragments;

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

import android.content.Intent;

import androidx.test.espresso.contrib.RecyclerViewActions;

import org.junit.Ignore;
import org.junit.Test;
import org.opendatakit.BaseUITest;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;
import org.opendatakit.services.sync.actions.activities.VerifyServerSettingsActivity;

public class VerifyUserPermissionTest extends BaseUITest<AppPropertiesActivity> {

    @Override
    protected void setUpPostLaunch() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();
        });
        onView(withId(R.id.app_properties_content)).check(matches(isDisplayed()));

    }

    @Test
    public void whenVerifyUserPermissionScreenIsClicked_launchVerifyServerSettingsActivity() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.verify_server_settings_header)),
                        click()));
        intended(hasComponent(VerifyServerSettingsActivity.class.getName()));
    }

    @Test
    public void whenVerifyUserPermissionIsClicked_configureServerUrl() {
        resetConfiguration();
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.verify_server_settings_header)),
                        click()));
        onView(withText(R.string.configure_server_settings))
                .inRoot(isDialog())
                .check(matches(isDisplayed()));
        onView(allOf(withId(android.R.id.button1), withText(R.string.yes))).perform(click());
        intended(hasComponent(VerifyServerSettingsActivity.class.getName()));
    }

    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), AppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        return intent;
    }

}
