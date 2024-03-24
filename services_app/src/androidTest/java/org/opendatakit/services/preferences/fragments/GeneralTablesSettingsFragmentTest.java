package org.opendatakit.services.preferences.fragments;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.hasDescendant;
import static androidx.test.espresso.matcher.ViewMatchers.isChecked;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isNotChecked;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;

import static org.hamcrest.Matchers.not;

import android.content.Intent;

import androidx.test.espresso.contrib.RecyclerViewActions;

import junit.framework.AssertionFailedError;

import org.junit.Ignore;
import org.junit.Test;
import org.opendatakit.BaseUITest;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;

public class GeneralTablesSettingsFragmentTest extends BaseUITest<AppPropertiesActivity> {

    @Override
    protected void setUpPostLaunch() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();
        });

        onView(withId(R.id.app_properties_content)).check(matches(isDisplayed()));
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.odkx_tables)),
                        click()));
    }

    @Test
    public void whenTableSpecificSettingIsClicked_useCustomHomeScreen() {
        onView(withText(R.string.custom_home_screen)).check(matches(isDisplayed()));

        onView(withId(android.R.id.checkbox)).perform(click(), setChecked(true));
        onView(withId(android.R.id.checkbox)).check(matches(isChecked()));

        onView(withId(android.R.id.checkbox)).perform(click(), setChecked(false));
        onView(withId(android.R.id.checkbox)).check(matches(not(isChecked())));
    }

    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), AppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        return intent;
    }

}