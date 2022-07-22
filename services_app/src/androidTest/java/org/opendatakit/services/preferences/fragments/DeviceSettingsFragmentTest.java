package org.opendatakit.services.preferences.fragments;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.hasDescendant;
import static androidx.test.espresso.matcher.ViewMatchers.isClickable;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;
import static org.hamcrest.Matchers.not;

import android.content.Intent;

import androidx.test.espresso.contrib.RecyclerViewActions;

import org.junit.Test;
import org.opendatakit.BaseUITest;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;

public class DeviceSettingsFragmentTest extends BaseUITest<AppPropertiesActivity> {

    @Override
    protected void setUpPostLaunch() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();
        });

        onView(withId(R.id.app_properties_content)).check(matches(isDisplayed()));
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.device)),
                        click()));

    }

    @Test
    public void whenShowSplashScreenIsClicked_selectSplashScreen() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(3, hasDescendant(withText(R.string.show_splash)))));
        onView(withId(android.R.id.checkbox)).perform(click(), setChecked(true));
        onView(withText(R.string.splash_path)).check(matches(not(isClickable())));
    }

    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), AppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        return intent;
    }

}