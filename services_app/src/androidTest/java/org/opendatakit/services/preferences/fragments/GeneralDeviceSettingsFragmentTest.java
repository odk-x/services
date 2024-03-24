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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.not;
import static org.opendatakit.utilities.ViewMatchers.childAtPosition;

import android.content.Intent;

import androidx.test.espresso.contrib.RecyclerViewActions;

import org.junit.Ignore;
import org.junit.Test;
import org.opendatakit.BaseUITest;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;

public class GeneralDeviceSettingsFragmentTest extends BaseUITest<AppPropertiesActivity> {

    @Override
    protected void setUpPostLaunch() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();
        });

        onView(withId(R.id.app_properties_content)).check(matches(isDisplayed()));
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.preferences)),
                        click()));

    }

    @Test
    public void whenTextFontSizeIsClicked_doChangeFontSize_checkIfSizeIsExtraLarge() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .check(matches(atPosition(2, hasDescendant(withText(R.string.font_size)))));
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(2,
                        click()));
        onView(withText(FONT_SIZE_XL)).perform(click());
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(FONT_SIZE_XL)));
    }
    @Test
    public void whenTextFontSizeIsClicked_doChangeFontSize_checkIfSizeIsLarge() {

        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(2,
                        click()));
        onView(withText(FONT_SIZE_L)).perform(click());
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(FONT_SIZE_L)));
    }
    @Test
    public void whenTextFontSizeIsClicked_doChangeFontSize_checkIfSizeIsMedium() {

        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(2,
                        click()));
        onView(withText(FONT_SIZE_M)).perform(click());
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(FONT_SIZE_M)));
    }
    @Test
    public void whenTextFontSizeIsClicked_doChangeFontSize_checkIfSizeIsSmall() {

        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(2,
                        click()));
        onView(withText(FONT_SIZE_S)).perform(click());
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(FONT_SIZE_S)));
    }
    @Test
    public void whenTextFontSizeIsClicked_doChangeFontSize_checkIfSizeIsExtraSmall() {

        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItemAtPosition(2,
                        click()));
        onView(withText(FONT_SIZE_XS)).perform(click());
        onView(allOf(withId(android.R.id.summary),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 2),
                isDisplayed())).check(matches(withText(FONT_SIZE_XS)));
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