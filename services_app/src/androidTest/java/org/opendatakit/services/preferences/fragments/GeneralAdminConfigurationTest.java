package org.opendatakit.services.preferences.fragments;

import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.action.ViewActions.replaceText;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.RootMatchers.isDialog;
import static androidx.test.espresso.matcher.ViewMatchers.hasDescendant;
import static androidx.test.espresso.matcher.ViewMatchers.isChecked;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isNotChecked;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;

import static org.hamcrest.Matchers.allOf;
import static org.opendatakit.utilities.ViewMatchers.childAtPosition;

import android.content.Intent;

import org.junit.Test;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.services.R;

import androidx.test.espresso.action.ViewActions;
import androidx.test.espresso.contrib.RecyclerViewActions;

import org.opendatakit.BaseUITest;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;

public class GeneralAdminConfigurationTest extends BaseUITest<AppPropertiesActivity> {

@Override
protected void setUpPostLaunch(){
        activityScenario.onActivity(activity->{
        PropertiesSingleton props=activity.getProps();
        assertThat(props).isNotNull();
        });

        onView(withId(R.id.app_properties_content)).check(matches(isDisplayed()));
        onView(withId(androidx.preference.R.id.recycler_view))
        .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.enable_admin_password)),
        click()));
        }

        @Test
        public void whenEnableUserRestrictionIsClicked_enterAdminPassword(){
            onView(withId(androidx.preference.R.id.recycler_view))
                    .perform(RecyclerViewActions.actionOnItemAtPosition(1,
                            click()));
            onView(withText(R.string.change_admin_password))
                    .inRoot(isDialog())
                    .check(matches(isDisplayed()));
            onView(withId(R.id.pwd_field)).perform(replaceText(TEST_PASSWORD));
            onView(withId(R.id.positive_button)).perform(ViewActions.click());
            onView(allOf(withId(android.R.id.summary),
                    childAtPosition(withId(androidx.preference.R.id.recycler_view), 1),
                    isDisplayed())).check(matches(withText(R.string.admin_password_settings_summary)));
        }
    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), AppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        return intent;
    }

}
