package org.opendatakit.services.preferences.fragments;


import static androidx.test.espresso.Espresso.onView;
import static androidx.test.espresso.action.ViewActions.click;
import static androidx.test.espresso.assertion.ViewAssertions.matches;
import static androidx.test.espresso.matcher.ViewMatchers.hasDescendant;
import static androidx.test.espresso.matcher.ViewMatchers.isClickable;
import static androidx.test.espresso.matcher.ViewMatchers.isDisplayed;
import static androidx.test.espresso.matcher.ViewMatchers.isEnabled;
import static androidx.test.espresso.matcher.ViewMatchers.isRoot;
import static androidx.test.espresso.matcher.ViewMatchers.withId;
import static androidx.test.espresso.matcher.ViewMatchers.withText;
import static com.google.common.truth.Truth.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.not;
import static org.opendatakit.utilities.ViewMatchers.childAtPosition;

import android.content.Intent;

import androidx.test.espresso.Espresso;
import androidx.test.espresso.contrib.RecyclerViewActions;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.opendatakit.BaseUITest;
import org.opendatakit.consts.IntentConsts;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.services.R;
import org.opendatakit.services.preferences.activities.AppPropertiesActivity;

public class AdminConfigurableTablesSettingsFragmentTest extends BaseUITest<AppPropertiesActivity> {

    @Override
    protected void setUpPostLaunch() {
        activityScenario.onActivity(activity -> {
            PropertiesSingleton props = activity.getProps();
            assertThat(props).isNotNull();
        });
        enableAdminMode();
        Espresso.pressBack();
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.admin_tool_tables_settings)),
                        click()));
    }

    @Test
    public void whenTableSpecificSettingsIsChangedInAdminMode_checkIfTableSettings_isDisabledInNonAdminMode() {
        setCheckboxValue(false);
        Espresso.pressBack();
        launchTableServerSettingPreferenceScreen();

        onView(allOf(withId(android.R.id.title),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 0)))
                .check(matches(withText(R.string.tool_tables_restrictions_apply)));

        onView(allOf(withId(android.R.id.title),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 1)))
                .check(matches(not(isClickable())));

    }

    @Test
    public void whenTableSpecificSettingsIsChangedInAdminMode_checkIfTableSettings_isEnabledInNonAdminMode() {
        setCheckboxValue(true);
        Espresso.pressBack();
        launchTableServerSettingPreferenceScreen();

        onView(allOf(withId(android.R.id.title),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 0)))
                .check(matches(withText(R.string.tool_tables_settings)));

        onView(allOf(withId(android.R.id.title),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 1)))
                .check(matches(isEnabled()));
    }

    @After
    public void after() {
        resetConfiguration();
    }

    @Override
    protected Intent getLaunchIntent() {
        Intent intent = new Intent(getContext(), AppPropertiesActivity.class);
        intent.putExtra(IntentConsts.INTENT_KEY_APP_NAME, APP_NAME);
        intent.putExtra(IntentConsts.INTENT_KEY_SETTINGS_IN_ADMIN_MODE, true);
        return intent;
    }

    private void launchTableServerSettingPreferenceScreen() {
        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.exit_admin_mode)),
                        click()));

        onView(isRoot()).perform(waitFor(1000));

        onView(withId(androidx.preference.R.id.recycler_view))
                .perform(RecyclerViewActions.actionOnItem(hasDescendant(withText(R.string.odkx_tables)),
                        click()));
    }

    public void setCheckboxValue(boolean checked) {
        onView(allOf(withId(android.R.id.checkbox),
                childAtPosition(withId(androidx.preference.R.id.recycler_view), 1),
                isDisplayed())).perform(setChecked(checked));
    }


}
