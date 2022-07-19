package org.opendatakit;

import static org.hamcrest.Matchers.isA;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.view.View;
import android.widget.Checkable;

import androidx.test.core.app.ActivityScenario;
import androidx.test.espresso.UiController;
import androidx.test.espresso.ViewAction;
import androidx.test.espresso.intent.Intents;
import androidx.test.platform.app.InstrumentationRegistry;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;

public abstract class BaseUITest<T extends Activity> {
    protected final static String APP_NAME = "testAppName";
    protected final static String TEST_SERVER_URL = "https://testUrl.com";
    protected final static String USER_NAME = "testUser";
    protected ActivityScenario<T> activityScenario;

    @Before
    public void setUp() {
        activityScenario = ActivityScenario.launch(getLaunchIntent());
        setUpPostLaunch();
        Intents.init();
    }

    protected abstract void setUpPostLaunch();
    protected abstract Intent getLaunchIntent();

    @After
    public void tearDown() throws Exception {
        if (activityScenario != null) activityScenario.close();
        Intents.release();
    }

    protected Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

    public static ViewAction setChecked(final boolean checked) {
        return new ViewAction() {
            @Override
            public BaseMatcher<View> getConstraints() {
                return new BaseMatcher<View>() {
                    @Override
                    public boolean matches(Object item) {
                        return isA(Checkable.class).matches(item);
                    }

                    @Override
                    public void describeMismatch(Object item, Description mismatchDescription) {
                    }

                    @Override
                    public void describeTo(Description description) {
                    }
                };
            }

            @Override
            public String getDescription() {
                return null;
            }

            @Override
            public void perform(UiController uiController, View view) {
                Checkable checkableView = (Checkable) view;
                if (checkableView.isChecked() != checked) {
                    checkableView.setChecked(checked);
                } else if (checkableView.isChecked() == checked) {
                    checkableView.setChecked(checked);
                }
            }

        };
    }
}

