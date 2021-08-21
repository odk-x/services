package org.opendatakit.properties;

import static com.google.common.truth.Truth.assertThat;

import android.content.Context;

import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendatakit.services.sync.actions.fragments.SetCredentialsFragment;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.utilities.StaticStateManipulator;

public class AuthenticatedUserStateTest {

    public final String APP_NAME = "AuthenticatedUserStatePropTest";

    private final String TEST_USERNAME = "testUsername";
    private final String TEST_PASSWORD = "testPassword";

    @Before
    public void setUp() {
        StaticStateManipulator.get().reset();

        PropertiesSingleton props = getProps(getContext());
        props.setProperties(SetCredentialsFragment.getCredentialsProperty(TEST_USERNAME, TEST_PASSWORD));
    }

    @Test
    public void verifyCurrentUserStateProperty() {
        PropertiesSingleton props = getProps(getContext());

        String currentUserStateStr = props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE);
        assertThat(currentUserStateStr).isNotNull();

        UserState userState = UserState.valueOf(currentUserStateStr);
        assertThat(userState).isEqualTo(UserState.AUTHENTICATED_USER);
    }

    @Test
    public void verifyUsernameProperty() {
        PropertiesSingleton props = getProps(getContext());

        String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
        assertThat(username).isNotNull();
        assertThat(username).isEqualTo(TEST_USERNAME);
    }

    @Test
    public void verifyIsUserAuthenticatedProperty() {
        PropertiesSingleton props = getProps(getContext());

        String isUserAuthenticatedStr = props.getProperty(CommonToolProperties.KEY_IS_USER_AUTHENTICATED);
        assertThat(isUserAuthenticatedStr).isNotNull();

        boolean isUserAuthenticated;
        isUserAuthenticated = Boolean.parseBoolean(isUserAuthenticatedStr);
        assertThat(isUserAuthenticated).isFalse();
    }

    @After
    public void clearProperties() {
        StaticStateManipulator.get().reset();
    }

    private Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

    private PropertiesSingleton getProps(Context context) {
        return CommonToolProperties.get(context, APP_NAME);
    }

}
