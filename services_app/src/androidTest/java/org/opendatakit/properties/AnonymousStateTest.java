package org.opendatakit.properties;

import static com.google.common.truth.Truth.assertThat;

import android.content.Context;

import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opendatakit.services.sync.actions.fragments.ChooseSignInTypeFragment;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.utilities.StaticStateManipulator;

public class AnonymousStateTest {

    public final String APP_NAME = "AnonymousStatePropTest";

    @Before
    public void setUp() {
        StaticStateManipulator.get().reset();

        PropertiesSingleton props = getProps(getContext());
        props.setProperties(ChooseSignInTypeFragment.getAnonymousProperties());
    }

    @Test
    public void verifyCurrentUserStateProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String currentUserStateStr = props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE);
        assertThat(currentUserStateStr).isNotNull();

        UserState userState = UserState.valueOf(currentUserStateStr);
        assertThat(userState).isEqualTo(UserState.ANONYMOUS);
    }

    @Test
    public void verifyUsernameProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String username = props.getProperty(CommonToolProperties.KEY_USERNAME);
        assertThat(username).isNotNull();
        assertThat(username).isEmpty();
    }

    @Test
    public void verifyIsUserAuthenticatedProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String isUserAuthenticatedStr = props.getProperty(CommonToolProperties.KEY_IS_USER_AUTHENTICATED);
        assertThat(isUserAuthenticatedStr).isNull();
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
