package org.opendatakit.properties;

import android.content.Context;

import androidx.test.platform.app.InstrumentationRegistry;

import org.junit.Before;
import org.junit.Test;
import org.opendatakit.services.utilities.UserState;
import org.opendatakit.utilities.StaticStateManipulator;
import static com.google.common.truth.Truth.*;

public class DefaultPropertiesTest {

    private static final String APP_NAME = "UnitTestProp";

    @Before
    public void setUp() {
        StaticStateManipulator.get().reset();
    }

    @Test
    public void verifyServerUrlProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String serverUrl = props.getProperty(CommonToolProperties.KEY_SYNC_SERVER_URL);

        assertThat(serverUrl).isNotNull();
        assertThat(serverUrl).isEqualTo(context.getString(org.opendatakit.androidlibrary.R.string.default_sync_server_url));
    }

    @Test
    public void verifyIsServerVerifiedProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String isServerVerifiedStr = props.getProperty(CommonToolProperties.KEY_IS_SERVER_VERIFIED);
        assertThat(isServerVerifiedStr).isNotNull();

        boolean isServerVerified = Boolean.parseBoolean(isServerVerifiedStr);
        assertThat(isServerVerified).isFalse();
    }

    @Test
    public void verifyIsAnonymousUsedProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String isAnonymousUsedStr = props.getProperty(CommonToolProperties.KEY_IS_ANONYMOUS_SIGN_IN_USED);
        assertThat(isAnonymousUsedStr).isNotNull();

        boolean isAnonymousUsed = Boolean.parseBoolean(isAnonymousUsedStr);
        assertThat(isAnonymousUsed).isFalse();
    }

    @Test
    public void verifyIsAnonymousAllowedProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String isAnonymousAllowed = props.getProperty(CommonToolProperties.KEY_IS_ANONYMOUS_ALLOWED);
        assertThat(isAnonymousAllowed).isNull();
    }

    @Test
    public void verifyCurrentUserStateProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String currentUserStateStr = props.getProperty(CommonToolProperties.KEY_CURRENT_USER_STATE);
        assertThat(currentUserStateStr).isNotNull();

        UserState userState = UserState.valueOf(currentUserStateStr);
        assertThat(userState).isEqualTo(UserState.LOGGED_OUT);
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

    @Test
    public void verifyLastSyncInfoProperty() {
        Context context = getContext();
        PropertiesSingleton props = getProps(context);

        String lastSyncInfo = props.getProperty(CommonToolProperties.KEY_LAST_SYNC_INFO);
        assertThat(lastSyncInfo).isNull();
    }

    private Context getContext() {
        return InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

    private PropertiesSingleton getProps(Context context) {
        return CommonToolProperties.get(context, APP_NAME);
    }
}
