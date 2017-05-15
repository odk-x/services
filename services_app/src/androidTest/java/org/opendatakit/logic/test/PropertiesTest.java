package org.opendatakit.logic.test;

import android.content.Context;
import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opendatakit.androidlibrary.R;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.StaticStateManipulator;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.desktop.WebLoggerDesktopFactoryImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author mitchellsundt@gmail.com
 */
@RunWith(AndroidJUnit4.class)
public class PropertiesTest {

    private static final String APPNAME = "unittestProp";

    @Before
    public void setUp() throws Exception {
        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(APPNAME);

        StaticStateManipulator.get().reset();
        WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
    }

    @Test
    public void testSimpleProperties() {

        Context context = InstrumentationRegistry.getTargetContext();

        PropertiesSingleton props = CommonToolProperties.get(context, APPNAME);
        // non-default value for font size
        props.setProperty(CommonToolProperties.KEY_FONT_SIZE, "29");
        // these are stored in devices
        props.setProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE,
            context.getString(R.string.credential_type_google_account));
        props.setProperty(CommonToolProperties.KEY_ACCOUNT, "mitchs.test@gmail.com");
        // this is stored in SharedPreferences
        props.setProperty(CommonToolProperties.KEY_PASSWORD, "asdf");

        StaticStateManipulator.get().reset();

        props = CommonToolProperties.get(context, APPNAME);
        assertEquals(props.getProperty(CommonToolProperties.KEY_FONT_SIZE), "29");
        assertEquals(props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE),
            context.getString(R.string.credential_type_google_account));
        assertEquals(props.getProperty(CommonToolProperties.KEY_ACCOUNT),
                "mitchs.test@gmail.com");
        assertEquals(props.getProperty(CommonToolProperties.KEY_PASSWORD), "asdf");
    }


    /**
     * Setting or removing secure properties from a
     * non-privileged APK should fail.
     */
    @Test
    public void testSecureSetProperties() {

        Context context = InstrumentationRegistry.getTargetContext();

        StaticStateManipulator.get().reset();

        PropertiesSingleton props = CommonToolProperties.get(context, APPNAME);
        String[] secureKeys = {
            CommonToolProperties.KEY_AUTH,
            CommonToolProperties.KEY_PASSWORD,
            CommonToolProperties.KEY_ROLES_LIST,
            CommonToolProperties.KEY_DEFAULT_GROUP,
            CommonToolProperties.KEY_USERS_LIST,
            CommonToolProperties.KEY_ADMIN_PW
        };

        for ( String secureKey : secureKeys ) {
            // this is stored in SharedPreferences
            props.setProperty(secureKey, "asdf" + secureKey.hashCode());
            assertEquals(props.getProperty(secureKey), "asdf" + secureKey.hashCode());

            // and verify remove works
            props.removeProperty(secureKey);

            assertNull("remove: " + secureKey, props.getProperty(secureKey));

        }
    }

}
