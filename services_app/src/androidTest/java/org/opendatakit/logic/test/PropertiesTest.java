package org.opendatakit.logic.test;

import android.test.AndroidTestCase;

import org.opendatakit.androidlibrary.R;
import org.opendatakit.properties.CommonToolProperties;
import org.opendatakit.properties.PropertiesSingleton;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.StaticStateManipulator;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.logging.desktop.WebLoggerDesktopFactoryImpl;

/**
 * @author mitchellsundt@gmail.com
 */
public class PropertiesTest extends AndroidTestCase {

    private static final String APPNAME = "unittestProp";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(APPNAME);

        StaticStateManipulator.get().reset();
        WebLogger.setFactory(new WebLoggerDesktopFactoryImpl());
    }

    public void testSimpleProperties() {

        PropertiesSingleton props = CommonToolProperties.get(getContext(), APPNAME);
        // non-default value for font size
        props.setProperty(CommonToolProperties.KEY_FONT_SIZE, "29");
        // these are stored in devices
        props.setProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE, getContext().getString(R.string.credential_type_google_account));
        props.setProperty(CommonToolProperties.KEY_ACCOUNT, "mitchs.test@gmail.com");
        // this is stored in SharedPreferences
        props.setProperty(CommonToolProperties.KEY_PASSWORD, "asdf");

        StaticStateManipulator.get().reset();

        props = CommonToolProperties.get(getContext(), APPNAME);
        assertEquals(props.getProperty(CommonToolProperties.KEY_FONT_SIZE), "29");
        assertEquals(props.getProperty(CommonToolProperties.KEY_AUTHENTICATION_TYPE),
                getContext().getString(R.string.credential_type_google_account));
        assertEquals(props.getProperty(CommonToolProperties.KEY_ACCOUNT),
                "mitchs.test@gmail.com");
        assertEquals(props.getProperty(CommonToolProperties.KEY_PASSWORD), "asdf");
    }


    /**
     * Setting or removing secure properties from a
     * non-privileged APK should fail.
     */
    public void testSecureSetProperties() {

        StaticStateManipulator.get().reset();

        PropertiesSingleton props = CommonToolProperties.get(getContext(), APPNAME);
        String[] secureKeys = {
            CommonToolProperties.KEY_AUTH,
            CommonToolProperties.KEY_PASSWORD,
            CommonToolProperties.KEY_ROLES_LIST,
            CommonToolProperties.KEY_USERS_LIST,
            CommonToolProperties.KEY_ADMIN_PW
        };

        for ( int i = 0 ; i < secureKeys.length ; ++i ) {
            // this is stored in SharedPreferences
            boolean threwError = false;

            props.setProperty(secureKeys[i], "asdf" + secureKeys[i].hashCode());
            assertEquals(props.getProperty(secureKeys[i]), "asdf" + secureKeys[i].hashCode());

            // and verify remove works
            props.removeProperty(secureKeys[i]);

            assertNull("remove: " + secureKeys[i], props.getProperty(secureKeys[i]));

        }
    }

}
