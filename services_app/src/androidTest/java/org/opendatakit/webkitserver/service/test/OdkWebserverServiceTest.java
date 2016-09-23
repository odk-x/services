package org.opendatakit.webkitserver.service.test;

import android.content.Intent;
import android.os.IBinder;
import android.os.RemoteException;
import android.support.annotation.NonNull;
import android.test.ServiceTestCase;

import org.opendatakit.TestConsts;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.httpclientandroidlib.HttpStatus;
import org.opendatakit.webkitserver.service.WebkitServerInterface;
import org.opendatakit.services.webkitservice.service.OdkWebkitServerService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by wrb on 9/14/2015.
 */
public class OdkWebserverServiceTest  extends ServiceTestCase<OdkWebkitServerService> {

    private static final String HELLO_WORLD_HTML_TXT = "<HTML><BODY>Hello World!!!</BODY></HTML>";
    private static final String TEST_FILE_NAME = "Hello.html";
    private static final String TEST_DIR = "testfiles";

    public OdkWebserverServiceTest() {
        super(OdkWebkitServerService.class);
    }

    public OdkWebserverServiceTest(Class<OdkWebkitServerService> serviceClass) {
        super(serviceClass);
    }

    @Override
    protected void setUp () throws Exception {
        super.setUp();
        setupService();
    }

    @NonNull
    private WebkitServerInterface getWebkitServerInterface() {
        Intent bind_intent = new Intent();
        bind_intent.setClass(getContext(), OdkWebkitServerService.class);
        IBinder service = this.bindService(bind_intent);
        WebkitServerInterface serviceInterface = WebkitServerInterface.Stub.asInterface(service);
        assertNotNull(serviceInterface);
        assertTrue(service.isBinderAlive());
        return serviceInterface;
    }

    public void testBindingNRestart() {
        WebkitServerInterface serviceInterface = getWebkitServerInterface();
        try {
            serviceInterface.restart();
        } catch (RemoteException e) {
            e.printStackTrace();
            fail();
        }
    }


    public void testBindingTwoBindings() {
        Intent bind_intent = new Intent();
        bind_intent.setClass(getContext(), OdkWebkitServerService.class);
        IBinder service = this.bindService(bind_intent);
        WebkitServerInterface serviceInterface = WebkitServerInterface.Stub.asInterface(service);
        assertNotNull(serviceInterface);
        assertTrue(service.isBinderAlive());
        IBinder service2 = this.bindService(bind_intent);
        WebkitServerInterface serviceInterface2 = WebkitServerInterface.Stub.asInterface(service2);
        assertNotNull(serviceInterface);
        assertTrue(service.isBinderAlive());
        assertNotNull(serviceInterface2);
        assertTrue(service2.isBinderAlive());
    }

    public void testServingHelloWorldHtml() {
        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(TestConsts.APPNAME);

        String directory = ODKFileUtils.getConfigFolder(TestConsts.APPNAME) + File.separator +
            TEST_DIR;
        String fileName = directory + File.separator + TEST_FILE_NAME;

        WebkitServerInterface serviceInterface = getWebkitServerInterface();

        PrintWriter writer = null;
        try {
            File directoryLocation = new File(directory);
            if(!directoryLocation.isDirectory()) {
                directoryLocation.mkdirs();
            }
            writer = new PrintWriter(fileName, "UTF-8");
            writer.println(HELLO_WORLD_HTML_TXT);
            writer.flush();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }


        try {
            serviceInterface.restart();
        } catch (RemoteException e) {
            e.printStackTrace();
            fail("Problem with service restart");
        }

        HttpURLConnection connection = null;
        try {
            String urlStr = "http://localhost:8635/" + TestConsts.APPNAME + "/" +
                ODKFileUtils.asUriFragment(TestConsts.APPNAME, new File(fileName));

            URL url = new URL(urlStr);
            connection = (HttpURLConnection) url.openConnection();
            if(connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String responseStr = "";
            String segment = br.readLine();
            while (segment != null) {
                responseStr = responseStr + segment;
                segment = br.readLine();
            }
            br.close();
            assertTrue("RECEIVED:" + responseStr, HELLO_WORLD_HTML_TXT.equals(responseStr));
        } catch(IOException e) {
            e.printStackTrace();
            fail("GOT an IOException when trying to use the web server:" + e.getMessage());
        } catch(Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server:" + e.getMessage());
        } finally {
            if(connection != null){
                connection.disconnect();
            }
        }
    }

}
