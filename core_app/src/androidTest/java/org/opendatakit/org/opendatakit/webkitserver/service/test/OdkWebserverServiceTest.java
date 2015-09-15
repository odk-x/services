package org.opendatakit.org.opendatakit.webkitserver.service.test;

import android.content.Intent;
import android.os.IBinder;
import android.os.RemoteException;
import android.support.annotation.NonNull;
import android.test.ServiceTestCase;

import org.opendatakit.TestConsts;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.webkitserver.service.OdkWebkitServerInterface;
import org.opendatakit.webkitserver.service.OdkWebkitServerService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by wrb on 9/14/2015.
 */
public class OdkWebserverServiceTest  extends ServiceTestCase<OdkWebkitServerService> {

    private static final String helloWorldHtmlTxt = "<HTML><BODY>Hello World!!!</BODY></HTML>";
    private static final String TEST_FILE_NAME = "Hello.html";
    private static final String TEST_DIR = "testfiles";
    private static final String SD_ODK = "/storage/sdcard/opendatakit/";

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
    private OdkWebkitServerInterface getOdkWebkitServerInterface() {
        Intent bind_intent = new Intent();
        bind_intent.setClass(getContext(), OdkWebkitServerService.class);
        IBinder service = this.bindService(bind_intent);
        OdkWebkitServerInterface serviceInterface = OdkWebkitServerInterface.Stub.asInterface(service);
        assertNotNull(serviceInterface);
        assertTrue(service.isBinderAlive());
        return serviceInterface;
    }

    public void testBindingNRestart() {
        OdkWebkitServerInterface serviceInterface = getOdkWebkitServerInterface();
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
        OdkWebkitServerInterface serviceInterface = OdkWebkitServerInterface.Stub.asInterface(service);
        assertNotNull(serviceInterface);
        assertTrue(service.isBinderAlive());
        IBinder service2 = this.bindService(bind_intent);
        OdkWebkitServerInterface serviceInterface2 = OdkWebkitServerInterface.Stub.asInterface(service2);
        assertNotNull(serviceInterface);
        assertTrue(service.isBinderAlive());
        assertNotNull(serviceInterface2);
        assertTrue(service2.isBinderAlive());
    }

    public void testServingHelloWorldHtml() {
        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(TestConsts.APPNAME);

        String directory = TestConsts.APPNAME + "/" + TEST_DIR;
        String fileLocation = SD_ODK + directory;
        String fileName = fileLocation + "/" + TEST_FILE_NAME;

        OdkWebkitServerInterface serviceInterface = getOdkWebkitServerInterface();

        PrintWriter writer = null;
        try {
            File directoryLocation = new File(fileLocation);
            if(!directoryLocation.isDirectory()) {
                directoryLocation.mkdirs();
            }
            writer = new PrintWriter(fileName, "UTF-8");
            writer.println(helloWorldHtmlTxt);
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
        try {
            String urlStr = "http://localhost:8635/" + directory + "/" + TEST_FILE_NAME;
            URLConnection connection = new URL(urlStr).openConnection();
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String responseStr = "";
            while (br.ready()) {
                responseStr = responseStr + br.readLine();
            }
            assertTrue(helloWorldHtmlTxt.equals(responseStr));
        } catch(IOException e) {
            e.printStackTrace();
            fail("GOT an IOException when trying to use the web server");
        }
    }

}
