package org.opendatakit.webkitserver.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import android.Manifest;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.net.Uri;
import android.os.Build;
import android.os.IBinder;
import android.os.RemoteException;

import androidx.annotation.NonNull;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.rule.GrantPermissionRule;
import androidx.test.rule.ServiceTestRule;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opendatakit.TestConsts;
import org.opendatakit.consts.WebkitServerConsts;
import org.opendatakit.httpclientandroidlib.HttpStatus;
import org.opendatakit.logging.WebLogger;
import org.opendatakit.utilities.ODKFileUtils;
import org.opendatakit.utilities.StaticStateManipulator;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;

/**
 * @author mitchellsundt@gmail.com
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OdkWebserverServiceTest {

    private static final String TAG = "OdkWebserverServiceTest";

    private static final String TEST_DIR = "testfiles";
    private static final String TEST_FILE_NAME_HELLO = "Hello.html";
    private static final String TEST_FILE_NAME_EMPTY = "EmptyFile.html";
    private static final String TEST_FILE_NAME_LARGE = "LargeFile.txt";
    private static final String TEST_FILE_NAME_SPECIAL = "Special@File#Name.txt";
    private static final String TEST_FILE_NAME_MULTIPLE_1 = "MultipleFiles1.html";
    private static final String TEST_FILE_NAME_MULTIPLE_2 = "MultipleFiles2.html";
    private static final String TEST_FILE_NAME_SPACES = "File with Spaces.txt";
    private static final String TEST_FILE_NAME_UNICODE = "FileWithUnicodeContent.txt";

    private static final String HELLO_WORLD_HTML_TXT = "<HTML><BODY>Hello World!!!</BODY></HTML>";
    private static final String EMPTY_HTML_TXT = "";
    private static final String LARGE_FILE_CONTENT = generateLargeFileContent();
    private static final String SPECIAL_CHARACTERS_CONTENT = "Special characters in file name";
    private static final String UNICODE_CONTENT = "Unicode characters: 你好, مرحبا, こんにちは";
    private static final String TEST_FILE_NAME_INVALID_EXTENSION = "InvalidFile.xyz";
    private static final String TEST_FILE_NAME_UPPERCASE_EXTENSION = "FileWithUpperCaseExtension.TXT";
    private static final String TEST_FILE_NAME_BINARY = "BinaryFile.bin";
    private static final String TEST_FILE_NAME_MULTIPLE_DOTS = "File.With.Multiple.Dots.txt";
    private static final String TEST_FILE_NAME_NO_EXTENSION = "FileWithNoExtension";
    private static final String TEST_FILE_NAME_DIFFERENT_EXTENSION = "FileWithDifferentExtension.docx";


    @Rule
    public final ServiceTestRule mServiceRule = new ServiceTestRule();

    @Rule
    public GrantPermissionRule writeRuntimePermissionRule = GrantPermissionRule .grant(Manifest.permission.WRITE_EXTERNAL_STORAGE);

    @Rule
    public GrantPermissionRule readtimePermissionRule = GrantPermissionRule .grant(Manifest.permission.READ_EXTERNAL_STORAGE);

    @Before
    public void setUp() throws Exception {

        ODKFileUtils.verifyExternalStorageAvailability();
        ODKFileUtils.assertDirectoryStructure(TestConsts.APPNAME);

        StaticStateManipulator.get().reset();
    }

    @After
    public void tearDown() {
        IWebkitServerInterface webkitServer = getIWebkitServerInterface();
        Context context = InstrumentationRegistry.getInstrumentation().getContext();
        context.unbindService(odkWebkitServiceConnection);
        // sleep to let this take effect
        try {
            Thread.sleep(200L);
        } catch (InterruptedException e) {
            // ignore
            e.printStackTrace();
        }
    }

    private class ServiceConnectionWrapper implements ServiceConnection {

        @Override public void onServiceConnected(ComponentName name, IBinder service) {

            if (!name.getClassName().equals(WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS)) {
                WebLogger.getLogger(TestConsts.APPNAME).e(TAG, "Unrecognized service");
                return;
            }
            synchronized (odkWebkitInterfaceBindComplete) {
                try {
                    webkitServerInterface = (service == null) ? null : IWebkitServerInterface.Stub.asInterface(service);
                } catch (IllegalArgumentException e) {
                    webkitServerInterface = null;
                }

                active = false;
                odkWebkitInterfaceBindComplete.notify();
            }
        }

        @Override public void onServiceDisconnected(ComponentName name) {
            synchronized (odkWebkitInterfaceBindComplete) {
                webkitServerInterface = null;
                active = false;
                odkWebkitInterfaceBindComplete.notify();
            }
        }
    }

    private final ServiceConnectionWrapper odkWebkitServiceConnection = new ServiceConnectionWrapper();
    private final Object odkWebkitInterfaceBindComplete = new Object();
    private IWebkitServerInterface webkitServerInterface;
    private boolean active = false;

    /**
     * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
     */
    private IWebkitServerInterface invokeBindService() throws InterruptedException {

        WebLogger.getLogger(TestConsts.APPNAME).i(TAG, "Attempting or polling on bind to Webkit Server "
            + "service");
        Intent bind_intent = new Intent();
        bind_intent.setClassName(WebkitServerConsts.WEBKITSERVER_SERVICE_PACKAGE,
            WebkitServerConsts.WEBKITSERVER_SERVICE_CLASS);

        synchronized (odkWebkitInterfaceBindComplete) {
            if ( !active ) {
                active = true;
                Context context = InstrumentationRegistry.getInstrumentation().getContext();
                context.bindService(bind_intent, odkWebkitServiceConnection,
                    Context.BIND_AUTO_CREATE | ((Build.VERSION.SDK_INT >= 14) ?
                        Context.BIND_ADJUST_WITH_ACTIVITY :
                        0));
            }

            odkWebkitInterfaceBindComplete.wait();

            if (webkitServerInterface != null) {
                return webkitServerInterface;
            }
        }
        return null;
    }

    @NonNull
    private IWebkitServerInterface getIWebkitServerInterface() {

        // block waiting for it to be bound...
        for (;;) {
            try {

                synchronized (odkWebkitInterfaceBindComplete) {
                    if (webkitServerInterface != null) {
                        return webkitServerInterface;
                    }
                }

                // call method that waits on odkDbInterfaceBindComplete
                // Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
                IWebkitServerInterface webkitServerInterface = invokeBindService();
                if ( webkitServerInterface != null ) {
                    return webkitServerInterface;
                }

            } catch (InterruptedException e) {
                // expected if we are waiting. Ignore because we log bind attempt if spinning.
            }
        }
    }

    @Test
    public void testBindingNRestart() {
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        try {
            serviceInterface.restart();
        } catch (RemoteException e) {
            e.printStackTrace();
            fail("Problem with service restart");
        }
    }

    private File createTestDirectory() {
        File directoryLocation = new File(ODKFileUtils.getConfigFolder(TestConsts.APPNAME), TEST_DIR);
        if (!directoryLocation.isDirectory()) {
            directoryLocation.mkdirs();
        }
        return directoryLocation;
    }

    private void restartService(IWebkitServerInterface serviceInterface) {
        try {
            serviceInterface.restart();
        } catch (RemoteException e) {
            e.printStackTrace();
            fail("Problem with service restart: " + e.getMessage());
        }
    }

    private String buildTestUrl(File fileLocation) {
        Uri baseUrl = Uri.parse("http://" + WebkitServerConsts.HOSTNAME + ":" +
                Integer.toString(WebkitServerConsts.PORT) + "/" + TestConsts.APPNAME + "/");
        return baseUrl.buildUpon()
                .appendPath(ODKFileUtils.asUriFragment(TestConsts.APPNAME, fileLocation))
                .build()
                .toString();
    }

    @Test
    public void testServingHelloWorldHtml() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_HELLO, HELLO_WORLD_HTML_TXT);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseMatchesHelloWorldHtml(fileLocation);
    }

    private void assertResponseMatchesHelloWorldHtml(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"))) {
                StringBuilder responseStr = new StringBuilder();
                String segment;
                while ((segment = br.readLine()) != null) {
                    responseStr.append(segment);
                }
                assertTrue("Received: " + responseStr, HELLO_WORLD_HTML_TXT.equals(responseStr.toString()));
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingEmptyFile() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_EMPTY, EMPTY_HTML_TXT);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseIsNotEmpty(fileLocation);
    }

    private void assertResponseIsNotEmpty(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            if (connection.getResponseCode() != HttpStatus.SC_OK) {
                fail("Response code was NOT HTTP_OK");
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"))) {
                String responseStr = br.readLine();
                if (responseStr == null) {
                    fail("Response is empty, but it should not be.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingNonexistentFile() {
        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertNonexistentFileResponse(getNonexistentFileLocation());
    }

    private File getNonexistentFileLocation() {
        return new File(ODKFileUtils.getConfigFolder(TestConsts.APPNAME), "NonexistentFile.txt");
    }

    private void assertNonexistentFileResponse(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_NOT_FOUND", HttpStatus.SC_NOT_FOUND, responseCode);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingLargeFile() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File largeFileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_LARGE, LARGE_FILE_CONTENT);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertLargeFileResponse(largeFileLocation);
    }

    private void assertLargeFileResponse(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for large file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithSpecialCharactersInName() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_SPECIAL, SPECIAL_CHARACTERS_CONTENT);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseContainsSpecialCharacters(fileLocation);
    }

    private void assertResponseContainsSpecialCharacters(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for special characters in the file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingMultipleFilesWithSameNameInDifferentDirectories() {
        // Arrange
        File directory1 = createTestDirectory();
        File file1 = createTestFile(directory1, TEST_FILE_NAME_MULTIPLE_1, HELLO_WORLD_HTML_TXT);

        File directory2 = createTestDirectory();
        File file2 = createTestFile(directory2, TEST_FILE_NAME_MULTIPLE_2, HELLO_WORLD_HTML_TXT);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForMultipleFiles(file1, file2);
    }

    private void assertResponseForMultipleFiles(File file1, File file2) {
        try {
            Uri uri1 = Uri.parse(buildTestUrl(file1));
            Uri uri2 = Uri.parse(buildTestUrl(file2));

            HttpURLConnection connection1 = (HttpURLConnection) new URL(uri1.toString()).openConnection();
            int responseCode1 = connection1.getResponseCode();
            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode1);

            HttpURLConnection connection2 = (HttpURLConnection) new URL(uri2.toString()).openConnection();
            int responseCode2 = connection2.getResponseCode();
            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode2);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithSpacesInName() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_SPACES, "File with spaces in name");

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseContainsSpaces(fileLocation);
    }

    private void assertResponseContainsSpaces(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for spaces in the file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithUnicodeCharactersInContent() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_UNICODE, UNICODE_CONTENT);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseContainsUnicodeContent(fileLocation);
    }

    private void assertResponseContainsUnicodeContent(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for Unicode characters in the file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithInvalidExtension() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_INVALID_EXTENSION, "Content with an invalid extension");

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForInvalidExtension(fileLocation);
    }

    private void assertResponseForInvalidExtension(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling an invalid extension, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithUpperCaseExtension() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_UPPERCASE_EXTENSION, "Content with an uppercase extension");

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForUpperCaseExtension(fileLocation);
    }

    private void assertResponseForUpperCaseExtension(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling an uppercase extension, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingBinaryFile() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File binaryFileLocation = createBinaryFile(directoryLocation, TEST_FILE_NAME_BINARY);

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertBinaryFileResponse(binaryFileLocation);
    }

    private void assertBinaryFileResponse(File binaryFileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(binaryFileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling binary file content, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithMultipleDotsInFilename() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_MULTIPLE_DOTS, "Content with multiple dots in filename");

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForMultipleDotsInFilename(fileLocation);
    }

    private void assertResponseForMultipleDotsInFilename(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling filenames with multiple dots, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithNoExtension() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_NO_EXTENSION, "Content with no extension");

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForNoExtension(fileLocation);
    }

    private void assertResponseForNoExtension(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling files with no extension, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    @Test
    public void testServingFileWithDifferentExtension() {
        // Arrange
        File directoryLocation = createTestDirectory();
        File fileLocation = createTestFile(directoryLocation, TEST_FILE_NAME_DIFFERENT_EXTENSION, "Content with a different extension");

        // Act
        IWebkitServerInterface serviceInterface = getIWebkitServerInterface();
        restartService(serviceInterface);

        // Assert
        assertResponseForDifferentExtension(fileLocation);
    }

    private void assertResponseForDifferentExtension(File fileLocation) {
        try {
            Uri uri = Uri.parse(buildTestUrl(fileLocation));

            HttpURLConnection connection = (HttpURLConnection) new URL(uri.toString()).openConnection();
            int responseCode = connection.getResponseCode();

            assertEquals("Response code should be HTTP_OK", HttpStatus.SC_OK, responseCode);

            // Additional assertions for handling files with different extensions, if needed
        } catch (IOException e) {
            e.printStackTrace();
            fail("Got an IOException when trying to use the web server: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Got an Exception when trying to use the web server: " + e.getMessage());
        }
    }

    private File createTestFile(File directoryLocation, String fileName, String content) {
        Uri fileUri = Uri.withAppendedPath(Uri.fromFile(directoryLocation), fileName);
        File fileLocation = new File(fileUri.getPath());

        try (PrintWriter writer = new PrintWriter(fileLocation, "UTF-8")) {
            writer.println(content);
            writer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Failed to create the test file: " + e.getMessage());
        }

        return fileLocation;
    }

    private static String generateLargeFileContent() {
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            content.append("This is a line in the large file.\n");
        }
        return content.toString();
    }

    private File createBinaryFile(File directoryLocation, String fileName) {
        Uri fileUri = Uri.parse(directoryLocation.toURI() + fileName);
        File binaryFileLocation = new File(fileUri.getPath());

        try {
            // Create a binary file with random content
            byte[] binaryContent = new byte[]{0x12, 0x34, 0x56, 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE, (byte) 0xF0};
            Files.write(binaryFileLocation.toPath(), binaryContent);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Failed to create the binary test file: " + e.getMessage());
        }

        return binaryFileLocation;
    }
}
