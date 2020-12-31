# Services

This project is __*actively maintained*__

It is part of the ODK-X Android tools suite.  

Prior to rev 200, this repo was the __*core*__ repo.

It is an APK that provides services (database, content providers, local webserver) used by all the other ODK-X tools.

The developer [wiki](https://github.com/odk-x/tool-suite-X/wiki) (including release notes) and [issues tracker](https://github.com/odk-x/tool-suite-X/issues) are located under the [**ODK-X Tool Suite**](https://github.com/odk-x) project.

Engage with the community and get technical support on [the ODK-X forum](https://forum.odk-x.org)

## Setting up your environment

General instructions for setting up an ODK-X environment can be found at our [Developer Environment Setup wiki page](https://github.com/odk-x/tool-suite-X/wiki/Developer-Environment-Setup)

Install [Android Studio](http://developer.android.com/tools/studio/index.html) and the [SDK](http://developer.android.com/sdk/index.html#Other).

This project depends on ODK-X's [androidlibrary](https://github.com/odk-x/androidlibrary) and [androidcommon](https://github.com/odk-x/androidcommon) projects; their binaries will be downloaded automatically fom our maven repository during the build phase. If you wish to modify them yourself, you must clone them into the same parent directory as survey. You directory stucture should resemble the following:

        |-- odk-x

            |-- services

            |-- androidlibrary


  * Note that this only applies if you are modifying androidlibrary. If you use the maven dependencies (the default option), the project will not show up in your directory.

Now you should be ready to build.

## Building the project

Open the Services project in Android Studio. Select `Build->Make Project' to build the app.

## Running

If the project builds properly, it should be able to run on an Android device without any other prerequisites.

## Source tree information
Quick description of the content in the root folder:

    |-- services_app     -- Source tree for Java components

        |-- src

            |-- main

                |-- res     -- Source tree for Android resources

                |-- java

                    |-- org

                        |-- opendatakit  -- The most relevant Java code lives here

            |-- androidTest     -- Source tree for Android implementation tests
            |-- test            -- Source tree for Java JUnit tests
