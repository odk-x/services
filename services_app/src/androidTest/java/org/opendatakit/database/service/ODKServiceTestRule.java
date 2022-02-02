/**
 * BASED ON Android's android.support.test.rule.ServiceTestRule
 *
 * below is the copyright and license for fiel.
 */

/*
 * Copyright (C) 2015 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opendatakit.database.service;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.IBinder;
import androidx.annotation.NonNull;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.internal.util.Checks;
import android.util.Log;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ODKServiceTestRule implements TestRule {
   private static final String TAG = "ODKServiceTestRule";
   private static final long DEFAULT_TIMEOUT = 5L; //seconds
   private static final int MAX_CONNECTION_ATTEMPTS =5;
   private Intent mServiceIntent;
   private long mTimeout;
   private TimeUnit mTimeUnit;
   private BlockingQueue<ProxyServiceConnection> mAttemptToBindConnections;

   /**
    * Creates a ODKServiceTestRule with a default timeout of 5 seconds
    */
   public ODKServiceTestRule() {
      this(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
   }
   /**
    * Factory method to create a ODKServiceTestRule} with a custom timeout
    *
    * @param timeout the amount of time to wait for a service to connect.
    * @param timeUnit the time unit representing how the timeout parameter should be interpreted
    * @return a ODKServiceTestRule with the desired timeout
    */
   public static ODKServiceTestRule withTimeout(long timeout, TimeUnit timeUnit) {
      return new ODKServiceTestRule(timeout, timeUnit);
   }
   private ODKServiceTestRule(long timeout, TimeUnit timeUnit) {
      mTimeout = timeout;
      mTimeUnit = timeUnit;
      mAttemptToBindConnections = new LinkedBlockingQueue<>();
   }




   /**   /**
    * This class is used to wait until a successful connection to the service was established. It
    * then serves as a proxy to original {@link ServiceConnection} passed by
    * the caller.
    */
   private class ProxyServiceConnection implements ServiceConnection {
      private ServiceConnection mCallerConnection;
      private CountDownLatch mConnectedLatch = new CountDownLatch(1);
      private IBinder mIBinder;

      private ProxyServiceConnection(ServiceConnection connection) {
         mCallerConnection = connection;
         mIBinder = null;
      }

      public CountDownLatch getConnectedLatch() {
         return mConnectedLatch;
      }


      public IBinder getBinder() {
         return mIBinder;
      }

      public boolean binderIsNull() {
         return mIBinder == null;
      }

      @Override
      public void onServiceConnected(ComponentName name, IBinder service) {
         Log.e(TAG, "On Service Connected");
         // store the service binder to return to the caller
         mIBinder = service;
         if (mCallerConnection != null) {
            // pass through everything to the callers ServiceConnection
            mCallerConnection.onServiceConnected(name, service);
         }
         mConnectedLatch.countDown();
      }
      @Override
      public void onServiceDisconnected(ComponentName name) {
         Log.e(TAG, "On Service Disconnected");
         //The process hosting the service has crashed or been killed.
         Log.e(TAG, "Connection to the Service has been lost!");
         mIBinder = null;
         if (mCallerConnection != null) {
            // pass through everything to the callers ServiceConnection
            mCallerConnection.onServiceDisconnected(name);
         }
      }
   }


   /*
    * Works just like
    * {@link #bindService(android.content.Intent, android.content.ServiceConnection, int)} except
    * uses an internal {@link android.content.ServiceConnection} to guarantee successful bound.
    * The operation option flag defaults to {@link android.content.Context#BIND_AUTO_CREATE}
    *
    * @see #bindService(android.content.Intent, android.content.ServiceConnection, int)
    */
   public IBinder bindService(@NonNull Intent intent) throws TimeoutException {
      // no extras are expected by unbind
      mServiceIntent = Checks.checkNotNull(intent, "intent can't be null").cloneFilter();
      return bindServiceAndWait(intent, null, Context.BIND_AUTO_CREATE);
   }

   /**
    * Starts the service under test, in the same way as if it were started by
    * {@link Context#bindService(Intent, ServiceConnection, int)
    * Context.bindService(Intent, ServiceConnection, flags)} with an
    * {@link Intent} that identifies a service. However, it waits for
    * {@link ServiceConnection#onServiceConnected(ComponentName,
    * IBinder)} to be called before returning.
    *
    * @param intent     Identifies the service to connect to.  The Intent may
    *                   specify either an explicit component name, or a logical
    *                   description (action, category, etc) to match an
    *                   {@link android.content.IntentFilter} published by a service.
    * @param connection Receives information as the service is started and stopped.
    *                   This must be a valid ServiceConnection object; it must not be null.
    * @param flags      Operation options for the binding.  May be 0,
    *                   {@link Context#BIND_AUTO_CREATE},
    *                   {@link Context#BIND_DEBUG_UNBIND},
    *                   {@link Context#BIND_NOT_FOREGROUND},
    *                   {@link Context#BIND_ABOVE_CLIENT},
    *                   {@link Context#BIND_ALLOW_OOM_MANAGEMENT}, or
    *                   {@link Context#BIND_WAIVE_PRIORITY}.
    * @return An object whose type is a subclass of IBinder, for making further calls into
    * the service.
    * @throws SecurityException if the called doesn't have permission to bind to the given service.
    * @throws TimeoutException  if timed out waiting for a successful connection with the service.
    * @see Context#BIND_AUTO_CREATE
    * @see Context#BIND_DEBUG_UNBIND
    * @see Context#BIND_NOT_FOREGROUND
    */
   public IBinder bindService(@NonNull Intent intent, @NonNull ServiceConnection connection,
       int flags) throws TimeoutException {
      // no extras are expected by unbind
      mServiceIntent = Checks.checkNotNull(intent, "intent can't be null").cloneFilter();
      ServiceConnection c = Checks.checkNotNull(connection, "connection can't be null");
      return bindServiceAndWait(mServiceIntent, c, flags);
   }

   private IBinder bindServiceAndWait(Intent intent, final ServiceConnection conn,
       int flags) throws TimeoutException {

      ProxyServiceConnection servConn = new ProxyServiceConnection(conn);

      Log.e(TAG, "bindNwait");

      for(int i=0; i < MAX_CONNECTION_ATTEMPTS; i++) {

         boolean isBound = InstrumentationRegistry.getInstrumentation().getContext().bindService(intent, servConn, flags);

         // block until service connection is established
         if (isBound) {
            try {
               CountDownLatch latch = servConn.getConnectedLatch();
               if (!latch.await(mTimeout, mTimeUnit)) {
                  throw new TimeoutException("Waited for " + mTimeout + " " + mTimeUnit.name() + ","
                      + " but service was never connected");
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new RuntimeException("Interrupted while waiting for service to be connected");
            }
         } else {
            Log.e(TAG, "Failed to bind to service");
         }


         if (!servConn.binderIsNull()) {
            try {
               mAttemptToBindConnections.put(servConn);
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
            return servConn.getBinder();
         }
      }
      return null;
   }

   /**
    * Makes the necessary calls to stop (or unbind) the service under test. This method is called
    * automatically called after test execution. This is not a blocking call since there is no
    * reliable way to guarantee successful disconnect without access to service lifecycle.
    */
   // Visible for testing
   void shutdownService() {
      Log.e(TAG, "READY TO UNBIND");
      while (!mAttemptToBindConnections.isEmpty()) {
         ProxyServiceConnection conn = null;
         try {
            conn = mAttemptToBindConnections.take();
         } catch (InterruptedException e) {
            e.printStackTrace();
         }
          assertNotNull(conn);
         if(!conn.binderIsNull()) {
            InstrumentationRegistry.getInstrumentation().getContext().unbindService(conn);
            Log.e(TAG, "CALLED UNBIND");
         }
      }

      try {
         Thread.sleep(2000);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException("Interrupted while waiting for service to shutdown");
      }
   }
   /**
    * Override this method to do your own service specific initialization before starting or
    * binding to the service. The method is called before each test method is executed including
    * any method annotated with
    * <a href="http://junit.sourceforge.net/javadoc/org/junit/Before.html"><code>Before</code></a>.
    * Do not start or bind to a service from here!
    */
   protected void beforeService() {
      // empty by default
   }
   /**
    * Override this method to do your own service specific clean up after the service is shutdown.
    * The method is called after each test method is executed including any method annotated with
    * <a href="http://junit.sourceforge.net/javadoc/org/junit/After.html"><code>After</code></a>
    * and after necessary calls to stop (or unbind) the service under test were called.
    */
   protected void afterService() {
      // empty by default
   }
   @Override
   public Statement apply(final Statement base, Description description) {
      return new ServiceStatement(base);
   }
   /**
    * {@link Statement} that executes the service lifecycle methods before and after the execution
    * of the test.
    */
   private class ServiceStatement extends Statement {
      private final Statement mBase;
      public ServiceStatement(Statement base) {
         mBase = base;
      }
      @Override
      public void evaluate() throws Throwable {
         try {
            beforeService();
            mBase.evaluate();
         } finally {
            shutdownService();
            afterService();
         }
      }
   }
}