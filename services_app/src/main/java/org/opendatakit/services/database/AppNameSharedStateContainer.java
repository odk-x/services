/*
 * Copyright (C) 2015 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.services.database;

import org.opendatakit.database.service.DbHandle;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Holds state shared across all open connections for a given appName
 *
 * @author mitchellsundt@gmail.com
 */
public class AppNameSharedStateContainer {

   static final Pattern TRIM_SQL_PATTERN = Pattern.compile("[\\s]*\\n+[\\s]*");

   public static String trimSqlForDisplay(String sql) {
      return TRIM_SQL_PATTERN.matcher(sql).replaceAll(" ");
   }

   private final String appName;
   private final Object appNameMutex = new Object();
   /**
    * Access only within appNameMutex
    *
    * Holds map of sessionQualifier to active connection.
    */
   private final Map<String, OdkConnectionInterface> sessionQualifierConnectionMap =
       new TreeMap<String, OdkConnectionInterface>();
   /**
    * Access only within appNameMutex
    *
    * holds connections that are in process of being destroyed.
    */
   private final WeakHashMap<OdkConnectionInterface, Long>
       pendingDestruction = new WeakHashMap<OdkConnectionInterface, Long>();

   private final OperationLog operationLog;

   private String beginTransactionSessionQualifier = null;
   private Long beginTransactionThreadId = null;

   AppNameSharedStateContainer(String appName) {
      this.appName = appName;
      this.operationLog = new OperationLog(appName);
   }

   /**
    * @return the appName
    */
   public String getAppName() {
      return appName;
   }

   /**
    * <p>If there is no existing connection for this sessionQualifier, then
    * put the supplied connection into the sessionQualifierConnectionMap and
    * increment its reference count (to indicate that it is managed by the map).
    * Return null.</p>
    * <p>Otherwise, increment the existing connection and return it.</p>
    * @param sessionQualifier
    * @param dbConnection
    * @return null if the dbConnection was added (and dbConnection inserted into
    * sessionQualifierMap with +1 reference count) or the existing connection in
    * the map for this sessionQualifier (with +1 reference count).
    */
   OdkConnectionInterface atomicSetOrGetExisting( String sessionQualifier,
       OdkConnectionInterface dbConnection) {

      OdkConnectionInterface dbConnectionExisting = null;
      synchronized (appNameMutex) {
         dbConnectionExisting = sessionQualifierConnectionMap.get(sessionQualifier);

         if (dbConnectionExisting == null) {
            sessionQualifierConnectionMap.put(sessionQualifier, dbConnection);
            // this map now holds a reference
            dbConnection.acquireReference();
         } else {
            // signal that getDbConnection() should release reference (do not call remove)
            // +1 reference to retain this
            dbConnectionExisting.acquireReference();
         }
      }
      return dbConnectionExisting;
   }

   /**
    * Atomically +1 reference and return the connection for a given sessionQualifier
    *
    * @param sessionQualifier
    * @return null if there is no connection, otherwise the connection with +1 reference count.
    */
   OdkConnectionInterface getExisting( String sessionQualifier ) {
      OdkConnectionInterface dbConnectionExisting = null;
      synchronized (appNameMutex) {
         dbConnectionExisting = sessionQualifierConnectionMap.get(sessionQualifier);
         if ( dbConnectionExisting != null ) {
            dbConnectionExisting.acquireReference();
         }
      }
      return dbConnectionExisting;
   }

   /**
    * Record that we expect a connection to be released.
    * And remove the connection from the sessionQualifierConnectionMap.
    * We may record the same connection multiple times.
    *
    * If we removed the connection from the sessionQualifierConnectionMap,
    * then -1 reference count outside of the mutex.
    *
    * Other than ensuring the connection is removed from the
    * sessionQualifierConnectionMap, this is mainly for debugging
    * purposes to ensure that all reference counts are properly
    * released. If a connection stays on the pendingDestruction list
    * for very long, then we have a problem.
    *
    * @param dbConnection
    * @return  true if the dbConnection should have -1 reference count to complete action.
    */
   boolean moveIntoPendingDestruction( OdkConnectionInterface dbConnection ) {
      OdkConnectionInterface reference = null;
      synchronized (appNameMutex) {
         // add the connection to the pending destruction list
         pendingDestruction.put(dbConnection, System.currentTimeMillis());
         // remove it from the sessionQualifierConnectionMap if it is there.
         reference = sessionQualifierConnectionMap.remove(dbConnection.getSessionQualifier());

         if ( sessionQualifierConnectionMap.isEmpty() ) {
            operationLog.clearOperations();
         }
      }
      // and report back whether the connection needs to have -1 reference adjustment.
      return ( reference != null );
   }

   TreeSet<String> getAllSessionQualifiers() {
      TreeSet<String> sessionQualifiers = new TreeSet<String>();
      synchronized (appNameMutex) {
         sessionQualifiers.addAll(sessionQualifierConnectionMap.keySet());
      }
      return sessionQualifiers;
   }

   void dumpInfo(StringBuilder b) {
      synchronized (appNameMutex) {
         b.append("\n---------------- ").append(appName).append(" ---------------------\n\n");
         operationLog.dump(b, true);

         b.append("beginTransactionSessionQualifier ")
             .append(beginTransactionSessionQualifier)
             .append("\n");
         b.append("beginTransactionThreadId ")
             .append(beginTransactionThreadId)
             .append("\n-----active------------------\n\n");

         for (String sessionQualifier : sessionQualifierConnectionMap.keySet()) {
            OdkConnectionInterface dbConnection = sessionQualifierConnectionMap
                .get(sessionQualifier);
            dbConnection.dumpDetail(b);
            b.append("\n-------\n");
         }

         b.append("\n-----pendingDestruction------------\n\n");

         for (WeakHashMap.Entry<OdkConnectionInterface, Long> dbconnectionPD : pendingDestruction.entrySet()) {
            OdkConnectionInterface dbConnection = dbconnectionPD.getKey();

            if (dbConnection != null) {
               Long value = dbconnectionPD.getValue();
               b.append("\n-----closed at ").append(value).append("\n");
               dbConnection.dumpDetail(b);
               b.append("\n-------\n");
            }
         }

         b.append("\n-------------------------------------\n\n");
      }
   }


   Object getSessionMutex() {
      return new Object();
   }

   public OperationLog getOperationLog() {
      return operationLog;
   }

   void setBeginTransactionSession(String sessionQualifier) {
      synchronized (appNameMutex) {
         beginTransactionSessionQualifier = sessionQualifier;
         beginTransactionThreadId = Thread.currentThread().getId();
      }
   }

  /**
   * Work-around for jacoco ART issue https://code.google.com/p/android/issues/detail?id=80961
   * @return
   */
   private String internalGetSessionQualifier() {
      synchronized (appNameMutex) {
         if (beginTransactionSessionQualifier != null) {
            return beginTransactionSessionQualifier;
         }
      }
      return null;
   }

   void releaseBeginTransactionSession() {
      String sessionQualifier = null;
      try {
         sessionQualifier = internalGetSessionQualifier();
      } finally {
         if (sessionQualifier != null) {
            OdkConnectionFactorySingleton.getOdkConnectionFactoryInterface()
                .removeConnection(appName, new DbHandle(sessionQualifier));
         }
      }
   }

}
