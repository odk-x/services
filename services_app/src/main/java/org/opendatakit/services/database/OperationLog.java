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

import org.opendatakit.logging.WebLogger;
import org.sqlite.database.sqlite.SQLiteDebug;

import java.util.ArrayList;
import java.util.Locale;

/**
 * Extracted from the SQLiteDatabase class.
 * Thread-safe.
 *
 * @author mitchellsundt@gmail.com
 */
public final class OperationLog {

   private static final int MAX_RECENT_OPERATIONS = 60;
   private static final int COOKIE_GENERATION_SHIFT = 8;
   private static final int COOKIE_INDEX_MASK = 0xff;
   private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];


   private final OperationLogEntry[] mOperations = new OperationLogEntry[MAX_RECENT_OPERATIONS];

   private final String appName;
   private int mIndex;
   private int mGeneration;
  /**
   * Access only within synchronized mOperations
   *
   * tracks the number of opens in the last 65 seconds
   */
  private final int[] opens = new int[8];
  private int totalOpens = 0;
  private int lastOpenIdx = 0;

  /**
   * Access only within synchronized mOperations
   *
   * tracks the number of closes in the last 65 seconds
   */
  private final int[] closes = new int[8];
  private int totalCloses = 0;
  private int lastCloseIdx = 0;

   public OperationLog(String appName) {
      this.appName = appName;
   }

   /**
    * Invoked when AppNameSharedStateContainer becomes empty
    */
   public void clearOperations() {
      synchronized (mOperations) {
         for ( int i = 0 ; i < mOperations.length ; ++i ) {
            mOperations[i] = null;
         }
      }
   }

   public int beginOperation(String sessionQualifier, String kind, String sql, Object[] bindArgs) {
      synchronized (mOperations) {
         final int index = (mIndex + 1) % MAX_RECENT_OPERATIONS;
         OperationLogEntry operation = mOperations[index];
         if (operation == null) {
            operation = new OperationLogEntry();
            mOperations[index] = operation;
         } else {
            operation.mFinished = false;
            operation.mThrowable = null;
            if (operation.mBindArgs != null) {
               operation.mBindArgs.clear();
            }
         }
         operation.mSessionQualifier = sessionQualifier;
         operation.mStartTime = System.currentTimeMillis();
         operation.mKind = kind;
         operation.mThreadId = Thread.currentThread().getId();
         operation.mSql = sql;
         if (bindArgs != null) {
            if (operation.mBindArgs == null) {
               operation.mBindArgs = new ArrayList<Object>();
            } else {
               operation.mBindArgs.clear();
            }
            for (int i = 0; i < bindArgs.length; i++) {
               final Object arg = bindArgs[i];
               if (arg != null && arg instanceof byte[]) {
                  // Don't hold onto the real byte array longer than necessary.
                  operation.mBindArgs.add(EMPTY_BYTE_ARRAY);
               } else {
                  operation.mBindArgs.add(arg);
               }
            }
         }
         operation.mCookie = newOperationCookieLocked(index);
         mIndex = index;
         return operation.mCookie;
      }
   }

   public void failOperation(int cookie, Throwable t) {
      String logString = null;
      final OperationLogEntry operation = getOperationLocked(cookie);
      if (operation != null) {
         operation.mThrowable = t;
         logString = logOperationLocked(operation, null);
      }
      if (logString != null) {
         WebLogger.getLogger(appName).i("operationLog",
             "failOperation: " + logString);
      }
      // silently ignore if not found -- we are processing requests too fast!
   }

   public void endOperation(int cookie) {
      String logString = null;
      final OperationLogEntry operation = getOperationLocked(cookie);
      if (operation != null) {
         if (endOperationDeferLogLocked(operation)) {
            logString = logOperationLocked(operation, null);
         }
      }
      if ( logString != null ) {
         WebLogger.getLogger(appName).i("operationLog",
             "endOperation (long runtime): " + logString);
      }
      // silently ignore if not found -- we are processing requests too fast!
   }

   public void endOperationDeferLogAdditional(int cookie, String logString) {
      final OperationLogEntry operation = getOperationLocked(cookie);
      boolean shouldLog = false;
      if (operation != null) {
         shouldLog = endOperationDeferLogLocked(operation);
      }
      if ( logString != null && shouldLog ) {
         WebLogger.getLogger(appName).i("operationLog",
             "endOperation (long runtime): " + logString);
      }
      // silently ignore if not found -- we are processing requests too fast!
   }

  /**
   * Function to track the number of new connection opens within the last 65 seconds
   */
  public void tickOpen() {
    synchronized (mOperations) {
      long now = System.currentTimeMillis();
      int idx = (int) ((now & 0xE000L) >> 13);

      while (idx != lastOpenIdx) {
        lastOpenIdx = (lastOpenIdx + 1) % 8;
        opens[lastOpenIdx] = 0;
      }
      ++opens[idx];
      ++totalOpens;
    }
  }

  /**
   * Function to track the number of connection closes within the last 65 seconds
   */
  public void tickClose() {
    synchronized (mOperations) {
      long now = System.currentTimeMillis();
      int idx = (int) ((now & 0xE000L) >> 13);

      while (idx != lastCloseIdx) {
        lastCloseIdx = (lastCloseIdx + 1) % 8;
        closes[lastCloseIdx] = 0;
      }
      ++closes[idx];
      ++totalCloses;
    }
  }

   public void logOperation(int cookie, String detail) {
      final OperationLogEntry operation = getOperationLocked(cookie);
      String logString = null;
      if (operation != null) {
         logString = logOperationLocked(operation, detail);
      }
      if (logString != null) {
         WebLogger.getLogger(appName).i("operationLog", logString);
      }
      // silently ignore if not found -- we are processing requests too fast!
   }

   public String describeCurrentOperation() {
      synchronized (mOperations) {
         final OperationLogEntry operation = mOperations[mIndex];
         if (operation != null && !operation.mFinished) {
            StringBuilder msg = new StringBuilder();
            operation.describe(msg, false);
            return msg.toString();
         }
         return null;
      }
   }

   public void dump(StringBuilder b, boolean verbose) {
      synchronized (mOperations) {
        //////////////////////////////////////////////////////
        // Display a time histogram of the number of opens and closes
        // in the last 65 seconds.

        // First: update the histogram to the present time.
        long now = System.currentTimeMillis();
        int idx;

        idx = (int) ((now & 0xE000L) >> 13);
        while (idx != lastOpenIdx) {
          lastOpenIdx = (lastOpenIdx + 1) % 8;
          opens[lastOpenIdx] = 0;
        }

        idx = (int) ((now & 0xE000L) >> 13);
        while (idx != lastCloseIdx) {
          lastCloseIdx = (lastCloseIdx + 1) % 8;
          closes[lastCloseIdx] = 0;
        }

        idx = (int) ((now & 0xE000L) >> 13);
        b.append("  Last 65 seconds of open and close activity on this appName\n");
        b.append("     opens | closes\n");
        for ( int i = 0 ; i < 8 ; ++i ) {
          b.append(String.format(Locale.US, "    %1$6d   %2$6d\n", opens[idx], closes[idx]));
          idx = (idx + 7) % 8;
        }

        b.append("Total opens: ").append(totalOpens).append(" closes: ").append(totalCloses)
            .append(" currently active: ").append(totalOpens-totalCloses).append("\n\n");

        b.append("  Most recently executed operations:\n");
         int index = mIndex;
         OperationLogEntry operation = mOperations[index];
         if (operation != null) {
            int n = 0;
            do {
               b.append(" ").append(n).append(": ");
               operation.describe(b, verbose);
               b.append("\n");
               if (index > 0) {
                  index -= 1;
               } else {
                  index = MAX_RECENT_OPERATIONS - 1;
               }
               n += 1;
               operation = mOperations[index];
            } while (operation != null && n < MAX_RECENT_OPERATIONS);
         } else {
            b.append("    <none>\n");
         }
      }
   }

   /**
    * This is ONLY called within a synchronized(mOperations){} block.
    *
    * @param operation
    * @return
    */
   private boolean endOperationDeferLogLocked(OperationLogEntry operation) {
      if (operation != null) {
         if ( !operation.mFinished ) {
            operation.mEndTime = System.currentTimeMillis();
            operation.mFinished = true;
         }
         return SQLiteDebug.shouldLogSlowQuery(operation.mEndTime - operation.mStartTime);
      }
      return false;
   }

   /**
    * This is ONLY called within a synchronized(mOperations){} block.
    *
    * @param operation
    * @param detail
    */
   private String logOperationLocked(OperationLogEntry operation, String detail) {
      StringBuilder msg = new StringBuilder();
      operation.describe(msg, false);
      if (detail != null) {
         msg.append(", ").append(detail);
      }
      return msg.toString();
   }

   /**
    * This is ONLY called within a synchronized(mOperations){} block.
    *
    * @param index
    * @return
    */
   private int newOperationCookieLocked(int index) {
      final int generation = mGeneration++;
      return generation << COOKIE_GENERATION_SHIFT | index;
   }

   /**
    * This is ONLY called within a synchronized(mOperations){} block.
    *
    * @param cookie
    * @return
    */
   private OperationLogEntry getOperationLocked(int cookie) {
     final int index = cookie & COOKIE_INDEX_MASK;
     synchronized (mOperations) {
       if (index >= mOperations.length) {
         return null;
       }
       final OperationLogEntry operation = mOperations[index];
       if (operation == null) {
         return null;
       }
       return operation.mCookie == cookie ? operation : null;
     }
   }
}
