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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

/**
 * @author mitchellsundt@gmail.com
 */
final class OperationLogEntry {

   /**
    * SimpleDateFormat is not thread-safe. synchronize
    * on it before accessing it.
    */
   public static void getFormattedStartTime(StringBuilder b, long startTime) {
     // the format() return is a String overlaying a common buffer.
     // the string content can change unexpectedly. Access it only
     // within this synchronized section.
     SimpleDateFormat restrictedDateFormat =
         new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US);
     b.append(restrictedDateFormat.format(new Date(startTime)));
   }

   public long mThreadId;
   public String mSessionQualifier;
   public long mStartTime;
   public long mEndTime;
   public String mKind;
   public String mSql;
   public ArrayList<Object> mBindArgs;
   public boolean mFinished;
   public Throwable mThrowable;
   public int mCookie;

   public void describe(StringBuilder msg, boolean verbose) {
      msg.append(mKind);
      if (mFinished) {
         msg.append(" took ").append(mEndTime - mStartTime).append("ms");
      } else {
         msg.append(" started ").append(System.currentTimeMillis() - mStartTime)
             .append("ms ago");
      }
      msg.append(" - ").append(getStatus());
      msg.append("\n      threadId:").append(mThreadId)
          .append(", sessionQualifier:").append(mSessionQualifier);
      msg.append(", startTime:");
      getFormattedStartTime(msg, mStartTime);
      if (mSql != null) {
         msg.append(", sql=\"")
             .append(AppNameSharedStateContainer.trimSqlForDisplay(mSql)).append("\"");
      }
      if (verbose && mBindArgs != null && mBindArgs.size() != 0) {
         msg.append(", bindArgs=[");
         final int count = mBindArgs.size();
         for (int i = 0; i < count; i++) {
            final Object arg = mBindArgs.get(i);
            if (i != 0) {
               msg.append(", ");
            }
            if (arg == null) {
               msg.append("null");
            } else if (arg instanceof byte[]) {
               msg.append("<byte[]>");
            } else if (arg instanceof String) {
               msg.append("\"").append((String) arg).append("\"");
            } else {
               msg.append(arg);
            }
         }
         msg.append("]");
      }
      if (mThrowable != null) {
         msg.append("\n      throwable=\"").append(mThrowable.getMessage()).append("\"");
         msg.append("\n--------begin stacktrace----------\n");
         {
            ByteArrayOutputStream ba = new ByteArrayOutputStream();
            PrintStream w;
            try {
               w = new PrintStream(ba, false, "UTF-8");
               mThrowable.printStackTrace(w);
               w.flush();
               w.close();
               msg.append(ba.toString("UTF-8")).append("\n--------end stacktrace----------");
            } catch (UnsupportedEncodingException e1) {
               // error if it ever occurs
               throw new IllegalStateException("unable to specify UTF-8 Charset!");
            }
         }
      }
   }

   private String getStatus() {
      if (!mFinished) {
         return "running";
      }
      return mThrowable != null ? "failed" : "succeeded";
   }
}
