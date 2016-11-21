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

package org.sqlite.database.sqlite;

/**
 * Encapsulates the management of software-only transactions.
 * Providing thread-safe management of this state.
 */
final class SQLiteTransactionManager {

   enum TransactionOutcome { NO_ACTION, COMMIT_ACTION, ROLLBACK_ACTION }

   /**
    * Track nested (software-only) transactions
    */
   private static final class Transaction {
      public Transaction mParent;
      public int mMode;
      public boolean mMarkedSuccessful;
      public boolean mChildFailed;
   }

   // reusable list of transaction objects
   private Transaction mTransactionPool;
   // managed stack of in-progress transactions.
   // only the outermost transaction is committed.
   // inner transactions do not actually do anything.
   private Transaction mTransactionStack;

   SQLiteTransactionManager() {
   }

   /**
    * Returns true if the session has a transaction in progress.
    *
    * @return True if the session has a transaction in progress.
    */
   boolean hasTransaction() {
      synchronized (this) {
         return mTransactionStack != null;
      }
   }
   /**
    * Marks the current transaction as successful. Do not do any more database work between
    * calling this and calling endTransaction. Do as little non-database work as possible in that
    * situation too. If any errors are encountered between this and endTransaction the transaction
    * will still be committed.
    *
    * @throws IllegalStateException if the current thread is not in a transaction or the
    * transaction is already marked as successful.
    *
    * Marks the current transaction as having completed successfully.
    * <p>
    * This method can be called at most once between {@link
    * SQLiteConnection#beginTransactionNonExclusive} and
    * {@link #endTransaction} to indicate that the changes made by the transaction should be
    * committed.  If this method is not called, the changes will be rolled back
    * when the transaction is ended.
    * </p>
    *
    * @throws IllegalStateException if there is no current transaction, or if
    * {@link #setTransactionSuccessful} has already been called for the current transaction.
    *
    * @see SQLiteConnection#beginTransactionNonExclusive
    * @see #endTransaction
    */
   void setTransactionSuccessful() {
      synchronized (this) {
         if (mTransactionStack == null) {
            throw new IllegalStateException("Cannot perform this operation because "
                + "there is no current transaction.");
         }
         if (mTransactionStack != null && mTransactionStack.mMarkedSuccessful) {
            throw new IllegalStateException("Cannot perform this operation because "
                + "the transaction has already been marked successful.  The only "
                + "thing you can do now is call endTransaction().");
         }

         mTransactionStack.mMarkedSuccessful = true;
      }
   }

   /**
    * Enqueue a transaction record
    *
    * @param transactionMode
    * @return true if a database-layer transaction should be created
    */
   boolean beginTransaction(int transactionMode) {
      synchronized (this) {
         if (mTransactionStack != null && mTransactionStack.mMarkedSuccessful) {
            throw new IllegalStateException("Cannot perform this operation because "
                + "the transaction has already been marked successful.  The only "
                + "thing you can do now is call endTransaction().");
         }

         boolean wasEmpty = (mTransactionStack == null);

         // push a transaction-begin record onto the active-transaction list
         Transaction transaction = mTransactionPool;
         if (transaction != null) {
            mTransactionPool = transaction.mParent;
            transaction.mParent = null;
            transaction.mMarkedSuccessful = false;
            transaction.mChildFailed = false;
         } else {
            transaction = new Transaction();
         }
         transaction.mMode = transactionMode;

         transaction.mParent = mTransactionStack;
         mTransactionStack = transaction;

         return wasEmpty;
      }
   }

   /**
    * Invoked when the attempt to begin a database layer transaction
    * fails. We need to undo the recorded top-level transaction.
    */
   void cancelTransaction() {
      synchronized (this) {
         if ( mTransactionStack == null ) {
            throw new IllegalStateException("Cannot perform this operation because "
                + "there is no current transaction.");
         }
         if (mTransactionStack.mParent != null) {
            throw new IllegalStateException("Cannot perform this operation because "
                + "the transaction is not a top-level transaction (logic error!).");
         }
         final Transaction top = mTransactionStack;
         mTransactionStack = top.mParent;

         // put it back on the re-use queue
         top.mParent = mTransactionPool;
         mTransactionPool = top;
      }
   }

   TransactionOutcome endTransaction() {
      synchronized (this) {
         if ( mTransactionStack == null ) {
            throw new IllegalStateException("Cannot perform this operation because "
                + "there is no current transaction.");
         }

         final Transaction top = mTransactionStack;
         boolean successful = (top.mMarkedSuccessful) && !top.mChildFailed;

         mTransactionStack = top.mParent;

         // put it back on the re-use queue
         top.mParent = mTransactionPool;
         mTransactionPool = top;

         if (mTransactionStack != null) {
            if (!successful) {
               mTransactionStack.mChildFailed = true;
            }
            return TransactionOutcome.NO_ACTION;
         } else {
            if (successful) {
               return TransactionOutcome.COMMIT_ACTION;
            } else {
               return TransactionOutcome.ROLLBACK_ACTION;
            }
         }
      }
   }
}
