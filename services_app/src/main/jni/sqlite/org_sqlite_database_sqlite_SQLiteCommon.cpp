/*
 * Copyright (C) 2011 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
** Modified to support SQLite extensions by the SQLite developers: 
** sqlite-dev@sqlite.org.
*/

#define LOG_TAG "SQLiteCommon"

#include <cstddef>
#include <unistd.h>
#include <pthread.h>
#include <string>
#include <sstream>
#include <iomanip>
#include <vector>
#include <map>
#include <android/log.h>

#include "sqlite3.h"
#include "MutexRegion.h"
#include "ScopedLocalRef.h"

#include "org_sqlite_database_sqlite_SQLiteCommon.h"
#include "org_sqlite_database_sqlite_SQLiteConnection.h"

#if __cplusplus < 201103L

#warning --------------compiling with obsolete (pre C++ 11) configuration

#ifndef nullptr
#error add -Dnullptr=0 to compiler options
#endif

#elif __cplusplus == 201103L

#if __STRICT_ANSI__

#warning --------------compiling with (Std C++ 11) configuration

#else

#warning --------------compiling with advanced (GNU C++ 11) configuration

#endif // __STRICT_ANSI__
#endif // __cplusplus

// wrapper for avoiding android log printf formatting
#define LOGIMPL(CONTENT, TAG, LEVEL) { std::stringstream strStream; strStream << CONTENT; __android_log_print(LEVEL, TAG, "%s", strStream.str().c_str()); }
#define LOGV(CONTENT) LOGIMPL(CONTENT, LOG_TAG, ANDROID_LOG_VERBOSE);
#define LOGI(CONTENT) LOGIMPL(CONTENT, LOG_TAG, ANDROID_LOG_INFO);
#define LOGW(CONTENT) LOGIMPL(CONTENT, LOG_TAG, ANDROID_LOG_WARN);
#define LOGE(CONTENT) LOGIMPL(CONTENT, LOG_TAG, ANDROID_LOG_ERROR);

static pthread_mutex_t g_init_mutex;
static jclass objectClass;
static jclass stringClass;
static jclass longClass;
static jmethodID boxLong;
static jclass doubleClass;
static jmethodID boxDouble;

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6) != JNI_OK) {
        return -1;
    }
    if ( pthread_mutex_init(&g_init_mutex, nullptr) != 0 ) {
        return -1;
    }

    jclass lcl_objectClass(env->FindClass("java/lang/Object"));
    jclass lcl_stringClass(env->FindClass("java/lang/String"));
    jclass lcl_longClass(env->FindClass("java/lang/Long"));
    jclass lcl_doubleClass(env->FindClass("java/lang/Double"));

    objectClass = reinterpret_cast<jclass>(env->NewGlobalRef(lcl_objectClass));
    stringClass = reinterpret_cast<jclass>(env->NewGlobalRef(lcl_stringClass));
    longClass = reinterpret_cast<jclass>(env->NewGlobalRef(lcl_longClass));
    doubleClass = reinterpret_cast<jclass>(env->NewGlobalRef(lcl_doubleClass));

    boxLong = env->GetStaticMethodID(longClass, "valueOf", "(J)Ljava/lang/Long;");
    boxDouble = env->GetStaticMethodID(doubleClass, "valueOf", "(D)Ljava/lang/Double;");

    return JNI_VERSION_1_6;
}

namespace org_opendatakit {

    static int jniThrowException(JNIEnv *env, const char *className, const char *msg);

    // ensure that a jlong is 64 bits (8 bytes) long
    extern char __JLONG_IS_64__[1/((sizeof(jlong)==8)?1:0)];

    static void stream_jlong(std::ostream& out, jlong& data) {
  
        union {
            uint64_t v64;
            uint32_t v32[2];
        } v;
        v.v64 = data;
        if ( v.v32[0] != 0 ) {
            out << v.v32[0] << ":";
        }
        out << v.v32[1];
    }

    // Limit heap to 8MB for now.  This is 4 times the maximum cursor window
    // size, as has been used by the original code in SQLiteDatabase for
    // a long time.
    const int SOFT_HEAP_LIMIT = 8 * 1024 * 1024;

    /* Busy timeout in milliseconds.
     * If another connection (possibly in another process) has the database locked for
     * longer than this amount of time then SQLite will generate a SQLITE_BUSY error.
     * The SQLITE_BUSY error is then raised as a SQLiteDatabaseLockedException.
     *
     * In ordinary usage, busy timeouts are quite rare.  Most databases only ever
     * have a single open connection at a time unless they are using WAL.  When using
     * WAL, a timeout could occur if one connection is busy performing an auto-checkpoint
     * operation.  The busy timeout needs to be long enough to tolerate slow I/O write
     * operations but not so long as to cause the application to hang indefinitely if
     * there is a problem acquiring a database lock.
     */
    const int BUSY_TIMEOUT_MS = 2500;

    const int CONNECTION_ACTIVE = 1;
    const int CONNECTION_DELETE_PENDING = 2;

    struct SQLiteConnection {
        // this may or may not be reentrant. Don't think we need reentrancy
        pthread_mutex_t mutex;
        // number of attempts to access connection
        int refCount;
        // status: one of 0, CONNECTION_ACTIVE or (CONNECTION_ACTIVE | CONNECTION_DELETE_PENDING)
        int status;
        // non-zero if the current action should be cancelled
        volatile int cancelled;
        char * pathStr;
        char * labelStr;
        sqlite3 *db;

        SQLiteConnection(JNIEnv* env, const char* szPath, sqlite3 *dbArg, const char* szLabel) {
            if ( pthread_mutex_init(&mutex, nullptr) != 0 ) {
                jniThrowException(env,
		      "org/sqlite/database/sqlite/SQLiteException",
		      "Unable to initialize connection");
	    }
            refCount = 0;
            status = 0;
            cancelled = 0;
            pathStr = strdup(szPath);
            labelStr = strdup(szLabel);
            db = dbArg;
        }

        ~SQLiteConnection() {
            free(pathStr);
            free(labelStr);
        }
    };

    typedef struct SQLiteConnection SQLiteConnection;

    // Called each time a message is logged.
    void sqliteLogCallback(void *data, int iErrCode, const char *zMsg) {

        bool verboseLog = !!data;
        if (iErrCode == 0 || iErrCode == SQLITE_CONSTRAINT || iErrCode == SQLITE_SCHEMA) {
            if (verboseLog) {
                LOGV("(" << iErrCode << ") " << zMsg);
            }
        } else {
            LOGE("(" << iErrCode << ") " << zMsg);
        }
    }

    // this is initialized within the above mutex
    static bool initialized = false;

    // protected by above mutex
    static jlong commonIdCounter = 0L;

    // protected by above mutex
    static std::map<jlong, SQLiteConnection*> activeConnections;

    static std::map<jlong, sqlite3_stmt*> activeStatements;

    // A smart pointer that deletes a JNI local reference when it goes out of scope.
    class ActiveConnection {
    public:
        ActiveConnection(jlong connectionPtr) :
                mConnection(nullptr), mConnectionPtr(connectionPtr), mMutex(nullptr) {
            pid_t tid = getpid();
            // gain global mutex to test and manipulate:
            //     activeConnections
            //     mConnection->status
            //     mConnection->refCount
            //
            {
                MutexRegion guard = MutexRegion(&g_init_mutex);

                std::map<jlong, SQLiteConnection*>::iterator found = activeConnections.find(connectionPtr);
                if (found != activeConnections.end()) {
                    // connection is in the list of active connections
                    mConnection = found->second;
                    if (mConnection->status & CONNECTION_DELETE_PENDING) {
                        // don't let new requests get the connection if it is pending deletion
                        LOGE("ActiveConnection(delete): tid " << tid << " Fetch of delete-pending connection ";
                 stream_jlong(strStream, mConnectionPtr);
                 strStream << " from map -- should already have been removed!");
                        mConnection = nullptr;
                        return;
                    }
                    ++(mConnection->refCount);
                    mConnection->status |= CONNECTION_ACTIVE;
                }
            }
            // this would block waiting for the other holder to release the mutex
            mMutex = new MutexRegion(&mConnection->mutex);
        }
        ActiveConnection(jlong connectionPtr, int cancellation) :
                mConnection(nullptr), mConnectionPtr(connectionPtr), mMutex(nullptr) {
            pid_t tid = getpid();
            // gain global mutex to test and manipulate:
            //     activeConnections
            //     mConnection->status
            //     mConnection->refCount
            //
            {
                MutexRegion guard = MutexRegion(&g_init_mutex);

                std::map<jlong, SQLiteConnection*>::iterator found = activeConnections.find(connectionPtr);
                if (found != activeConnections.end()) {
                    // connection is in the list of active connections
                    mConnection = found->second;
                    if (mConnection->status & CONNECTION_DELETE_PENDING) {
                        // don't let new requests get the connection if it is pending deletion
                        LOGE("ActiveConnection(delete): tid " << tid << " Fetch of delete-pending connection ";
                stream_jlong(strStream, mConnectionPtr);
                strStream << " from map -- should already have been removed!");
                        mConnection = nullptr;
                        return;
                    }
                    mConnection->cancelled = cancellation;
                    if ( cancellation == 0 ) {
                        // we are resetting a cancellation request
                        ++(mConnection->refCount);
                        mConnection->status |= CONNECTION_ACTIVE;
                    } else {
                        // we are making a cancellation request
                        // don't give out this value...
                        mConnection = nullptr;
                    }
                }
            }
            if ( cancellation == 0 ) {
                // this would block waiting for the other holder to release the mutex
                mMutex = new MutexRegion(&mConnection->mutex);
            }
        }


        ActiveConnection(jlong connectionPtr, bool ignored) :
                mConnection(nullptr), mConnectionPtr(connectionPtr), mMutex(nullptr) {
            pid_t tid = getpid();
            // gain global mutex to test and manipulate:
            //     activeConnections
            //     mConnection->status
            //     mConnection->refCount
            //
            {
                MutexRegion guard = MutexRegion(&g_init_mutex);

                std::map<jlong, SQLiteConnection*>::iterator found = activeConnections.find(connectionPtr);
                if (found != activeConnections.end()) {
                    // connection is in the list of active connections
                    mConnection = found->second;
                    if (mConnection->status & CONNECTION_DELETE_PENDING) {
                        // don't let new requests get the connection if it is pending deletion
                        LOGE("ActiveConnection(delete): tid " << tid << " Fetch of delete-pending connection ";
                stream_jlong(strStream, mConnectionPtr);
                strStream << " from map -- should already have been removed!");
                        mConnection = nullptr;
                        return;
                    }
                    ++(mConnection->refCount);
                    mConnection->status |= CONNECTION_ACTIVE | CONNECTION_DELETE_PENDING;
                    activeConnections.erase(mConnectionPtr);
                }
            }
            // this would block waiting for the other holder to release the mutex
            mMutex = new MutexRegion(&mConnection->mutex);
        }

        ~ActiveConnection() {
            pid_t tid = getpid();
            bool shouldDelete = false;
            {
                MutexRegion guard = MutexRegion(&g_init_mutex);

                if (mConnection != nullptr) {
                    --(mConnection->refCount);
                    if (mConnection->refCount == 0) {
                        mConnection->status &= ~CONNECTION_ACTIVE;

                        if (mConnection->status == CONNECTION_DELETE_PENDING) {
                            activeConnections.erase(mConnectionPtr);
                            shouldDelete = true;
                            LOGE("~ActiveConnection: tid " << tid << " Removing delete-pending connection ";
                    stream_jlong(strStream, mConnectionPtr);
                    strStream << " from map -- should already have been removed!");
                        }
                    }
                }
            }

            if ( mMutex != nullptr ) {
                delete mMutex;
            }

            if ( shouldDelete && mConnection != nullptr ) {
                delete mConnection;
                LOGW("~ActiveConnection: tid " << tid << " delete Connection ";
            stream_jlong(strStream, mConnectionPtr));
            }
        }

        SQLiteConnection* get() const {
            return mConnection;
        }

    private:
        jlong mConnectionPtr;
        SQLiteConnection* mConnection;
        MutexRegion * mMutex;

        // disallow copy
        ActiveConnection(const ActiveConnection&) {};

        // disallow assign
        void operator=(const ActiveConnection&) {};
    };

    class LogRegion {
        const char * method;

    public:
        ~LogRegion() {
            // LOGI(method << " -- left");
        }

        LogRegion(const char * methodName) : method(methodName) {
            // LOGI(method << " -- entered");
        }
    };

    static sqlite3_stmt* getActiveStatement(jlong statementId) {
        MutexRegion guard(&g_init_mutex);
        std::map<jlong,sqlite3_stmt*>::iterator found = activeStatements.find(statementId);
        if ( found != activeStatements.end() ) {
            return found->second;
        }
        return nullptr;
    }

    static jlong registerActiveStatement(sqlite3_stmt* statement) {
        MutexRegion guard(&g_init_mutex);
        jlong statementId = ++commonIdCounter;
        activeStatements[statementId] = statement;
        return statementId;
    }

    static void removeActiveStatement(jlong statementId) {
        MutexRegion guard(&g_init_mutex);
        std::map<jlong,sqlite3_stmt*>::iterator found = activeStatements.find(statementId);
        if ( found != activeStatements.end() ) {
            activeStatements.erase(found);
        } else {
            pid_t tid = getpid();
            LOGE("removeActiveStatement tid " << tid << " -- did not find statement ";
         stream_jlong(strStream, statementId));
        }
    }

    // Sets the global SQLite configuration.
    // This must be called before any other SQLite functions are called.
    void sqliteInitialize(JNIEnv *env) {
        LogRegion rgn("sqliteInitialize");

        pid_t tid = getpid();
        LOGI("sqliteInitialize tid " << tid << " -- entered");

        MutexRegion guard(&g_init_mutex);
        LOGI("sqliteInitialize tid " << tid << " -- gained mutex");

        if (!initialized) {
            LOGW("sqliteInitialize tid " << tid << " -- executing sqlite3_config statements");

            // Enable multi-threaded mode.  In this mode, SQLite is safe to use by multiple
            // threads as long as no two threads use the same database connection at the same
            // time (which we guarantee in the SQLite database wrappers).
            // sqlite3_config(SQLITE_CONFIG_MULTITHREAD);

            // Redirect SQLite log messages to the Android log.
#if 0
            bool verboseLog = android_util_Log_isVerboseLogEnabled(SQLITE_LOG_TAG);
#endif
            bool verboseLog = false;
            void *verboseLogging = (void *) 1L;
            void *quietLogging = (void *) 0L;
            sqlite3_config(SQLITE_CONFIG_LOG, &sqliteLogCallback,
                           verboseLog ? verboseLogging : quietLogging);

            // The soft heap limit prevents the page cache allocations from growing
            // beyond the given limit, no matter what the max page cache sizes are
            // set to. The limit does not, as of 3.5.0, affect any other allocations.
            sqlite3_soft_heap_limit(SOFT_HEAP_LIMIT);

            // Initialize SQLite.
            sqlite3_initialize();

            // finally,
            initialized = true;
        }
        LOGI("sqliteInitialize tid " << tid << " -- done!");
    }

    static int jniThrowException(JNIEnv *env, const char *className, const char *msg) {
        if (env->ExceptionCheck()) {
            return -2;
        }

        ScopedLocalRef<jclass> exceptionClass(env, env->FindClass(className));
        if (exceptionClass.get() == nullptr) {
            LOGE("Unable to find exception class " << className);
            /* ClassNotFoundException now pending */
            return -1;
        }

        if (env->ThrowNew(exceptionClass.get(), msg) != JNI_OK) {
            LOGE("Failed throwing '" << className << "' '" << msg << "'");
            /* an exception, most likely OOM, will now be pending */
            return -1;
        }

        return 0;
    }

    /* map the error code to a Java exception class */
    static const char * getExceptionClass(int errcode) {
        const char *exceptionClass;
        switch (errcode & 0xff) { /* mask off extended error code */
            case SQLITE_IOERR:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteDiskIOException";
                break;
            case SQLITE_CORRUPT:
            case SQLITE_NOTADB: // treat "unsupported file format" error as corruption also
                exceptionClass = "org/sqlite/database/sqlite/SQLiteDatabaseCorruptException";
                break;
            case SQLITE_CONSTRAINT:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteConstraintException";
                break;
            case SQLITE_ABORT:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteAbortException";
                break;
            case SQLITE_DONE:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteDoneException";
                break;
            case SQLITE_FULL:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteFullException";
                break;
            case SQLITE_MISUSE:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteMisuseException";
                break;
            case SQLITE_PERM:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteAccessPermException";
                break;
            case SQLITE_BUSY:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteDatabaseLockedException";
                break;
            case SQLITE_LOCKED:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteTableLockedException";
                break;
            case SQLITE_READONLY:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteReadOnlyDatabaseException";
                break;
            case SQLITE_CANTOPEN:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteCantOpenDatabaseException";
                break;
            case SQLITE_TOOBIG:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteBlobTooBigException";
                break;
            case SQLITE_RANGE:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteBindOrColumnIndexOutOfRangeException";
                break;
            case SQLITE_NOMEM:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteOutOfMemoryException";
                break;
            case SQLITE_MISMATCH:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteDatatypeMismatchException";
                break;
            case SQLITE_INTERRUPT:
                exceptionClass = "android/os/OperationCanceledException";
                break;
            default:
                exceptionClass = "org/sqlite/database/sqlite/SQLiteException";
                break;
        }
        return exceptionClass;
    }

    /* throw a SQLiteException with a message appropriate for the error in handle
       concatenated with the given message
     */
    static void throw_sqlite3_exception_db(JNIEnv *env,
                                           jlong connectionPtr, SQLiteConnection * connection,
                                           const char *message) {
        pid_t tid = getpid();
        std::ostringstream stringStream;

        // get the error code and message from the SQLite connection
        // the error message may contain more information than the error code
        // because it is based on the extended error code rather than the simplified
        // error code that SQLite normally returns.
        int extendedErrCode = sqlite3_extended_errcode(connection->db);
        const char *extendedMsg = sqlite3_errmsg(connection->db);

        stringStream << " tid " << tid << " connection ";
    stream_jlong(stringStream, connectionPtr);
        stringStream << " '" << connection->labelStr << "' ";
        if ( extendedMsg != nullptr ) {
            stringStream << extendedMsg << " ";
        }

        stringStream << "(extendedErrCode " << extendedErrCode << ")";
        if ( message != nullptr ) {
            stringStream << " " << message;
        }

        jniThrowException(env, getExceptionClass(extendedErrCode), stringStream.str().c_str());
    }

    static void throw_sqlite3_open_exception_db(JNIEnv *env, const char* label, sqlite3* db,
                                           const char *message) {
        pid_t tid = getpid();
        std::ostringstream stringStream;

        // get the error code and message from the SQLite connection
        // the error message may contain more information than the error code
        // because it is based on the extended error code rather than the simplified
        // error code that SQLite normally returns.
        int extendedErrCode = sqlite3_extended_errcode(db);
        const char *extendedMsg = sqlite3_errmsg(db);

        stringStream << " tid " << tid << " openConnection '" << label << "' ";
        if ( extendedMsg != nullptr ) {
            stringStream << extendedMsg << " ";
        }

        stringStream << "(extendedErrCode " << extendedErrCode << ")";
        if ( message != nullptr ) {
            stringStream << " " << message;
        }

        jniThrowException(env, getExceptionClass(extendedErrCode), stringStream.str().c_str());
    }

    /* throw a SQLiteException for a given error code
     * should only be used when the database connection is not available because the
     * error information will not be quite as rich */
    static void throw_sqlite3_open_exception_errcode(JNIEnv *env, const char* label,
                                                int errcode, const char *message) {
        pid_t tid = getpid();
        std::ostringstream stringStream;

        stringStream << " tid " << tid << " openConnection '" << label
                     << "' (code " << errcode << ")";
        if ( message != nullptr ) {
            stringStream << " " << message;
        }

        jniThrowException(env, getExceptionClass(errcode), stringStream.str().c_str());
    }

    // Called each time a statement begins execution, when tracing is enabled.
    static void sqliteTraceCallback(void *data, const char *sql) {
        SQLiteConnection *connection = static_cast<SQLiteConnection *>(data);

        LOGIMPL(connection->labelStr << ": \"" << sql << "\"", SQLITE_TRACE_TAG, ANDROID_LOG_VERBOSE);
    }

    // Called each time a statement finishes execution, when profiling is enabled.
    static void sqliteProfileCallback(void *data, const char *sql, sqlite3_uint64 tm) {
        SQLiteConnection *connection = static_cast<SQLiteConnection *>(data);
        double ms = 0.000001 * tm;

        LOGIMPL(connection->labelStr << ": \"" << sql << "\"took " << ms << " ms", SQLITE_PROFILE_TAG, ANDROID_LOG_VERBOSE);
    }

    // Called after each SQLite VM instruction when cancellation is enabled.
    static int sqliteProgressHandlerCallback(void *data) {
        SQLiteConnection *connection = static_cast<SQLiteConnection *>(data);
        return connection->cancelled;
    }

    /*
    ** This function is a collation sequence callback equivalent to the built-in
    ** BINARY sequence.
    **
    ** Stock Android uses a modified version of sqlite3.c that calls out to a module
    ** named "sqlite3_android" to add extra built-in collations and functions to
    ** all database handles. Specifically, collation sequence "LOCALIZED". For now,
    ** this module does not include sqlite3_android (since it is difficult to build
    ** with the NDK only). Instead, this function is registered as "LOCALIZED" for all
    ** new database handles.
    */
    static int coll_localized(
            void *not_used,
            int nKey1, const void *pKey1,
            int nKey2, const void *pKey2
    ) {
        int rc, n;
        n = nKey1 < nKey2 ? nKey1 : nKey2;
        rc = memcmp(pKey1, pKey2, n);
        if (rc == 0) {
            rc = nKey1 - nKey2;
        }
        return rc;
    }

    jlong openConnection(JNIEnv *env,
                         jstring pathStr, jint openFlags, jstring labelStr,
                         jboolean enableTrace, jboolean enableProfile) {
        LogRegion rgn("openConnection");

        if ( pathStr == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "pathStr value is null");
            return 0L;
        }

        if ( labelStr == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "labelStr value is null");
            return 0L;
        }

        const char* path = env->GetStringUTFChars(pathStr, nullptr);

        if ( path == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Unable to access String pathStr value");
            return 0L;
        }

        const char* label = env->GetStringUTFChars(labelStr, nullptr);

        if ( label == nullptr ) {
            env->ReleaseStringUTFChars(pathStr, path);

            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Unable to access String labelStr value");
            return 0L;
        }

        pid_t tid = getpid();

        int sqliteFlags;

        if (openFlags & org_sqlite_database_sqlite_SQLiteConnection_CREATE_IF_NECESSARY) {
            sqliteFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
        } else {
            sqliteFlags = SQLITE_OPEN_READWRITE;
        }

        sqlite3 *db;
        int err = sqlite3_open_v2(path, &db,
                                  sqliteFlags, nullptr);
        if (err != SQLITE_OK) {
            env->ReleaseStringUTFChars(pathStr, path);
            env->ReleaseStringUTFChars(labelStr, label);

            LOGE("openConnection tid " << tid << " -- failed sqlite3_open_v2 with label '" << label << "'");
            throw_sqlite3_open_exception_errcode(env, label, err, "Could not open database");
            return 0L;
        }
        err = sqlite3_create_collation(db, "localized", SQLITE_UTF8, nullptr,
                                       coll_localized);
        if (err != SQLITE_OK) {
            env->ReleaseStringUTFChars(pathStr, path);
            env->ReleaseStringUTFChars(labelStr, label);

            LOGE("openConnection tid " << tid << " -- failed sqlite3_create_collation with label '" << label << "'");
            throw_sqlite3_open_exception_db(env, label, db, "Could not register collation");
            sqlite3_close_v2(db);
            return 0L;
        }

        // Check that the database is really read/write when that is what we asked for.
        if ((sqliteFlags & SQLITE_OPEN_READWRITE) && sqlite3_db_readonly(db, nullptr)) {
            env->ReleaseStringUTFChars(pathStr, path);
            env->ReleaseStringUTFChars(labelStr, label);

            LOGE("openConnection tid " << tid << " -- failed sqlite3_db_readonly with label '" << label << "'");
            throw_sqlite3_open_exception_db(env, label, db, "Could not open the database in read/write mode.");
            sqlite3_close_v2(db);
            return 0L;
        }

        // Set the default busy handler to retry automatically before returning SQLITE_BUSY.
        err = sqlite3_busy_timeout(db, BUSY_TIMEOUT_MS);
        if (err != SQLITE_OK) {
            env->ReleaseStringUTFChars(pathStr, path);
            env->ReleaseStringUTFChars(labelStr, label);

            LOGE("openConnection tid " << tid << " -- failed sqlite3_busy_timeout with label '" << label << "'");
            throw_sqlite3_open_exception_db(env, label, db, "Could not set busy timeout");
            sqlite3_close_v2(db);
            return 0L;
        }

        // Create wrapper object.
        SQLiteConnection *connection = new SQLiteConnection(env, path, db, label);

        // Enable tracing and profiling if requested.
        if (enableTrace) {
            sqlite3_trace(db, &sqliteTraceCallback, connection);
        }
        if (enableProfile) {
            sqlite3_profile(db, &sqliteProfileCallback, connection);
        }

        jlong connectionId;
        {
            MutexRegion guard = MutexRegion(&g_init_mutex);
            connectionId = ++commonIdCounter;
            activeConnections[connectionId] = connection;
        }

        LOGI("openConnection tid " << tid << " returns: connection ";
        stream_jlong(strStream, connectionId);
        strStream << " '" << label << "'");

        env->ReleaseStringUTFChars(pathStr, path);
        env->ReleaseStringUTFChars(labelStr, label);

        return connectionId;
    }

    void closeConnection(JNIEnv *env, jlong connectionPtr) {
        LogRegion rgn("closeConnection");

        pid_t tid = getpid();

        ActiveConnection connection(connectionPtr, true);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        int err = sqlite3_close_v2(connection.get()->db);
        if (err != SQLITE_OK) {
            // This can happen if sub-objects aren't closed first.  Make sure the caller knows.
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(), "Unable to close db.");
            return;
        }

        LOGI("closeConnection tid " << tid << " connection ";
        stream_jlong(strStream, connectionPtr);
        strStream << " '" << connection.get()->labelStr << "'");
    }

    jlong prepareStatement(JNIEnv *env, jlong connectionPtr, jstring sqlString) {
        LogRegion rgn("prepareStatement");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return 0L;
        }

        if ( sqlString == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "sqlString value is null");
            return 0L;
        }

        sqlite3_stmt *stmt = nullptr;

        jsize sqlLength = env->GetStringLength(sqlString);
        const jchar *sql = env->GetStringChars(sqlString, nullptr);

        if ( sql == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Unable to access String sql");
            return 0L;
        }

        int err = sqlite3_prepare16_v2(connection.get()->db,
                                       sql, sqlLength * sizeof(jchar), &stmt, nullptr);

        env->ReleaseStringChars(sqlString, sql);

        if (err != SQLITE_OK) {
            // Error messages like 'near ")": syntax error' are not
            // always helpful enough, so construct an error string that
            // includes the query itself.
            std::ostringstream stringStream;
            // sql string needs to be re-fetched as UTF-8 for error message construction.
            const char* sql = env->GetStringUTFChars(sqlString, nullptr);
            stringStream << ", while compiling: " << sql;
            env->ReleaseStringUTFChars(sqlString, sql);
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(), stringStream.str().c_str());
            return 0L;
        }

        jlong statementPtr = registerActiveStatement(stmt);
        return statementPtr;
    }

    void finalizeStatement(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("finalizeStatement");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return;
        }

        // We ignore the result of sqlite3_finalize because it is really telling us about
        // whether any errors occurred while executing the statement.  The statement itself
        // is always finalized regardless.
        sqlite3_finalize(statement);
        // and forget about this statement...
        removeActiveStatement(statementPtr);
    }

    jint bindParameterCount(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("bindParameterCount");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return 0;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return 0;
        }

        return sqlite3_bind_parameter_count(statement);
    }

    jboolean statementIsReadOnly(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("statementIsReadOnly");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return JNI_TRUE;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return JNI_TRUE;
        }

        return sqlite3_stmt_readonly(statement) ? JNI_TRUE : JNI_FALSE;
    }

    void bindNull(JNIEnv *env, jlong connectionPtr, jlong statementPtr, int index) {
        LogRegion rgn("bindNull");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return;
        }

        int err = sqlite3_bind_null(statement, index);

        if (err != SQLITE_OK) {
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(), "Error while binding null value");
        }
    }

    void bindLong(JNIEnv *env, jlong connectionPtr, jlong statementPtr, int index,
                  jlong value) {
        LogRegion rgn("bindLong");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return;
        }

        int err = sqlite3_bind_int64(statement, index, value);

        if (err != SQLITE_OK) {
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(), "Error while binding long value");
        }
    }


    void bindDouble(JNIEnv *env, jlong connectionPtr, jlong statementPtr, int index,
                    jdouble value) {
        LogRegion rgn("bindDouble");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return;
        }

        int err = sqlite3_bind_double(statement, index, value);

        if (err != SQLITE_OK) {
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(), "Error while binding double value");
        }
    }

    void bindString(JNIEnv *env, jlong connectionPtr, jlong statementPtr, int index,
                    jstring valueString) {
        LogRegion rgn("bindString");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);

        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return;
        }

        if ( valueString == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "bindString value is null");
            return;
        }

        jsize valueLength = env->GetStringLength(valueString);
        const jchar* value = env->GetStringChars(valueString, nullptr);

        if ( value == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Unable to access String value");
            return;
        }

        int err = sqlite3_bind_text16(statement, index, value, valueLength * sizeof(jchar),
                                      SQLITE_TRANSIENT);

        env->ReleaseStringChars(valueString, value);

        if (err != SQLITE_OK) {
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(), "Error while binding string value");
        }
    }

    void bindBlob(JNIEnv *env, jlong connectionPtr, jlong statementPtr, int index,
                  jbyteArray valueArray) {
        LogRegion rgn("bindBlob");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return;
        }

        if ( valueArray == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "bindBlob value is null");
            return;
        }

        jsize valueLength = env->GetArrayLength(valueArray);
        jbyte* value = env->GetByteArrayElements(valueArray, nullptr);

        if ( value == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Unable to access byte[] value");
            return;
        }

        int err = sqlite3_bind_blob(statement, index, value, valueLength, SQLITE_TRANSIENT);

        env->ReleaseByteArrayElements(valueArray, value, JNI_ABORT);

        if (err != SQLITE_OK) {
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(), "Error while binding blob value");
        }
    }

    void resetAndClearBindings(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("resetAndClearBindings");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return;
        }

        int err = sqlite3_reset(statement);
        if (err == SQLITE_OK) {
            err = sqlite3_clear_bindings(statement);
        }

        if (err != SQLITE_OK) {
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(), "Error during resetAndClearBindings");
        }
    }

    static int internalExecuteNonQuery(JNIEnv *env, jlong connectionPtr, SQLiteConnection *connection, sqlite3_stmt *statement) {
        int err = sqlite3_step(statement);
        if (err == SQLITE_ROW) {
            throw_sqlite3_exception_db(env, connectionPtr, connection,
                     "Queries can be performed using SQLiteDatabase query or rawQuery methods only.");
        } else if (err != SQLITE_DONE) {
            throw_sqlite3_exception_db(env, connectionPtr, connection,
                     "Requested command did not complete!");
        }
        return err;
    }

    void executeNonQuery(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("executeNonQuery");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return;
        }

        internalExecuteNonQuery(env, connectionPtr, connection.get(), statement);
    }

    jint executeForChangedRowCount(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("executeForChangedRowCount");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return 0;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return 0;
        }

        int err = internalExecuteNonQuery(env, connectionPtr, connection.get(), statement);
        return err == SQLITE_DONE ? sqlite3_changes(connection.get()->db) : -1;
    }

    jlong executeForLastInsertedRowId(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("executeForLastInsertedRowId");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return -1L;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return -1L;
        }

        int err = internalExecuteNonQuery(env, connectionPtr, connection.get(), statement);
        return err == SQLITE_DONE && sqlite3_changes(connection.get()->db) > 0
               ? sqlite3_last_insert_rowid(connection.get()->db) : -1L;
    }

    static int executeOneRowQuery(JNIEnv *env, jlong connectionPtr, SQLiteConnection *connection, sqlite3_stmt *statement) {

        int err = sqlite3_step(statement);
        if (err != SQLITE_ROW) {
            throw_sqlite3_exception_db(env, connectionPtr, connection, "SQL command did not yield a result row");
        }
        return err;
    }


    jlong executeForLong(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("executeForLong");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return -1L;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return -1L;
        }

        int err = executeOneRowQuery(env, connectionPtr, connection.get(), statement);
        if (err == SQLITE_ROW && sqlite3_column_count(statement) >= 1) {
            return sqlite3_column_int64(statement, 0);
        }
        return -1L;
    }

    jstring executeForString(JNIEnv *env, jlong connectionPtr, jlong statementPtr) {
        LogRegion rgn("executeForString");

        ActiveConnection connection(connectionPtr);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return nullptr;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);
        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return nullptr;
        }

        int err = executeOneRowQuery(env, connectionPtr, connection.get(), statement);
        if (err == SQLITE_ROW && sqlite3_column_count(statement) >= 1) {
            // Strings returned by sqlite3_column_text16() are always null terminated.
            const jchar *text = static_cast<const jchar *>(sqlite3_column_text16(statement, 0));
            if (text) {
                size_t length = 0;
                while (text[length]) {
                    length += 1;
                }
                return env->NewString(text, length);
            }
            return nullptr;
        }
        return nullptr;
    }

    /**
     * Helper function to clean up the cursor content (result arrays) when an exception or error
     * occurs.
     */
    static jobjectArray clearContents(JNIEnv *env, std::vector< jobject >& contents,
                                      jobjectArray& contentsArchive ) {

        /**
         * This is the error case. We need to release our local references
         * and then throw an exception (either the existing one or a synthesized one).
         */
        for ( int i = 0 ; i < contents.size() ; ++i ) {
            jobject oa = contents[i];
            contents[i] = 0L;
            // don't have to worry about what is in it -- we can just release it
            env->DeleteLocalRef(oa);
        }
        contents.clear();

        if ( contentsArchive != 0L) {
            // similarly, if this is not null, we can just release it. Java deals with the rest.
            env->DeleteLocalRef(contentsArchive);
        }

        if ( env->ExceptionCheck() != JNI_TRUE ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Unable to build result set");
        }
        return 0L;
    }

    /*
    ** This method returns an array of (array of objects).
    ** The first entry is the list of column names in the result set.
    ** The remaining entries are the rows in the result set.
    **
    ** The result can be easily converted into a MatrixCursor.
    **
    ** No attempt is made to provide a roaming cursor on the result set.
    ** If the user needs to do that, they should use limit and offset
    ** in the query.
    **
    ** This function executes the SQLite statement object passed as the 3rd
    ** argument.
    */
    jobjectArray executeIntoObjectArray(JNIEnv *env, jlong connectionPtr,
                                  jlong statementPtr) {
        LogRegion rgn("executeIntoObjectArray");

        // inc_size must be smaller than max local ref (512) and greater than 2
        jsize inc_size = 500;
        ActiveConnection connection(connectionPtr);


        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return 0L;
        }

        sqlite3_stmt* statement = getActiveStatement(statementPtr);

        if ( statement == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Statement already finalized");
            return 0L;
        }

        /* Set the number of columns in the window */
        int nCol = sqlite3_column_count(statement);

        // contentsArchive is only used if contents gets bigger than inc_size
        jobjectArray contentsArchive = 0L;
        std::vector< jobject > contents;
        std::vector< char > dataTypes(nCol, 0);
        const char INTEGER_TYPE = 0x1;
        const char DOUBLE_TYPE = 0x2;
        const char STRING_TYPE = 0x4;
        const char BYTEARRAY_TYPE = 0x8;

        // build the headings row
        {
            jobjectArray headings(env->NewObjectArray(nCol, stringClass, 0L));

            if ( headings == nullptr ) {
                // exception (e.g., OutOfMemoryException)
                return clearContents(env, contents, contentsArchive);
            }

            contents.push_back(headings);

            for (int i = 0; i < nCol; ++i) {
                // sqlite3_column_name16 returns a null-terminated UTF-16 string.
                const jchar *name = static_cast<const jchar *>(sqlite3_column_name16(statement, i));
                if (name) {
                    size_t length = 0;
                    while (name[length]) {
                        length += 1;
                    }
                    jstring val(env->NewString(name, length));

                    if ( val == nullptr ) {
                        // exception (e.g., OutOfMemoryException)
                        return clearContents(env, contents, contentsArchive);
                    }

                    env->SetObjectArrayElement(headings, i, val);
                    env->DeleteLocalRef(val);
                } else {
                    env->SetObjectArrayElement(headings, i, 0L);
                }
            }
        }
        // build a column data-type row (placeholder)
        {
            jcharArray charTypeCode(env->NewCharArray(nCol));

            if ( charTypeCode == nullptr ) {
                // exception (e.g., OutOfMemoryException)
                return clearContents(env, contents, contentsArchive);
            }

            contents.push_back(charTypeCode);
        }

        int iRow = 0;
        while (sqlite3_step(statement) == SQLITE_ROW) {
            jobjectArray row(env->NewObjectArray(nCol, objectClass, 0L));

            if ( row == nullptr ) {
                // exception (e.g., OutOfMemoryException)
                return clearContents(env, contents, contentsArchive);
            }

            contents.push_back(row);

            /*
            ** Append the contents of the row that SQL statement statement currently points to
            ** to the contents array.
            */
            for (int i = 0; i < nCol; i++) {

                switch (sqlite3_column_type(statement, i)) {
                    case SQLITE_NULL: {
                        env->SetObjectArrayElement(row, i, 0L);
                        break;
                    }

                    case SQLITE_INTEGER: {
                        jlong val = sqlite3_column_int64(statement, i);
                        jobject boxedLong(env->CallStaticObjectMethod(longClass, boxLong, val));

                        if ( env->ExceptionCheck() == JNI_TRUE ) {
                            // exception (e.g., OutOfMemoryException)
                            return clearContents(env, contents, contentsArchive);
                        }

                        env->SetObjectArrayElement(row, i, boxedLong);
                        env->DeleteLocalRef(boxedLong);
                        dataTypes[i] = dataTypes[i] | INTEGER_TYPE;
                        break;
                    }

                    case SQLITE_FLOAT: {
                        jdouble val = sqlite3_column_double(statement, i);
                        jobject boxedDouble(env->CallStaticObjectMethod(doubleClass, boxDouble, val));

                        if ( env->ExceptionCheck() == JNI_TRUE ) {
                            // exception (e.g., OutOfMemoryException)
                            return clearContents(env, contents, contentsArchive);
                        }

                        env->SetObjectArrayElement(row, i, boxedDouble);
                        env->DeleteLocalRef(boxedDouble);
                        dataTypes[i] = dataTypes[i] | DOUBLE_TYPE;
                        break;
                    }

                    case SQLITE_TEXT: {
                        // Strings returned by sqlite3_column_text16() are always null terminated.
                        jchar *pStr = (jchar *) sqlite3_column_text16(statement, i);
                        if (pStr) {
                            size_t nStr = 0;
                            while (pStr[nStr]) {
                                nStr += 1;
                            }
                            jstring val(env->NewString(pStr, nStr));

                            if ( val == nullptr ) {
                                // exception (e.g., OutOfMemoryException)
                                return clearContents(env, contents, contentsArchive);
                            }

                            env->SetObjectArrayElement(row, i, val);
                            env->DeleteLocalRef(val);
                            dataTypes[i] = dataTypes[i] | STRING_TYPE;
                        } else {
                            env->SetObjectArrayElement(row, i, 0L);
                        }
                        break;
                    }

                    default: {
                        if (sqlite3_column_type(statement, i) == SQLITE_BLOB) {
                            const jbyte *p = (const jbyte *) sqlite3_column_blob(statement, i);
                            if (p) {
                                int n = sqlite3_column_bytes(statement, i);
                                jbyteArray val(env->NewByteArray(n));

                                if ( val == nullptr ) {
                                    // exception (e.g., OutOfMemoryException)
                                    return clearContents(env, contents, contentsArchive);
                                }

                                env->SetByteArrayRegion(val, 0, n, p);
                                env->SetObjectArrayElement(row, i, val);
                                env->DeleteLocalRef(val);
                                dataTypes[i] = dataTypes[i] | BYTEARRAY_TYPE;
                            } else {
                                env->SetObjectArrayElement(row, i, 0L);
                            }
                        } else {
                            throw_sqlite3_exception_db(env, connectionPtr, connection.get(),
                                                       "SQL statement did not complete sucessfully.");
                            return clearContents(env, contents, contentsArchive);
                        }
                        break;
                    }
                }
            }

            if ( contents.size() == inc_size ) {
                if ( contentsArchive == 0L ) {
                    // first time
                    contentsArchive = env->NewObjectArray(inc_size, objectClass, 0L);

                    if ( contentsArchive == nullptr ) {
                        // exception (e.g., OutOfMemoryException)
                        contentsArchive = 0L;
                        return clearContents(env, contents, contentsArchive);
                    }

                    for ( int i = 0 ; i < contents.size() ; ++i ) {
                        jobject oa = contents[i];
                        contents[i] = 0L;
                        env->SetObjectArrayElement(contentsArchive, i, oa);
                        env->DeleteLocalRef(oa);
                    }
                } else {
                    // append to existing list
                    jsize size = env->GetArrayLength(contentsArchive);
                    jobjectArray newArchive = env->NewObjectArray(size+inc_size, objectClass, 0L);

                    if ( newArchive == nullptr ) {
                        // exception (e.g., OutOfMemoryException)
                        return clearContents(env, contents, contentsArchive);
                    }

                    for ( int i = 0 ; i < size ; ++i ) {
                        jobject oa = env->GetObjectArrayElement(contentsArchive, i);
                        env->SetObjectArrayElement(newArchive, i, oa);
                        env->DeleteLocalRef(oa);
                    }
                    env->DeleteLocalRef(contentsArchive);
                    contentsArchive = newArchive;
                    for ( int i = 0 ; i < contents.size() ; ++i ) {
                        jobject oa = contents[i];
                        contents[i] = 0L;
                        env->SetObjectArrayElement(contentsArchive, size+i, oa);
                        env->DeleteLocalRef(oa);
                    }
                }
                contents.clear();
            }
            iRow++;
        }

        /* Finalize the statement. If this indicates an error occurred, throw an
        ** SQLiteException exception.  */
        int rc = sqlite3_reset(statement);
        if (rc != SQLITE_OK) {
            throw_sqlite3_exception_db(env, connectionPtr, connection.get(),
                                       "SQL statement did not complete sucessfully.");
            return clearContents(env, contents, contentsArchive);
        }

        inc_size = contents.size();
        if ( !contents.empty() ) {
            if ( contentsArchive == 0L ) {
                // first time
                contentsArchive = env->NewObjectArray(inc_size, objectClass, 0L);

                if ( contentsArchive == nullptr ) {
                    // exception (e.g., OutOfMemoryException)
                    return clearContents(env, contents, contentsArchive);
                }

                for ( int i = 0 ; i < contents.size() ; ++i ) {
                    jobject oa = contents[i];
                    contents[i] = 0L;
                    env->SetObjectArrayElement(contentsArchive, i, oa);
                    env->DeleteLocalRef(oa);
                }
            } else {
                // append to existing list
                jsize size = env->GetArrayLength(contentsArchive);
                jobjectArray newArchive = env->NewObjectArray(size+inc_size, objectClass, 0L);

                if ( newArchive == nullptr ) {
                    // exception (e.g., OutOfMemoryException)
                    return clearContents(env, contents, contentsArchive);
                }

                for ( int i = 0 ; i < size ; ++i ) {
                    jobject oa = env->GetObjectArrayElement(contentsArchive, i);
                    env->SetObjectArrayElement(newArchive, i, oa);
                    env->DeleteLocalRef(oa);
                }
                env->DeleteLocalRef(contentsArchive);
                contentsArchive = newArchive;
                for ( int i = 0 ; i < contents.size() ; ++i ) {
                    jobject oa = contents[i];
                    contents[i] = 0L;
                    env->SetObjectArrayElement(contentsArchive, size+i, oa);
                    env->DeleteLocalRef(oa);
                }
            }
            contents.clear();
        }

        // finally: update the dataTypes object array with type strings
        if ( contentsArchive != 0L && env->GetArrayLength(contentsArchive) >= 2) {
            jcharArray oa = (jcharArray) env->GetObjectArrayElement(contentsArchive, 1);
            jchar* oachars = env->GetCharArrayElements(oa, nullptr);

            if ( oachars == nullptr ) {
                // exception (e.g., OutOfMemoryException)
                return clearContents(env, contents, contentsArchive);
            }

            const jchar nullType = 'n';
            const jchar stringType = 's';
            const jchar longType = 'l';
            const jchar doubleType = 'd';
            const jchar byteArrayType = 'b';
            const jchar objectType = 'o';

            for ( int i = 0 ; i < nCol ; ++i ) {
                char type = dataTypes[i];
                switch ( type ) {
                    case 0:
                        // null values only
                        oachars[i] = nullType;
                        break;
                    case INTEGER_TYPE:
                        // long values only
                        oachars[i] = longType;
                        break;
                    case DOUBLE_TYPE:
                        // double values only
                        oachars[i] = doubleType;
                        break;
                    case STRING_TYPE:
                        // string values only
                        oachars[i] = stringType;
                        break;
                    case BYTEARRAY_TYPE:
                        // byte[] values only
                        oachars[i] = byteArrayType;
                        break;
                    default:
                        // overloaded values
                        oachars[i] = objectType;
                        break;
                }
            }
            env->ReleaseCharArrayElements(oa, oachars, 0);
            env->DeleteLocalRef(oa);
        }

        if ( env->ExceptionCheck() == JNI_TRUE ) {
            // exception (e.g., OutOfMemoryException)
            return clearContents(env, contents, contentsArchive);
        }

        return contentsArchive;
    }

    void cancel(JNIEnv *env, jlong connectionPtr) {
        LogRegion rgn("cancel");
        // this does not throw an error but is a no-op if the connection
        // doesn't exist
        ActiveConnection connection(connectionPtr, 1);
    }

    void resetCancel(JNIEnv *env, jlong connectionPtr, jboolean cancelable) {
        LogRegion rgn("resetCancel");
        ActiveConnection connection(connectionPtr, 0);

        if ( connection.get() == nullptr ) {
            jniThrowException(env,
                              "org/sqlite/database/sqlite/SQLiteException",
                              "Connection already closed");
            return;
        }

        if (cancelable) {
            sqlite3_progress_handler(connection.get()->db, 4, sqliteProgressHandlerCallback,
                                     connection.get());
        } else {
            sqlite3_progress_handler(connection.get()->db, 0, nullptr, nullptr);
        }
    }

} // namespace android
