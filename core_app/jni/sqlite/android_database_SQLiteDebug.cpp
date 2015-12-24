/*
 * Copyright (C) 2007 The Android Open Source Project
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

#define LOG_TAG "SQLiteDebug"

#include <jni.h>
#include <JNIHelp.h>
#include <ScopedLocalRef.h>
#include <ALog-priv.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sqlite3.h>

namespace android {

#define GET_FIELD_ID(var, clazz, fieldName, fieldDescriptor) \
        var = env->GetFieldID(clazz, fieldName, fieldDescriptor); \
        LOG_FATAL_IF(! var, "Unable to find field " fieldName);

static void nativeGetPagerStats(JNIEnv *env, jobject clazz, jobject statsObj)
{
    int memoryUsed;
    int pageCacheOverflow;
    int largestMemAlloc;
    int unused;

    sqlite3_status(SQLITE_STATUS_MEMORY_USED, &memoryUsed, &unused, 0);
    sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &unused, &largestMemAlloc, 0);
    sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &pageCacheOverflow, &unused, 0);

	// these are local references.
	ScopedLocalRef<jclass> pagerStatsClass(env, env->FindClass("org/sqlite/database/sqlite/SQLiteDebug$PagerStats"));
    if (pagerStatsClass.get() == NULL) {
		// unable to locate the class -- silently exit
		ALOGE("Unable to find class org/sqlite/database/sqlite/SQLiteDebug$PagerStats");
        return;
    }
	
    jfieldID f_memoryUsed ;
    jfieldID f_pageCacheOverflow;
    jfieldID f_largestMemAlloc;

    GET_FIELD_ID(f_memoryUsed, pagerStatsClass.get(), "memoryUsed", "I");
    GET_FIELD_ID(f_largestMemAlloc, pagerStatsClass.get(), "largestMemAlloc", "I");
    GET_FIELD_ID(f_pageCacheOverflow, pagerStatsClass.get(), "pageCacheOverflow", "I");
	
    env->SetIntField(statsObj, f_memoryUsed, memoryUsed);
    env->SetIntField(statsObj, f_pageCacheOverflow, pageCacheOverflow);
    env->SetIntField(statsObj, f_largestMemAlloc, largestMemAlloc);
}

/*
 * JNI registration.
 */

static JNINativeMethod gMethods[] =
{
    { "nativeGetPagerStats", "(Lorg/sqlite/database/sqlite/SQLiteDebug$PagerStats;)V",
            (void*) nativeGetPagerStats },
};

int register_android_database_SQLiteDebug(JNIEnv *env)
{
    return jniRegisterNativeMethods(env, "org/sqlite/database/sqlite/SQLiteDebug",
            gMethods, NELEM(gMethods));
}

} // namespace android
