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

#include "org_sqlite_database_sqlite_SQLiteDebug.h"
#include "org_sqlite_database_sqlite_SQLiteCommon.h"

using org_opendatakit::ScopedLocalRef;
using org_opendatakit::getStatus;

extern "C" {

/*
 * Class:     org_sqlite_database_sqlite_SQLiteDebug
 * Method:    nativeGetPagerStats
 * Signature: (Lorg/sqlite/database/sqlite/SQLiteDebug/PagerStats;)V
 */
JNIEXPORT void JNICALL Java_org_sqlite_database_sqlite_SQLiteDebug_nativeGetPagerStats
  (JNIEnv *env, jclass clazz, jobject statsObj) {

    jint memoryUsed = 0;
    jint largestMemAlloc = 0;
    jint pageCacheOverflow = 0;

    getStatus(env, &memoryUsed, &largestMemAlloc, &pageCacheOverflow);

    // these are local references.
    ScopedLocalRef<jclass> pagerStatsClass(env,
        env->FindClass("org/sqlite/database/sqlite/SQLiteDebug$PagerStats"));

    if (pagerStatsClass.get() == nullptr) {
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

}
