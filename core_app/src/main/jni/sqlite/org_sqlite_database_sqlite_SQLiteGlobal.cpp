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

#define LOG_TAG "SQLiteGlobal"

#include "org_sqlite_database_sqlite_SQLiteGlobal.h"
#include "org_sqlite_database_sqlite_SQLiteCommon.h"

using org_opendatakit::releaseMemory;

extern "C" {
/*
 * Class:     org_sqlite_database_sqlite_SQLiteGlobal
 * Method:    nativeReleaseMemory
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_sqlite_database_sqlite_SQLiteGlobal_nativeReleaseMemory
  (JNIEnv * env, jclass clazz) {
	return releaseMemory();
}

}
