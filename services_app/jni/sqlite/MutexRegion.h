/*
 * Copyright (C) 2016 University of Washington
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

#ifndef MUTEX_REGION_H_included
#define MUTEX_REGION_H_included

#include <pthread.h>

namespace org_opendatakit {

// A class that manages pthread mutex gain/release
class MutexRegion {
 private:
    pthread_mutex_t* mutex;

 public:
    MutexRegion(pthread_mutex_t* mutexArg) : mutex(mutexArg) {
        pthread_mutex_lock(mutex);
    }

    ~MutexRegion() {
        pthread_mutex_unlock(mutex);
    }

    // disallow copy
    MutexRegion(const MutexRegion&) {};

    // disallow assign
    void operator=(const MutexRegion&) {};
};

}

#endif  // MUTEX_REGION_H_included
