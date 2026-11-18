//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_MEMORY_RELEASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_MEMORY_RELEASE_H_

#include <cstddef>

namespace google {
namespace spanner {
namespace emulator {
namespace memory {

// The rate at which the background thread returns free memory to the system.
inline constexpr size_t kBackgroundReleaseBytesPerSecond = 32 << 20;

// Starts tcmalloc's background thread, which returns free memory to the system
// at kBackgroundReleaseBytesPerSecond and drains the caches of idle CPUs.
// Without it tcmalloc keeps every page the process ever used, so freeing a
// database shrinks nothing. Does nothing with
// SPANNER_EMULATOR_DISABLE_MEMORY_RELEASE=1, or when the process is not linked
// against tcmalloc. Call once, from main.
void StartBackgroundRelease();

// Returns all of the allocator's free memory to the system now, as after a
// database is destroyed. Costs a walk of the free pages, and a page that is
// allocated again later faults in again.
void ReleaseFreeMemory();

// The bytes the allocator holds free but has not returned to the system, or
// zero if it does not say.
size_t FreeBytesHeld();

}  // namespace memory
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_MEMORY_RELEASE_H_
