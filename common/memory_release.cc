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

#include "common/memory_release.h"

#include <cstddef>
#include <cstdlib>
#include <optional>
#include <thread>  // NOLINT(build/c++11)

#include "absl/log/absl_log.h"
#include "absl/time/time.h"
#include "tcmalloc/malloc_extension.h"

namespace google {
namespace spanner {
namespace emulator {
namespace memory {

void StartBackgroundRelease() {
  if (std::getenv("SPANNER_EMULATOR_DISABLE_MEMORY_RELEASE") != nullptr) {
    return;
  }
  if (!tcmalloc::MallocExtension::NeedsProcessBackgroundActions()) {
    return;
  }
  tcmalloc::MallocExtension::SetBackgroundReleaseRate(
      tcmalloc::MallocExtension::BytesPerSecond{
          kBackgroundReleaseBytesPerSecond});
  // ProcessBackgroundActions never returns.
  std::thread(tcmalloc::MallocExtension::ProcessBackgroundActions).detach();
  ABSL_LOG(INFO) << "Returning free memory to the system at "
                 << (kBackgroundReleaseBytesPerSecond >> 20) << " MiB/s";
}

void ReleaseFreeMemory() {
  if (std::getenv("SPANNER_EMULATOR_DISABLE_MEMORY_RELEASE") != nullptr) {
    return;
  }
  // tcmalloc keeps a hugepage that still holds an allocation whole while the
  // last minutes' peak demand says the memory may be needed again, and a freed
  // database leaves most of its hugepages that way. Release them all: what the
  // dropped database needed, nothing else will soon.
  using tcmalloc::MallocExtension;
  const absl::Duration short_interval =
      MallocExtension::GetSkipSubreleaseShortInterval();
  const absl::Duration long_interval =
      MallocExtension::GetSkipSubreleaseLongInterval();
  MallocExtension::SetSkipSubreleaseShortInterval(absl::ZeroDuration());
  MallocExtension::SetSkipSubreleaseLongInterval(absl::ZeroDuration());
  // More than the emulator ever holds. Not SIZE_MAX: tcmalloc rounds the
  // request up to whole pages, which wraps SIZE_MAX around to almost nothing.
  MallocExtension::ReleaseMemoryToSystem(size_t{1} << 40);
  MallocExtension::SetSkipSubreleaseShortInterval(short_interval);
  MallocExtension::SetSkipSubreleaseLongInterval(long_interval);
}

size_t FreeBytesHeld() {
  std::optional<size_t> free_bytes =
      tcmalloc::MallocExtension::GetNumericProperty(
          "tcmalloc.pageheap_free_bytes");
  return free_bytes.value_or(0);
}

}  // namespace memory
}  // namespace emulator
}  // namespace spanner
}  // namespace google
