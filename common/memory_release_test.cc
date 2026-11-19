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
#include <cstring>
#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "tcmalloc/malloc_extension.h"

namespace google {
namespace spanner {
namespace emulator {
namespace memory {
namespace {

size_t PhysicalBytes() {
  return tcmalloc::MallocExtension::GetNumericProperty(
             "generic.physical_memory_used")
      .value_or(0);
}

TEST(MemoryReleaseTest, ReleaseFreeMemoryReturnsFreedPagesToTheSystem) {
  if (!tcmalloc::MallocExtension::NeedsProcessBackgroundActions()) {
    GTEST_SKIP() << "Not linked against tcmalloc";
  }
  // Allocate, touch and free 256 MiB in 64 KiB blocks, as rows and their
  // values are freed when a database is dropped.
  {
    std::vector<std::unique_ptr<char[]>> blocks;
    for (int i = 0; i < 4096; ++i) {
      blocks.push_back(std::make_unique<char[]>(64 << 10));
      std::memset(blocks.back().get(), 1, 64 << 10);
    }
  }
  const size_t held = PhysicalBytes();
  ReleaseFreeMemory();
  const size_t after = PhysicalBytes();
  EXPECT_LT(FreeBytesHeld(), size_t{4} << 20);
  EXPECT_GE(held - after, size_t{128} << 20)
      << "held " << held << " bytes, " << after << " after the release";
}

}  // namespace
}  // namespace memory
}  // namespace emulator
}  // namespace spanner
}  // namespace google
