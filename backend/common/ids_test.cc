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

#include "backend/common/ids.h"

#include <thread>  // NOLINT
#include <unordered_set>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/node_hash_set.h"
#include "absl/synchronization/mutex.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

TEST(UniqueIdGeneratorTest, basic) {
  UniqueIdGenerator<std::string> id_generator(1);
  std::vector<std::thread> threads;
  absl::Mutex mu;
  absl::node_hash_set<std::string> id_set;

  // Generate 100 unique ids and put them into id_set.
  for (int i = 0; i < 100; ++i) {
    threads.emplace_back([&id_generator, &mu, &id_set]() {
      std::string id = id_generator.NextId("my-table");
      absl::MutexLock lock(&mu);
      id_set.insert(id);
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  // Verifies that each ID is unique.
  EXPECT_EQ(id_set.size(), 100);
  EXPECT_EQ(id_generator.NextId("my-table"), "my-table:101");
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
