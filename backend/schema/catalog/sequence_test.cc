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

#include "backend/schema/catalog/sequence.h"

#include <cstdint>
#include <string>
#include <thread>  // NOLINT
#include <vector>

#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "backend/schema/builders/sequence_builder.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

TEST(SequenceTest, GetSequenceValuesFromMultipleThreads) {
  Sequence::Builder builder;
  builder.set_name("test_seq");
  const Sequence* sequence = builder.get();
  std::vector<std::thread> threads;
  threads.reserve(100);
  absl::Mutex mu;
  absl::flat_hash_set<int64_t> sequence_values;

  for (int i = 0; i < 100; ++i) {
    threads.emplace_back([&sequence_values, &mu, &sequence]() {
      ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value value,
                           sequence->GetNextSequenceValue());
      absl::MutexLock lock(&mu);
      ASSERT_FALSE(sequence_values.contains(value.int64_value()));
      sequence_values.insert(value.int64_value());
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(sequence_values.size(), 100);
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
