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

#include "backend/common/case.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_set.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

TEST(CaseTest, CaseInsensitiveHashContainer) {
  absl::flat_hash_set<std::string, CaseInsensitiveHash, CaseInsensitiveEqual>
      hash_set;
  EXPECT_TRUE(hash_set.insert("my-TeSt").second);

  // Verify case-insensitive comparison on insert, find and count.
  EXPECT_FALSE(hash_set.insert("MY-tEsT").second);
  EXPECT_NE(hash_set.find("mY-tESt"), hash_set.end());
  EXPECT_EQ(hash_set.count("My-TesT"), 1);

  // The key string in the hash set is still case sensitive.
  EXPECT_NE(*hash_set.begin(), "MY-TEST");
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
