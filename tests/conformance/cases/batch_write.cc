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

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "google/cloud/spanner/commit_result.h"
#include "google/cloud/spanner/keys.h"
#include "google/cloud/spanner/mutations.h"
#include "google/cloud/status_or.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {
class BatchWriteTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("batchwrite.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectBatchWriteTest, BatchWriteTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<BatchWriteTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(BatchWriteTest, BatchWriteSucceedsWithInsertMutation) {
  std::vector<cloud::spanner::Mutations> mutations;

  cloud::spanner::Mutation insert_mutation =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(1, "Levin", 27)
          .Build();

  mutations.push_back(cloud::spanner::Mutations{});
  mutations[0].emplace_back(std::move(insert_mutation));

  auto result_stream = BatchWrite(mutations);

  for (const auto& result : result_stream) {
    EXPECT_THAT(result->indexes, testing::ElementsAre(0));
    EXPECT_TRUE(result->commit_timestamp.ok());
  }
}

TEST_P(BatchWriteTest, BatchWriteFailsWithPrimaryKeyAlreadyExists) {
  std::vector<cloud::spanner::Mutations> mutations;

  cloud::spanner::Mutation insert_mutation =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(1, "Levin", 27)
          .Build();

  mutations.push_back(cloud::spanner::Mutations{});
  mutations[0].emplace_back(std::move(insert_mutation));

  auto result_stream_1 = BatchWrite(mutations);
  int result_count = 0;
  for (const auto& result : result_stream_1) {
    result_count++;
    EXPECT_THAT(result->indexes, testing::ElementsAre(0));
    EXPECT_TRUE(result->commit_timestamp.ok());
  }
  EXPECT_EQ(result_count, 1);

  auto result_stream_2 = BatchWrite(mutations);

  for (const auto& result : result_stream_2) {
    EXPECT_THAT(result->indexes, testing::ElementsAre(0));
    EXPECT_FALSE(result->commit_timestamp.ok());
  }
}

TEST_P(BatchWriteTest, BatchWriteMultipleMutationsWithInsertAndDelete) {
  std::vector<cloud::spanner::Mutations> mutations_list;

  // Single Mutations object with an insert and a delete
  mutations_list.push_back(cloud::spanner::Mutations{});
  cloud::spanner::Mutation mutation =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(1, "Levin", 27)  // Insert a row
          .Build();

  // Add a delete mutation to remove the same row
  cloud::spanner::Mutation delete_mutation =
      cloud::spanner::DeleteMutationBuilder(
          "users", cloud::spanner::KeySet().AddKey(cloud::spanner::MakeKey(1)))
          .Build();

  mutations_list[0].emplace_back(std::move(mutation));
  mutations_list[0].emplace_back(std::move(delete_mutation));

  auto result_stream = BatchWrite(mutations_list);

  int result_count = 0;
  for (const auto& result : result_stream) {
    result_count++;
    EXPECT_THAT(result->indexes, testing::ElementsAre(0));
    EXPECT_TRUE(result->commit_timestamp.ok());
  }
  EXPECT_EQ(result_count, 1);
}

TEST_P(BatchWriteTest, BatchWriteMultipleMutationGroupsWithMultipleMutations) {
  std::vector<cloud::spanner::Mutations> mutations_list;

  cloud::spanner::Mutation insert1 =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(3, "Test1", 30)
          .Build();
  cloud::spanner::Mutation insert2 =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(3, "Test2", 31)  // Duplicate id=3
          .Build();

  mutations_list.push_back(cloud::spanner::Mutations{});
  mutations_list[0].emplace_back(std::move(insert1));
  mutations_list[0].emplace_back(std::move(insert2));

  cloud::spanner::Mutation insert3 =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(1, "Test1", 30)
          .Build();
  cloud::spanner::Mutation insert4 =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(2, "Test2", 31)
          .Build();

  mutations_list.push_back(cloud::spanner::Mutations{});
  mutations_list[1].emplace_back(std::move(insert3));
  mutations_list[1].emplace_back(std::move(insert4));

  cloud::spanner::Mutation insert5 =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(4, "Test1", 30)
          .Build();
  cloud::spanner::Mutation insert6 =
      cloud::spanner::InsertMutationBuilder("users", {"id", "name", "age"})
          .EmplaceRow(5, "Test2", 31)
          .Build();

  mutations_list.push_back(cloud::spanner::Mutations{});
  mutations_list[2].emplace_back(std::move(insert5));
  mutations_list[2].emplace_back(std::move(insert6));

  cloud::spanner::BatchedCommitResultStream result_stream =
      BatchWrite(mutations_list);
  int result_count = 0;
  int success_count = 0;
  int failure_count = 0;

  // Last two mutation groups will succeed
  std::vector<std::size_t> expected_success_indexes = {1, 2};

  // The first mutation group will fail
  std::vector<std::size_t> expected_failure_indexes = {0};

  // Spanner batch mutations do not guarantee a one-to-one correspondence
  // between mutation groups and result streams. Some mutations may be grouped
  // into a single result stream.
  for (const auto& result : result_stream) {
    result_count += result->indexes.size();
    for (const auto& index : result->indexes) {
      if (std::find(expected_success_indexes.begin(),
                    expected_success_indexes.end(),
                    index) != expected_success_indexes.end()) {
        EXPECT_TRUE(result->commit_timestamp.ok());
        success_count++;
      }
      if (std::find(expected_failure_indexes.begin(),
                    expected_failure_indexes.end(),
                    index) != expected_failure_indexes.end()) {
        EXPECT_FALSE(result->commit_timestamp.ok());
        failure_count++;
      }
    }
  }
  EXPECT_EQ(result_count, 3);
  EXPECT_EQ(success_count, 2);
  EXPECT_EQ(failure_count, 1);
}
}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
