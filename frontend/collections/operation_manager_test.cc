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

#include "frontend/collections/operation_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/match.h"
#include "frontend/entities/operation.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

class OperationManagerTest : public testing::Test {
 protected:
  OperationManager* manager() { return &manager_; }

 private:
  OperationManager manager_;
};

TEST_F(OperationManagerTest, CreatesNewOperationWithUserSpecifiedID) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Operation> operation,
      manager()->CreateOperation("projects/123/instances/456", "789"));
  google::longrunning::Operation operation_pb;
  operation->ToProto(&operation_pb);
  EXPECT_EQ("projects/123/instances/456/operations/789", operation_pb.name());
}

TEST_F(OperationManagerTest, CreatesNewOperationWithSystemGeneratedId) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Operation> operation,
      manager()->CreateOperation("projects/123/instances/456", ""));
  google::longrunning::Operation operation_pb;
  operation->ToProto(&operation_pb);
  EXPECT_TRUE(absl::StartsWith(operation_pb.name(),
                               "projects/123/instances/456/operations/_auto"));
}

TEST_F(OperationManagerTest, FailsToCreateOperationWithExistingURI) {
  ZETASQL_ASSERT_OK(manager()->CreateOperation("projects/123/instances/456", "789"));
  EXPECT_THAT(manager()->CreateOperation("projects/123/instances/456", "789"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(OperationManagerTest, GetExistingOperation) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Operation> expected,
      manager()->CreateOperation("projects/123/instances/456", "789"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Operation> actual,
      manager()->GetOperation("projects/123/instances/456/operations/789"));
  EXPECT_EQ(expected, actual);
}

TEST_F(OperationManagerTest, CannotGetNonExistingOperation) {
  EXPECT_THAT(
      manager()->GetOperation("projects/123/instances/456/operations/789"),
      zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(OperationManagerTest, DeletesAnExistingOperation) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Operation> expected,
      manager()->CreateOperation("projects/123/instances/456", "789"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<Operation> actual,
      manager()->GetOperation("projects/123/instances/456/operations/789"));
  EXPECT_EQ(expected, actual);

  ZETASQL_ASSERT_OK(
      manager()->DeleteOperation("projects/123/instances/456/operations/789"));
  EXPECT_THAT(
      manager()->GetOperation("projects/123/instances/456/operations/789"),
      zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(OperationManagerTest, DeletesANonExistingOperation) {
  ZETASQL_ASSERT_OK(
      manager()->DeleteOperation("projects/123/instances/456/operations/789"));
}

TEST_F(OperationManagerTest, ListsOperations) {
  // Create a set of operations.
  std::string instance_uri = "projects/test-project/instances/test-instance";
  const int kNumOperations = 5;
  for (int i = 0; i < kNumOperations; ++i) {
    ZETASQL_ASSERT_OK(manager()->CreateOperation(instance_uri, absl::StrCat(i)));
  }

  // Expect that they are returned in order.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Operation>> operations,
                       manager()->ListOperations(instance_uri));
  EXPECT_EQ(kNumOperations, operations.size());
  for (int i = 0; i < kNumOperations; ++i) {
    google::longrunning::Operation operation_pb;
    operations[i]->ToProto(&operation_pb);
    EXPECT_EQ(absl::StrCat(instance_uri, "/operations/", i),
              operation_pb.name());
  }
}

TEST_F(OperationManagerTest, ListsOperationsWithSimilarInstanceURI) {
  const std::string kInstanceURIa =
      absl::StrCat("projects/test-project/instances/test-instance-a");
  const std::string kInstanceURIb =
      absl::StrCat("projects/test-project/instances/test-instance-b");
  ZETASQL_ASSERT_OK(manager()->CreateOperation(kInstanceURIa, "1"));
  ZETASQL_ASSERT_OK(manager()->CreateOperation(kInstanceURIb, "1"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Operation>> operations,
                       manager()->ListOperations(
                           "projects/test-project/instances/test-instance-a"));
  EXPECT_EQ(operations.size(), 1);
  google::longrunning::Operation operation_pb;
  operations[0]->ToProto(&operation_pb);
  EXPECT_EQ("projects/test-project/instances/test-instance-a/operations/1",
            operation_pb.name());
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
