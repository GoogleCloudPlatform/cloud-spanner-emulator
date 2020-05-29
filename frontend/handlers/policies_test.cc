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

#include "google/iam/v1/iam_policy.pb.h"
#include "google/iam/v1/policy.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test_env.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

namespace iam_api = ::google::iam::v1;

class PolicyApiTest : public test::ServerTest {};

TEST_F(PolicyApiTest, PolicyApiIsNotImplementedForInstances) {
  // SetIamPolicy.
  {
    iam_api::SetIamPolicyRequest request;
    iam_api::Policy response;
    grpc::ClientContext ctx;
    EXPECT_THAT(test_env()->instance_admin_client()->SetIamPolicy(&ctx, request,
                                                                  &response),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kUnimplemented,
                    testing::MatchesRegex(".*does not support IAM.*")));
  }

  // GetIamPolicy.
  {
    iam_api::GetIamPolicyRequest request;
    iam_api::Policy response;
    grpc::ClientContext ctx;
    EXPECT_THAT(test_env()->instance_admin_client()->GetIamPolicy(&ctx, request,
                                                                  &response),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kUnimplemented,
                    testing::MatchesRegex(".*does not support IAM.*")));
  }

  // TestIamPermissions.
  {
    iam_api::TestIamPermissionsRequest request;
    iam_api::TestIamPermissionsResponse response;
    grpc::ClientContext ctx;
    EXPECT_THAT(test_env()->instance_admin_client()->TestIamPermissions(
                    &ctx, request, &response),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kUnimplemented,
                    testing::MatchesRegex(".*does not support IAM.*")));
  }
}

TEST_F(PolicyApiTest, PolicyApiIsNotImplementedForDatabases) {
  // SetIamPolicy.
  {
    iam_api::SetIamPolicyRequest request;
    iam_api::Policy response;
    grpc::ClientContext ctx;
    EXPECT_THAT(test_env()->database_admin_client()->SetIamPolicy(&ctx, request,
                                                                  &response),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kUnimplemented,
                    testing::MatchesRegex(".*does not support IAM.*")));
  }

  // GetIamPolicy.
  {
    iam_api::GetIamPolicyRequest request;
    iam_api::Policy response;
    grpc::ClientContext ctx;
    EXPECT_THAT(test_env()->database_admin_client()->GetIamPolicy(&ctx, request,
                                                                  &response),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kUnimplemented,
                    testing::MatchesRegex(".*does not support IAM.*")));
  }

  // TestIamPermissions.
  {
    iam_api::TestIamPermissionsRequest request;
    iam_api::TestIamPermissionsResponse response;
    grpc::ClientContext ctx;
    EXPECT_THAT(test_env()->database_admin_client()->TestIamPermissions(
                    &ctx, request, &response),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kUnimplemented,
                    testing::MatchesRegex(".*does not support IAM.*")));
  }
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
