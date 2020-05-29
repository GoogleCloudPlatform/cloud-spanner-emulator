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
#include "common/errors.h"
#include "frontend/server/handler.h"

namespace iam_api = ::google::iam::v1;

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Sets the access control policy on a resource.
absl::Status SetIamPolicy(RequestContext* ctx,
                          const iam_api::SetIamPolicyRequest* request,
                          iam_api::Policy* response) {
  return error::IAMPoliciesNotSupported();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, SetIamPolicy);
REGISTER_GRPC_HANDLER(DatabaseAdmin, SetIamPolicy);

// Gets the access control policy for a resource.
absl::Status GetIamPolicy(RequestContext* ctx,
                          const iam_api::GetIamPolicyRequest* request,
                          iam_api::Policy* response) {
  return error::IAMPoliciesNotSupported();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, GetIamPolicy);
REGISTER_GRPC_HANDLER(DatabaseAdmin, GetIamPolicy);

// Returns permissions that the caller has on the specified resource.
absl::Status TestIamPermissions(
    RequestContext* ctx, const iam_api::TestIamPermissionsRequest* request,
    iam_api::TestIamPermissionsResponse* response) {
  return error::IAMPoliciesNotSupported();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, TestIamPermissions);
REGISTER_GRPC_HANDLER(DatabaseAdmin, TestIamPermissions);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
