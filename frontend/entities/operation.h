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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_OPERATION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_OPERATION_H_

#include <string>

#include "google/longrunning/operations.pb.h"
#include "google/protobuf/message.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Operation represents a long-running operation in the emulator.
//
// An operation encapsulates an optional metadata proto and either a response
// proto or an error status. If either response or error is set, the operation
// is considered done.
//
// More details about the nature of an operation can be found at:
//    https://cloud.google.com/spanner/docs/reference/rpc/google.longrunning#google.longrunning.Operation
class Operation {
 public:
  // Constructs an empty operation.
  explicit Operation(const std::string& operation_uri);

  // Sets the metadata for an operation.
  void SetMetadata(const google::protobuf::Message& metadata) ABSL_LOCKS_EXCLUDED(mu_);

  // Sets the error for an operation (puts the operation in a done state).
  // If a response has been set previously, it will be cleared.
  void SetError(const absl::Status& status) ABSL_LOCKS_EXCLUDED(mu_);

  // Sets the response for an operation (puts the operation in a done state).
  // If an error status was set previously, it will be cleared.
  void SetResponse(const google::protobuf::Message& response) ABSL_LOCKS_EXCLUDED(mu_);

  // Converts an operation to its proto version.
  void ToProto(google::longrunning::Operation* operation_pb)
      ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // The immutable URI for an operation.
  const std::string operation_uri_;

  // Mutex to guard state below.
  absl::Mutex mu_;

  // The metadata for this operation.
  std::unique_ptr<google::protobuf::Message> metadata_ ABSL_GUARDED_BY(mu_);

  // The response for this operation if the operation was successful.
  std::unique_ptr<google::protobuf::Message> response_ ABSL_GUARDED_BY(mu_);

  // The status for this operation if this operation was not successful.
  absl::Status status_ ABSL_GUARDED_BY(mu_);
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_OPERATION_H_
