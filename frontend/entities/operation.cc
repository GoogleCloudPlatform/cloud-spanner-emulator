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

#include "frontend/entities/operation.h"

#include "google/rpc/status.pb.h"
#include "absl/strings/str_cat.h"
#include "frontend/common/protos.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

Operation::Operation(const std::string& operation_uri)
    : operation_uri_(operation_uri) {}

void Operation::SetMetadata(const google::protobuf::Message& metadata) {
  absl::MutexLock lock(&mu_);
  metadata_.reset(metadata.New());
  metadata_->CopyFrom(metadata);
}

void Operation::SetError(const absl::Status& status) {
  absl::MutexLock lock(&mu_);
  status_ = status;
  response_.reset();
}

void Operation::SetResponse(const google::protobuf::Message& response) {
  absl::MutexLock lock(&mu_);
  response_.reset(response.New());
  response_->CopyFrom(response);
  status_ = absl::OkStatus();
}

void Operation::ToProto(google::longrunning::Operation* operation_pb) {
  absl::MutexLock lock(&mu_);

  operation_pb->set_name(operation_uri_);
  operation_pb->set_done(!status_.ok() || response_ != nullptr);

  if (metadata_ != nullptr) {
    ToAnyProto(*metadata_, operation_pb->mutable_metadata());
  }

  if (response_ != nullptr) {
    ToAnyProto(*response_, operation_pb->mutable_response());
  } else if (!status_.ok()) {
    operation_pb->mutable_error()->set_code(status_.raw_code());
    operation_pb->mutable_error()->set_message(std::string(status_.message()));
  }
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
