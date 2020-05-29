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

#include "frontend/common/status.h"

#include "google/protobuf/any.pb.h"
#include "google/rpc/status.pb.h"
#include "absl/strings/cord.h"
#include "common/limits.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

google::rpc::Status ToRpcStatus(const absl::Status& status,
                                const std::string& status_message) {
  google::rpc::Status result;
  result.set_code(static_cast<int>(status.code()));
  result.set_message(status_message);
  status.ForEachPayload(
      [&](absl::string_view type_url, const absl::Cord& payload) {
        google::protobuf::Any* any = result.add_details();
        std::string type(type_url);
        any->set_type_url(type);
        std::string value(payload);
        any->set_value(value);
      });
  return result;
}

}  // namespace

grpc::Status ToGRPCStatus(const absl::Status& status) {
  std::string message(status.message());
  if (message.size() > limits::kMaxGRPCErrorMessageLength) {
    // Truncate message if it exceeds the limit.
    message.resize(limits::kMaxGRPCErrorMessageLength);
    message.replace(message.size() - 3, 3, "...");
  }
  google::rpc::Status rpc_status = ToRpcStatus(status, message);
  std::string serialized_metadata = rpc_status.SerializeAsString();
  return grpc::Status(static_cast<grpc::StatusCode>(status.raw_code()), message,
                      serialized_metadata);
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
