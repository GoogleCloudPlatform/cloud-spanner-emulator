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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_API_PROTOS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_API_PROTOS_H_

#include "google/protobuf/any.pb.h"
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/rpc/status.pb.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {

// Populates any proto message "out" using the content of custom message passed.
// The type_url on "out" is set to type.googleapis.com/<message.type>.
void ToAnyProto(const google::protobuf::Message& message, google::protobuf::Any* out);

// Returns backend transaction ID from given bytes.
backend::TransactionID TransactionIDFromProto(const std::string& bytes);

// Returns the given status as status proto.
google::rpc::Status StatusToProto(const absl::Status& error);

}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_COMMON_API_PROTOS_H_
