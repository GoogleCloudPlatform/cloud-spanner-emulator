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

#include "frontend/common/protos.h"

#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "common/errors.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {

static const char kAnyTypeUrlBase[] = "type.googleapis.com";

void ToAnyProto(const google::protobuf::Message& message, google::protobuf::Any* out) {
  out->set_type_url(absl::StrCat(kAnyTypeUrlBase, "/", message.GetTypeName()));
  message.SerializeToString(out->mutable_value());
}

backend::TransactionID TransactionIDFromProto(const std::string& bytes) {
  return std::stoll(bytes, nullptr);
}

google::rpc::Status StatusToProto(const absl::Status& error) {
  google::rpc::Status status;
  status.set_code(error.raw_code());
  status.set_message(std::string(error.message()));
  return status;
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
