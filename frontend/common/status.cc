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

#include "common/limits.h"

namespace google {
namespace spanner {
namespace emulator {

grpc::Status ToGRPCStatus(const zetasql_base::Status& status) {
  std::string message(status.message());
  if (message.size() > limits::kMaxGRPCErrorMessageLength) {
    message.resize(limits::kMaxGRPCErrorMessageLength);
    message.replace(message.size() - 3, 3, "...");
  }
  return grpc::Status(static_cast<grpc::StatusCode>(status.error_code()),
                      message);
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
