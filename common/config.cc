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

#include "common/config.h"

#include "absl/flags/flag.h"

ABSL_FLAG(std::string, host_port, "localhost:10007",
          "Emulator host IP and port that serves Cloud Spanner gRPC requests.");

ABSL_FLAG(bool, log_requests, false,
          "If true, gRPC request and response messages are streamed to the "
          "INFO log. This switch is intended for emulator debugging.");

// TODO: Client libraries handle ABORT errors by retrying with a
// new transaction on the same session. This needs to be correctly handled
// by the frontend::Transaction to maintain the following between retries:
// - transaction priority
// - abort retry count
ABSL_FLAG(
    bool, randomly_abort_txn_on_first_commit, false,
    "If true, the emulator will randomly ABORT Commit requests for "
    "customers to develop and test against such failure scenarios. Customers "
    "should handle ABORTs within a retry loop in their application (which will "
    "happen in production occasionally). Client applications that use Cloud "
    "Spanner's client libraries don't need to handle this "
    "case, but instead use the TransactionManager/TransactionRunner "
    "implementations to manage their transaction.");

namespace google {
namespace spanner {
namespace emulator {
namespace config {

std::string grpc_host_port() { return absl::GetFlag(FLAGS_host_port); }

bool should_log_requests() { return absl::GetFlag(FLAGS_log_requests); }

bool randomly_abort_txn_on_first_commit() {
  return absl::GetFlag(FLAGS_randomly_abort_txn_on_first_commit);
}

}  // namespace config
}  // namespace emulator
}  // namespace spanner
}  // namespace google
