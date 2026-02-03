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

#include <string>

#include "google/protobuf/any.pb.h"
#include "google/rpc/error_details.pb.h"
#include "google/rpc/status.pb.h"
#include "absl/strings/cord.h"
#include "common/limits.h"
#include "grpcpp/support/status.h"
#include "third_party/spanner_pg/errors/errors.pb.h"
#include "third_party/spanner_pg/errors/errors.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

const char* kSpannerErrorDomainName = "spanner.googleapis.com";
const char* kSpannerSqlErrorReason = "SQL_ERROR";
const char* kSpannerPgSqlErrorCodeFieldName = "pg_sqlerrcode";

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

// Attaches the Spangres error payloads to the rpc status. For
// spangres::error::PgErrorInfo, we extract PG error info and attach it as an
// entry of details in the safe cloud status.
void AttachSpangresErrorPayloads(const absl::Status& pg_status,
                                 google::rpc::Status& rpc_status) {
  rpc_status.set_code(static_cast<int>(pg_status.code()));
  rpc_status.set_message(pg_status.message());
    absl::Cord pg_error_info_cord =
        pg_status.GetPayload(spangres::error::kPgErrorInfoTypeUrl).value();
    spangres::error::PgErrorInfo pg_error_info;
    if (!pg_error_info.ParseFromCord(pg_error_info_cord)) {
      return;
    }
  // Add the PG error info as a google::rpc::ErrorInfo detail. The metadata
  // can contain the PG error code and other PG error info which can help
  // the customer understand the error.
  google::rpc::ErrorInfo detail;
  detail.set_reason(kSpannerSqlErrorReason);
  detail.set_domain(kSpannerErrorDomainName);
  detail.mutable_metadata()->emplace(kSpannerPgSqlErrorCodeFieldName,
                                     pg_error_info.unpacked_sql_state());
  rpc_status.add_details()->PackFrom(detail);
}

}  // namespace

grpc::Status ToGRPCStatus(const absl::Status& status) {
  std::string message(status.message());
  if (message.size() > limits::kMaxGRPCErrorMessageLength) {
    // Truncate message if it exceeds the limit.
    message.resize(limits::kMaxGRPCErrorMessageLength);
    message.replace(message.size() - 3, 3, "...");
  }

  google::rpc::Status rpc_status;
      if (status.GetPayload(spangres::error::kPgErrorInfoTypeUrl).has_value()) {
    AttachSpangresErrorPayloads(status, rpc_status);
  } else {
    rpc_status = ToRpcStatus(status, message);
  }
  std::string serialized_metadata = rpc_status.SerializeAsString();
  grpc::Status grpc_status =
      grpc::Status(static_cast<grpc::StatusCode>(status.raw_code()), message,
                   serialized_metadata);
  return grpc_status;
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
