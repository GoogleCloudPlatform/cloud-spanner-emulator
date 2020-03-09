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

#include "google/spanner/v1/spanner.pb.h"
#include "common/errors.h"
#include "frontend/server/handler.h"
#include "zetasql/base/status.h"

namespace spanner_api = ::google::spanner::v1;

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

zetasql_base::Status ValidateTransactionSelectorForPartitionRead(
    const spanner_api::TransactionSelector& selector) {
  // PartitionRead and PartitionQuery only support read only snapshot
  // transactions. Read/write and single use transactions are not supported.
  switch (selector.selector_case()) {
    case spanner_api::TransactionSelector::SelectorCase::kBegin: {
      if (!selector.begin().has_read_only()) {
        return error::PartitionReadOnlySupportsReadOnlyTransaction();
      }
      return zetasql_base::OkStatus();
    }
    case spanner_api::TransactionSelector::SelectorCase::kId:
      return zetasql_base::OkStatus();
    case spanner_api::TransactionSelector::SelectorCase::kSingleUse:
    default:
      return error::PartitionReadDoesNotSupportSingleUseTransaction();
  }
}

}  //  namespace

// Creates a set of partition tokens for executing parallel read operations.
zetasql_base::Status PartitionRead(RequestContext* ctx,
                           const spanner_api::PartitionReadRequest* request,
                           spanner_api::PartitionResponse* response) {
  // Take shared ownerships of session and transaction so that they will keep
  // valid throughout this function.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(
      ValidateTransactionSelectorForPartitionRead(request->transaction()));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));

  return zetasql_base::Status(zetasql_base::StatusCode::kUnimplemented, "");
}
REGISTER_GRPC_HANDLER(Spanner, PartitionRead);

// Creates a set of partition tokens for executing parallel query operations.
zetasql_base::Status PartitionQuery(RequestContext* ctx,
                            const spanner_api::PartitionQueryRequest* request,
                            spanner_api::PartitionResponse* response) {
  // Take shared ownerships of session and transaction so that they will keep
  // valid throughout this function.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(
      ValidateTransactionSelectorForPartitionRead(request->transaction()));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));

  return zetasql_base::Status(zetasql_base::StatusCode::kUnimplemented, "");
}
REGISTER_GRPC_HANDLER(Spanner, PartitionQuery);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
