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

#include "frontend/converters/reads.h"

#include <memory>

#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "backend/common/ids.h"
#include "common/errors.h"
#include "frontend/common/protos.h"
#include "frontend/entities/session.h"
#include "frontend/entities/transaction.h"
#include "frontend/server/handler.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

namespace {

zetasql_base::Status ValidateTransactionSelectorForRead(
    const spanner_api::TransactionSelector& selector) {
  if (selector.selector_case() ==
          spanner_api::TransactionSelector::SelectorCase::kSingleUse &&
      selector.single_use().mode_case() != v1::TransactionOptions::kReadOnly) {
    return error::InvalidModeForReadOnlySingleUseTransaction();
  }
  return zetasql_base::OkStatus();
}

}  //  namespace

// Reads rows from the database, returning all results in a single reply.
zetasql_base::Status Read(RequestContext* ctx, const spanner_api::ReadRequest* request,
                  spanner_api::ResultSet* response) {
  // Get session information.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(ValidateTransactionSelectorForRead(request->transaction()));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));

  // Wrap all operations on this transaction so they are atomic .
  return txn->GuardedCall([&]() -> zetasql_base::Status {
    // Cannot read after commit or rollback.
    if (txn->IsCommitted() || txn->IsRolledback()) {
      return error::CannotReadOrQueryAfterCommitOrRollback();
    }

    // Parse read request.
    backend::ReadArg read_arg;
    ZETASQL_RETURN_IF_ERROR(ReadArgFromProto(*txn->schema(), *request, &read_arg));

    // Execute read on backend.
    std::unique_ptr<backend::RowCursor> cursor;
    ZETASQL_RETURN_IF_ERROR(txn->Read(read_arg, &cursor));

    // Populate transaction metadata.
    ZETASQL_RETURN_IF_ERROR(txn->MaybeFillTransactionMetadata(
        request->transaction(), response->mutable_metadata()));

    // Convert read results to proto.
    return RowCursorToResultSetProto(cursor.get(), request->limit(), response);
  });
}
REGISTER_GRPC_HANDLER(Spanner, Read);

// Reads rows from the database, returning all results as a stream.
//
// StreamingReads do not support resume_tokens in the emulator. This
// implementation does not limit the size of the response and therefore,
// chunked_value will always be false.
zetasql_base::Status StreamingRead(
    RequestContext* ctx, const spanner_api::ReadRequest* request,
    ServerStream<spanner_api::PartialResultSet>* stream) {
  // Get session information.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(ValidateTransactionSelectorForRead(request->transaction()));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));

  // Wrap all operations on this transaction so they are atomic .
  return txn->GuardedCall([&]() -> zetasql_base::Status {
    // Cannot read after commit or rollback.
    if (txn->IsCommitted() || txn->IsRolledback()) {
      return error::CannotReadOrQueryAfterCommitOrRollback();
    }

    // Parse read request.
    backend::ReadArg read_arg;
    ZETASQL_RETURN_IF_ERROR(ReadArgFromProto(*txn->schema(), *request, &read_arg));

    // Execute read on backend.
    std::unique_ptr<backend::RowCursor> cursor;
    ZETASQL_RETURN_IF_ERROR(txn->Read(read_arg, &cursor));

    // Populate transaction metadata.
    spanner_api::PartialResultSet response;
    ZETASQL_RETURN_IF_ERROR(txn->MaybeFillTransactionMetadata(
        request->transaction(), response.mutable_metadata()));

    // Convert read results to proto.
    ZETASQL_RETURN_IF_ERROR(RowCursorToPartialResultSetProto(
        cursor.get(), request->limit(), &response));

    // Send result back to client.
    stream->Send(response);
    return zetasql_base::OkStatus();
  });
}
REGISTER_GRPC_HANDLER(Spanner, StreamingRead);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
