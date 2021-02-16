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

#include "google/protobuf/empty.pb.h"
#include "google/spanner/v1/mutation.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_only_transaction.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/errors.h"
#include "frontend/common/protos.h"
#include "frontend/converters/mutations.h"
#include "frontend/converters/time.h"
#include "frontend/entities/session.h"
#include "frontend/entities/transaction.h"
#include "frontend/server/handler.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace spanner_api = ::google::spanner::v1;
namespace protobuf_api = ::google::protobuf;

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Begins a new transaction.
absl::Status BeginTransaction(
    RequestContext* ctx, const spanner_api::BeginTransactionRequest* request,
    spanner_api::Transaction* response) {
  // Get session information.
  SessionManager* session_manager = ctx->env()->session_manager();
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   session_manager->GetSession(request->session()));

  // Create a new transaction.
  ZETASQL_ASSIGN_OR_RETURN(
      std::shared_ptr<Transaction> txn,
      session->CreateMultiUseTransaction(
          request->options(), Session::TransactionActivation::kInitializeOnly));

  // Populate transaction proto in response.
  ZETASQL_ASSIGN_OR_RETURN(*response, txn->ToProto());

  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, BeginTransaction);

// Commits a transaction.
absl::Status Commit(RequestContext* ctx,
                    const spanner_api::CommitRequest* request,
                    spanner_api::CommitResponse* response) {
  // Get session information.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get transaction object to commit.
  zetasql_base::StatusOr<std::shared_ptr<Transaction>> maybe_txn;
  switch (request->transaction_case()) {
    case spanner_api::CommitRequest::kSingleUseTransaction:
      maybe_txn = session->CreateSingleUseTransaction(
          request->single_use_transaction());
      break;
    case spanner_api::CommitRequest::kTransactionId:
      maybe_txn = session->FindAndUseTransaction(request->transaction_id());
      break;
    case spanner_api::CommitRequest::TRANSACTION_NOT_SET:
      return error::MissingRequiredFieldError("CommitRequest.transaction");
  }

  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn, maybe_txn);

  // Wrap all operations on this transaction so they are atomic .
  return txn->GuardedCall(Transaction::OpType::kCommit, [&]() -> absl::Status {
    // Cannot commit a ReadOnlyTransaction.
    if (txn->IsReadOnly()) {
      return error::CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
    }

    // Cannot commit after transaction has been rolled back or encountered a
    // non-recoverable error.
    if (txn->IsInvalid()) {
      return error::CannotUseTransactionAfterConstraintError();
    }
    if (txn->IsRolledback()) {
      return error::CannotCommitAfterRollback();
    }

    // Commit should be indempotent.
    if (txn->IsCommitted()) {
      ZETASQL_ASSIGN_OR_RETURN(absl::Time commit_timestamp, txn->GetCommitTimestamp());
      ZETASQL_ASSIGN_OR_RETURN(*response->mutable_commit_timestamp(),
                       TimestampToProto(commit_timestamp));
      return absl::OkStatus();
    }

    // Process mutations and write to transaction store.
    backend::Mutation mutation;
    ZETASQL_RETURN_IF_ERROR(
        MutationFromProto(*txn->schema(), request->mutations(), &mutation));
    ZETASQL_RETURN_IF_ERROR(txn->Write(mutation));

    // Actually commit the request.
    ZETASQL_RETURN_IF_ERROR(txn->Commit());

    // Return commit timestamp to user.
    ZETASQL_ASSIGN_OR_RETURN(absl::Time commit_timestamp, txn->GetCommitTimestamp());
    ZETASQL_ASSIGN_OR_RETURN(*response->mutable_commit_timestamp(),
                     TimestampToProto(commit_timestamp));
    return absl::OkStatus();
  });
}
REGISTER_GRPC_HANDLER(Spanner, Commit);

// Rolls back a transaction, releasing any locks it holds.
absl::Status Rollback(RequestContext* ctx,
                      const spanner_api::RollbackRequest* request,
                      protobuf_api::Empty* response) {
  // Get session information.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get transaction object to rollback.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindAndUseTransaction(request->transaction_id()));

  // Wrap all operations on this transaction so they are atomic.
  return txn->GuardedCall(
      Transaction::OpType::kRollback, [&]() -> absl::Status {
        // Can not rollback a ReadOnlyTransaction.
        if (txn->IsReadOnly()) {
          return error::
              CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
        }

        // Committed transaction can not be rolled back.
        if (txn->IsCommitted()) {
          return error::CannotRollbackAfterCommit();
        }

        // Rollback should be idempotent.
        if (txn->IsRolledback()) {
          return absl::OkStatus();
        }

        // Rollback the transaction.
        return txn->Rollback();
      });
}
REGISTER_GRPC_HANDLER(Spanner, Rollback);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
