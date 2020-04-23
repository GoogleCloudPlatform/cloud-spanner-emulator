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

#include "frontend/entities/transaction.h"

#include <cstddef>
#include <memory>
#include <string>

#include "zetasql/public/value.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/variant.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/common/ids.h"
#include "backend/common/variant.h"
#include "backend/database/database.h"
#include "backend/query/query_engine.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/errors.h"
#include "frontend/converters/time.h"
#include "frontend/converters/types.h"
#include "frontend/converters/values.h"
#include "frontend/entities/database.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"
#include "zetasql/base/time_proto_util.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

using ReadWriteTransactionPtr = std::unique_ptr<backend::ReadWriteTransaction>;
using ReadOnlyTransactionPtr = std::unique_ptr<backend::ReadOnlyTransaction>;

Transaction::Transaction(
    absl::variant<std::unique_ptr<backend::ReadWriteTransaction>,
                  std::unique_ptr<backend::ReadOnlyTransaction>>
        backend_transaction,
    const backend::QueryEngine* query_engine, bool is_single_use,
    bool return_read_timestamp)
    : transaction_(std::move(backend_transaction)),
      query_engine_(query_engine),
      is_single_use_(is_single_use),
      return_read_timestamp_(return_read_timestamp) {}

void Transaction::Close() {
  absl::MutexLock lock(&mu_);
  closed_ = true;
  absl::visit(backend::overloaded{
                  [&](const ReadWriteTransactionPtr& transaction) {
                    transaction->Rollback().IgnoreError();
                  },
                  [&](const ReadOnlyTransactionPtr& transaction) {},
              },
              transaction_);
}

zetasql_base::StatusOr<spanner_api::Transaction> Transaction::ToProto() {
  spanner_api::Transaction txn;
  if (!is_single_use_) {
    *txn.mutable_id() = std::to_string(id());
  }
  if (return_read_timestamp_) {
    ZETASQL_ASSIGN_OR_RETURN(absl::Time read_timestamp, GetReadTimestamp());
    ZETASQL_ASSIGN_OR_RETURN(*txn.mutable_read_timestamp(),
                     TimestampToProto(read_timestamp));
  }
  return txn;
}

bool Transaction::IsClosed() const {
  absl::MutexLock lock(&mu_);
  return closed_;
}

bool Transaction::HasState(
    const backend::ReadWriteTransaction::State& state) const {
  return absl::visit(
      backend::overloaded{
          [&](const ReadWriteTransactionPtr& transaction) -> bool {
            return transaction->state() == state;
          },
          [&](const ReadOnlyTransactionPtr& transaction) -> bool {
            return false;
          },
      },
      transaction_);
}

bool Transaction::IsRolledback() const {
  mu_.AssertHeld();
  return HasState(backend::ReadWriteTransaction::State::kRolledback);
}

bool Transaction::IsCommitted() const {
  mu_.AssertHeld();
  return HasState(backend::ReadWriteTransaction::State::kCommitted);
}

bool Transaction::IsReadOnly() const {
  return absl::visit(
      backend::overloaded{
          [&](const ReadWriteTransactionPtr& transaction) -> bool {
            return false;
          },
          [&](const ReadOnlyTransactionPtr& transaction) -> bool {
            return true;
          },
      },
      transaction_);
}

const backend::Schema* Transaction::schema() const {
  return absl::visit(backend::overloaded{
                         [&](const ReadWriteTransactionPtr& transaction) {
                           return transaction->schema();
                         },
                         [&](const ReadOnlyTransactionPtr& transaction) {
                           return transaction->schema();
                         },
                     },
                     transaction_);
}

backend::TransactionID Transaction::id() const {
  return absl::visit(backend::overloaded{
                         [&](const ReadWriteTransactionPtr& transaction) {
                           return transaction->id();
                         },
                         [&](const ReadOnlyTransactionPtr& transaction) {
                           return transaction->id();
                         },
                     },
                     transaction_);
}

zetasql_base::Status Transaction::Read(const backend::ReadArg& read_arg,
                               std::unique_ptr<backend::RowCursor>* cursor) {
  mu_.AssertHeld();
  return absl::visit(backend::overloaded{
                         [&](const ReadWriteTransactionPtr& transaction) {
                           return transaction->Read(read_arg, cursor);
                         },
                         [&](const ReadOnlyTransactionPtr& transaction) {
                           return transaction->Read(read_arg, cursor);
                         },
                     },
                     transaction_);
}

zetasql_base::StatusOr<backend::QueryResult> Transaction::ExecuteSql(
    const backend::Query& query) {
  mu_.AssertHeld();
  return absl::visit(
      backend::overloaded{
          [&](const ReadWriteTransactionPtr& transaction) {
            return query_engine_->ExecuteSql(
                query, backend::QueryContext{.schema = schema(),
                                             .reader = transaction.get(),
                                             .writer = transaction.get()});
          },
          [&](const ReadOnlyTransactionPtr& transaction) {
            return query_engine_->ExecuteSql(
                query, backend::QueryContext{.schema = schema(),
                                             .reader = transaction.get()});
          },
      },
      transaction_);
}

zetasql_base::Status Transaction::Write(const backend::Mutation& mutation) {
  mu_.AssertHeld();
  return absl::visit(
      backend::overloaded{
          [&](const ReadWriteTransactionPtr& transaction) {
            return transaction->Write(mutation);
          },
          [&](const ReadOnlyTransactionPtr& transaction) {
            return error::CannotCommitRollbackReadOnlyTransaction();
          },
      },
      transaction_);
}

zetasql_base::Status Transaction::Commit() {
  mu_.AssertHeld();
  return absl::visit(
      backend::overloaded{
          [&](const ReadWriteTransactionPtr& transaction) {
            return transaction->Commit();
          },
          [&](const ReadOnlyTransactionPtr& transaction) {
            return error::CannotCommitRollbackReadOnlyTransaction();
          },
      },
      transaction_);
}

zetasql_base::Status Transaction::Rollback() {
  mu_.AssertHeld();
  return absl::visit(
      backend::overloaded{
          [&](const ReadWriteTransactionPtr& transaction) {
            return transaction->Rollback();
          },
          [&](const ReadOnlyTransactionPtr& transaction) {
            return error::CannotCommitRollbackReadOnlyTransaction();
          },
      },
      transaction_);
}

zetasql_base::StatusOr<absl::Time> Transaction::GetReadTimestamp() const {
  return absl::visit(
      backend::overloaded{
          [&](const ReadWriteTransactionPtr& transaction)
              -> zetasql_base::StatusOr<absl::Time> {
            return error::CannotReturnReadTimestampForReadWriteTransaction();
          },
          [&](const ReadOnlyTransactionPtr& transaction)
              -> zetasql_base::StatusOr<absl::Time> {
            return transaction->read_timestamp();
          },
      },
      transaction_);
}

zetasql_base::StatusOr<absl::Time> Transaction::GetCommitTimestamp() {
  mu_.AssertHeld();
  return absl::visit(
      backend::overloaded{
          [&](const ReadWriteTransactionPtr& transaction)
              -> zetasql_base::StatusOr<absl::Time> {
            return transaction->GetCommitTimestamp();
          },
          [&](const ReadOnlyTransactionPtr& transaction)
              -> zetasql_base::StatusOr<absl::Time> {
            return error::CannotCommitRollbackReadOnlyTransaction();
          },
      },
      transaction_);
}

zetasql_base::Status Transaction::GuardedCall(const std::function<zetasql_base::Status()>& fn) {
  absl::MutexLock lock(&mu_);

  // Can not reuse a transaction that previously encountered an error.
  // Replay the last error status for the given transaction.
  ZETASQL_RETURN_IF_ERROR(status_);

  const zetasql_base::Status status = fn();
  if (!status.ok() && status.code() != zetasql_base::StatusCode::kAborted) {
    status_ = status;
    // For all errors (except ABORT), reset the transaction state.
    Rollback().IgnoreError();
  }
  return status;
}

bool ShouldReturnTransaction(
    const google::spanner::v1::TransactionSelector& selector) {
  if (selector.selector_case() ==
      spanner_api::TransactionSelector::SelectorCase::kBegin) {
    return true;
  }
  if (selector.selector_case() ==
      spanner_api::TransactionSelector::SelectorCase::kSingleUse) {
    return selector.single_use().has_read_only() &&
           selector.single_use().read_only().return_read_timestamp();
  }
  return false;
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
