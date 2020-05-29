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
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"
#include "zetasql/base/time_proto_util.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

namespace {

Transaction::Type TypeFromTransactionOptions(
    const spanner_api::TransactionOptions& options) {
  switch (options.mode_case()) {
    case v1::TransactionOptions::kReadWrite: {
      return Transaction::Type::kReadWrite;
    }
    case v1::TransactionOptions::kReadOnly: {
      return Transaction::Type::kReadOnly;
    }
    case v1::TransactionOptions::kPartitionedDml: {
      return Transaction::Type::kPartitionedDml;
    }
    case v1::TransactionOptions::MODE_NOT_SET: {
      return Transaction::Type::kReadOnly;
    }
  }
}

absl::Status ValidatePartitionedDmlQuery(const backend::Query& query) {
  if (!backend::IsDMLQuery(query.sql) || backend::IsDMLInsertQuery(query.sql)) {
    return error::InvalidOperationUsingPartitionedDmlTransaction();
  }
  return absl::OkStatus();
}

}  // namespace

using ReadWriteTransactionPtr = std::unique_ptr<backend::ReadWriteTransaction>;
using ReadOnlyTransactionPtr = std::unique_ptr<backend::ReadOnlyTransaction>;

Transaction::Transaction(
    absl::variant<std::unique_ptr<backend::ReadWriteTransaction>,
                  std::unique_ptr<backend::ReadOnlyTransaction>>
        backend_transaction,
    const backend::QueryEngine* query_engine,
    const spanner_api::TransactionOptions& options, const Usage& usage)
    : transaction_(std::move(backend_transaction)),
      query_engine_(query_engine),
      usage_type_(usage),
      type_(TypeFromTransactionOptions(options)),
      options_(options) {}

void Transaction::Close() {
  absl::MutexLock lock(&mu_);
  closed_ = true;
  if (type_ == kReadWrite || type_ == kPartitionedDml) {
    read_write()->Rollback().IgnoreError();
  }
}

zetasql_base::StatusOr<spanner_api::Transaction> Transaction::ToProto() {
  spanner_api::Transaction txn;
  if (usage_type_ != kSingleUse) {
    *txn.mutable_id() = std::to_string(id());
  }
  if (options_.has_read_only() &&
      options_.read_only().return_read_timestamp()) {
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
  switch (type_) {
    case kReadOnly: {
      return false;
    }
    case kReadWrite:
    case kPartitionedDml: {
      return read_write()->state() == state;
    }
  }
}

bool Transaction::IsRolledback() const {
  mu_.AssertHeld();
  return HasState(backend::ReadWriteTransaction::State::kRolledback);
}

bool Transaction::IsCommitted() const {
  mu_.AssertHeld();
  return HasState(backend::ReadWriteTransaction::State::kCommitted);
}

const backend::Schema* Transaction::schema() const {
  switch (type_) {
    case kReadOnly: {
      return read_only()->schema();
    }
    case kReadWrite:
    case kPartitionedDml: {
      return read_write()->schema();
    }
  }
}

backend::TransactionID Transaction::id() const {
  switch (type_) {
    case kReadOnly: {
      return read_only()->id();
    }
    case kReadWrite:
    case kPartitionedDml: {
      return read_write()->id();
    }
  }
}

absl::Status Transaction::Read(const backend::ReadArg& read_arg,
                               std::unique_ptr<backend::RowCursor>* cursor) {
  mu_.AssertHeld();
  switch (type_) {
    case kReadOnly: {
      return read_only()->Read(read_arg, cursor);
    }
    case kReadWrite: {
      return read_write()->Read(read_arg, cursor);
    }
    case kPartitionedDml: {
      return error::InvalidOperationUsingPartitionedDmlTransaction();
    }
  }
}

zetasql_base::StatusOr<backend::QueryResult> Transaction::ExecuteSql(
    const backend::Query& query) {
  mu_.AssertHeld();
  switch (type_) {
    case kReadOnly: {
      return query_engine_->ExecuteSql(
          query,
          backend::QueryContext{
              .schema = schema(), .reader = read_only(), .writer = nullptr});
    }
    case kReadWrite: {
      return query_engine_->ExecuteSql(
          query, backend::QueryContext{.schema = schema(),
                                       .reader = read_write(),
                                       .writer = read_write()});
    }
    case kPartitionedDml: {
      ZETASQL_RETURN_IF_ERROR(ValidatePartitionedDmlQuery(query));
      // PartitionedDml will auto-commit transactions and cannot be reused.
      ZETASQL_ASSIGN_OR_RETURN(
          backend::QueryResult result,
          query_engine_->ExecuteSql(
              query, backend::QueryContext{.schema = schema(),
                                           .reader = read_write(),
                                           .writer = read_write()}));
      ZETASQL_RETURN_IF_ERROR(read_write()->Commit());
      return result;
    }
  }
}

absl::Status Transaction::Write(const backend::Mutation& mutation) {
  mu_.AssertHeld();
  if (type_ == kReadWrite) {
    return read_write()->Write(mutation);
  }
  return error::CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
}

absl::Status Transaction::Commit() {
  mu_.AssertHeld();
  if (type_ == kReadWrite) {
    return read_write()->Commit();
  }
  return error::CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
}

absl::Status Transaction::Rollback() {
  mu_.AssertHeld();
  if (type_ == kReadWrite) {
    return read_write()->Rollback();
  }
  return error::CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
}

zetasql_base::StatusOr<absl::Time> Transaction::GetReadTimestamp() const {
  if (type_ == kReadOnly) {
    return read_only()->read_timestamp();
  }
  return error::CannotReturnReadTimestampForReadWriteTransaction();
}

zetasql_base::StatusOr<absl::Time> Transaction::GetCommitTimestamp() const {
  mu_.AssertHeld();
  if (type_ == kReadWrite) {
    return read_write()->GetCommitTimestamp();
  }
  return error::CannotCommitRollbackReadOnlyOrPartitionedDmlTransaction();
}

absl::Status Transaction::GuardedCall(const std::function<absl::Status()>& fn) {
  absl::MutexLock lock(&mu_);

  // Can not reuse a transaction that previously encountered an error.
  // Replay the last error status for the given transaction.
  ZETASQL_RETURN_IF_ERROR(status_);

  const absl::Status status = fn();
  if (!status.ok() && status.code() != absl::StatusCode::kAborted) {
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
