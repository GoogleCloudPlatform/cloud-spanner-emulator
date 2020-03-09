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

#include "backend/transaction/read_write_transaction.h"

#include <memory>
#include <string>
#include <utility>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "zetasql/base/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/actions/context.h"
#include "backend/actions/manager.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/value.h"
#include "backend/locking/request.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/storage/in_memory_iterator.h"
#include "backend/storage/iterator.h"
#include "backend/storage/storage.h"
#include "backend/transaction/actions.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_util.h"
#include "backend/transaction/row_cursor.h"
#include "backend/transaction/write_util.h"
#include "common/clock.h"
#include "common/errors.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

zetasql_base::Status ValidateColumnsAreNotDuplicate(
    const std::vector<std::string>& column_names) {
  CaseInsensitiveStringSet columns;
  for (const std::string& column_name : column_names) {
    if (columns.find(column_name) != columns.end()) {
      return error::MultipleValuesForColumn(column_name);
    }
    columns.insert(column_name);
  }
  return zetasql_base::OkStatus();
}

}  // namespace

ReadWriteTransaction::ReadWriteTransaction(
    const ReadWriteOptions& options, TransactionID transaction_id, Clock* clock,
    Storage* storage, LockManager* lock_manager,
    const VersionedCatalog* const versioned_catalog,
    ActionManager* action_manager)
    : options_(options),
      id_(transaction_id),
      clock_(clock),
      base_storage_(storage),
      versioned_catalog_(versioned_catalog),
      lock_handle_(lock_manager->CreateHandle(
          transaction_id, /*priority_=*/absl::ToUnixMicros(clock_->Now()))),
      transaction_store_(absl::make_unique<TransactionStore>(
          base_storage_, lock_handle_.get())),
      action_manager_(action_manager),
      action_context_(absl::make_unique<ActionContext>(
          absl::make_unique<TransactionReadOnlyStore>(transaction_store_.get()),
          absl::make_unique<TransactionEffectsBuffer>(&write_ops_queue_),
          clock)),
      schema_(versioned_catalog_->GetLatestSchema()) {}

zetasql_base::StatusOr<absl::Time> ReadWriteTransaction::GetCommitTimestamp() {
  absl::MutexLock lock(&mu_);
  if (state_ != State::kCommitted) {
    return error::Internal(
        absl::StrCat("Commit timestamp is only available after call to "
                     "Transaction Commit. Transaction: ",
                     id(), " is in state: ", state_));
  }
  return commit_timestamp_;
}

zetasql_base::Status ReadWriteTransaction::Read(const ReadArg& read_arg,
                                        std::unique_ptr<RowCursor>* cursor) {
  return GuardedCall([&]() -> zetasql_base::Status {
    mu_.AssertHeld();

    const Table* table;
    std::vector<const Column*> columns;
    ZETASQL_RETURN_IF_ERROR(
        ExtractTableAndColumnsFromReadArg(read_arg, schema_, &table, &columns));

    std::vector<std::unique_ptr<StorageIterator>> iterators;
    std::vector<KeyRange> key_ranges;
    CanonicalizeKeySetForTable(read_arg.key_set, table, &key_ranges);
    for (const auto& key_range : key_ranges) {
      std::unique_ptr<StorageIterator> itr;
      ZETASQL_RETURN_IF_ERROR(transaction_store_->Read(
          table, key_range, columns, &itr,
          false /*allow_pending_commit_timestamps_in_read*/));
      iterators.push_back(std::move(itr));
    }
    *cursor = absl::make_unique<StorageIteratorRowCursor>(std::move(iterators),
                                                          std::move(columns));
    return zetasql_base::OkStatus();
  });
}

zetasql_base::Status ReadWriteTransaction::BufferRow(const WriteOp& op) {
  return std::visit(overloaded{
                        [&](const InsertOp& op) {
                          return transaction_store_->BufferInsert(
                              op.table, op.key, op.columns, op.values);
                        },
                        [&](const UpdateOp& op) {
                          return transaction_store_->BufferUpdate(
                              op.table, op.key, op.columns, op.values);
                        },
                        [&](const DeleteOp& op) {
                          return transaction_store_->BufferDelete(op.table,
                                                                  op.key);
                        },
                    },
                    op);
}

zetasql_base::Status ReadWriteTransaction::ApplyValidators(const WriteOp& op) {
  return action_registry_->ExecuteValidators(action_context_.get(), op);
}

zetasql_base::Status ReadWriteTransaction::ApplyEffectors(const WriteOp& op) {
  ZETASQL_RETURN_IF_ERROR(
      action_registry_->ExecuteEffectors(action_context_.get(), op));
  return zetasql_base::OkStatus();
}

zetasql_base::Status ReadWriteTransaction::ApplyStatementVerifiers() {
  for (const auto& write_op : transaction_store_->GetBufferedOps()) {
    ZETASQL_RETURN_IF_ERROR(
        action_registry_->ExecuteVerifiers(action_context_.get(), write_op));
  }
  return zetasql_base::OkStatus();
}

const Schema* ReadWriteTransaction::schema() const {
  absl::MutexLock lock(&mu_);
  if (state_ == State::kUninitialized) {
    return versioned_catalog_->GetLatestSchema();
  }
  return schema_;
}

void ReadWriteTransaction::Reset() {
  mu_.AssertHeld();

  lock_handle_->UnlockAll();
  transaction_store_->Clear();
  std::queue<WriteOp> empty;
  write_ops_queue_.swap(empty);
  state_ = State::kUninitialized;
}

zetasql_base::Status ReadWriteTransaction::GuardedCall(
    const std::function<zetasql_base::Status()>& fn) {
  absl::MutexLock lock(&mu_);
  switch (state_) {
    case State::kRolledback: {
      return error::Internal(absl::StrCat(
          "Invalid call to Rolledback transaction. Transaction: ", id()));
      break;
    }
    case State::kCommitted:
      return error::Internal(absl::StrCat(
          "Invalid call to Committed transaction. Transaction: ", id()));
    case State::kUninitialized: {
      schema_ = versioned_catalog_->GetLatestSchema();
      auto maybe_action_registry =
          action_manager_->GetActionsForSchema(schema_);
      if (!maybe_action_registry.ok()) {
        Reset();
        return maybe_action_registry.status();
      }
      action_registry_ = maybe_action_registry.ValueOrDie();
      state_ = State::kActive;
      break;
    }
    case State::kActive: {
      if (schema_ != versioned_catalog_->GetLatestSchema()) {
        Reset();
        abort_retry_count_++;
        return error::AbortDueToConcurrentSchemaChange(id_);
      }
      break;
    }
  }

  const zetasql_base::Status status = fn();

  if (!status.ok()) {
    // For all errors, reset the transaction state.
    Reset();

    if (status.code() == zetasql_base::StatusCode::kAborted) {
      abort_retry_count_++;
    }
  }
  return status;
}

zetasql_base::Status ReadWriteTransaction::ProcessWriteOpsQueue() {
  mu_.AssertHeld();

  while (!write_ops_queue_.empty()) {
    WriteOp write_op = write_ops_queue_.front();
    write_ops_queue_.pop();
    ZETASQL_RETURN_IF_ERROR(ApplyValidators(write_op));
    ZETASQL_RETURN_IF_ERROR(ApplyEffectors(write_op));

    // Buffer the operation.
    ZETASQL_RETURN_IF_ERROR(BufferRow(write_op));
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ReadWriteTransaction::Write(const Mutation& mutation) {
  return GuardedCall([&]() -> zetasql_base::Status {
    mu_.AssertHeld();

    for (const MutationOp& mutation_op : mutation.ops()) {
      const Table* table = schema_->FindTable(mutation_op.table);
      if (table == nullptr) {
        return error::Internal(absl::StrCat(
            "Table '", mutation_op.table,
            "' not found while flattening MutationOp to WriteOp. Schema "
            "generation used: ",
            schema_->generation()));
      }
      ZETASQL_RETURN_IF_ERROR(ValidateColumnsAreNotDuplicate(mutation_op.columns));
      std::vector<const Column*> columns;
      ZETASQL_ASSIGN_OR_RETURN(columns, GetColumnsByName(table, mutation_op.columns));
      absl::Time now = clock_->Now();

      // Process Delete.
      if (mutation_op.type == MutationOpType::kDelete) {
        ZETASQL_RETURN_IF_ERROR(FlattenDelete(mutation_op, table, columns,
                                      transaction_store_.get(),
                                      &write_ops_queue_, now));
        ZETASQL_RETURN_IF_ERROR(ProcessWriteOpsQueue());
        continue;
      }

      // Process Insert, Update, Replace and InsertOrUpdate.
      ZETASQL_ASSIGN_OR_RETURN(
          std::vector<absl::optional<int>> key_indices,
          ExtractPrimaryKeyIndices(mutation_op.columns, table->primary_key()));
      for (ValueList row : mutation_op.rows) {
        ZETASQL_RET_CHECK_EQ(row.size(), columns.size())
            << "MutationOp has difference in size of column and value vectors, "
               "mutation op: "
            << mutation_op.DebugString();

        ZETASQL_RETURN_IF_ERROR(FlattenMutationOpRow(
            table, columns, key_indices, row, mutation_op.type,
            transaction_store_.get(), &write_ops_queue_, now));
        ZETASQL_RETURN_IF_ERROR(ProcessWriteOpsQueue());
      }
    }
    return ApplyStatementVerifiers();
  });
}

zetasql_base::Status ReadWriteTransaction::Commit() {
  return GuardedCall([&]() -> zetasql_base::Status {
    mu_.AssertHeld();

    // Pick a commit timestamp.
    ZETASQL_ASSIGN_OR_RETURN(commit_timestamp_, lock_handle_->ReserveCommitTimestamp());

    // Write the mutations to the base storage.
    zetasql_base::Status flush_status =
        transaction_store_->FlushMutation(commit_timestamp_);
    ZETASQL_RETURN_IF_ERROR(lock_handle_->MarkCommitted());
    if (!flush_status.ok()) {
      return flush_status;
    }

    // Marks the transaction as committed.
    state_ = State::kCommitted;

    // Unlock all locks.
    lock_handle_->UnlockAll();

    return zetasql_base::OkStatus();
  });
}

zetasql_base::Status ReadWriteTransaction::Rollback() {
  return GuardedCall([&]() -> zetasql_base::Status {
    mu_.AssertHeld();

    // Reset the transaction state.
    Reset();

    // Mark the transaction as rolledback.
    state_ = State::kRolledback;

    return zetasql_base::OkStatus();
  });
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
