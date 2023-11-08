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

#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/random/random.h"
#include "absl/random/uniform_int_distribution.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/actions/change_stream.h"
#include "backend/actions/context.h"
#include "backend/actions/manager.h"
#include "backend/actions/ops.h"
#include "backend/common/ids.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/value.h"
#include "backend/locking/manager.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/storage/iterator.h"
#include "backend/storage/storage.h"
#include "backend/transaction/actions.h"
#include "backend/transaction/commit_timestamp.h"
#include "backend/transaction/flush.h"
#include "backend/transaction/options.h"
#include "backend/transaction/resolve.h"
#include "backend/transaction/row_cursor.h"
#include "backend/transaction/transaction_store.h"
#include "common/change_stream.h"
#include "common/clock.h"
#include "common/config.h"
#include "common/constants.h"
#include "common/errors.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Flattens delete mutation to one write op for each key being deleted.
absl::StatusOr<std::vector<WriteOp>> FlattenDeleteOp(
    const Table* table, const std::vector<KeyRange>& key_ranges,
    const TransactionStore* transaction_store) {
  std::vector<WriteOp> write_ops;
  for (const KeyRange& key_range : key_ranges) {
    std::unique_ptr<StorageIterator> itr;
    ZETASQL_RETURN_IF_ERROR(transaction_store->Read(table, key_range,
                                            /*columns= */ {}, &itr));
    while (itr->Next()) {
      write_ops.push_back(DeleteOp{table, itr->Key()});
    }
  }
  return std::move(write_ops);
}

// Converts each MutationOp row to a WriteOp based on the MutationOpType:
// - MutatioOpTyp::kReplace: converts to DeleteOp followed by InsertOp.
// - MutationOpType::kInsertOrUpdate: if the row already exists, converts
//   to UpdateOp. Otherwise converts to InsertOp.
// - MutationOpType::kInsert | kDelete | kUpdate: converts to
//   corresponding WriteOp of the same type.
absl::StatusOr<std::vector<WriteOp>> FlattenNonDeleteOpRow(
    MutationOpType type, const Table* table,
    const std::vector<const Column*>& columns, const Key& key,
    const ValueList& row, const TransactionStore* transaction_store) {
  std::vector<WriteOp> write_ops;
  switch (type) {
    case MutationOpType::kInsert: {
      write_ops.push_back(InsertOp{table, key, columns, row});
      break;
    }
    case MutationOpType::kUpdate: {
      write_ops.push_back(UpdateOp{table, key, columns, row});
      break;
    }
    case MutationOpType::kInsertOrUpdate: {
      absl::StatusOr<ValueList> maybe_row =
          transaction_store->Lookup(table, key,
                                    /*columns= */ {});
      if (maybe_row.ok()) {
        // Row exists and therefore we should only update.
        write_ops.push_back(UpdateOp{table, key, columns, row});
      } else if (maybe_row.status().code() == absl::StatusCode::kNotFound) {
        write_ops.push_back(InsertOp{table, key, columns, row});
      } else {
        return maybe_row.status();
      }
      break;
    }
    case MutationOpType::kReplace: {
      write_ops.push_back(DeleteOp{table, key});
      write_ops.push_back(InsertOp{table, key, columns, row});
      break;
    }
    case MutationOpType::kDelete: {
      break;
    }
  }
  return std::move(write_ops);
}

bool ShouldAbortOnFirstCommit() {
  absl::BitGen gen;
  return config::fault_injection_enabled() &&
         absl::uniform_int_distribution<int>(1, 100)(gen) <= 5;
}

RetryState MakeRetryState(const RetryState& retry_state, Clock* clock) {
  RetryState state = retry_state;
  state.priority = (retry_state.priority == 0 ? absl::ToUnixMicros(clock->Now())
                                              : retry_state.priority);
  return state;
}

// Splits key_range into 2 parts by key with the key itself being removed. The
// resulting key ranges are appended to range_list. Returns true if the key was
// found within the given range, false otherwise.
bool SplitKeyRangeAndAppend(const KeyRange& key_range, const Key& key,
                            std::vector<KeyRange>* range_list) {
  if (!key_range.Contains(key)) {
    return false;
  }

  if (key == key_range.start_key() && key == key_range.limit_key()) {
    // Range only contains the one key.
    return true;
  } else if (key == key_range.start_key()) {
    range_list->push_back(KeyRange(EndpointType::kOpen, key,
                                   key_range.limit_type(),
                                   key_range.limit_key()));
  } else if (key == key_range.limit_key()) {
    range_list->push_back(KeyRange(key_range.start_type(),
                                   key_range.start_key(), EndpointType::kOpen,
                                   key));
  } else {
    range_list->push_back(KeyRange(key_range.start_type(),
                                   key_range.start_key(), EndpointType::kOpen,
                                   key));
    range_list->push_back(KeyRange(EndpointType::kOpen, key,
                                   key_range.limit_type(),
                                   key_range.limit_key()));
  }
  return true;
}

}  // namespace

ReadWriteTransaction::ReadWriteTransaction(
    const ReadWriteOptions& options, const RetryState& retry_state,
    TransactionID transaction_id, Clock* clock, Storage* storage,
    LockManager* lock_manager, const VersionedCatalog* const versioned_catalog,
    ActionManager* action_manager)
    : options_(options),
      retry_state_(MakeRetryState(retry_state, clock)),
      id_(transaction_id),
      clock_(clock),
      base_storage_(storage),
      versioned_catalog_(versioned_catalog),
      lock_handle_(
          lock_manager->CreateHandle(transaction_id, retry_state_.priority)),
      transaction_store_(std::make_unique<TransactionStore>(
          base_storage_, lock_handle_.get())),
      action_manager_(action_manager),
      action_context_(std::make_unique<ActionContext>(
          std::make_unique<TransactionReadOnlyStore>(transaction_store_.get()),
          std::make_unique<TransactionEffectsBuffer>(&write_ops_queue_),
          clock)),
      schema_(versioned_catalog_->GetLatestSchema()) {}

absl::StatusOr<absl::Time> ReadWriteTransaction::GetCommitTimestamp() {
  absl::MutexLock lock(&mu_);
  if (state_ != State::kCommitted) {
    return error::Internal(
        absl::StrCat("Commit timestamp is only available after call to "
                     "Transaction Commit. Transaction: ",
                     id(), " is in state: ", state_));
  }
  return commit_timestamp_;
}

absl::Status ReadWriteTransaction::Read(const ReadArg& read_arg,
                                        std::unique_ptr<RowCursor>* cursor) {
  return GuardedCall(OpType::kRead, [&]() -> absl::Status {
    mu_.AssertHeld();

    ZETASQL_ASSIGN_OR_RETURN(const ResolvedReadArg& resolved_read_arg,
                     ResolveReadArg(read_arg, schema_));

    std::vector<std::unique_ptr<StorageIterator>> iterators;
    for (const auto& key_range : resolved_read_arg.key_ranges) {
      std::unique_ptr<StorageIterator> itr;
      ZETASQL_RETURN_IF_ERROR(transaction_store_->Read(
          resolved_read_arg.table, key_range, resolved_read_arg.columns, &itr,
          false /*allow_pending_commit_timestamps_in_read*/));
      iterators.push_back(std::move(itr));
    }
    *cursor = std::make_unique<StorageIteratorRowCursor>(
        std::move(iterators), resolved_read_arg.columns);
    return absl::OkStatus();
  });
}

absl::Status ReadWriteTransaction::ApplyValidators(const WriteOp& op) {
  return action_registry_->ExecuteValidators(action_context_.get(), op);
}

absl::Status ReadWriteTransaction::ApplyEffectors(const WriteOp& op) {
  return action_registry_->ExecuteEffectors(action_context_.get(), op);
}

absl::Status ReadWriteTransaction::ApplyStatementVerifiers() {
  for (const auto& write_op : transaction_store_->GetBufferedOps()) {
    ZETASQL_RETURN_IF_ERROR(
        action_registry_->ExecuteVerifiers(action_context_.get(), write_op));
  }
  return absl::OkStatus();
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

absl::Status ReadWriteTransaction::GuardedCall(
    OpType op, const std::function<absl::Status()>& fn) {
  absl::MutexLock lock(&mu_);
  switch (state_) {
    case State::kRolledback: {
      return error::Internal(absl::StrCat(
          "Invalid call to Rolledback transaction. Transaction: ", id()));
      break;
    }
    case State::kInvalid: {
      if (op != OpType::kRollback) {
        return error::Internal(absl::StrCat(
            "Invalid call to Aborted transaction. Transaction: ", id()));
      }
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
      action_registry_ = maybe_action_registry.value();
      state_ = State::kActive;
      break;
    }
    case State::kActive: {
      if (schema_ != versioned_catalog_->GetLatestSchema()) {
        Reset();
        ++retry_state_.abort_retry_count;
        return error::AbortDueToConcurrentSchemaChange(id_);
      }
      break;
    }
  }

  absl::Status status = fn();

  if (!status.ok()) {
    if (status.code() == absl::StatusCode::kAborted) {
      // Reset the transaction and release the lock handle. Always reset the
      // transaction in the case of an abort error. Aborts do not invalidate the
      // transaction.
      Reset();
      ++retry_state_.abort_retry_count;
    } else if (op != OpType::kRead) {
      // A failing read should never invalidate a transaction, but constraint
      // errors on other operation types will invalidate it.
      Reset();
      status.SetPayload(kConstraintError, absl::Cord(""));
    }
  }
  return status;
}

absl::Status ReadWriteTransaction::ProcessWriteOps(
    const std::vector<WriteOp>& write_ops) {
  mu_.AssertHeld();

  for (const auto& write_op : write_ops) {
    write_ops_queue_.push(write_op);
  }

  while (!write_ops_queue_.empty()) {
    WriteOp write_op = write_ops_queue_.front();
    write_ops_queue_.pop();

    // Process the operation.
    ZETASQL_RETURN_IF_ERROR(ApplyValidators(write_op));
    ZETASQL_RETURN_IF_ERROR(ApplyEffectors(write_op));

    // Apply to transaction store.
    ZETASQL_RETURN_IF_ERROR(transaction_store_->BufferWriteOp(write_op));
  }

  return absl::OkStatus();
}

absl::Status ReadWriteTransaction::ProcessChangeStreamWriteOps() {
  mu_.AssertHeld();
  auto write_ops =
      BuildChangeStreamWriteOps(schema_, transaction_store_->GetBufferedOps(),
                                action_context_->store(), id_);
  for (const WriteOp& writeop : write_ops) {
    ZETASQL_RETURN_IF_ERROR(transaction_store_->BufferWriteOp(writeop));
  }
  return absl::OkStatus();
}

absl::StatusOr<ResolvedMutationOp>
ReadWriteTransaction::ResolveNonDeleteMutationOp(const MutationOp& mutation_op,
                                                 const Schema* schema) {
  ZETASQL_RET_CHECK(mutation_op.type != MutationOpType::kDelete);

  const Table* table = schema->FindTable(mutation_op.table);

  if (IsChangeStreamPartitionTable(mutation_op.table)) {
    ZETASQL_ASSIGN_OR_RETURN(table,
                     FindChangeStreamPartitionTable(schema, mutation_op.table));
  }

  if (table == nullptr) {
    return error::TableNotFound(mutation_op.table);
  }

  ResolvedMutationOp resolved_mutation_op;
  resolved_mutation_op.table = table;
  resolved_mutation_op.type = mutation_op.type;

  // Vector of generated values for each row of operation.
  std::vector<std::vector<zetasql::Value>> generated_values(
      mutation_op.rows.size(), std::vector<zetasql::Value>());
  std::vector<const Column*> columns_with_generated_values;
  // Compute values for default primary keys that don't appear in this
  // mutation op:
  ZETASQL_RETURN_IF_ERROR(action_registry_->ExecuteGeneratedKeyEffectors(
      mutation_op, &generated_values, &columns_with_generated_values));

  ZETASQL_ASSIGN_OR_RETURN(std::vector<const Column*> columns,
                   GetColumnsByName(table, mutation_op.columns));
  // If we have key columns with generated default values, append them here:
  if (!columns_with_generated_values.empty()) {
    columns.insert(columns.end(), columns_with_generated_values.begin(),
                   columns_with_generated_values.end());
  }

  ZETASQL_ASSIGN_OR_RETURN(std::vector<std::optional<int>> key_indices,
                   ExtractPrimaryKeyIndices(columns, table->primary_key()));

  for (int i = 0; i < mutation_op.rows.size(); i++) {
    const ValueList& row = mutation_op.rows[i];
    ValueList new_row = row;
    // If we have key columns with generated/default values, append them here:
    new_row.insert(new_row.end(), generated_values[i].begin(),
                   generated_values[i].end());

    ZETASQL_RET_CHECK_EQ(new_row.size(), columns.size())
        << "MutationOp has difference in size of column and value vectors, "
           "mutation op: "
        << mutation_op.DebugString();

    ZETASQL_ASSIGN_OR_RETURN(resolved_mutation_op.rows.emplace_back(),
                     MaybeSetCommitTimestampSentinel(columns, new_row));

    resolved_mutation_op.keys.push_back(ComputeKey(
        resolved_mutation_op.rows.back(), table->primary_key(), key_indices));
  }

  resolved_mutation_op.columns = std::move(columns);

  return resolved_mutation_op;
}

absl::Status ReadWriteTransaction::Write(const Mutation& mutation) {
  return GuardedCall(OpType::kWrite, [&]() -> absl::Status {
    mu_.AssertHeld();

    // When writing, comparison may be required in the process. Comparison of
    // PG.NUMERIC calls PG to get comparison result (see function
    // ValueContentLess in datatypes/extended/pg_numeric_type.cc) and therefore
    // requires a PG arena.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
        postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));

    for (const MutationOp& mutation_op : mutation.ops()) {
      if (mutation_op.type == MutationOpType::kDelete) {
        // Process Delete.
        ZETASQL_ASSIGN_OR_RETURN(
            ResolvedMutationOp resolved_mutation_op,
            ResolveDeleteMutationOp(mutation_op, schema_, clock_->Now()));
        const std::string& table_name = resolved_mutation_op.table->Name();

        std::vector<KeyRange>& key_ranges =
            deleted_key_ranges_by_table_[table_name];
        key_ranges.insert(key_ranges.end(),
                          resolved_mutation_op.key_ranges.begin(),
                          resolved_mutation_op.key_ranges.end());
        ZETASQL_ASSIGN_OR_RETURN(std::vector<WriteOp> write_ops,
                         FlattenDeleteOp(resolved_mutation_op.table,
                                         resolved_mutation_op.key_ranges,
                                         transaction_store_.get()));

        ZETASQL_RETURN_IF_ERROR(ProcessWriteOps(write_ops));
      } else {
        // Process non-delete Mutation ops.
        ZETASQL_RETURN_IF_ERROR(ValidateNonDeleteMutationOp(mutation_op, schema_));
        ZETASQL_ASSIGN_OR_RETURN(ResolvedMutationOp resolved_mutation_op,
                         ResolveNonDeleteMutationOp(mutation_op, schema_));
        const std::string& table_name = resolved_mutation_op.table->Name();

        // Process Insert, Update, Replace and InsertOrUpdate.
        for (int i = 0; i < resolved_mutation_op.rows.size(); i++) {
          // Spanner allows deleted entries to be reinserted within the same
          // transaction, so we must update the deleted ranges list in this
          // case.
          if (resolved_mutation_op.type == MutationOpType::kInsert ||
              resolved_mutation_op.type == MutationOpType::kInsertOrUpdate) {
            std::vector<KeyRange> split_key_ranges;
            auto& deleted_key_ranges = deleted_key_ranges_by_table_[table_name];
            for (auto it = deleted_key_ranges.begin();
                 it != deleted_key_ranges.end();) {
              if (SplitKeyRangeAndAppend(*it, resolved_mutation_op.keys[i],
                                         &split_key_ranges)) {
                it = deleted_key_ranges.erase(it);
              } else {
                ++it;
              }
            }
            // Insert split key ranges back into the list of deleted key ranges.
            for (const KeyRange& key_range : split_key_ranges) {
              deleted_key_ranges.push_back(key_range);
            }
          }
          if (resolved_mutation_op.type == MutationOpType::kUpdate) {
            for (const KeyRange& key_range :
                 deleted_key_ranges_by_table_[table_name]) {
              if (key_range.Contains(resolved_mutation_op.keys[i])) {
                return error::UpdateDeletedRowInTransaction(
                    table_name, resolved_mutation_op.keys[i].DebugString());
              }
            }
          }
          ZETASQL_ASSIGN_OR_RETURN(
              std::vector<WriteOp> write_ops,
              FlattenNonDeleteOpRow(
                  resolved_mutation_op.type, resolved_mutation_op.table,
                  resolved_mutation_op.columns, resolved_mutation_op.keys[i],
                  resolved_mutation_op.rows[i], transaction_store_.get()));

          ZETASQL_RETURN_IF_ERROR(ProcessWriteOps(write_ops));
        }
      }
    }
    ZETASQL_RETURN_IF_ERROR(ProcessChangeStreamWriteOps());
    return ApplyStatementVerifiers();
  });
}

absl::Status ReadWriteTransaction::Commit() {
  return GuardedCall(OpType::kCommit, [&]() -> absl::Status {
    mu_.AssertHeld();

    if (retry_state_.abort_retry_count == 0 && ShouldAbortOnFirstCommit()) {
      return error::AbortReadWriteTransactionOnFirstCommit(id_);
    }

    // When committing, comparison may be required in the process. Comparison of
    // PG.NUMERIC calls PG to get comparison result (see function
    // ValueContentLess in datatypes/extended/pg_numeric_type.cc) and therefore
    // requires a PG arena.
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
        postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));

    // Pick a commit timestamp.
    ZETASQL_ASSIGN_OR_RETURN(commit_timestamp_, lock_handle_->ReserveCommitTimestamp());

    // Write the mutations to the base storage.
    absl::Status flush_status = FlushWriteOpsToStorage(
        transaction_store_->GetBufferedOps(), base_storage_, commit_timestamp_);
    ZETASQL_RETURN_IF_ERROR(lock_handle_->MarkCommitted());
    if (!flush_status.ok()) {
      return flush_status;
    }

    // Mark the transaction as committed.
    state_ = State::kCommitted;

    // Unlock all locks.
    lock_handle_->UnlockAll();

    return absl::OkStatus();
  });
}

absl::Status ReadWriteTransaction::Rollback() {
  return GuardedCall(OpType::kRollback, [&]() -> absl::Status {
    mu_.AssertHeld();

    // Reset the transaction state. This is done to release the locks. The
    // transaction cannot be reused.
    Reset();

    // Mark the transaction as rolledback.
    state_ = State::kRolledback;

    return absl::OkStatus();
  });
}

absl::Status ReadWriteTransaction::Invalidate() {
  return GuardedCall(OpType::kInvalidate, [&]() -> absl::Status {
    mu_.AssertHeld();

    // Reset the transaction state. This is done to release the locks. The
    // transaction cannot be reused.
    Reset();

    // Mark the transaction as invalidated.
    state_ = State::kInvalid;

    return absl::OkStatus();
  });
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
