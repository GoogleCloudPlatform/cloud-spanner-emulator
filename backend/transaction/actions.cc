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

#include "backend/transaction/actions.h"

#include <memory>

#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/actions/context.h"
#include "backend/actions/existence.h"
#include "backend/actions/interleave.h"
#include "backend/datamodel/key_range.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "backend/transaction/transaction_store.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

zetasql_base::StatusOr<bool> TransactionReadOnlyStore::Exists(const Table* table,
                                                      const Key& key) const {
  zetasql_base::StatusOr<ValueList> maybe_row =
      read_only_store_->Lookup(table, key, {});
  if (maybe_row.status().code() == absl::StatusCode::kNotFound) {
    return false;
  } else if (!maybe_row.ok()) {
    return maybe_row.status();
  }
  return true;
}

zetasql_base::StatusOr<bool> TransactionReadOnlyStore::PrefixExists(
    const Table* table, const Key& prefix_key) const {
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(
      read_only_store_->Read(table, KeyRange::Point(prefix_key), {}, &itr));
  if (itr->Next() && itr->Status().ok()) {
    return true;
  }
  ZETASQL_RETURN_IF_ERROR(itr->Status());
  return false;
}

zetasql_base::StatusOr<std::unique_ptr<StorageIterator>> TransactionReadOnlyStore::Read(
    const Table* table, const KeyRange& key_range,
    absl::Span<const Column* const> columns) const {
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(read_only_store_->Read(table, key_range, columns, &itr));
  return itr;
}

void TransactionEffectsBuffer::Insert(
    const Table* table, const Key& key, absl::Span<const Column* const> columns,
    const std::vector<zetasql::Value>& values) {
  ops_queue_->push(
      InsertOp{table, key, {columns.begin(), columns.end()}, values});
}

void TransactionEffectsBuffer::Update(
    const Table* table, const Key& key, absl::Span<const Column* const> columns,
    const std::vector<zetasql::Value>& values) {
  ops_queue_->push(
      UpdateOp{table, key, {columns.begin(), columns.end()}, values});
}

void TransactionEffectsBuffer::Delete(const Table* table, const Key& key) {
  ops_queue_->push(DeleteOp{table, key});
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
