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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/json_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "google/cloud/spanner/bytes.h"
#include "backend/actions/ops.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "backend/transaction/transaction_store.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
#include "nlohmann/detail/value_t.hpp"
#include "nlohmann/json_fwd.hpp"
#include "nlohmann/json.hpp"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
absl::StatusOr<bool> TransactionReadOnlyStore::Exists(const Table* table,
                                                      const Key& key) const {
  absl::StatusOr<ValueList> maybe_row =
      read_only_store_->Lookup(table, key, {});
  if (maybe_row.status().code() == absl::StatusCode::kNotFound) {
    return false;
  } else if (!maybe_row.ok()) {
    return maybe_row.status();
  }
  return true;
}

absl::StatusOr<bool> TransactionReadOnlyStore::PrefixExists(
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

absl::StatusOr<std::unique_ptr<StorageIterator>> TransactionReadOnlyStore::Read(
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
