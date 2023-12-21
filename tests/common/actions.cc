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

#include "tests/common/actions.h"

#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "zetasql/public/json_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/actions/ops.h"
#include "backend/common/rows.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/iterator.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
#include "nlohmann/json_fwd.hpp"
#include "nlohmann/json.hpp"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {
using JSON = ::nlohmann::json;

absl::Status TestReadOnlyStore::Insert(
    const Table* table, const Key& key, absl::Span<const Column* const> columns,
    const std::vector<zetasql::Value>& values) {
  return store_.Write(absl::InfiniteFuture(), table->id(), key,
                      GetColumnIDs(columns), values);
}

absl::StatusOr<bool> TestReadOnlyStore::Exists(const Table* table,
                                               const Key& key) const {
  absl::Status s =
      store_.Lookup(absl::InfiniteFuture(), table->id(), key, {}, {});
  if (s.code() == absl::StatusCode::kNotFound) {
    return false;
  } else if (!s.ok()) {
    return s;
  }
  return true;
}

absl::StatusOr<bool> TestReadOnlyStore::PrefixExists(
    const Table* table, const Key& prefix_key) const {
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(store_.Read(absl::InfiniteFuture(), table->id(),
                              KeyRange::Point(prefix_key), {}, &itr));
  if (itr->Next() && itr->Status().ok()) {
    return true;
  }
  ZETASQL_RETURN_IF_ERROR(itr->Status());
  return false;
}

absl::StatusOr<ValueList> TestReadOnlyStore::ReadCommitted(
    const Table* table, const Key& key,
    std::vector<const Column*> columns) const {
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(store_.Read(absl::InfiniteFuture(), table->id(),
                              KeyRange::Point(key), GetColumnIDs(columns),
                              &itr));
  ValueList values;
  while (itr->Next()) {
    for (int i = 0; i < itr->NumColumns(); i++) {
      if (itr->ColumnValue(i).is_valid()) {
        values.push_back(itr->ColumnValue(i));
      } else {
        values.push_back(zetasql::values::Null(columns[i]->GetType()));
      }
    }
  }
  return values;
}

absl::StatusOr<std::unique_ptr<StorageIterator>> TestReadOnlyStore::Read(
    const Table* table, const KeyRange& key_range,
    const absl::Span<const Column* const> columns) const {
  std::unique_ptr<StorageIterator> itr;
  ZETASQL_RETURN_IF_ERROR(store_.Read(absl::InfiniteFuture(), table->id(), key_range,
                              GetColumnIDs(columns), &itr));
  return itr;
}

void TestEffectsBuffer::Insert(const Table* table, const Key& key,
                               const absl::Span<const Column* const> columns,
                               const std::vector<zetasql::Value>& values) {
  ops_queue_->push(
      InsertOp{table, key, {columns.begin(), columns.end()}, values});
}

void TestEffectsBuffer::Update(const Table* table, const Key& key,
                               const absl::Span<const Column* const> columns,
                               const std::vector<zetasql::Value>& values) {
  ops_queue_->push(
      UpdateOp{table, key, {columns.begin(), columns.end()}, values});
}

void TestEffectsBuffer::Delete(const Table* table, const Key& key) {
  ops_queue_->push(DeleteOp{table, key});
}

WriteOp ActionsTest::Insert(const Table* table, const Key& key,
                            absl::Span<const Column* const> columns,
                            const std::vector<zetasql::Value> values) {
  return InsertOp{table, key, {columns.begin(), columns.end()}, values};
}

WriteOp ActionsTest::Update(const Table* table, const Key& key,
                            absl::Span<const Column* const> columns,
                            const std::vector<zetasql::Value> values) {
  return UpdateOp{table, key, {columns.begin(), columns.end()}, values};
}

WriteOp ActionsTest::Delete(const Table* table, const Key& key) {
  return DeleteOp{table, key};
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
