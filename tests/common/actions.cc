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

#include <memory>
#include <queue>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/time/time.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/storage/in_memory_iterator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

absl::Status TestReadOnlyStore::Insert(
    const Table* table, const Key& key, absl::Span<const Column* const> columns,
    const std::vector<zetasql::Value>& values) {
  return store_.Write(absl::InfiniteFuture(), table->id(), key,
                      GetColumnIDs(columns), values);
}

zetasql_base::StatusOr<bool> TestReadOnlyStore::Exists(const Table* table,
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

zetasql_base::StatusOr<bool> TestReadOnlyStore::PrefixExists(
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

zetasql_base::StatusOr<std::unique_ptr<StorageIterator>> TestReadOnlyStore::Read(
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
