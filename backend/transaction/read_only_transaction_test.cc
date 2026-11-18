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

#include "backend/transaction/read_only_transaction.h"

#include <ctime>

#include "googlesql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/versioned_catalog.h"
#include "backend/storage/in_memory_storage.h"
#include "backend/transaction/options.h"
#include "common/clock.h"
#include "tests/common/schema_constructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

class ReadOnlyTransactionTest : public testing::Test {
 protected:
  TransactionID txn_id_ = 1;
  Clock clock_;
  absl::Time t0_ = clock_.Now();
  InMemoryStorage storage_;
  LockManager lock_manager_ = LockManager(&clock_);
  VersionedCatalog versioned_catalog_;
  absl::Time read_timestamp_;
};

TEST_F(ReadOnlyTransactionTest, StongSnapshotReadTimestamp) {
  ReadOnlyOptions opts;
  opts.bound = TimestampBound::kStrongRead;

  ReadOnlyTransaction txn(opts, txn_id_, &clock_, &storage_, &lock_manager_,
                          &versioned_catalog_);
  read_timestamp_ = txn.read_timestamp();
  EXPECT_GT(clock_.Now(), read_timestamp_);
}

TEST_F(ReadOnlyTransactionTest, ExactStalenessSnapshotReadTimestamp) {
  ReadOnlyOptions opts;
  opts.bound = TimestampBound::kExactStaleness;
  opts.staleness = absl::Microseconds(1);

  ReadOnlyTransaction txn(opts, txn_id_, &clock_, &storage_, &lock_manager_,
                          &versioned_catalog_);
  read_timestamp_ = txn.read_timestamp();
  EXPECT_GT(clock_.Now() - absl::Microseconds(1), read_timestamp_);
}

TEST_F(ReadOnlyTransactionTest, MaxStalenessSnapshotReadTimestamp) {
  ReadOnlyOptions opts;
  opts.bound = TimestampBound::kMaxStaleness;
  opts.staleness = absl::Microseconds(1);

  ReadOnlyTransaction txn(opts, txn_id_, &clock_, &storage_, &lock_manager_,
                          &versioned_catalog_);
  read_timestamp_ = txn.read_timestamp();
  EXPECT_GT(clock_.Now(), read_timestamp_);
  EXPECT_LE(opts.timestamp - opts.staleness, read_timestamp_);
}

TEST_F(ReadOnlyTransactionTest, MinTimestampSnapshotReadTimestamp) {
  ReadOnlyOptions opts;
  opts.bound = TimestampBound::kMinTimestamp;
  opts.timestamp = t0_;

  ReadOnlyTransaction txn(opts, txn_id_, &clock_, &storage_, &lock_manager_,
                          &versioned_catalog_);
  read_timestamp_ = txn.read_timestamp();
  EXPECT_GT(clock_.Now(), read_timestamp_);
  EXPECT_LE(t0_, read_timestamp_);
}

TEST_F(ReadOnlyTransactionTest, ExactTimestampSnapshotReadTimestamp) {
  ReadOnlyOptions opts;
  opts.bound = TimestampBound::kExactTimestamp;
  opts.timestamp = t0_;

  ReadOnlyTransaction txn(opts, txn_id_, &clock_, &storage_, &lock_manager_,
                          &versioned_catalog_);
  read_timestamp_ = txn.read_timestamp();
  EXPECT_EQ(t0_, read_timestamp_);
}

TEST_F(ReadOnlyTransactionTest, GetSchema) {
  VersionedCatalog catalog;
  googlesql::TypeFactory type_factory{};
  GOOGLESQL_EXPECT_OK(
      catalog.AddSchema(t0_, test::CreateSchemaWithOneTable(&type_factory)));

  // A strong read finds the schema created at t0_.
  ReadOnlyOptions opts1;
  opts1.bound = TimestampBound::kStrongRead;

  ReadOnlyTransaction txn1(opts1, txn_id_, &clock_, &storage_, &lock_manager_,
                           &catalog);
  ASSERT_NE(txn1.schema(), nullptr);
  ASSERT_NE(txn1.schema()->FindTable("test_table"), nullptr);

  // A snapshot read at t0_ - 1 us gets an empty schema.
  ReadOnlyOptions opts2;
  opts2.bound = TimestampBound::kExactTimestamp;
  opts2.timestamp = t0_ - absl::Microseconds(1);

  ReadOnlyTransaction txn2(opts2, txn_id_, &clock_, &storage_, &lock_manager_,
                           &catalog);
  ASSERT_NE(txn2.schema(), nullptr);
  EXPECT_EQ(txn2.schema()->FindTable("test_table"), nullptr);
}

TEST_F(ReadOnlyTransactionTest, WaitsForFutureReadTime) {
  VersionedCatalog catalog;
  googlesql::TypeFactory type_factory{};
  GOOGLESQL_EXPECT_OK(
      catalog.AddSchema(t0_, test::CreateSchemaWithOneTable(&type_factory)));

  ReadOnlyOptions opts;
  opts.bound = TimestampBound::kExactTimestamp;
  opts.timestamp = clock_.Now() + absl::Microseconds(1000);

  ReadOnlyTransaction txn(opts, txn_id_, &clock_, &storage_, &lock_manager_,
                          &catalog);
  ASSERT_NE(txn.schema(), nullptr);
  ASSERT_NE(txn.schema()->FindTable("test_table"), nullptr);

  // A snapshot read at t0_ + 1000 will wait for read time to become current
  // before being able to access database and return the schema.
  EXPECT_GE(clock_.Now(), opts.timestamp);
}

TEST_F(ReadOnlyTransactionTest, ReadOlderThanCollectedVersionsFails) {
  VersionedCatalog catalog;
  googlesql::TypeFactory type_factory{};
  GOOGLESQL_EXPECT_OK(
      catalog.AddSchema(t0_, test::CreateSchemaWithOneTable(&type_factory)));
  const Table* table = catalog.GetLatestSchema()->FindTable("test_table");
  ASSERT_NE(table, nullptr);
  const std::vector<ColumnID> columns = {
      table->FindColumn("int64_col")->id(),
      table->FindColumn("string_col")->id()};

  // A row written twice, and its first version collected by a storage whose
  // retention period is shorter than the database's hour.
  absl::Time t1 = clock_.Now();
  GOOGLESQL_EXPECT_OK(storage_.Write(t1, table->id(), Key({googlesql::values::Int64(1)}),
                           columns,
                           {googlesql::values::Int64(1),
                            googlesql::values::String("first")}));
  absl::Time t2 = clock_.Now();
  GOOGLESQL_EXPECT_OK(storage_.Write(t2, table->id(), Key({googlesql::values::Int64(1)}),
                           columns,
                           {googlesql::values::Int64(1),
                            googlesql::values::String("second")}));
  storage_.SetVersionRetentionPeriod(absl::ZeroDuration());
  storage_.CollectGarbage(t2);
  ASSERT_EQ(storage_.EarliestVersionTime(), t2);

  ReadArg read_arg;
  read_arg.table = "test_table";
  read_arg.columns = {"int64_col", "string_col"};
  read_arg.key_set = KeySet::All();
  std::unique_ptr<RowCursor> cursor;

  // A read at t1 is within the database's retention period, but what it would
  // see is gone: it fails as a read past the retention period does.
  ReadOnlyOptions before;
  before.bound = TimestampBound::kExactTimestamp;
  before.timestamp = t1;
  ReadOnlyTransaction before_txn(before, txn_id_, &clock_, &storage_,
                                 &lock_manager_, &catalog);
  EXPECT_THAT(before_txn.Read(read_arg, &cursor),
              googlesql_base::testing::StatusIs(
                  absl::StatusCode::kFailedPrecondition));

  ReadOnlyOptions at;
  at.bound = TimestampBound::kExactTimestamp;
  at.timestamp = t2;
  ReadOnlyTransaction at_txn(at, txn_id_, &clock_, &storage_, &lock_manager_,
                             &catalog);
  GOOGLESQL_ASSERT_OK(at_txn.Read(read_arg, &cursor));
  ASSERT_TRUE(cursor->Next());
  EXPECT_EQ(cursor->ColumnValue(1), googlesql::values::String("second"));
  EXPECT_FALSE(cursor->Next());
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
