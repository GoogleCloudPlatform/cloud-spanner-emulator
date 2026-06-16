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

#include "backend/storage/persistent_storage.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/storage/persistence.pb.h"
#include "backend/storage/wal_writer.h"
#include "zetasql/public/value.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace {

using zetasql::values::Int64;
using zetasql::values::String;

class PersistentStorageTest : public testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = testing::TempDir() + "/persistent_storage_test";
    mkdir(test_dir_.c_str(), 0755);

    auto wal_or = WalWriter::Create(test_dir_);
    ASSERT_TRUE(wal_or.ok());
    wal_writer_ = std::shared_ptr<WalWriter>(std::move(*wal_or));

    auto storage_or =
        PersistentStorage::Create("test://db/uri", wal_writer_);
    ASSERT_TRUE(storage_or.ok());
    storage_ = std::move(*storage_or);
  }

  void TearDown() override {
    storage_.reset();
    wal_writer_.reset();
    WalWriter::Clear(test_dir_).IgnoreError();
    rmdir(test_dir_.c_str());
  }

  const TableID kTableId = "test_table:0";
  const ColumnID kColumnId = "test_column:0";

  std::string test_dir_;
  std::shared_ptr<WalWriter> wal_writer_;
  std::unique_ptr<PersistentStorage> storage_;
};

TEST_F(PersistentStorageTest, WriteAndReadBack) {
  absl::Time t0 = absl::Now();

  ZETASQL_EXPECT_OK(storage_->Write(t0, kTableId, Key({Int64(1)}), {kColumnId},
                             {String("value-1")}));

  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_->Lookup(t0, kTableId, Key({Int64(1)}), {kColumnId},
                              &values));
  EXPECT_THAT(values, testing::ElementsAre(String("value-1")));
}

TEST_F(PersistentStorageTest, WriteCreatesWalRecords) {
  absl::Time t0 = absl::Now();

  ZETASQL_EXPECT_OK(storage_->Write(t0, kTableId, Key({Int64(1)}), {kColumnId},
                             {String("value-1")}));
  ZETASQL_EXPECT_OK(storage_->Write(t0, kTableId, Key({Int64(2)}), {kColumnId},
                             {String("value-2")}));

  // Flush the WAL writer.
  ZETASQL_EXPECT_OK(wal_writer_->Sync());

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  ASSERT_EQ(records.size(), 2);

  // Verify the first record is a write for key 1.
  EXPECT_TRUE(records[0].has_entry());
  ASSERT_EQ(records[0].entry().mutations_size(), 1);
  EXPECT_TRUE(records[0].entry().mutations(0).has_write());
  EXPECT_EQ(records[0].entry().mutations(0).write().table_id(), kTableId);
  EXPECT_EQ(records[0].entry().database_uri(), "test://db/uri");

  // Verify the second record is a write for key 2.
  EXPECT_TRUE(records[1].has_entry());
  ASSERT_EQ(records[1].entry().mutations_size(), 1);
  EXPECT_TRUE(records[1].entry().mutations(0).has_write());
}

TEST_F(PersistentStorageTest, DeleteCreatesWalRecords) {
  absl::Time t0 = absl::Now();

  // Write some data first.
  ZETASQL_EXPECT_OK(storage_->Write(t0, kTableId, Key({Int64(1)}), {kColumnId},
                             {String("value-1")}));

  // Delete a range.
  KeyRange range = KeyRange::ClosedOpen(Key({Int64(0)}), Key({Int64(5)}));
  ZETASQL_EXPECT_OK(storage_->Delete(t0, kTableId, range));

  ZETASQL_EXPECT_OK(wal_writer_->Sync());

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  ASSERT_EQ(records.size(), 2);

  // First record is the write.
  EXPECT_TRUE(records[0].entry().mutations(0).has_write());

  // Second record is the delete.
  EXPECT_TRUE(records[1].has_entry());
  ASSERT_EQ(records[1].entry().mutations_size(), 1);
  EXPECT_TRUE(records[1].entry().mutations(0).has_delete_op());
  EXPECT_EQ(records[1].entry().mutations(0).delete_op().table_id(), kTableId);
}

TEST_F(PersistentStorageTest, ReadsDoNotCreateWalRecords) {
  absl::Time t0 = absl::Now();

  // Write one record.
  ZETASQL_EXPECT_OK(storage_->Write(t0, kTableId, Key({Int64(1)}), {kColumnId},
                             {String("value-1")}));

  // Perform a Lookup (read).
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_->Lookup(t0, kTableId, Key({Int64(1)}), {kColumnId},
                              &values));

  // Perform a Read (range scan).
  std::unique_ptr<StorageIterator> itr;
  KeyRange range = KeyRange::ClosedOpen(Key({Int64(0)}), Key({Int64(5)}));
  ZETASQL_EXPECT_OK(storage_->Read(t0, kTableId, range, {kColumnId}, &itr));

  ZETASQL_EXPECT_OK(wal_writer_->Sync());

  // Only the one write should have produced a WAL record.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  EXPECT_EQ(records.size(), 1);
}

TEST_F(PersistentStorageTest, MultipleWritesAndReads) {
  absl::Time t0 = absl::Now();

  // Write multiple rows.
  ZETASQL_EXPECT_OK(storage_->Write(t0, kTableId, Key({Int64(1)}), {kColumnId},
                             {String("alpha")}));
  ZETASQL_EXPECT_OK(storage_->Write(t0, kTableId, Key({Int64(2)}), {kColumnId},
                             {String("beta")}));
  ZETASQL_EXPECT_OK(storage_->Write(t0, kTableId, Key({Int64(3)}), {kColumnId},
                             {String("gamma")}));

  // Read them back individually.
  std::vector<zetasql::Value> values;
  ZETASQL_EXPECT_OK(storage_->Lookup(t0, kTableId, Key({Int64(1)}), {kColumnId},
                              &values));
  EXPECT_THAT(values, testing::ElementsAre(String("alpha")));

  ZETASQL_EXPECT_OK(storage_->Lookup(t0, kTableId, Key({Int64(2)}), {kColumnId},
                              &values));
  EXPECT_THAT(values, testing::ElementsAre(String("beta")));

  ZETASQL_EXPECT_OK(storage_->Lookup(t0, kTableId, Key({Int64(3)}), {kColumnId},
                              &values));
  EXPECT_THAT(values, testing::ElementsAre(String("gamma")));

  // Verify 3 WAL records were created.
  ZETASQL_EXPECT_OK(wal_writer_->Sync());
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto records, WalWriter::ReadAll(test_dir_));
  EXPECT_EQ(records.size(), 3);
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
