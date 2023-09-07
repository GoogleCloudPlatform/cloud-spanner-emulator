//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"

#include <memory>
#include <type_traits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"

namespace postgres_translator::test {
namespace {

using ::postgres_translator::spangres::test::GetSpangresTestAnalyzerOptions;
using ::postgres_translator::spangres::test::
    GetSpangresTestCatalogAdapterHolder;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

const TableName kKeyValueTableName({"public", "keyvalue"});

TEST(CatalogAdapterHolderTest, HolderBasicTest) {
  // Without the holder, expect the thread local catalog adapter to be
  // uninitialized.
  EXPECT_THAT(GetCatalogAdapter(), StatusIs(absl::StatusCode::kInternal,
                                            HasSubstr("is not initialized")));
  {
    zetasql::AnalyzerOptions analyzer_options =
        GetSpangresTestAnalyzerOptions();
    std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
        GetSpangresTestCatalogAdapterHolder(analyzer_options);

    // The thread local catalog adapter should be initialized now.
    ZETASQL_ASSERT_OK_AND_ASSIGN(CatalogAdapter* catalog_adapter, GetCatalogAdapter());

    // The user and system catalogs should not be nullptrs.
    EXPECT_NE(catalog_adapter->GetEngineUserCatalog(), nullptr);
    EXPECT_NE(catalog_adapter->GetEngineSystemCatalog(), nullptr);

    // Insert 1 table to the adapter and validate correct mappings
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Oid kvtable_oid,
        catalog_adapter->GetOrGenerateOidFromTableName(kKeyValueTableName));

    EXPECT_THAT(
        catalog_adapter->GetOrGenerateOidFromTableName(kKeyValueTableName),
        IsOkAndHolds(kvtable_oid));
    EXPECT_THAT(catalog_adapter->GetTableNameFromOid(kvtable_oid),
                IsOkAndHolds(kKeyValueTableName));
  }
  // Now the holder has gone out of scope, expect that the adapter pointer is
  // null again.
  EXPECT_THAT(GetCatalogAdapter(), StatusIs(absl::StatusCode::kInternal,
                                            HasSubstr("is not initialized")));
}

// The following fixture allows the same adapter holder to be used by
// multiple tests. Its main purpose is to validate that the bug
// b/172567495 does not exist anymore.
class HolderTestFixture : public testing::Test {
 public:
  static void SetUpTestSuite() {
    analyzer_options_ = std::make_unique<zetasql::AnalyzerOptions>(
        GetSpangresTestAnalyzerOptions());
    adapter_holder_ = GetSpangresTestCatalogAdapterHolder(*analyzer_options_);
    ZETASQL_ASSERT_OK_AND_ASSIGN(CatalogAdapter * catalog_adapter, GetCatalogAdapter());

    // Insert 1 table to the adapter and validate correct mappings
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        key_value_table_oid_,
        catalog_adapter->GetOrGenerateOidFromTableName(kKeyValueTableName));

    EXPECT_THAT(
        catalog_adapter->GetOrGenerateOidFromTableName(kKeyValueTableName),
        IsOkAndHolds(key_value_table_oid_));
    EXPECT_THAT(catalog_adapter->GetTableNameFromOid(key_value_table_oid_),
                IsOkAndHolds(kKeyValueTableName));
  }

  static void TearDownTestSuite() {
    adapter_holder_ = nullptr;
  }

 protected:
  // A holder for the catalog adapter used by the analyzer and transformer
  static std::unique_ptr<zetasql::AnalyzerOptions> analyzer_options_;
  static std::unique_ptr<CatalogAdapterHolder> adapter_holder_;
  static Oid key_value_table_oid_;
};

std::unique_ptr<zetasql::AnalyzerOptions>
    HolderTestFixture::analyzer_options_ = nullptr;
std::unique_ptr<CatalogAdapterHolder> HolderTestFixture::adapter_holder_ =
    nullptr;
Oid HolderTestFixture::key_value_table_oid_ = InvalidOid;

TEST_F(HolderTestFixture, ValidateOneTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(CatalogAdapter* catalog_adapter, GetCatalogAdapter());

  // Expect one table:
  EXPECT_THAT(
      catalog_adapter->GetOrGenerateOidFromTableName(kKeyValueTableName),
      IsOkAndHolds(key_value_table_oid_));
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(key_value_table_oid_),
              IsOkAndHolds(kKeyValueTableName));
}

TEST_F(HolderTestFixture, AddTheSameTable) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(CatalogAdapter* catalog_adapter, GetCatalogAdapter());

  // Try getting oid of the same table, expect same oid:
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid oid_take2,
                       catalog_adapter->GetOrGenerateOidFromTableName(
                           TableName({kKeyValueTableName})));
  EXPECT_EQ(oid_take2, key_value_table_oid_);
}

}  // namespace
}  // namespace postgres_translator::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
