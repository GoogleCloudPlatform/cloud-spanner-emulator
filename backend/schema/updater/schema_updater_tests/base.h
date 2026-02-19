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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCHEMA_UPDATER_TESTS_BASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCHEMA_UPDATER_TESTS_BASE_H_

#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/common/ids.h"
#include "backend/database/pg_oid_assigner/pg_oid_assigner.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_updater.h"
#include "backend/storage/in_memory_storage.h"
#include "common/errors.h"
#include "common/limits.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "third_party/spanner_pg/ddl/spangres_direct_schema_printer_impl.h"
#include "third_party/spanner_pg/ddl/spangres_schema_printer.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

using ::google::spanner::emulator::test::ScopedEmulatorFeatureFlagsSetter;
using postgres_translator::spangres::CreateSpangresDirectSchemaPrinter;
using postgres_translator::spangres::SpangresSchemaPrinter;

namespace database_api = ::google::spanner::admin::database::v1;

// Matcher for matching an absl::StatusOr with an expected status.
MATCHER_P(StatusIs, status, "") { return arg.status() == status; }

MATCHER_P(NameIs, name, "") {
  return arg->GetSchemaNameInfo().value().name == name;
}

MATCHER_P2(ColumnIs, name, type, "") {
  bool match = true;
  match &= ::testing::ExplainMatchResult(::testing::Eq(name), arg->Name(),
                                         result_listener);
  match &= arg->GetType()->Equals(type);
  if (!match) {
    (*result_listener) << "("
                       << absl::StrJoin(
                              {arg->Name(), arg->GetType()->DebugString()}, ",")
                       << ")";
  }
  return match;
}

MATCHER_P2(IsKeyColumnOf, table, order, "") {
  auto pk = table->FindKeyColumn(arg->Name());
  if (pk == nullptr) {
    return ::testing::ExplainMatchResult(::testing::Eq(arg), nullptr,
                                         result_listener);
  }
  return ::testing::ExplainMatchResult(::testing::Eq(pk->is_descending()),
                                       std::string(order) == "DESC",
                                       result_listener);
}

MATCHER_P2(IsInterleavedIn, parent, on_delete, "") {
  // Check parent-child relationship.
  EXPECT_EQ(arg->parent(), parent);
  auto children = parent->children();
  auto it = std::find(children.begin(), children.end(), arg);
  EXPECT_NE(it, children.end());
  EXPECT_EQ(arg->on_delete_action(), on_delete);

  // Check primary keys.
  auto parent_pk = parent->primary_key();
  auto child_pk = arg->primary_key();
  EXPECT_LE(parent_pk.size(), child_pk.size());

  // Nullability of interleaved null-filtered index key columns can be
  // different from parent key columns.
  bool ignore_nullability =
      arg->owner_index() != nullptr && arg->owner_index()->is_null_filtered();

  for (int i = 0; i < parent_pk.size(); ++i) {
    EXPECT_THAT(child_pk[i]->column(),
                ColumnIs(parent_pk[i]->column()->Name(),
                         parent_pk[i]->column()->GetType()));
    EXPECT_EQ(child_pk[i]->is_descending(), parent_pk[i]->is_descending());
    EXPECT_EQ(child_pk[i]->column()->declared_max_length(),
              parent_pk[i]->column()->declared_max_length());
    if (!ignore_nullability) {
      EXPECT_EQ(child_pk[i]->column()->is_nullable(),
                parent_pk[i]->column()->is_nullable());
    }
  }
  return true;
}

MATCHER_P(SourceColumnIs, source, "") {
  EXPECT_EQ(arg->source_column(), source);
  EXPECT_EQ(arg->Name(), source->Name());
  EXPECT_TRUE(arg->GetType()->Equals(source->GetType()));
  EXPECT_EQ(arg->declared_max_length(), source->declared_max_length());
  return true;
}

template <typename T>
std::string PrintNames(absl::Span<const T* const> nodes) {
  return absl::StrJoin(nodes, ",", [](std::string* out, const T* node) {
    absl::StrAppend(out, node->GetSchemaNameInfo().value().name);
  });
}

template <typename T>
std::string PrintNames(const std::vector<const T*> nodes) {
  return PrintNames(absl::Span<const T* const>(nodes));
}

#define ASSERT_NOT_NULL(v) AssertNotNull((v), __FILE__, __LINE__)

template <typename T>
const T* AssertNotNull(const T* value, const char* file, int line) {
  auto assert_not_null = [](const T* value, const char* file, int line) {
    // Must be called in a function returning void.
    ASSERT_NE(value, nullptr) << "  at " << file << ":" << line;
  };
  assert_not_null(value, file, line);
  return value;
}

class SchemaUpdaterTest
    : public testing::Test,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  SchemaUpdaterTest()
      : feature_flags_({
            .enable_identity_columns = true,
            .enable_serial_auto_increment = true,
            .enable_user_defined_functions = true,
            .enable_search_index = true,
            .enable_default_time_zone = true,
            .enable_interleave_in = true,
            .enable_alter_table_if_exists = true,
        }) {}
  absl::StatusOr<std::unique_ptr<const Schema>> CreateSchema(
      absl::Span<const std::string> statements,
      absl::string_view proto_descriptor_bytes = "",
      const database_api::DatabaseDialect& dialect = GetParam(),
      bool use_gsql_to_pg_translation = true);

  absl::StatusOr<std::unique_ptr<const Schema>> UpdateSchema(
      const Schema* base_schema, absl::Span<const std::string> statements,
      absl::string_view proto_descriptor_bytes = "",
      const database_api::DatabaseDialect& dialect = GetParam(),
      bool use_gsql_to_pg_translation = true,
      std::string_view database_id = "");

 protected:
  void SetUp() override {
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      pg_schema_printer_ = CreateSpangresDirectSchemaPrinter().value();
      pg_oid_assigner_ = std::make_unique<PgOidAssigner>(/*enabled=*/true);
    } else {
      pg_oid_assigner_ = std::make_unique<PgOidAssigner>(/*enabled=*/false);
    }
  }

  std::unique_ptr<SpangresSchemaPrinter> pg_schema_printer_;
  zetasql::TypeFactory type_factory_;
  TableIDGenerator table_id_generator_;
  ColumnIDGenerator column_id_generator_;
  std::unique_ptr<PgOidAssigner> pg_oid_assigner_;
  ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

INSTANTIATE_TEST_SUITE_P(
    SchemaUpdaterPerDialectTests, SchemaUpdaterTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<SchemaUpdaterTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_SCHEMA_UPDATER_TESTS_BASE_H_
