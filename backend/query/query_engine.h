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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_H_

#include <memory>
#include <optional>
#include <string>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/query/catalog.h"
#include "backend/query/change_stream/change_stream_query_validator.h"
#include "backend/query/function_catalog.h"
#include "backend/query/query_context.h"
#include "backend/schema/catalog/schema.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Query specifies the input of a query request.
struct Query {
  // The SQL string to be executed.
  std::string sql;

  // The query parameters. Values in this map have already been deserialized as
  // their type was provided in the query.
  std::map<std::string, zetasql::Value> declared_params;

  // Parameters that did not have a type supplied. They will be deserialized in
  // the backend once the ZetaSQL analyzer provides types.
  std::map<std::string, google::protobuf::Value> undeclared_params;

  // If not empty,the current query is an internal query against a non public
  // partition or data table of this change stream
  std::optional<std::string> change_stream_internal_lookup;
};

// Returns true if the given query is a DML statement.
bool IsDMLQuery(const std::string& query);

// Returns the returning clause of a DML statement.
const zetasql::ResolvedReturningClause* GetReturningClause(
    const zetasql::ResolvedStatement* resolved_statement);

// QueryResult specifies the output of a query request.
struct QueryResult {
  // A row cursor containing the query result rows. It's null for DML requests
  // with returning clause.
  std::unique_ptr<RowCursor> rows;

  // Map containing the types of all query parameters.
  zetasql::QueryParametersMap parameter_types;

  // The number of modified rows.
  int64_t modified_row_count = 0;

  // The number of rows in the returned row cursor.
  int64_t num_output_rows = 0;

  // Query execution elapsed time.
  absl::Duration elapsed_time;
};

// QueryEngine handles SQL-related requests.
class QueryEngine {
 public:
  explicit QueryEngine(zetasql::TypeFactory* type_factory,
                       const Schema* schema)
      : type_factory_(type_factory),
        function_catalog_(type_factory,
                          kCloudSpannerEmulatorFunctionCatalogName, schema) {}

  // Returns the name of the table that a given DML query modifies.
  absl::StatusOr<std::string> GetDmlTargetTable(const Query& query,
                                                const Schema* schema) const;

  // Executes a SQL query (SELECT query or DML).
  // Skip execution if validate_only is true.
  absl::StatusOr<QueryResult> ExecuteSql(const Query& query,
                                         const QueryContext& context) const;

  // Executes a SQL query (SELECT query or DML) using the given query mode.
  absl::StatusOr<QueryResult> ExecuteSql(
      const Query& query, const QueryContext& context,
      v1::ExecuteSqlRequest_QueryMode query_mode) const;

  absl::StatusOr<QueryResult> ExecuteInsertOnConflictDml(
      const Query& query,
      const zetasql::ResolvedStatement* resolved_statement,
      const std::map<std::string, zetasql::Value>& params,
      google::spanner::emulator::backend::Catalog& catalog,
      zetasql::AnalyzerOptions& analyzer_options,
      const QueryContext& context) const;

  // Returns OK if query is partitionable.
  absl::Status IsPartitionable(const Query& query,
                               const QueryContext& context) const;

  // Returns OK if the 'query' is a DML statement that can be executed through
  // partitioned DML.
  absl::Status IsValidPartitionedDML(const Query& query,
                                     const QueryContext& context) const;

  // Returns an empty change stream metadata if current query is a valid regular
  // query. Returns the complete change stream metadata if current query is a
  // valid change stream query. Returns corresponding error status if current
  // query is an invalid regular query, a non-change stream tvf query or an
  // invalid change stream tvf query.
  static absl::StatusOr<ChangeStreamQueryValidator::ChangeStreamMetadata>
  TryGetChangeStreamMetadata(const Query& query, const Schema* schema,
                             bool in_read_write_txn = false);

  zetasql::TypeFactory* type_factory() const { return type_factory_; }

  const FunctionCatalog* function_catalog() const { return &function_catalog_; }

  void SetLatestSchemaForFunctionCatalog(const Schema* schema) {
    function_catalog_.SetLatestSchema(schema);
  }

 private:
  static std::string GetTimeZone(const Schema* schema);

  zetasql::TypeFactory* type_factory_;
  FunctionCatalog function_catalog_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_H_
