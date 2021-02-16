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
#include <string>

#include "google/protobuf/struct.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/access/read.h"
#include "backend/access/write.h"
#include "backend/query/function_catalog.h"
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
};

// Returns true if the given query is a DML statement.
bool IsDMLQuery(const std::string& query);

// QueryResult specifies the output of a query request.
struct QueryResult {
  // A row cursor containing the query result rows, null for DML requests.
  std::unique_ptr<RowCursor> rows;

  // The number of modified rows.
  int64_t modified_row_count = 0;

  // The number of rows in the returned row cursor.
  int64_t num_output_rows = 0;

  // Query execution elapsed time.
  absl::Duration elapsed_time;
};

// QueryContext provides resources required to execute a query.
struct QueryContext {
  // The database schema.
  const Schema* schema;

  // A reader for reading data.
  RowReader* reader;

  // A writer for writing data for DML requests. Can be null for SELECT queries.
  RowWriter* writer;
};

// QueryEngine handles SQL-related requests.
class QueryEngine {
 public:
  explicit QueryEngine(zetasql::TypeFactory* type_factory)
      : type_factory_(type_factory), function_catalog_(type_factory) {}

  // Returns the name of the table that a given DML query modifies.
  zetasql_base::StatusOr<std::string> GetDmlTargetTable(const Query& query,
                                                const Schema* schema) const;

  // Executes a SQL query (SELECT query or DML).
  // Skip execution if validate_only is true.
  zetasql_base::StatusOr<QueryResult> ExecuteSql(const Query& query,
                                         const QueryContext& context) const;

  // Returns OK if query is partitionable.
  absl::Status IsPartitionable(const Query& query,
                               const QueryContext& context) const;

  // Returns OK if the 'query' is a DML statement that can be executed through
  // partitioned DML.
  absl::Status IsValidPartitionedDML(const Query& query,
                                     const QueryContext& context) const;

  zetasql::TypeFactory* type_factory() const { return type_factory_; }

  const FunctionCatalog* function_catalog() const { return &function_catalog_; }

 private:
  zetasql::TypeFactory* type_factory_;
  FunctionCatalog function_catalog_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERY_ENGINE_H_
