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

#include "backend/query/change_stream/queryable_change_stream_tvf.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/array_type.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "backend/schema/catalog/change_stream.h"
#include "common/constants.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
absl::StatusOr<std::unique_ptr<QueryableChangeStreamTvf>>
QueryableChangeStreamTvf::Create(const backend::ChangeStream* change_stream,
                                 const zetasql::AnalyzerOptions options,
                                 zetasql::Catalog* catalog,
                                 zetasql::TypeFactory* type_factory,
                                 bool is_pg) {
  std::vector<zetasql::FunctionArgumentType> args;
  std::vector<zetasql::TVFSchemaColumn> output_columns;
  std::vector<std::pair<std::string, const zetasql::Type*>> columns;
  const zetasql::Type* output_type = nullptr;
  std::string tvf_name;
  if (is_pg) {
    tvf_name = absl::StrCat(kChangeStreamTvfJsonPrefix, change_stream->Name());
    output_type =
        postgres_translator::spangres::types::PgJsonbMapping()->mapped_type();
  } else {
    tvf_name =
        absl::StrCat(kChangeStreamTvfStructPrefix, change_stream->Name());
    ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeType(
        absl::Substitute(kChangeStreamTvfOutputFormat, "JSON"), options,
        catalog, type_factory, &output_type));
  }
  const zetasql::ArrayType* read_options_array;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeArrayType(type_factory->get_string(),
                                              &read_options_array));
  columns.emplace_back(
      std::make_pair(kChangeStreamTvfOutputColumn, output_type));
  columns.emplace_back(std::make_pair(kChangeStreamTvfStartTimestamp,
                                      type_factory->get_timestamp()));
  columns.emplace_back(std::make_pair(kChangeStreamTvfEndTimestamp,
                                      type_factory->get_timestamp()));
  columns.emplace_back(std::make_pair(kChangeStreamTvfPartitionToken,
                                      type_factory->get_string()));
  columns.emplace_back(std::make_pair(kChangeStreamTvfHeartbeatMilliseconds,
                                      type_factory->get_int64()));
  columns.emplace_back(
      std::make_pair(kChangeStreamTvfReadOptions, read_options_array));

  for (int i = 0; i < columns.size(); ++i) {
    std::pair<std::string, const zetasql::Type*> curr_column = columns[i];
    if (curr_column.first != kChangeStreamTvfOutputColumn) {
      auto arg_option =
          zetasql::FunctionArgumentTypeOptions().set_argument_name(
              curr_column.first, zetasql::kPositionalOrNamed);
      arg_option.set_cardinality(zetasql::FunctionArgumentType::OPTIONAL);
      arg_option.set_default(zetasql::values::Null(curr_column.second));
      args.emplace_back(curr_column.second, arg_option);
    } else {
      output_columns.emplace_back(curr_column.first, curr_column.second);
    }
  }
  zetasql::TVFRelation result_schema(output_columns);
  const auto result_type = zetasql::FunctionArgumentType::RelationWithSchema(
      result_schema, /*extra_relation_input_columns_allowed=*/false);
  const auto signature = zetasql::FunctionSignature(result_type, args,
                                                      /*context_ptr=*/nullptr);
  return std::make_unique<QueryableChangeStreamTvf>(
      absl::StrSplit(tvf_name, '.'), signature, result_schema, change_stream);
}
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
