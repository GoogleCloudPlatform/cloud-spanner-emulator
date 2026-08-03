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

#include "google/spanner/v1/change_stream.pb.h"
#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/types/array_type.h"
#include "absl/strings/str_split.h"
#include "absl/strings/substitute.h"
#include "common/constants.h"
#include "googlesql/base/status_macros.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "google/protobuf/descriptor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
absl::StatusOr<std::unique_ptr<QueryableChangeStreamTvf>>
QueryableChangeStreamTvf::Create(const std::string& tvf_name,
                                 const googlesql::AnalyzerOptions options,
                                 googlesql::Catalog* catalog,
                                 googlesql::TypeFactory* type_factory,
                                 bool is_pg, bool is_mutable_key_range) {
  std::vector<googlesql::FunctionArgumentType> args;
  std::vector<googlesql::TVFSchemaColumn> output_columns;
  std::vector<std::pair<std::string, const googlesql::Type*>> columns;
  const googlesql::Type* output_type = nullptr;
  if (is_pg) {
    if (is_mutable_key_range) {
      output_type = postgres_translator::types::PgByteaMapping()->mapped_type();
    } else {
      output_type =
          postgres_translator::spangres::types::PgJsonbMapping()->mapped_type();
    }
  } else {
    if (is_mutable_key_range) {
      const google::protobuf::Descriptor* descriptor =
          google::spanner::v1::ChangeStreamRecord::descriptor();
      GOOGLESQL_RETURN_IF_ERROR(type_factory->MakeProtoType(descriptor, &output_type));
    } else {
      GOOGLESQL_RETURN_IF_ERROR(googlesql::AnalyzeType(
          absl::Substitute(kChangeStreamTvfOutputFormat, "JSON"), options,
          catalog, type_factory, &output_type));
    }
  }
  const googlesql::ArrayType* read_options_array;
  GOOGLESQL_RETURN_IF_ERROR(type_factory->MakeArrayType(type_factory->get_string(),
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
    std::pair<std::string, const googlesql::Type*> curr_column = columns[i];
    if (curr_column.first != kChangeStreamTvfOutputColumn) {
      auto arg_option =
          googlesql::FunctionArgumentTypeOptions().set_argument_name(
              curr_column.first, googlesql::kPositionalOrNamed);
      arg_option.set_cardinality(googlesql::FunctionArgumentType::OPTIONAL);
      arg_option.set_default(googlesql::values::Null(curr_column.second));
      args.emplace_back(curr_column.second, arg_option);
    } else {
      output_columns.emplace_back(curr_column.first, curr_column.second);
    }
  }
  googlesql::TVFRelation result_schema(output_columns);
  const auto result_type = googlesql::FunctionArgumentType::RelationWithSchema(
      result_schema, /*extra_relation_input_columns_allowed=*/false);
  const auto signature = googlesql::FunctionSignature(result_type, args,
                                                      /*context_ptr=*/nullptr);
  return std::make_unique<QueryableChangeStreamTvf>(
      absl::StrSplit(tvf_name, '.'), signature, result_schema);
}
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
