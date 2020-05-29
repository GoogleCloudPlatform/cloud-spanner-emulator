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

#include "frontend/converters/mutations.h"

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/keys.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/strings/str_cat.h"
#include "backend/access/write.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"
#include "frontend/converters/keys.h"
#include "frontend/converters/types.h"
#include "frontend/converters/values.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

namespace {

absl::Status WriteFromProto(const backend::Schema& schema,
                            const spanner_api::Mutation::Write& write_pb,
                            backend::MutationOpType op_type,
                            backend::Mutation* mutation) {
  if (write_pb.table().empty()) {
    return error::MutationTableRequired();
  }
  const backend::Table* table = schema.FindTable(write_pb.table());
  if (table == nullptr) {
    return error::TableNotFound(write_pb.table());
  }

  // Check that columns exist within table and get the column names.
  std::vector<const backend::Column*> columns(write_pb.columns_size());
  std::vector<std::string> column_names(write_pb.columns_size());
  for (int i = 0; i < write_pb.columns_size(); ++i) {
    columns[i] = table->FindColumn(write_pb.columns(i));
    if (columns[i] == nullptr) {
      return error::ColumnNotFound(write_pb.table(), write_pb.columns(i));
    }
    column_names[i] = columns[i]->Name();
  }

  if (write_pb.values_size() == 0) {
    return error::MissingRequiredFieldError("Write.values");
  }

  // Populate the list of values for the rows that will be written to.
  std::vector<backend::ValueList> value_list;
  for (const google::protobuf::ListValue& values : write_pb.values()) {
    backend::ValueList row_values;
    if (values.values_size() != columns.size()) {
      return error::MutationColumnAndValueSizeMismatch(columns.size(),
                                                       values.values_size());
    }
    for (int i = 0; i < columns.size(); ++i) {
      ZETASQL_ASSIGN_OR_RETURN(row_values.emplace_back(),
                       ValueFromProto(values.values(i), columns[i]->GetType()));
    }
    value_list.push_back(std::move(row_values));
  }
  mutation->AddWriteOp(op_type, table->Name(), std::move(column_names),
                       std::move(value_list));
  return absl::OkStatus();
}

absl::Status ValidateDeleteRange(const backend::KeyRange& range) {
  int max_columns =
      std::max(range.start_key().NumColumns(), range.limit_key().NumColumns());
  int min_columns =
      std::min(range.start_key().NumColumns(), range.limit_key().NumColumns());

  // No-op if no columns were specified at all.
  if (max_columns == 0) {
    return absl::OkStatus();
  }

  // Early exit if one of the keys has more than one extra part.
  if (max_columns > min_columns + 1) {
    return error::BadDeleteRange(range.start_key().DebugString(),
                                 range.limit_key().DebugString());
  }

  // If we reached here, the number of columns in start and limit are either
  // equal, or differ by one. We check up to the prefix for equality.
  int num_compare_columns =
      max_columns == min_columns ? min_columns - 1 : min_columns;
  for (int i = 0; i < num_compare_columns; ++i) {
    if (range.start_key().ColumnValue(i) != range.limit_key().ColumnValue(i)) {
      return error::BadDeleteRange(range.start_key().DebugString(),
                                   range.limit_key().DebugString());
    }
  }

  return absl::OkStatus();
}

absl::Status DeleteFromProto(const backend::Schema& schema,
                             const spanner_api::Mutation::Delete& delete_pb,
                             backend::Mutation* mutation) {
  // Verify that the table exists.
  if (delete_pb.table().empty()) {
    return error::MutationTableRequired();
  }
  const backend::Table* table = schema.FindTable(delete_pb.table());
  if (table == nullptr) {
    return error::TableNotFound(delete_pb.table());
  }

  // Parse the delete key set from the request proto.
  ZETASQL_ASSIGN_OR_RETURN(backend::KeySet key_set,
                   KeySetFromProto(delete_pb.key_set(), *table));

  // Perform extra validations for delete ranges in the request key set.
  for (const backend::KeyRange& range : key_set.ranges()) {
    ZETASQL_RETURN_IF_ERROR(ValidateDeleteRange(range));
  }

  mutation->AddDeleteOp(table->Name(), key_set);
  return absl::OkStatus();
}

}  // namespace

absl::Status MutationFromProto(
    const backend::Schema& schema,
    const google::protobuf::RepeatedPtrField<spanner_api::Mutation>& mutation_pbs,
    backend::Mutation* mutation) {
  for (const spanner_api::Mutation& mutation_pb : mutation_pbs) {
    switch (mutation_pb.operation_case()) {
      case spanner_api::Mutation::kInsert:
        ZETASQL_RETURN_IF_ERROR(WriteFromProto(schema, mutation_pb.insert(),
                                       backend::MutationOpType::kInsert,
                                       mutation));
        break;
      case spanner_api::Mutation::kUpdate:
        ZETASQL_RETURN_IF_ERROR(WriteFromProto(schema, mutation_pb.update(),
                                       backend::MutationOpType::kUpdate,
                                       mutation));
        break;
      case spanner_api::Mutation::kInsertOrUpdate:
        ZETASQL_RETURN_IF_ERROR(WriteFromProto(schema, mutation_pb.insert_or_update(),
                                       backend::MutationOpType::kInsertOrUpdate,
                                       mutation));
        break;
      case spanner_api::Mutation::kReplace:
        ZETASQL_RETURN_IF_ERROR(WriteFromProto(schema, mutation_pb.replace(),
                                       backend::MutationOpType::kReplace,
                                       mutation));
        break;
      case spanner_api::Mutation::kDelete:
        ZETASQL_RETURN_IF_ERROR(
            DeleteFromProto(schema, mutation_pb.delete_(), mutation));
        break;
      case spanner_api::Mutation::OPERATION_NOT_SET:
        return error::MissingRequiredFieldError("Mutation.operation");
    }
  }
  return absl::OkStatus();
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
