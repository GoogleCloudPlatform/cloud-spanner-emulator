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

#include "frontend/converters/change_streams.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/change_stream.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/type.h"
#include "googlesql/public/types/array_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/query/analyzer_options.h"
#include "common/constants.h"
#include "common/limits.h"
#include "frontend/converters/chunking.h"
#include "frontend/converters/types.h"
#include "frontend/converters/values.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/json/json.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;
namespace {

struct ChangeStreamOutputTypes {
  enum ReturningType { DATA_CHANGE, HEARTBEAT, CHILD_PARTITIONS };
  ChangeStreamOutputTypes() {
    ConstructChangeRecordTypes();
    ConstructChildPartitionRecordTypes();
    ConstructHeartbeatRecordTypes();
    ConstructDataChangeRecordTypes();
    ConstructChangeStreamRecordProtoType();
  }

  googlesql::TypeFactory type_factory;
  googlesql::SimpleCatalog catalog = googlesql::SimpleCatalog(
      "For analyzing change stream output types only.");
  googlesql::AnalyzerOptions analyzer_options =
      backend::MakeGoogleSqlAnalyzerOptions(kDefaultTimeZone);
  // Outter most change record output related types.
  const googlesql::Type* change_record_arr;
  const googlesql::StructType* change_record_struct;
  // Heartbeat record related types.
  const googlesql::ArrayType* heartbeat_record_arr;
  const googlesql::StructType* heartbeat_record_struct;
  // Child partition record related types.
  const googlesql::ArrayType* child_partitions_record_arr;
  const googlesql::StructType* child_partitions_record_struct;
  const googlesql::ArrayType* child_partitions_arr;
  const googlesql::StructType* child_partitions_struct;
  const googlesql::ArrayType* parents_arr;
  // Data change record related types
  const googlesql::ArrayType* data_change_record_arr;
  const googlesql::StructType* data_change_record_struct;
  const googlesql::ArrayType* column_types_arr;
  const googlesql::StructType* column_types_struct;
  const googlesql::ArrayType* mods_arr;
  const googlesql::StructType* mods_struct;
  // ChangeStreamRecord Proto Type
  const googlesql::ProtoType* change_stream_record_proto_type;

  void ConstructChangeRecordTypes() {
    ABSL_CHECK_OK(googlesql::AnalyzeType(
        absl::Substitute(kChangeStreamTvfOutputFormat, "JSON"),
        analyzer_options, &catalog, &type_factory, &change_record_arr));
    change_record_struct =
        change_record_arr->AsArray()->element_type()->AsStruct();
  }

  void ConstructHeartbeatRecordTypes() {
    bool is_ambiguous;
    heartbeat_record_arr = change_record_arr->AsArray()
                               ->element_type()
                               ->AsStruct()
                               ->FindField("heartbeat_record", &is_ambiguous)
                               ->type->AsArray();
    heartbeat_record_struct = heartbeat_record_arr->element_type()->AsStruct();
  }

  void ConstructChildPartitionRecordTypes() {
    bool is_ambiguous;
    child_partitions_record_arr =
        change_record_arr->AsArray()
            ->element_type()
            ->AsStruct()
            ->FindField("child_partitions_record", &is_ambiguous)
            ->type->AsArray();
    child_partitions_record_struct =
        child_partitions_record_arr->element_type()->AsStruct();
    child_partitions_arr = child_partitions_record_struct
                               ->FindField("child_partitions", &is_ambiguous)
                               ->type->AsArray();
    child_partitions_struct = child_partitions_arr->element_type()->AsStruct();
    parents_arr = googlesql::types::StringArrayType();
  }

  void ConstructDataChangeRecordTypes() {
    bool is_ambigous;
    data_change_record_arr = change_record_arr->AsArray()
                                 ->element_type()
                                 ->AsStruct()
                                 ->FindField("data_change_record", &is_ambigous)
                                 ->type->AsArray();
    data_change_record_struct =
        data_change_record_arr->AsArray()->element_type()->AsStruct();
    column_types_arr =
        data_change_record_struct->FindField("column_types", &is_ambigous)
            ->type->AsArray();
    column_types_struct =
        column_types_arr->AsArray()->element_type()->AsStruct();
    mods_arr = data_change_record_struct->FindField("mods", &is_ambigous)
                   ->type->AsArray();
    mods_struct = mods_arr->AsArray()->element_type()->AsStruct();
  }

  void ConstructChangeStreamRecordProtoType() {
    const google::protobuf::Descriptor* descriptor =
        google::spanner::v1::ChangeStreamRecord::descriptor();
    ABSL_CHECK_OK(type_factory.MakeProtoType(descriptor,  // Crash OK
                                        &change_stream_record_proto_type));
  }
};

// Singleton static struct that contains all the fixed record types.
const ChangeStreamOutputTypes* GetChangeStreamOutputTypes() {
  static const absl::NoDestructor<ChangeStreamOutputTypes>
      kChangeStreamOutputTypes{};
  return kChangeStreamOutputTypes.get();
}

std::string ToFragmentIdString(int64_t record_sequence) {
  return absl::StrFormat("%08d", record_sequence);
}

googlesql::Value CreateEmptyArrayForRecordType(
    ChangeStreamOutputTypes::ReturningType type) {
  const ChangeStreamOutputTypes* types = GetChangeStreamOutputTypes();
  switch (type) {
    case ChangeStreamOutputTypes::ReturningType::HEARTBEAT:
      return googlesql::Value::EmptyArray(types->heartbeat_record_arr);
    case ChangeStreamOutputTypes::ReturningType::CHILD_PARTITIONS:
      return googlesql::Value::EmptyArray(types->child_partitions_record_arr);
    case ChangeStreamOutputTypes::DATA_CHANGE:
      return googlesql::Value::EmptyArray(types->data_change_record_arr);
  }
}

absl::StatusOr<googlesql::Value> CreateChangeRecord(
    googlesql::Value data_change_record, googlesql::Value heartbeat_record,
    googlesql::Value child_partitions_record) {
  googlesql::Value change_record_arr_val;
  googlesql::Value change_record_struct_val;
  const ChangeStreamOutputTypes* types = GetChangeStreamOutputTypes();
  GOOGLESQL_ASSIGN_OR_RETURN(
      change_record_struct_val,
      googlesql::Value::MakeStruct(
          types->change_record_struct,
          {data_change_record, heartbeat_record, child_partitions_record}));
  return googlesql::Value::MakeArray(types->change_record_arr->AsArray(),
                                     {change_record_struct_val});
}

absl::StatusOr<googlesql::Value> CreateHeartbeatRecord(absl::Time timestamp) {
  const ChangeStreamOutputTypes* types = GetChangeStreamOutputTypes();
  std::vector<googlesql::Value> values;
  values.push_back(googlesql::Value::Timestamp(timestamp));
  GOOGLESQL_ASSIGN_OR_RETURN(auto heartbeat, googlesql::Value::MakeStruct(
                                       types->heartbeat_record_struct, values));

  return googlesql::Value::MakeArray(types->heartbeat_record_arr, {heartbeat});
}

absl::StatusOr<googlesql::Value> CreateChildPartitionRecord(
    backend::RowCursor* cursor, int64_t record_sequence,
    std::optional<absl::Time> initial_start_timestamp) {
  // The passed in cursor is guaranteed to have the shape of
  // {start_time(TIMESTAMP),partition_token(STRING), parents(ARRAY<STRING>)}
  // due to the fixed shape of change stream internal partition table.
  const ChangeStreamOutputTypes* types = GetChangeStreamOutputTypes();
  std::vector<googlesql::Value> values;
  values.push_back(
      initial_start_timestamp.has_value()
          ? googlesql::Value::Timestamp(initial_start_timestamp.value())
          : cursor->ColumnValue(0));
  values.push_back(
      googlesql::Value::String(ToFragmentIdString(record_sequence)));

  std::vector<googlesql::Value> child_partitions_struct_values;

  // If a split event happens during partition query, one child partition record
  // can contain up to two partition tokens after split. If initial_start_time
  // doesn't have value and there are more than one row contain in the partition
  // table row cursor, we know a split event happened and keep inserting the
  // remaining splitted partition tokens in current row cursor.
  do {
    googlesql::Value partition_token_val = cursor->ColumnValue(1);
    // Prevent populating parent tokens for initial partition queries.
    googlesql::Value parent_partitions_token_val =
        initial_start_timestamp.has_value()
            ? googlesql::Value::EmptyArray(types->parents_arr)
            : cursor->ColumnValue(2);
    GOOGLESQL_ASSIGN_OR_RETURN(auto child_partitions_struct_val,
                     googlesql::Value::MakeStruct(
                         types->child_partitions_struct,
                         {partition_token_val, parent_partitions_token_val}));
    child_partitions_struct_values.push_back(child_partitions_struct_val);
  } while (!initial_start_timestamp.has_value() && cursor->Next());

  GOOGLESQL_ASSIGN_OR_RETURN(auto child_partitions_struct_array_val,
                   googlesql::Value::MakeArray(types->child_partitions_arr,
                                               child_partitions_struct_values));
  values.push_back(child_partitions_struct_array_val);
  GOOGLESQL_ASSIGN_OR_RETURN(auto child_partitions_record_struct_val,
                   googlesql::Value::MakeStruct(
                       types->child_partitions_record_struct, values));
  googlesql::Value final_child_partition_record;
  GOOGLESQL_ASSIGN_OR_RETURN(
      final_child_partition_record,
      googlesql::Value::MakeArray(types->child_partitions_record_arr->AsArray(),
                                  {child_partitions_record_struct_val}));
  return final_child_partition_record;
}

absl::StatusOr<googlesql::Value> CreateDataChangeRecord(
    backend::RowCursor* cursor) {
  const ChangeStreamOutputTypes* types = GetChangeStreamOutputTypes();
  std::vector<googlesql::Value> values;
  // The passed in cursor is guaranteed to have the shape of
  // {"partition_token(STRING)",
  //  "commit_timestamp(TIMESTAMP)",
  //  "server_transaction_id(STRING)",
  //  "record_sequence(STRING)",
  //  "is_last_record_in_transaction_in_partition(BOOL)",
  //  "table_name(STRING)",
  //  "column_types_name(ARRAY<STRING>)",
  //  "column_types_type(ARRAY<STRING>)",
  //  "column_types_is_primary_key(ARRAY<BOOL>)"
  //  "column_types_ordinal_position(ARRAY<INT64>)"
  //  "mods_keys(ARRAY<STRING>)",
  //  "mods_new_values(ARRAY<STRING>)",
  //  "mods_old_values(ARRAY<STRING>)",
  //  "mod_type(STRING)",
  //  "value_capture_type(STRING)",
  //  "number_of_records_in_transaction(INT64)",
  //  "number_of_partitions_in_transaction(INT64)",
  //  "transaction_tag(STRING)",
  //  "is_system_transaction(BOOL)"},
  // due to the fixed shape of change stream internal data table.
  GOOGLESQL_RET_CHECK(cursor->NumColumns() == 19);
  values.reserve(18);
  // Skip the first column partition_token(STRING) since partition token is not
  // part of the actual data change record that will be returned to the users.
  for (int i = 1; i < 6; i++) {
    // Swap the order of server transaction id and record sequence due to the
    // different table schema.
    if (i == 2) {
      values.push_back(cursor->ColumnValue(3));
    } else if (i == 3) {
      values.push_back(cursor->ColumnValue(2));
    } else {
      values.push_back(cursor->ColumnValue(i));
    }
  }

  // Reconstruct each column type JSON into a struct:
  // column_types ARRAY<STRUCT<
  //     name STRING,
  //     type JSON,
  //     is_primary_key BOOL,
  //     ordinal_position INT64>>
  googlesql::Value column_types_name_arr = cursor->ColumnValue(6);
  googlesql::Value column_types_type_arr = cursor->ColumnValue(7);
  googlesql::Value column_types_is_primary_key = cursor->ColumnValue(8);
  googlesql::Value column_types_ordinal_position = cursor->ColumnValue(9);
  std::vector<googlesql::Value> column_types_arr_structs;
  for (int i = 0; i < column_types_name_arr.num_elements(); i++) {
    std::vector<googlesql::Value> curr_column_type_vals{
        column_types_name_arr.element(i),
        googlesql::Value::Json(
            googlesql::JSONValue::ParseJSONString(
                column_types_type_arr.element(i).string_value())
                .value()),
        column_types_is_primary_key.element(i),
        column_types_ordinal_position.element(i)};
    GOOGLESQL_ASSIGN_OR_RETURN(auto curr_column_type_struct,
                     googlesql::Value::MakeStruct(types->column_types_struct,
                                                  curr_column_type_vals));
    column_types_arr_structs.push_back(curr_column_type_struct);
  }
  GOOGLESQL_ASSIGN_OR_RETURN(auto column_types_struct_array_val,
                   googlesql::Value::MakeArray(types->column_types_arr,
                                               column_types_arr_structs));
  values.push_back(column_types_struct_array_val);

  // Reconstruct each column type JSON into a struct:
  // mods ARRAY<STRUCT<
  //   keys JSON,
  //   new_values JSON,
  //   old_values JSON>>
  std::vector<googlesql::Value> mods_arr_vals;
  googlesql::Value mods_keys_arr = cursor->ColumnValue(10);
  googlesql::Value mods_new_values_arr = cursor->ColumnValue(11);
  googlesql::Value mods_old_values_arr = cursor->ColumnValue(12);
  for (int i = 0; i < mods_keys_arr.num_elements(); i++) {
    std::vector<googlesql::Value> curr_mod_vals{
        googlesql::Value::Json(googlesql::JSONValue::ParseJSONString(
                                   mods_keys_arr.element(i).string_value())
                                   .value()),
        googlesql::Value::Json(
            googlesql::JSONValue::ParseJSONString(
                mods_new_values_arr.element(i).string_value())
                .value()),
        googlesql::Value::Json(
            googlesql::JSONValue::ParseJSONString(
                mods_old_values_arr.element(i).string_value())
                .value()),
    };

    GOOGLESQL_ASSIGN_OR_RETURN(
        auto curr_mods_struct_val,
        googlesql::Value::MakeStruct(types->mods_struct, curr_mod_vals));
    mods_arr_vals.push_back(curr_mods_struct_val);
  }

  GOOGLESQL_ASSIGN_OR_RETURN(auto mods_struct_array_val,
                   googlesql::Value::MakeArray(types->mods_arr, mods_arr_vals));
  values.push_back(mods_struct_array_val);
  for (int i = 13; i < 19; i++) {
    values.push_back(cursor->ColumnValue(i));
  }
  GOOGLESQL_ASSIGN_OR_RETURN(
      auto data_change_record_struct_val,
      googlesql::Value::MakeStruct(types->data_change_record_struct, values));
  GOOGLESQL_ASSIGN_OR_RETURN(
      auto final_data_change_record,
      googlesql::Value::MakeArray(types->data_change_record_arr,
                                  {data_change_record_struct_val}));
  return final_data_change_record;
}

absl::Status PopulateMetadata(
    std::vector<spanner_api::PartialResultSet>* responses) {
  auto* result_metadata_pb = responses->at(0).mutable_metadata();
  auto* field_pb = result_metadata_pb->mutable_row_type()->add_fields();
  field_pb->set_name(kChangeStreamTvfOutputColumn);
  GOOGLESQL_RETURN_IF_ERROR(TypeToProto(GetChangeStreamOutputTypes()->change_record_arr,
                              field_pb->mutable_type()));
  return absl::OkStatus();
}

void PopulateFakeResumeTokens(
    std::vector<spanner_api::PartialResultSet>* responses) {
  for (auto& response : *responses) {
    *response.mutable_resume_token() = kChangeStreamDummyResumeToken;
  }
}

}  // namespace

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertHeartbeatTimestampToStruct(absl::Time timestamp, bool expect_metadata) {
  spanner_api::ResultSet result_pb;
  auto* row_pb = result_pb.add_rows();
  GOOGLESQL_ASSIGN_OR_RETURN(auto heartbeat_record, CreateHeartbeatRecord(timestamp));
  GOOGLESQL_ASSIGN_OR_RETURN(
      auto change_record,
      CreateChangeRecord(
          CreateEmptyArrayForRecordType(
              ChangeStreamOutputTypes::ReturningType::DATA_CHANGE),
          heartbeat_record,
          CreateEmptyArrayForRecordType(
              ChangeStreamOutputTypes::ReturningType::CHILD_PARTITIONS)));
  GOOGLESQL_ASSIGN_OR_RETURN(*row_pb->add_values(), ValueToProto(change_record));
  GOOGLESQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    GOOGLESQL_RETURN_IF_ERROR(PopulateMetadata(&responses));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertPartitionTableRowCursorToStruct(
    backend::RowCursor* row_cursor,
    std::optional<absl::Time> initial_start_timestamp, bool expect_metadata) {
  spanner_api::ResultSet result_pb;
  int64_t record_sequence = 0;
  while (row_cursor->Next()) {
    auto* row_pb = result_pb.add_rows();
    GOOGLESQL_ASSIGN_OR_RETURN(auto child_partitions_record,
                     CreateChildPartitionRecord(row_cursor, record_sequence,
                                                initial_start_timestamp));
    GOOGLESQL_ASSIGN_OR_RETURN(
        auto change_record,
        CreateChangeRecord(
            CreateEmptyArrayForRecordType(ChangeStreamOutputTypes::DATA_CHANGE),
            CreateEmptyArrayForRecordType(
                ChangeStreamOutputTypes::ReturningType::HEARTBEAT),
            child_partitions_record));
    GOOGLESQL_ASSIGN_OR_RETURN(*row_pb->add_values(), ValueToProto(change_record));
    record_sequence++;
  }
  GOOGLESQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    GOOGLESQL_RETURN_IF_ERROR(PopulateMetadata(&responses));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertDataTableRowCursorToStruct(backend::RowCursor* row_cursor,
                                  bool expect_metadata) {
  spanner_api::ResultSet result_pb;
  while (row_cursor->Next()) {
    auto* row_pb = result_pb.add_rows();
    GOOGLESQL_ASSIGN_OR_RETURN(auto data_change_record,
                     CreateDataChangeRecord(row_cursor));
    GOOGLESQL_ASSIGN_OR_RETURN(
        auto change_record,
        CreateChangeRecord(
            data_change_record,
            CreateEmptyArrayForRecordType(
                ChangeStreamOutputTypes::ReturningType::HEARTBEAT),
            CreateEmptyArrayForRecordType(
                ChangeStreamOutputTypes::ReturningType::CHILD_PARTITIONS)));
    GOOGLESQL_ASSIGN_OR_RETURN(*row_pb->add_values(), ValueToProto(change_record));
  }
  GOOGLESQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    GOOGLESQL_RETURN_IF_ERROR(PopulateMetadata(&responses));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

namespace {

absl::Status JsonValueRefToProtoValue(googlesql::JSONValueConstRef ref,
                                      google::protobuf::Value* proto) {
  if (ref.IsNull()) {
    proto->set_null_value(google::protobuf::NullValue::NULL_VALUE);
  } else if (ref.IsBoolean()) {
    proto->set_bool_value(ref.GetBoolean());
  } else if (ref.IsInt64()) {
    proto->set_number_value(static_cast<double>(ref.GetInt64()));
  } else if (ref.IsDouble()) {
    proto->set_number_value(ref.GetDouble());
  } else if (ref.IsString()) {
    proto->set_string_value(ref.GetString());
  } else if (ref.IsArray()) {
    auto* list = proto->mutable_list_value();
    for (int i = 0; i < ref.GetArraySize(); ++i) {
      GOOGLESQL_RETURN_IF_ERROR(
          JsonValueRefToProtoValue(ref.GetArrayElement(i), list->add_values()));
    }
  } else if (ref.IsObject()) {
    auto* struct_pb = proto->mutable_struct_value();
    for (const auto& [key, val] : ref.GetMembers()) {
      GOOGLESQL_RETURN_IF_ERROR(
          JsonValueRefToProtoValue(val, &(*struct_pb->mutable_fields())[key]));
    }
  }
  return absl::OkStatus();
}

absl::Status ParseModsJson(
    absl::string_view json_str,
    const absl::flat_hash_map<std::string, int32_t>& column_name_to_index,
    google::protobuf::RepeatedPtrField<
        spanner_api::ChangeStreamRecord::DataChangeRecord::ModValue>*
        proto_field) {
  if (json_str.empty() || json_str == "null") {
    return absl::OkStatus();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(auto json_val,
                   googlesql::JSONValue::ParseJSONString(json_str));
  googlesql::JSONValueConstRef ref = json_val.GetConstRef();
  if (!ref.IsObject()) {
    return absl::InternalError(
        absl::StrCat("Expected JSON object for mods, got: ", json_str));
  }
  for (const auto& [name, val] : ref.GetMembers()) {
    auto it = column_name_to_index.find(name);
    if (it == column_name_to_index.end()) {
      return absl::InternalError(
          absl::StrCat("Column ", name, " not found in column_metadata"));
    }
    int32_t index = it->second;
    auto* mod_val = proto_field->Add();
    mod_val->set_column_metadata_index(index);
    GOOGLESQL_RETURN_IF_ERROR(JsonValueRefToProtoValue(val, mod_val->mutable_value()));
  }
  return absl::OkStatus();
}

absl::Status RowCursorToProtoDataChangeRecord(
    backend::RowCursor* cursor,
    spanner_api::ChangeStreamRecord::DataChangeRecord* proto) {
  absl::Time commit_time = cursor->ColumnValue(1).ToTime();
  int64_t s = absl::ToUnixSeconds(commit_time);
  int64_t ns = absl::ToInt64Nanoseconds(commit_time - absl::FromUnixSeconds(s));
  proto->mutable_commit_timestamp()->set_seconds(s);
  proto->mutable_commit_timestamp()->set_nanos(ns);

  proto->set_record_sequence(cursor->ColumnValue(3).string_value());
  proto->set_server_transaction_id(cursor->ColumnValue(2).string_value());
  proto->set_is_last_record_in_transaction_in_partition(
      cursor->ColumnValue(4).bool_value());
  proto->set_table(cursor->ColumnValue(5).string_value());

  googlesql::Value column_types_name_arr = cursor->ColumnValue(6);
  googlesql::Value column_types_type_arr = cursor->ColumnValue(7);
  googlesql::Value column_types_is_primary_key = cursor->ColumnValue(8);
  googlesql::Value column_types_ordinal_position = cursor->ColumnValue(9);
  absl::flat_hash_map<std::string, int32_t> column_name_to_index;
  column_name_to_index.reserve(column_types_name_arr.num_elements());
  for (int i = 0; i < column_types_name_arr.num_elements(); i++) {
    std::string col_name = column_types_name_arr.element(i).string_value();
    column_name_to_index[col_name] = i;

    auto* column_metadata = proto->add_column_metadata();
    column_metadata->set_name(col_name);
    column_metadata->set_is_primary_key(
        column_types_is_primary_key.element(i).bool_value());
    column_metadata->set_ordinal_position(
        column_types_ordinal_position.element(i).int64_value());

    std::string type_json_str = column_types_type_arr.element(i).string_value();
    auto status = google::protobuf::json::JsonStringToMessage(
        type_json_str, column_metadata->mutable_type());
    if (!status.ok()) {
      return absl::InternalError(
          absl::StrCat("Failed to parse column type JSON: ", type_json_str,
                       " error: ", status.ToString()));
    }
  }

  googlesql::Value mods_keys_arr = cursor->ColumnValue(10);
  googlesql::Value mods_new_values_arr = cursor->ColumnValue(11);
  googlesql::Value mods_old_values_arr = cursor->ColumnValue(12);
  for (int i = 0; i < mods_keys_arr.num_elements(); i++) {
    auto* mod = proto->add_mods();
    GOOGLESQL_RETURN_IF_ERROR(ParseModsJson(mods_keys_arr.element(i).string_value(),
                                  column_name_to_index, mod->mutable_keys()));
    GOOGLESQL_RETURN_IF_ERROR(ParseModsJson(mods_new_values_arr.element(i).string_value(),
                                  column_name_to_index,
                                  mod->mutable_new_values()));
    GOOGLESQL_RETURN_IF_ERROR(ParseModsJson(mods_old_values_arr.element(i).string_value(),
                                  column_name_to_index,
                                  mod->mutable_old_values()));
  }

  std::string mod_type_str = cursor->ColumnValue(13).string_value();
  spanner_api::ChangeStreamRecord::DataChangeRecord::ModType mod_type;
  if (!spanner_api::ChangeStreamRecord_DataChangeRecord_ModType_Parse(
          mod_type_str, &mod_type)) {
    return absl::InternalError(
        absl::StrCat("Failed to parse ModType: ", mod_type_str));
  }
  proto->set_mod_type(mod_type);

  std::string value_capture_type_str = cursor->ColumnValue(14).string_value();
  spanner_api::ChangeStreamRecord::DataChangeRecord::ValueCaptureType
      value_capture_type;
  if (!spanner_api::ChangeStreamRecord_DataChangeRecord_ValueCaptureType_Parse(
          value_capture_type_str, &value_capture_type)) {
    return absl::InternalError(absl::StrCat(
        "Failed to parse ValueCaptureType: ", value_capture_type_str));
  }
  proto->set_value_capture_type(value_capture_type);

  proto->set_number_of_records_in_transaction(
      cursor->ColumnValue(15).int64_value());
  proto->set_number_of_partitions_in_transaction(
      cursor->ColumnValue(16).int64_value());
  proto->set_transaction_tag(cursor->ColumnValue(17).string_value());
  proto->set_is_system_transaction(cursor->ColumnValue(18).bool_value());

  return absl::OkStatus();
}

absl::Status AddChangeRecordProtoToResultSet(
    const spanner_api::ChangeStreamRecord& change_record_proto,
    spanner_api::ResultSet& result_pb) {
  const googlesql::ProtoType* proto_type =
      GetChangeStreamOutputTypes()->change_stream_record_proto_type;
  googlesql::Value change_record_val =
      googlesql::values::Proto(proto_type, change_record_proto);
  auto* row_pb = result_pb.add_rows();
  GOOGLESQL_ASSIGN_OR_RETURN(*row_pb->add_values(), ValueToProto(change_record_val));
  return absl::OkStatus();
}

void PopulateProtoPartitionStartRecord(
    absl::Time start_time, absl::string_view partition_token,
    int64_t record_sequence,
    spanner_api::ChangeStreamRecord::PartitionStartRecord* proto) {
  int64_t s = absl::ToUnixSeconds(start_time);
  int64_t ns = absl::ToInt64Nanoseconds(start_time - absl::FromUnixSeconds(s));
  proto->mutable_start_timestamp()->set_seconds(s);
  proto->mutable_start_timestamp()->set_nanos(ns);
  proto->set_record_sequence(ToFragmentIdString(record_sequence));
  proto->add_partition_tokens(std::string(partition_token));
}

void PopulateProtoPartitionMoveInEventRecord(
    absl::Time commit_time, absl::string_view partition_token,
    absl::Span<const std::string> source_partition_tokens,
    int64_t record_sequence,
    spanner_api::ChangeStreamRecord::PartitionEventRecord* proto) {
  int64_t s = absl::ToUnixSeconds(commit_time);
  int64_t ns = absl::ToInt64Nanoseconds(commit_time - absl::FromUnixSeconds(s));
  proto->mutable_commit_timestamp()->set_seconds(s);
  proto->mutable_commit_timestamp()->set_nanos(ns);
  proto->set_record_sequence(ToFragmentIdString(record_sequence));
  proto->set_partition_token(std::string(partition_token));
  for (const auto& src_token : source_partition_tokens) {
    auto* move_in = proto->add_move_in_events();
    move_in->set_source_partition_token(src_token);
  }
}

void PopulateProtoPartitionMoveOutEventRecord(
    absl::Time commit_time, absl::string_view partition_token,
    absl::Span<const std::string> destination_partition_tokens,
    int64_t record_sequence,
    spanner_api::ChangeStreamRecord::PartitionEventRecord* proto) {
  int64_t s = absl::ToUnixSeconds(commit_time);
  int64_t ns = absl::ToInt64Nanoseconds(commit_time - absl::FromUnixSeconds(s));
  proto->mutable_commit_timestamp()->set_seconds(s);
  proto->mutable_commit_timestamp()->set_nanos(ns);
  proto->set_record_sequence(ToFragmentIdString(record_sequence));
  proto->set_partition_token(std::string(partition_token));
  for (const auto& dest_token : destination_partition_tokens) {
    auto* move_out = proto->add_move_out_events();
    move_out->set_destination_partition_token(dest_token);
  }
}

void PopulateProtoPartitionEndRecord(
    absl::Time end_time, absl::string_view partition_token,
    int64_t record_sequence,
    spanner_api::ChangeStreamRecord::PartitionEndRecord* proto) {
  int64_t s = absl::ToUnixSeconds(end_time);
  int64_t ns = absl::ToInt64Nanoseconds(end_time - absl::FromUnixSeconds(s));
  proto->mutable_end_timestamp()->set_seconds(s);
  proto->mutable_end_timestamp()->set_nanos(ns);
  proto->set_record_sequence(ToFragmentIdString(record_sequence));
  proto->set_partition_token(std::string(partition_token));
}

absl::Status RowCursorToProtoPartitionStartRecord(
    backend::RowCursor* cursor, int64_t record_sequence,
    std::optional<absl::Time> initial_start_timestamp,
    spanner_api::ChangeStreamRecord::PartitionStartRecord* proto) {
  absl::Time start_time = initial_start_timestamp.has_value()
                              ? initial_start_timestamp.value()
                              : cursor->ColumnValue(0).ToTime();
  PopulateProtoPartitionStartRecord(start_time,
                                    cursor->ColumnValue(1).string_value(),
                                    record_sequence, proto);
  return absl::OkStatus();
}

absl::Status PopulateMetadataForProto(
    std::vector<spanner_api::PartialResultSet>* responses) {
  auto* result_metadata_pb = responses->at(0).mutable_metadata();
  auto* field_pb = result_metadata_pb->mutable_row_type()->add_fields();
  field_pb->set_name(kChangeStreamTvfOutputColumn);
  GOOGLESQL_RETURN_IF_ERROR(
      TypeToProto(GetChangeStreamOutputTypes()->change_stream_record_proto_type,
                  field_pb->mutable_type()));
  return absl::OkStatus();
}

// The passed in cursor is guaranteed to have the shape of
// {start_time(TIMESTAMP), partition_token(STRING), parents(ARRAY<STRING>)}
// due to the fixed shape of change stream internal partition table.
struct PartitionRow {
  absl::Time start_time;
  std::string child_token;
  std::vector<std::string> parents;
};

absl::Status ConvertMergeEventToProto(const PartitionRow& row,
                                      absl::string_view partition_token,
                                      spanner_api::ResultSet& result_pb) {
  int64_t record_sequence = 0;

  bool is_first_parent =
      !row.parents.empty() && partition_token == row.parents[0];

  // PartitionStartRecord for merged child (only generated in 1st parent).
  if (is_first_parent) {
    spanner_api::ChangeStreamRecord start_record_proto;
    auto* start_proto = start_record_proto.mutable_partition_start_record();
    PopulateProtoPartitionStartRecord(row.start_time, row.child_token,
                                      record_sequence++, start_proto);
    GOOGLESQL_RETURN_IF_ERROR(
        AddChangeRecordProtoToResultSet(start_record_proto, result_pb));
  }

  // Key range move out PartitionEventRecord from partition_token to
  // child_token.
  {
    spanner_api::ChangeStreamRecord event_record_proto;
    auto* event_proto = event_record_proto.mutable_partition_event_record();
    PopulateProtoPartitionMoveOutEventRecord(row.start_time, partition_token,
                                             {row.child_token},
                                             record_sequence++, event_proto);
    GOOGLESQL_RETURN_IF_ERROR(
        AddChangeRecordProtoToResultSet(event_record_proto, result_pb));
  }

  // PartitionEndRecord for partition_token.
  {
    spanner_api::ChangeStreamRecord end_record_proto;
    auto* end_proto = end_record_proto.mutable_partition_end_record();
    PopulateProtoPartitionEndRecord(row.start_time, partition_token,
                                    record_sequence++, end_proto);
    GOOGLESQL_RETURN_IF_ERROR(
        AddChangeRecordProtoToResultSet(end_record_proto, result_pb));
  }

  return absl::OkStatus();
}

absl::Status ConvertSplitEventToProto(const std::vector<PartitionRow>& rows,
                                      absl::string_view partition_token,
                                      spanner_api::ResultSet& result_pb) {
  int64_t record_sequence = 0;

  // PartitionStartRecord for each split child partition.
  std::vector<std::string> destination_child_tokens;
  destination_child_tokens.reserve(rows.size());
  for (const auto& row : rows) {
    destination_child_tokens.push_back(row.child_token);
    spanner_api::ChangeStreamRecord start_record_proto;
    auto* start_proto = start_record_proto.mutable_partition_start_record();
    PopulateProtoPartitionStartRecord(row.start_time, row.child_token,
                                      record_sequence++, start_proto);
    GOOGLESQL_RETURN_IF_ERROR(
        AddChangeRecordProtoToResultSet(start_record_proto, result_pb));
  }

  absl::Time event_time = rows[0].start_time;

  // Key range move out PartitionEventRecord from partition_token to child
  // partitions.
  {
    spanner_api::ChangeStreamRecord event_record_proto;
    auto* event_proto = event_record_proto.mutable_partition_event_record();
    PopulateProtoPartitionMoveOutEventRecord(event_time, partition_token,
                                             destination_child_tokens,
                                             record_sequence++, event_proto);
    GOOGLESQL_RETURN_IF_ERROR(
        AddChangeRecordProtoToResultSet(event_record_proto, result_pb));
  }

  // PartitionEndRecord for partition_token.
  {
    spanner_api::ChangeStreamRecord end_record_proto;
    auto* end_proto = end_record_proto.mutable_partition_end_record();
    PopulateProtoPartitionEndRecord(event_time, partition_token,
                                    record_sequence++, end_proto);
    GOOGLESQL_RETURN_IF_ERROR(
        AddChangeRecordProtoToResultSet(end_record_proto, result_pb));
  }

  return absl::OkStatus();
}

absl::Status RowCursorToProtoPartitionChurningRecord(
    backend::RowCursor* row_cursor, absl::string_view partition_token,
    spanner_api::ResultSet& result_pb) {
  std::vector<PartitionRow> rows;
  while (row_cursor->Next()) {
    PartitionRow r;
    r.start_time = row_cursor->ColumnValue(0).ToTime();
    r.child_token = row_cursor->ColumnValue(1).string_value();
    googlesql::Value parents_val = row_cursor->ColumnValue(2);
    r.parents.reserve(parents_val.num_elements());
    for (int i = 0; i < parents_val.num_elements(); ++i) {
      r.parents.push_back(parents_val.element(i).string_value());
    }
    rows.push_back(r);
  }

  if (rows.size() == 1 && rows[0].parents.size() == 1) {
    return absl::InternalError(
        "Move event partition churning is not supported in mutable key range "
        "Change Streams.");
  }
  if (rows.size() == 1 && rows[0].parents.size() > 1) {
    return ConvertMergeEventToProto(rows[0], partition_token, result_pb);
  }
  if (rows.size() > 1) {
    return ConvertSplitEventToProto(rows, partition_token, result_pb);
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertPartitionTableRowCursorToProto(
    backend::RowCursor* row_cursor,
    std::optional<absl::Time> initial_start_timestamp,
    absl::string_view partition_token, bool expect_metadata) {
  spanner_api::ResultSet result_pb;

  if (initial_start_timestamp.has_value()) {
    // Initial Query: Return PartitionStartRecord for each partition.
    int64_t record_sequence = 0;
    spanner_api::ChangeStreamRecord change_record_proto;
    while (row_cursor->Next()) {
      change_record_proto.Clear();
      auto* start_proto = change_record_proto.mutable_partition_start_record();
      GOOGLESQL_RETURN_IF_ERROR(RowCursorToProtoPartitionStartRecord(
          row_cursor, record_sequence++, initial_start_timestamp, start_proto));
      GOOGLESQL_RETURN_IF_ERROR(
          AddChangeRecordProtoToResultSet(change_record_proto, result_pb));
    }
  } else {
    // Partition Query: Return PartitionStartRecords, PartitionEventRecords, and
    // PartitionEndRecords if needed.
    GOOGLESQL_RETURN_IF_ERROR(RowCursorToProtoPartitionChurningRecord(
        row_cursor, partition_token, result_pb));
  }

  GOOGLESQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    GOOGLESQL_RETURN_IF_ERROR(PopulateMetadataForProto(&responses));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertQueryStartPartitionTableRowCursorToProto(backend::RowCursor* row_cursor,
                                                absl::Time query_start_time,
                                                bool expect_metadata) {
  spanner_api::ResultSet result_pb;
  int64_t record_sequence = 0;
  spanner_api::ChangeStreamRecord change_record_proto;

  while (row_cursor->Next()) {
    absl::Time start_time = row_cursor->ColumnValue(0).ToTime();
    // Skip emitting PartitionEventRecords with partition start time before
    // query start time.
    if (query_start_time > start_time) {
      continue;
    }
    change_record_proto.Clear();
    std::string partition_token = row_cursor->ColumnValue(1).string_value();
    std::vector<std::string> source_partition_tokens;
    googlesql::Value source_partition_tokens_val = row_cursor->ColumnValue(2);
    source_partition_tokens.reserve(source_partition_tokens_val.num_elements());
    for (int i = 0; i < source_partition_tokens_val.num_elements(); ++i) {
      source_partition_tokens.push_back(
          source_partition_tokens_val.element(i).string_value());
    }

    auto* event_proto = change_record_proto.mutable_partition_event_record();
    PopulateProtoPartitionMoveInEventRecord(start_time, partition_token,
                                            source_partition_tokens,
                                            record_sequence++, event_proto);
    GOOGLESQL_RETURN_IF_ERROR(
        AddChangeRecordProtoToResultSet(change_record_proto, result_pb));
  }
  if (result_pb.rows_size() == 0) {
    return std::vector<spanner_api::PartialResultSet>();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    GOOGLESQL_RETURN_IF_ERROR(PopulateMetadataForProto(&responses));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertHeartbeatTimestampToProto(absl::Time timestamp, bool expect_metadata) {
  spanner_api::ResultSet result_pb;

  spanner_api::ChangeStreamRecord change_record_proto;
  auto* heartbeat_record_proto = change_record_proto.mutable_heartbeat_record();
  int64_t s = absl::ToUnixSeconds(timestamp);
  int64_t ns = absl::ToInt64Nanoseconds(timestamp - absl::FromUnixSeconds(s));
  heartbeat_record_proto->mutable_timestamp()->set_seconds(s);
  heartbeat_record_proto->mutable_timestamp()->set_nanos(ns);

  GOOGLESQL_RETURN_IF_ERROR(
      AddChangeRecordProtoToResultSet(change_record_proto, result_pb));

  GOOGLESQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    GOOGLESQL_RETURN_IF_ERROR(PopulateMetadataForProto(&responses));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

absl::StatusOr<std::vector<spanner_api::PartialResultSet>>
ConvertDataTableRowCursorToProto(backend::RowCursor* row_cursor,
                                 bool expect_metadata) {
  spanner_api::ResultSet result_pb;
  spanner_api::ChangeStreamRecord change_record_proto;
  while (row_cursor->Next()) {
    change_record_proto.Clear();
    auto* data_change_record_proto =
        change_record_proto.mutable_data_change_record();
    GOOGLESQL_RETURN_IF_ERROR(
        RowCursorToProtoDataChangeRecord(row_cursor, data_change_record_proto));

    GOOGLESQL_RETURN_IF_ERROR(
        AddChangeRecordProtoToResultSet(change_record_proto, result_pb));
  }

  GOOGLESQL_ASSIGN_OR_RETURN(auto responses,
                   ChunkResultSet(result_pb, limits::kMaxStreamingChunkSize));
  if (expect_metadata) {
    GOOGLESQL_RETURN_IF_ERROR(PopulateMetadataForProto(&responses));
  } else {
    responses.at(0).clear_metadata();
  }
  PopulateFakeResumeTokens(&responses);
  return responses;
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
