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

#include "frontend/converters/keys.h"

#include "zetasql/public/value.h"
#include "zetasql/base/statusor.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"
#include "frontend/converters/values.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

namespace spanner_api = ::google::spanner::v1;

zetasql_base::StatusOr<backend::Key> KeyFromProtoInternal(
    const google::protobuf::ListValue& list_pb, const backend::Table& table,
    bool allow_prefix_key) {
  // Check that the user did not specify more than the required number of key
  // parts. For normal tables, this is the size of the primary key. For index
  // data tables, this is the number of index columns specified in the index
  // definition.
  size_t required_key_parts =
      (table.owner_index() ? table.owner_index()->key_columns().size()
                           : table.primary_key().size());
  if (list_pb.values_size() > required_key_parts) {
    return error::WrongNumberOfKeyParts(
        (table.owner_index() ? table.owner_index()->Name() : table.Name()),
        required_key_parts, list_pb.values_size(), list_pb.ShortDebugString());
  }

  // Prefix keys are allowed only when part of key ranges. When prefix keys are
  // not allowed in other cases, check that the required number of key parts are
  // present.
  if (!allow_prefix_key && list_pb.values_size() < required_key_parts) {
    return error::WrongNumberOfKeyParts(
        (table.owner_index() ? table.owner_index()->Name() : table.Name()),
        required_key_parts, list_pb.values_size(), list_pb.ShortDebugString());
  }

  backend::Key key;
  for (int i = 0; i < list_pb.values_size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(
        zetasql::Value value,
        ValueFromProto(list_pb.values(i),
                       table.primary_key()[i]->column()->GetType()));
    key.AddColumn(value, table.primary_key()[i]->is_descending());
  }

  return key;
}

}  // namespace

zetasql_base::StatusOr<backend::Key> KeyFromProto(
    const google::protobuf::ListValue& list_pb, const backend::Table& table) {
  // Prefix key parts are only allowed in key ranges.
  return KeyFromProtoInternal(list_pb, table, /*allow_prefix_key=*/false);
}

zetasql_base::StatusOr<backend::KeyRange> KeyRangeFromProto(
    const spanner_api::KeyRange& range_pb, const backend::Table& table) {
  // Parse the start endpoint.
  backend::EndpointType start_type;
  backend::Key start_key;
  if (range_pb.has_start_open()) {
    start_type = backend::EndpointType::kOpen;
    ZETASQL_ASSIGN_OR_RETURN(start_key,
                     KeyFromProtoInternal(range_pb.start_open(), table,
                                          /*allow_prefix_key=*/true));
  } else if (range_pb.has_start_closed()) {
    start_type = backend::EndpointType::kClosed;
    ZETASQL_ASSIGN_OR_RETURN(start_key,
                     KeyFromProtoInternal(range_pb.start_closed(), table,
                                          /*allow_prefix_key=*/true));
  } else {
    return error::KeyRangeMissingStart();
  }

  // Parse the limit endpoint.
  backend::EndpointType limit_type;
  backend::Key limit_key;
  if (range_pb.has_end_open()) {
    limit_type = backend::EndpointType::kOpen;
    ZETASQL_ASSIGN_OR_RETURN(limit_key,
                     KeyFromProtoInternal(range_pb.end_open(), table,
                                          /*allow_prefix_key=*/true));
  } else if (range_pb.has_end_closed()) {
    limit_type = backend::EndpointType::kClosed;
    ZETASQL_ASSIGN_OR_RETURN(limit_key,
                     KeyFromProtoInternal(range_pb.end_closed(), table,
                                          /*allow_prefix_key=*/true));
  } else {
    return error::KeyRangeMissingEnd();
  }

  return backend::KeyRange(start_type, start_key, limit_type, limit_key);
}

zetasql_base::StatusOr<backend::KeySet> KeySetFromProto(
    const spanner_api::KeySet& key_set_pb, const backend::Table& table) {
  backend::KeySet key_set;

  // Parse individual keys.
  for (const auto& key_pb : key_set_pb.keys()) {
    ZETASQL_ASSIGN_OR_RETURN(
        backend::Key key,
        KeyFromProtoInternal(key_pb, table, /*allow_prefix_key=*/false));
    key_set.AddKey(key);
  }

  // Parse key ranges.
  for (const auto& range_pb : key_set_pb.ranges()) {
    ZETASQL_ASSIGN_OR_RETURN(backend::KeyRange range,
                     KeyRangeFromProto(range_pb, table));
    key_set.AddRange(range);
  }

  // Handle the special "all" key set.
  if (key_set_pb.all()) {
    key_set.AddRange(backend::KeyRange::All());
  }

  return key_set;
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
