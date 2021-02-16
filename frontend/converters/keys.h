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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_KEYS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_KEYS_H_

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/keys.pb.h"
#include "zetasql/base/statusor.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Converts a CloudSpanner key proto (encoded as a list) to a backend Key.
zetasql_base::StatusOr<backend::Key> KeyFromProto(
    const google::protobuf::ListValue& list_pb, const backend::Table& table);

// Converts a CloudSpanner key range proto to a backend KeyRange.
zetasql_base::StatusOr<backend::KeyRange> KeyRangeFromProto(
    const google::spanner::v1::KeyRange& range_pb, const backend::Table& table);

// Converts a Cloud Spanner key set proto to a backend KeySet.
zetasql_base::StatusOr<backend::KeySet> KeySetFromProto(
    const google::spanner::v1::KeySet& key_set_pb, const backend::Table& table);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_KEYS_H_
