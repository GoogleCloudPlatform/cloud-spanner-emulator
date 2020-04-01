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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_TIME_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_TIME_H_

#include "google/protobuf/any.pb.h"
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/timestamp.pb.h"
#include "absl/time/time.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace google {
namespace spanner {
namespace emulator {

zetasql_base::Status TimestampToProto(absl::Time time,
                              google::protobuf::Timestamp* proto);

// TODO: Add tests for TimestampFromProto and DurationFromProto.
zetasql_base::StatusOr<absl::Time> TimestampFromProto(
    const google::protobuf::Timestamp& proto);

// Parse "duration" in seconds and nanoseconds. Returns an error if it does not
// meet the requirements of a staleness bound.
zetasql_base::StatusOr<absl::Duration> DurationFromProto(
    const google::protobuf::Duration& proto);

}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_TIME_H_
