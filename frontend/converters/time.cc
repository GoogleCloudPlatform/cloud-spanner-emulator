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

#include "frontend/converters/time.h"

#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "common/errors.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {

namespace {

// The following validation requirements are taken from timestamp.proto and
// duration.proto

absl::Status Validate(const google::protobuf::Timestamp& time) {
  const auto sec = time.seconds();
  const auto ns = time.nanos();
  // sec must be [0001-01-01T00:00:00Z, 9999-12-31T23:59:59.999999999Z]
  if (sec < -62135596800 || sec > 253402300799) {
    return error::InvalidTime(absl::StrCat("seconds=", sec));
  }
  if (ns < 0 || ns > 999999999) {
    return error::InvalidTime(absl::StrCat("nanos=", ns));
  }
  return absl::OkStatus();
}

absl::Status Validate(const google::protobuf::Duration& d) {
  const auto sec = d.seconds();
  const auto ns = d.nanos();
  if (sec < -315576000000 || sec > 315576000000) {
    return error::InvalidTime(absl::StrCat("seconds=", sec));
  }
  if (ns < -999999999 || ns > 999999999) {
    return error::InvalidTime(absl::StrCat("nanos=", ns));
  }
  if ((sec < 0 && ns > 0) || (sec > 0 && ns < 0)) {
    return absl::Status(absl::StatusCode::kInvalidArgument, "sign mismatch");
  }
  return absl::OkStatus();
}

}  // namespace

zetasql_base::StatusOr<google::protobuf::Timestamp> TimestampToProto(absl::Time time) {
  const int64_t s = absl::ToUnixSeconds(time);
  google::protobuf::Timestamp proto;
  proto.set_seconds(s);
  proto.set_nanos((time - absl::FromUnixSeconds(s)) / absl::Nanoseconds(1));
  ZETASQL_RETURN_IF_ERROR(Validate(proto));
  return proto;
}

zetasql_base::StatusOr<absl::Time> TimestampFromProto(
    const google::protobuf::Timestamp& proto) {
  ZETASQL_RETURN_IF_ERROR(Validate(proto));
  return absl::FromUnixSeconds(proto.seconds()) +
         absl::Nanoseconds(proto.nanos());
}

zetasql_base::StatusOr<absl::Duration> DurationFromProto(
    const google::protobuf::Duration& proto) {
  ZETASQL_RETURN_IF_ERROR(Validate(proto));
  return absl::Seconds(proto.seconds()) + absl::Nanoseconds(proto.nanos());
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
