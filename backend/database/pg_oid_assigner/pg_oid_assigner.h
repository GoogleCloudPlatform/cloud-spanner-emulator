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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_PG_OID_ASSIGNER_PG_OID_ASSIGNER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_PG_OID_ASSIGNER_PG_OID_ASSIGNER_H_

#include <cstdint>
#include <optional>
#include <vector>

#include "absl/status/status.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Assigns OIDs to objects during DDL statements. The OIDs are assigned in
// monotonically increasing order and are checkpointed between each statement
// because OID assignment occurs as database objects are created. If a statement
// fails after creating some objects, the OID assigner needs to be rolled back
// to a state before the failed statement.
class PgOidAssigner {
 public:
  explicit PgOidAssigner(bool enabled) : enabled_(enabled) {
    next_postgresql_oid_ = kMinPostgresqlOid;
    tentative_next_postgresql_oid_ = kMinPostgresqlOid;
  }

  // Marks the start of OID assignment. If EndAssignment is not called after,
  // the OID assignment will be be reset to the same state the next time this
  // function is called.
  void BeginAssignment() {
    if (!enabled_) return;
    tentative_next_postgresql_oid_ = next_postgresql_oid_;
  }

  // Returns and autoincrements the tentative next OID that is used during OID
  // assignment.
  std::optional<uint32_t> GetNextPostgresqlOid() {
    return enabled_ ? std::optional<uint32_t>(tentative_next_postgresql_oid_++)
                    : std::nullopt;
  }

  // Marks the current next OID as of the last statement. This is used to
  // checkpoint the OID assignment between each statement so if only a subset of
  // the statements are successful, the OID assignment can be rolled back
  // correctly.
  void MarkNextPostgresqlOidForIntermediateSchema() {
    if (!enabled_) return;
    intermediate_next_postgresql_oids_.push_back(
        tentative_next_postgresql_oid_);
  }

  // Marks the end of OID assignment at a specific intermediate index. This is
  // used when not all statements succeed to specify which intermediate
  // OID to rollback to.
  absl::Status EndAssignmentAtIntermediateSchema(
      uint32_t last_successful_index) {
    if (enabled_) {
      ZETASQL_RET_CHECK_LT(last_successful_index,
                   intermediate_next_postgresql_oids_.size());
      next_postgresql_oid_ =
          intermediate_next_postgresql_oids_[last_successful_index];
      intermediate_next_postgresql_oids_.clear();
      tentative_next_postgresql_oid_ = next_postgresql_oid_;
    }
    return absl::OkStatus();
  }

  // Marks the end of OID assignment. This updates the true next OID to the same
  // value as the last intermediate OID.
  absl::Status EndAssignment() {
    if (enabled_) {
      ZETASQL_RET_CHECK(!intermediate_next_postgresql_oids_.empty());
      next_postgresql_oid_ = intermediate_next_postgresql_oids_.back();
      intermediate_next_postgresql_oids_.clear();
      tentative_next_postgresql_oid_ = next_postgresql_oid_;
    }
    return absl::OkStatus();
  }

  uint32_t TEST_next_postgresql_oid() { return next_postgresql_oid_; }

  uint32_t TEST_tentative_next_postgresql_oid() {
    return tentative_next_postgresql_oid_;
  }

 private:
  constexpr static uint32_t kMinPostgresqlOid = 100000;
  // The true next OID. Once the schema is updated, this value is updated.
  uint32_t next_postgresql_oid_;
  // The tentative next OID which is used during OID assignment.
  uint32_t tentative_next_postgresql_oid_;
  // The intermediate next OIDs which are used to checkpoint the OID assignment
  // between each statement. If a statement fails, the OID assignment can be
  // rolled back to the last checkpoint.
  std::vector<uint32_t> intermediate_next_postgresql_oids_;
  // Whether the OID assigner is enabled. OID assignment should only be enabled
  // for POSTGRESQL dialect databases.
  bool enabled_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_PG_OID_ASSIGNER_PG_OID_ASSIGNER_H_
