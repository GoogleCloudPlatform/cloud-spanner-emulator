//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#ifndef DDL_TRANSLATION_UTILS_H_
#define DDL_TRANSLATION_UTILS_H_

#include <cstdint>
#include <string>
#include <tuple>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {
namespace spangres {
namespace internal {

template <typename ExpectedFieldType>
class FieldTypeChecker {
 public:
  template <typename ActualFieldType>
  constexpr FieldTypeChecker(const ActualFieldType& field) {
    static_assert(std::is_same<ExpectedFieldType, ActualFieldType>::value,
                  "Field type doesn't match the expected one");
  }
};

// Asserts at the compile time that a class or structure <StructT> contains only
// fields of the <FieldTypes> types. This can be used in the code that uses some
// structure to make sure that this structure hasn't changed since the time the
// code was written and thus the code is still valid.
//
// Usage example:
// struct TestStruct {
//  int field1;
//  char* field2;
// };
// TestStruct ts;
// AssertStructConsistsOf(
//                        ts,
//                        FieldTypeChecker<int>(ts.field1),
//                        FieldTypeChecker<char*>(ts.field2));
// Code will not compile if new field is added to TestStruct or any fields type
// is changed.
template <typename StructT, typename... FieldTypes>
void AssertStructConsistsOf(const StructT& structure,
                            const FieldTypeChecker<FieldTypes>&...) {
  // Check that no new fields were added to the node.
  static_assert(sizeof(std::tuple<FieldTypes...>) == sizeof(structure),
                "Structure is different from what is expected");
}

// Asserts at the compile time that PG parser node <NodeT> contains only
// <fields>. This check is needed to make sure that translator code is still
// valid after we upgraded PostgreSQL code to a new version.
template <typename NodeT, typename... FieldTypes>
void AssertPGNodeConsistsOf(const NodeT& node,
                            const FieldTypeChecker<FieldTypes>&... fields) {
  AssertStructConsistsOf(node, FieldTypeChecker<NodeTag>(node.type), fields...);
}

// A class to hold definitions of PostgreSQL-related constants
struct PostgreSQLConstants {
  static constexpr absl::string_view kSpangresOptimizerVersionName =
      "spanner.optimizer_version";
  static constexpr absl::string_view
      kSpangresDatabaseVersionRetentionPeriodName =
          "spanner.version_retention_period";
  static constexpr absl::string_view
      kSpangresDatabaseOptimizerStatisticsPackageName =
          "spanner.optimizer_statistics_package";
  static constexpr absl::string_view kSpangresKeyVisualizerDatabaseOptionName =
      "spanner.enable_key_visualizer";
  static constexpr absl::string_view kSpangresDatabaseDefaultLeaderOptionName =
      "spanner.default_leader";
  static constexpr absl::string_view
      kSpangresDatabaseWitnessLocationOptionName = "spanner.witness_location";
  static constexpr absl::string_view
      kSpangresDatabaseReadLeaseLabelsOptionName = "spanner.read_lease_regions";
  static constexpr absl::string_view kSearchIndexDisableUidOptionName =
      "disable_automatic_uid_column";
  static constexpr absl::string_view kSearchIndexSortOrderOptionName =
      "sort_order_sharding";

  // TODO: use kDatabaseOptimizerVersionName,
  // kInternalDatabaseVersionRetentionPeriodName,
  // kInternalCommitTimestampOptionName, kCloudDatabaseNameOption directly from
  // spanner/common/schema/schema_util.h after Blaze target visibility is
  // adjusted.
  static constexpr absl::string_view kDatabaseOptimizerVersionName =
      "optimizer_version";
  static constexpr absl::string_view
      kInternalDatabaseVersionRetentionPeriodName =
          "spanner.internal.minimum_version_retention_period";
  static constexpr absl::string_view kInternalCommitTimestampOptionName =
      "commit_timestamp";
  static constexpr absl::string_view kCloudDatabaseNameOption =
      "cloud_database_name";
  static constexpr absl::string_view kDatabaseOptimizerStatisticsPackageName =
      "optimizer_statistics_package";
  static constexpr absl::string_view kInternalKeyVisualizerDatabaseOptionName =
      "enable_row_range_stats";
  static constexpr absl::string_view kChangeStreamValueCaptureTypeOptionName =
      "value_capture_type";
  static constexpr absl::string_view kChangeStreamRetentionPeriodOptionName =
      "retention_period";
  static constexpr absl::string_view kChangeStreamExcludeInsertOptionName =
      "exclude_insert";
  static constexpr absl::string_view kChangeStreamExcludeUpdateOptionName =
      "exclude_update";
  static constexpr absl::string_view kChangeStreamExcludeDeleteOptionName =
      "exclude_delete";
  static constexpr absl::string_view kChangeStreamExcludeTtlDeletesOptionName =
      "exclude_ttl_deletes";
  static constexpr absl::string_view kChangeStreamAllowTxnExclusionOptionName =
      "allow_txn_exclusion";
  static constexpr absl::string_view kChangeStreamPartitionModeOptionName =
      "partition_mode";
  static constexpr char kInternalLocalityGroupStorageOptionName[] = "inflash";
  static constexpr char kInternalLocalityGroupSpillTimeSpanOptionName[] =
      "age_based_spill_policy";
  // TODO: expose when queue is implemented.
  static constexpr absl::string_view kSpangresDefaultSequenceKindOptionName =
      "spanner.default_sequence_kind";
  static constexpr absl::string_view
      kInternalDatabaseDefaultSequenceKindOptionName =
          "spanner.internal.cloud_default_sequence_kind";
  static constexpr absl::string_view kSpangresDefaultTimeZoneOptionName =
      "spanner.default_time_zone";
  static constexpr absl::string_view
      kInternalDatabaseDefaultTimeZoneOptionName =
          "spanner.internal.cloud_default_time_zone";
  static constexpr absl::string_view kInternalDatabaseDefaultLeaderOptionName =
      "spanner.internal.cloud_default_leader";
  static constexpr absl::string_view
      kInternalDatabaseWitnessLocationOptionName =
          "spanner.internal.cloud_witness_location";
  static constexpr absl::string_view
      kInternalDatabaseReadLeaseLabelsOptionName =
          "spanner.internal.read_lease_labels";

  // Options for Spanner Bit-reversed Sequences.
  // Copied from google3/spanner/common/schema/schema_util.h
  static constexpr absl::string_view kSequenceKindOptionName = "sequence_kind";
  static constexpr absl::string_view kSequenceKindBitReversedPositive =
      "bit_reversed_positive";
  static constexpr absl::string_view kSequenceSkipRangeMinOptionName =
      "skip_range_min";
  static constexpr absl::string_view kSequenceSkipRangeMaxOptionName =
      "skip_range_max";
  static constexpr absl::string_view kSequenceStartWithCounterOptionName =
      "start_with_counter";

  // Options for user-defined functions.
  static constexpr absl::string_view kFunctionAsOptionName = "as";
  static constexpr absl::string_view kFunctionLanguageOptionName = "language";
  static constexpr absl::string_view kFunctionSecurityOptionName = "security";
  static constexpr absl::string_view kFunctionVolatilityOptionName =
      "volatility";
  static constexpr absl::string_view kFunctionVolatile = "volatile";
  static constexpr absl::string_view kFunctionStable = "stable";
  static constexpr absl::string_view kFunctionImmutable = "immutable";

  // Spanner statistics packages are expected to be prefixed with this namespace
  // when modified via ALTER STATISTICS.
  static constexpr absl::string_view kNamespace = "spanner";

  // Statistics package option name for allow_gc option.
  static constexpr absl::string_view kStatsAllowGcOption = "allow_gc";

  // Booleans in PostgreSQL Value nodes are represented as strings with these
  // values.
  static constexpr absl::string_view kPgTrueLiteral = "true";
  static constexpr absl::string_view kPgFalseLiteral = "false";

  // Measured in chars, not bytes - the same way as in PostgreSQL
  static constexpr int kMaxStringLength = 2621440;
  // Measured in bytes
  static constexpr int kMaxBytesLength = 10485760;

  // OPTION to specify that a view is SQL SECURITY INVOKER.
  // ("SQL SECURITY INVOKER" is ZetaSQL syntax and a Spanner extension.
  // The OPTION syntax is a recent equivalent upstream-PostgreSQL addition.)
  static constexpr absl::string_view kSecurityViewOptionName =
      "security_invoker";
};

class PGAlterOption {
 public:
  PGAlterOption(absl::string_view pg_name, NodeTag pg_type,
                absl::string_view spanner_name, NodeTag spanner_type)
      : _pg_name(pg_name),
        _pg_type(pg_type),
        _spanner_name(spanner_name),
        _spanner_type(spanner_type) {}
  virtual ~PGAlterOption() = default;

  absl::string_view PGName() { return _pg_name; };
  NodeTag PGType() { return _pg_type; };
  absl::string_view SpannerName() { return _spanner_name; };
  NodeTag SpannerType() { return _spanner_type; };

 private:
  absl::string_view _pg_name;
  NodeTag _pg_type;
  absl::string_view _spanner_name;
  NodeTag _spanner_type;
};

// Quotes string literals according to PostgreSQL rules, e.g. - puts them in
// single quotes and escapes any single quote by doubling it.
std::string QuoteStringLiteral(absl::string_view str);

// Quotes identifier if necessary. Replicates the logic used by PostgreSQL to
// quote identifier, e.g. identifier will be left unquoted only if:
// - it starts with lowercase letter or underscore
// - it contains only lowercase letters, digits and underscores
// - it is not a reserved keyword, type name or function
std::string QuoteIdentifier(absl::string_view identifier);
std::string QuoteQualifiedIdentifier(absl::string_view identifier);

// Checks that database option is supported by <ALTER DATABASE> statement
bool IsDatabaseOptionSupported(absl::string_view option_name);

absl::optional<absl::string_view> GetSpangresOptionName(
    absl::string_view internal_option_name);
absl::optional<PGAlterOption> GetOptionByInternalName(
    absl::string_view spangres_option_name);

// Checks if the name is reserved.
bool IsReservedName(absl::string_view name);

bool IsPostgresReservedName(absl::string_view name);

int64_t ParseSchemaTimeSpec(absl::string_view spec);

// Converts ObjectType enum directly to a string: OBJECT_FOO_BAR -> "FOO BAR".
absl::StatusOr<std::string> ObjectTypeToString(ObjectType object_type);

}  // namespace internal
}  // namespace spangres
}  // namespace postgres_translator

#endif  // DDL_TRANSLATION_UTILS_H_
