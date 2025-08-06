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

#include "backend/schema/updater/schema_updater.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/functional/function_ref.h"
#include "absl/log/log.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/common/ids.h"
#include "backend/common/utils.h"
#include "backend/database/pg_oid_assigner/pg_oid_assigner.h"
#include "backend/datamodel/types.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/query/prepare_property_graph_catalog.h"
#include "backend/schema/backfills/change_stream_backfill.h"
#include "backend/schema/backfills/column_value_backfill.h"
#include "backend/schema/backfills/index_backfill.h"
#include "backend/schema/builders/change_stream_builder.h"
#include "backend/schema/builders/check_constraint_builder.h"
#include "backend/schema/builders/column_builder.h"
#include "backend/schema/builders/database_options_builder.h"
#include "backend/schema/builders/foreign_key_builder.h"
#include "backend/schema/builders/index_builder.h"
#include "backend/schema/builders/locality_group_builder.h"
#include "backend/schema/builders/model_builder.h"
#include "backend/schema/builders/named_schema_builder.h"
#include "backend/schema/builders/placement_builder.h"
#include "backend/schema/builders/property_graph_builder.h"
#include "backend/schema/builders/sequence_builder.h"
#include "backend/schema/builders/table_builder.h"
#include "backend/schema/builders/udf_builder.h"
#include "backend/schema/builders/view_builder.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/database_options.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/locality_group.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/catalog/named_schema.h"
#include "backend/schema/catalog/placement.h"
#include "backend/schema/catalog/property_graph.h"
#include "backend/schema/catalog/proto_bundle.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/parser/ddl_parser.h"
#include "backend/schema/updater/ddl_type_conversion.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/updater/sql_expression_validators.h"
#include "backend/schema/verifiers/check_constraint_verifiers.h"
#include "backend/schema/verifiers/foreign_key_verifiers.h"
#include "backend/schema/verifiers/interleaving_verifiers.h"
#include "backend/storage/storage.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "common/limits.h"
#include "third_party/spanner_pg/ddl/ddl_translator.h"
#include "third_party/spanner_pg/ddl/pg_to_spanner_ddl_translator.h"
#include "third_party/spanner_pg/interface/emulator_parser.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "zetasql/public/functions/uuid.h"

#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(bool, cloud_spanner_emulator_disable_cs_retention_check, false,
          "whether we want to check the retention limit when altering a change "
          "stream's retention period.");

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

namespace database_api = ::google::spanner::admin::database::v1;
using ::postgres_translator::interfaces::ExpressionTranslateResult;
typedef google::protobuf::RepeatedPtrField<ddl::SetOption> OptionList;

// A struct that defines the columns used by an index.
struct ColumnsUsedByIndex {
  std::vector<const KeyColumn*> index_key_columns;
  std::vector<const Column*> stored_columns;
  std::vector<const Column*> null_filtered_columns;
  std::vector<const Column*> partition_by_columns;
  std::vector<const KeyColumn*> order_by_columns;
};

// Get all the names of objects in a vector of objects. Useful for creating
// error messages with multiple objects, i.e. getting all the names of tables in
// a schema.
template <typename T>
std::vector<std::string> GetObjectNames(absl::Span<const T* const> objects) {
  std::vector<std::string> names;
  for (const auto* object : objects) {
    names.push_back(object->Name());
  }
  return names;
}

template <typename SetOptions>
void SetSequenceOptionsForIdentityColumn(
    ddl::ColumnDefinition::IdentityColumnDefinition identity_column,
    SetOptions* set_options) {
  if (identity_column.has_start_with_counter()) {
    ddl::SetOption* start_with_counter = set_options->Add();
    start_with_counter->set_option_name("start_with_counter");
    start_with_counter->set_int64_value(identity_column.start_with_counter());
  }
  if (identity_column.has_skip_range_min()) {
    ddl::SetOption* skip_range_min = set_options->Add();
    skip_range_min->set_option_name("skip_range_min");
    skip_range_min->set_int64_value(identity_column.skip_range_min());
    ddl::SetOption* skip_range_max = set_options->Add();
    skip_range_max->set_option_name("skip_range_max");
    skip_range_max->set_int64_value(identity_column.skip_range_max());
  }
}

// A class that processes a set of Cloud Spanner DDL statements, and applies
// them to an existing (or empty) `Schema` to obtain the updated `Schema`.
//
// The effects of the DDL statements are checked for semantic validity during
// the process and appropriate errors returned on any violations.
//
// Implementation note:
// Semantic violation checks other than existence checks (required to build
// proper reference relationships in the schema graph) should be avoided in this
// class and should instead be encoded in the `Validate()` and
// `ValidateUpdate()` implementations of `SchemaNode`(s) so that they are
// executed during both database schema creation and update.
class SchemaUpdaterImpl {
 public:
  SchemaUpdaterImpl() = delete;  // Private construction only.
  ~SchemaUpdaterImpl() = default;
  // Disallow copies but enable moves.
  SchemaUpdaterImpl(const SchemaUpdaterImpl&) = delete;
  SchemaUpdaterImpl& operator=(const SchemaUpdaterImpl&) = delete;
  SchemaUpdaterImpl(SchemaUpdaterImpl&&) = default;
  // Assignment move is not supported by absl::Time.
  SchemaUpdaterImpl& operator=(SchemaUpdaterImpl&&) = delete;

  absl::StatusOr<SchemaUpdaterImpl> static Build(
      zetasql::TypeFactory* type_factory,
      TableIDGenerator* table_id_generator,
      ColumnIDGenerator* column_id_generator, Storage* storage,
      absl::Time schema_change_ts, PgOidAssigner* pg_oid_assigner,
      const Schema* existing_schema, std::string_view database_id) {
    SchemaUpdaterImpl impl(type_factory, table_id_generator,
                           column_id_generator, storage, schema_change_ts,
                           pg_oid_assigner, existing_schema, database_id);
    ZETASQL_RETURN_IF_ERROR(impl.Init());
    return impl;
  }

  // Apply DDL statements returning the SchemaValidationContext containing
  // the schema change actions resulting from each statement. Please note that
  // it can return an empty vector when all statements are no-op and no changes
  // are made.
  absl::StatusOr<std::vector<SchemaValidationContext>> ApplyDDLStatements(
      const SchemaChangeOperation& schema_change_operation);

  std::vector<std::unique_ptr<const Schema>> GetIntermediateSchemas() {
    return std::move(intermediate_schemas_);
  }

 private:
  SchemaUpdaterImpl(zetasql::TypeFactory* type_factory,
                    TableIDGenerator* table_id_generator,
                    ColumnIDGenerator* column_id_generator, Storage* storage,
                    absl::Time schema_change_ts, PgOidAssigner* pg_oid_assigner,
                    const Schema* existing_schema, std::string_view database_id)
      : type_factory_(type_factory),
        table_id_generator_(table_id_generator),
        column_id_generator_(column_id_generator),
        storage_(storage),
        schema_change_timestamp_(schema_change_ts),
        latest_schema_(existing_schema),
        editor_(nullptr),
        pg_oid_assigner_(pg_oid_assigner),
        database_id_(database_id) {}

  // Initializes potentially failing components after construction.
  absl::Status Init();

  // Applies the given `statement` on to `latest_schema_`. Please note that
  // it can return a nullptr if the statement is a no-op and no changes are
  // made.
  absl::StatusOr<std::unique_ptr<const Schema>> ApplyDDLStatement(
      absl::string_view statement, absl::string_view proto_descriptor_bytes,
      const database_api::DatabaseDialect& dialect);

  // Run any pending schema actions resulting from the schema change statements.
  absl::Status RunPendingActions(
      const std::vector<SchemaValidationContext>& pending_work,
      int* num_succesful);

  absl::Status InitColumnNameAndTypesFromTable(
      const Table* table, const ddl::CreateTable* ddl_create_table,
      std::vector<zetasql::SimpleTable::NameAndType>* name_and_types);

  absl::Status AnalyzeGeneratedColumn(
      absl::string_view expression, const std::string& column_name,
      const zetasql::Type* column_type, const Table* table,
      const ddl::CreateTable* ddl_create_table,
      absl::flat_hash_set<std::string>* dependent_column_names,
      absl::flat_hash_set<const SchemaNode*>* udf_dependencies);

  absl::Status AnalyzeColumnDefaultValue(
      absl::string_view expression, const std::string& column_name,
      const zetasql::Type* column_type, const Table* table,
      const ddl::CreateTable* ddl_create_table,
      absl::flat_hash_set<const SchemaNode*>* dependent_sequences,
      absl::flat_hash_set<const SchemaNode*>* udf_dependencies);

  absl::Status AnalyzeCheckConstraint(
      absl::string_view expression, const Table* table,
      const ddl::CreateTable* ddl_create_table,
      absl::flat_hash_set<std::string>* dependent_column_names,
      CheckConstraint::Builder* builder,
      absl::flat_hash_set<const SchemaNode*>* udf_dependencies);

  template <typename ColumnModifier>
  absl::Status SetColumnOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      const database_api::DatabaseDialect& dialect, ColumnModifier* modifier);

  template <typename Modifier>
  absl::Status ProcessLocalityGroupOption(const ddl::SetOption& option,
                                          Modifier* modifier);

  template <typename TableModifier>
  absl::Status SetTableOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      TableModifier* modifier);

  template <typename IndexModifier>
  absl::Status SetIndexOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      IndexModifier* modifier, bool allow_other_options = false);
  template <typename ColumnDef, typename ColumnDefModifer>
  absl::StatusOr<std::string> TranslatePGExpression(
      const ColumnDef& ddl_column, const Table* table,
      const ddl::CreateTable* ddl_create_table, ColumnDefModifer& modifier);

  template <typename ColumnDefModifier>
  absl::Status SetColumnDefinition(const ddl::ColumnDefinition& ddl_column,
                                   const Table* table,
                                   const ddl::CreateTable* ddl_create_table,
                                   const database_api::DatabaseDialect& dialect,
                                   bool is_alter,
                                   ColumnDefModifier* modifier);

  absl::Status AlterColumnDefinition(
      const ddl::ColumnDefinition& ddl_column, const Table* table,
      const database_api::DatabaseDialect& dialect, Column::Editor* editor);

  absl::Status AlterColumnSetDropDefault(
      const ddl::AlterTable::AlterColumn& alter_column, const Table* table,
      const Column* column, const database_api::DatabaseDialect& dialect,
      Column::Editor* editor);

  absl::StatusOr<const Column*> CreateColumn(
      const ddl::ColumnDefinition& ddl_column, const Table* table,
      const ddl::CreateTable* ddl_table,
      const database_api::DatabaseDialect& dialect);

  absl::StatusOr<const KeyColumn*> CreatePrimaryKeyColumn(
      const ddl::KeyPartClause& ddl_key_part, const Table* table,
      KeyColumn::Builder* builder);

  absl::Status CreatePrimaryKeyConstraint(
      const ddl::KeyPartClause& ddl_key_part, Table::Builder* builder,
      bool with_oid = true);

  absl::Status CreateInterleaveConstraintForTable(
      const ddl::InterleaveClause& interleave, Table::Builder* builder);
  absl::Status CreateInterleaveConstraint(
      const Table* parent, std::optional<Table::OnDeleteAction> on_delete,
      Table::InterleaveType interleave_type, Table::Builder* builder);
  absl::Status ValidateChangeStreamForClause(
      const ddl::ChangeStreamForClause& change_stream_for_clause,
      absl::string_view change_stream_name);
  absl::Status ValidateChangeStreamLimits(
      const ddl::ChangeStreamForClause& change_stream_for_clause,
      absl::string_view change_stream_name);
  absl::Status ValidateLimitsForTrackedObjects(
      const ddl::ChangeStreamForClause& change_stream_for_clause,
      absl::FunctionRef<absl::Status(const Table*)> table_cb,
      absl::FunctionRef<absl::Status(const Column*)> column_cb);
  absl::Status RegisterTrackedObjects(
      const ddl::ChangeStreamForClause& change_stream_for_clause,
      const ChangeStream* change_stream);
  absl::Status UnregisterChangeStreamFromTrackedObjects(
      const ChangeStream* change_stream);
  absl::Status BuildChangeStreamTrackedObjects(
      const ddl::ChangeStreamForClause& change_stream_for_clause,
      const ChangeStream* change_stream, ChangeStream::Builder* builder);
  absl::Status EditChangeStreamTrackedObjects(
      const ddl::ChangeStreamForClause& change_stream_for_clause,
      const ChangeStream* change_stream);
  absl::Status UnregisterChangeStreamFromTrackedObjects(
      const ChangeStream* change_stream,
      absl::FunctionRef<absl::Status(Table::Editor*)> table_cb,
      absl::FunctionRef<absl::Status(Column::Editor*)> column_cb);
  absl::Status RegisterTrackedObjects(
      const ChangeStream* change_stream,
      const ddl::ChangeStreamForClause& change_stream_for_clause,
      absl::FunctionRef<absl::Status(Table::Editor*)> table_cb,
      absl::FunctionRef<absl::Status(Column::Editor*)> column_cb);
  template <typename PlacementModifier>
  absl::Status SetPlacementOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      PlacementModifier* modifier);
  template <typename ChangeStreamModifier>
  absl::Status SetChangeStreamOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      ChangeStreamModifier* modifier);
  template <typename Modifier>
  absl::Status SetDatabaseOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      const database_api::DatabaseDialect& dialect, Modifier* modifier);
  absl::Status ValidateChangeStreamOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options);
  static std::string MakeChangeStreamTvfName(
      std::string change_stream_name,
      const database_api::DatabaseDialect& dialect);

  absl::StatusOr<const Table*> GetInterleaveConstraintTable(
      const std::string& interleave_in_table_name,
      const Table::Builder& builder) const;
  static std::optional<Table::OnDeleteAction> GetInterleaveConstraintOnDelete(
      const ddl::InterleaveClause& interleave);
  static Table::InterleaveType GetInterleaveType(
      const ddl::InterleaveClause& interleave);
  absl::Status CreateForeignKeyConstraint(
      const ddl::ForeignKey& ddl_foreign_key, const Table* referencing_table);
  absl::StatusOr<const ForeignKey*> BuildForeignKeyConstraint(
      const ddl::ForeignKey& ddl_foreign_key, const Table* referencing_table);
  absl::Status EvaluateForeignKeyReferencedPrimaryKey(
      const Table* table,
      const google::protobuf::RepeatedPtrField<std::string>& column_names,
      std::vector<int>* column_order, bool* index_required) const;
  absl::Status EvaluateForeignKeyReferencingPrimaryKey(
      const Table* table,
      const google::protobuf::RepeatedPtrField<std::string>& column_names,
      const std::vector<int>& column_order, bool* index_required) const;
  absl::StatusOr<const Index*> CreateForeignKeyIndex(
      const ForeignKey* foreign_key, const Table* table,
      const std::vector<std::string>& column_names, bool unique);
  bool CanInterleaveForeignKeyIndex(
      const Table* table, const std::vector<std::string>& column_names) const;

  absl::StatusOr<ExpressionTranslateResult> TranslatePostgreSqlExpression(
      const Table* table, const ddl::CreateTable* ddl_create_table,
      absl::string_view expression);
  absl::StatusOr<ExpressionTranslateResult> TranslatePostgreSqlQueryInView(
      absl::string_view query);

  absl::Status CreateCheckConstraint(
      const ddl::CheckConstraint& ddl_check_constraint, const Table* table,
      const ddl::CreateTable* ddl_create_table);
  absl::Status CreateRowDeletionPolicy(
      const ddl::RowDeletionPolicy& row_deletion_policy,
      Table::Builder* builder);
  absl::StatusOr<std::shared_ptr<const ProtoBundle>> CreateProtoBundle(
      const ddl::CreateProtoBundle& ddl_proto_bundle,
      absl::string_view proto_descriptor_bytes);
  absl::Status CreateTable(const ddl::CreateTable& ddl_table,
                           const database_api::DatabaseDialect& dialect);

  absl::StatusOr<const Column*> CreateIndexDataTableColumn(
      const Table* indexed_table, const std::string& source_column_name,
      const Table* index_data_table, bool is_null_filtered);

  absl::Status AddIndexColumnsByName(const std::string& column_name,
                                     const Table* indexed_table,
                                     bool is_null_filtered,
                                     std::vector<const Column*>& columns,
                                     Table::Builder& builder);

  absl::Status AddSearchIndexColumns(
      const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>& key_parts,
      const Table* indexed_table, bool is_null_filtered,
      std::vector<const Column*>& columns, Table::Builder& builder);

  absl::StatusOr<std::unique_ptr<const Table>> CreateIndexDataTable(
      absl::string_view index_name,
      const std::vector<ddl::KeyPartClause>& index_pk,
      const std::string* interleave_in_table,
      const ::google::protobuf::RepeatedPtrField<ddl::StoredColumnDefinition>&
          stored_columns,
      const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>* partition_by,
      const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>* order_by,
      const ::google::protobuf::RepeatedPtrField<std::string>* null_filtered_columns,
      const Index* index, const Table* indexed_table,
      ColumnsUsedByIndex* columns_used_by_index);

  absl::StatusOr<std::unique_ptr<const Table>> CreateChangeStreamDataTable(
      const ChangeStream* change_stream,
      const Table* change_stream_partition_table);
  absl::StatusOr<std::unique_ptr<const Table>> CreateChangeStreamPartitionTable(
      const ChangeStream* change_stream);
  absl::StatusOr<const Column*> CreateChangeStreamTableColumn(
      const std::string& column_name, const Table* change_stream_table,
      const zetasql::Type* type);
  absl::StatusOr<const KeyColumn*> CreateChangeStreamTablePKColumn(
      const std::string& pk_column_name, const Table* change_stream_table);
  absl::Status CreateChangeStreamTablePKConstraint(
      const std::string& pk_column_name, Table::Builder* builder);

  absl::StatusOr<const Index*> CreateIndex(
      const ddl::CreateIndex& ddl_index, const Table* indexed_table = nullptr);
  absl::StatusOr<const Index*> CreateVectorIndex(
      const ddl::CreateVectorIndex& ddl_index,
      const Table* indexed_table = nullptr);
  absl::StatusOr<const Index*> CreateSearchIndex(
      const ddl::CreateSearchIndex& ddl_index,
      const Table* indexed_table = nullptr);

  absl::StatusOr<const ChangeStream*> CreateChangeStream(
      const ddl::CreateChangeStream& ddl_change_stream,
      const database_api::DatabaseDialect& dialect);
  absl::StatusOr<const Index*> CreateIndexHelper(
      const std::string& index_name, const std::string& index_base_name,
      bool is_unique, bool is_null_filtered,
      const std::string* interleave_in_table,
      const std::vector<ddl::KeyPartClause>& table_pk,
      const ::google::protobuf::RepeatedPtrField<ddl::StoredColumnDefinition>&
          stored_columns,
      bool is_search_index, bool is_vector_index,
      const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>* partition_by,
      const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>* order_by,
      const ::google::protobuf::RepeatedPtrField<std::string>* null_filtered_columns,
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>* set_options,
      const Table* indexed_table);

  absl::flat_hash_set<const SchemaNode*>
  GatherTransitiveDependenciesForSchemaNode(
      const absl::flat_hash_set<const SchemaNode*>& initial_set);
  bool IsBuiltInFunction(const std::string& function_name);
  bool CanFunctionReplaceTakenName(const ddl::CreateFunction& ddl_function);
  std::string GetFunctionKindAsString(const ddl::CreateFunction& ddl_function);
  absl::Status AnalyzeFunctionDefinition(
      const ddl::CreateFunction& ddl_function, bool replace,
      absl::flat_hash_set<const SchemaNode*>* dependencies,
      std::unique_ptr<zetasql::FunctionSignature>* function_signature,
      Udf::Determinism* determinism_level);
  absl::Status AnalyzeFunctionDefinition(
      const ddl::CreateFunction& ddl_function, bool replace,
      std::vector<View::Column>* output_columns,
      absl::flat_hash_set<const SchemaNode*>* dependencies);
  absl::StatusOr<Udf::Builder> CreateFunctionBuilder(
      const ddl::CreateFunction& ddl_function,
      std::unique_ptr<zetasql::FunctionSignature> function_signature,
      Udf::Determinism determinism_level,
      absl::flat_hash_set<const SchemaNode*> dependencies);
  absl::StatusOr<View::Builder> CreateFunctionBuilder(
      const ddl::CreateFunction& ddl_function,
      std::vector<View::Column> output_columns,
      absl::flat_hash_set<const SchemaNode*> dependencies);
  absl::Status CreateFunction(const ddl::CreateFunction& ddl_function);

  template <typename SequenceModifier>
  absl::Status SetSequenceOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      SequenceModifier* modifier);
  absl::Status ValidateSequenceOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      const Sequence* current_sequence);
  void SetOptionsForSequenceClauses(
      const ddl::CreateSequence& create_sequence,
      ::google::protobuf::RepeatedPtrField<ddl::SetOption>* set_options);
  bool IsDefaultSequenceKindSet();

  absl::StatusOr<const Sequence*> CreateSequence(
      const ddl::CreateSequence& create_sequence,
      const database_api::DatabaseDialect& dialect,
      bool is_internal_use = false);

  absl::Status CreateLocalityGroup(
      const ddl::CreateLocalityGroup& create_locality_group);
  absl::Status CreateNamedSchema(const ddl::CreateSchema& create_schema);

  absl::Status AlterRowDeletionPolicy(
      std::optional<ddl::RowDeletionPolicy> row_deletion_policy,
      const Table* table);
  absl::Status ValidateAlterDatabaseOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options);
  absl::Status AlterDatabase(const ddl::AlterDatabase& alter_database,
                             const database_api::DatabaseDialect& dialect);
  absl::Status AlterTable(const ddl::AlterTable& alter_table,
                          const database_api::DatabaseDialect& dialect);
  absl::Status AlterChangeStream(
      const ddl::AlterChangeStream& alter_change_stream);
  absl::Status AlterChangeStreamForClause(
      const ddl::ChangeStreamForClause& ddl_change_stream_for_clause,
      const ChangeStream* change_stream);
  absl::Status AlterIndex(const ddl::AlterIndex& alter_index);
  absl::Status AlterVectorIndex(const ddl::AlterVectorIndex& alter_index);
  absl::Status AlterSetInterleave(
      const ddl::AlterTable::SetInterleaveClause& set_interleave_clause,
      const Table* table);
  absl::Status AlterInterleaveAction(
      const ddl::InterleaveClause::Action& ddl_interleave_action,
      const Table* table);
  absl::Status AlterSequence(const ddl::AlterSequence& alter_sequence,
                             const Sequence* current_sequence);
  absl::Status AlterNamedSchema(const ddl::AlterSchema& alter_schema);

  absl::Status AlterLocalityGroup(
      const ddl::AlterLocalityGroup& alter_locality_group);
  absl::StatusOr<std::shared_ptr<const ProtoBundle>> AlterProtoBundle(
      const ddl::AlterProtoBundle& ddl_alter_proto_bundle,
      absl::string_view proto_descriptor_bytes);
  absl::Status AlterProtoColumnTypes(
      const ProtoBundle* proto_bundle,
      const ddl::AlterProtoBundle& ddl_alter_proto_bundle);
  absl::Status AlterProtoColumnType(const Column* column,
                                    const ProtoBundle* proto_bundle,
                                    Column::Editor* editor);
  absl::StatusOr<const zetasql::Type*> GetProtoTypeFromBundle(
      const zetasql::Type* type, const ProtoBundle* proto_bundle);
  absl::Status AddCheckConstraint(
      const ddl::CheckConstraint& ddl_check_constraint, const Table* table);
  absl::Status AddForeignKey(const ddl::ForeignKey& ddl_foreign_key,
                             const Table* table);
  absl::Status DropConstraint(const std::string& constraint_name,
                              const Table* table);
  absl::Status RenameTo(const ddl::AlterTable::RenameTo& rename_to,
                        const Table* table);
  absl::Status RenameTable(const ddl::RenameTable& rename_table);
  absl::Status AddSynonym(const std::string& synonym, const Table* table);
  absl::Status DropSynonym(const ddl::AlterTable::DropSynonym& drop_synonym,
                           const Table* table);

  absl::Status DropTable(const ddl::DropTable& drop_table);

  absl::Status DropIndex(const ddl::DropIndex& drop_index);

  absl::Status DropSearchIndex(const ddl::DropSearchIndex& drop_search_index);

  absl::Status DropVectorIndex(const ddl::DropVectorIndex& drop_vector_index);

  absl::Status DropChangeStream(
      const ddl::DropChangeStream& drop_change_stream);

  absl::Status DropSequence(const Sequence* drop_sequence);

  absl::Status DropNamedSchema(const ddl::DropSchema& drop_schema);

  absl::Status DropLocalityGroup(
      const ddl::DropLocalityGroup& drop_locality_group);

  template <typename Modifier>
  absl::Status AssignNewLocalityGroup(const std::string& locality_group_name,
                                      Modifier* modifier);

  template <typename LocalityGroupModifier>
  absl::Status SetLocalityGroupOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      LocalityGroupModifier* modifier);
  absl::Status ApplyImplSetColumnOptions(
      const ddl::SetColumnOptions& set_column_options,
      const database_api::DatabaseDialect& dialect);

  absl::StatusOr<std::shared_ptr<const ProtoBundle>> DropProtoBundle();

  absl::Status DropFunction(const ddl::DropFunction& drop_function);

  absl::StatusOr<Model::ModelColumn> CreateModelColumn(
      const ddl::ColumnDefinition& ddl_column, const Model* model,
      const ddl::CreateModel* ddl_model,
      const database_api::DatabaseDialect& dialect);
  absl::Status CreateModel(const ddl::CreateModel& ddl_model,
                           const database_api::DatabaseDialect& dialect);
  absl::Status AlterModel(const ddl::AlterModel& alter_model);
  absl::Status DropModel(const ddl::DropModel& drop_model);
  template <typename ModelModifier>
  absl::Status SetModelOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      ModelModifier* modifier);

  std::string GetTimeZone() const;

  absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
  AnalyzeCreatePropertyGraph(
      const ddl::CreatePropertyGraph& ddl_create_property_graph,
      const zetasql::AnalyzerOptions& analyzer_options, Catalog* catalog);
  absl::Status CreatePropertyGraph(
      const ddl::CreatePropertyGraph& ddl_create_property_graph,
      const database_api::DatabaseDialect& dialect);
  absl::Status DropPropertyGraph(
      const ddl::DropPropertyGraph& ddl_drop_property_graph);
  absl::Status PopulatePropertyGraph(
      const ddl::CreatePropertyGraph& ddl_create_property_graph,
      const zetasql::ResolvedCreatePropertyGraphStmt* graph_stmt,
      PropertyGraph::Builder* property_graph_builder);
  absl::Status AddGraphElementTable(
      const zetasql::ResolvedGraphElementTable* element, bool is_node,
      PropertyGraph::Builder* graph_builder);

  // Adds a new schema object `node` to the schema copy being edited by
  // `editor_`.
  absl::Status AddNode(std::unique_ptr<const SchemaNode> node);

  // Drops the schema object `node` in `latest_schema_` from the schema copy
  // being edited by `editor_`.
  absl::Status DropNode(const SchemaNode* node);

  // Modifies the schema object `node` in `LatestSchema` in the schema
  // copy being edited by `editor_`.
  template <typename T>
  absl::Status AlterNode(const T* node,
                         const SchemaGraphEditor::EditCallback<T>& alter_cb) {
    ZETASQL_RET_CHECK_NE(node, nullptr);
    ZETASQL_RETURN_IF_ERROR(editor_->EditNode<T>(node, alter_cb));
    return absl::OkStatus();
  }

  absl::Status AlterInNamedSchema(
      const std::string& object_name,
      const SchemaGraphEditor::EditCallback<NamedSchema>& alter_cb);

  // Type factory for the database. Not owned.
  zetasql::TypeFactory* const type_factory_;

  // Unique table ID generator for the database. Not owned.
  TableIDGenerator* const table_id_generator_;

  // Unique column ID generator for the database. Not owned.
  ColumnIDGenerator* const column_id_generator_;

  // Database's storage. For doing data-dependent validations and index
  // backfills.
  Storage* storage_;

  // The timestamp at which the schema changes should be applied/validated
  // against the database's contents.
  const absl::Time schema_change_timestamp_;

  // The latest schema snapshot corresponding to the statements preceding the
  // statement currently being applied. Note that that does not guarantee that
  // any verfication/backfill effects of those statements have been applied.
  // Not owned.
  const Schema* latest_schema_;

  // The intermediate schema snapshots representing the schema state after
  // applying each statement.
  std::vector<std::unique_ptr<const Schema>> intermediate_schemas_;

  // Validation context for the statement being currently processed.
  // This is also being used in SchemaGraphEditor. Please make sure this is only
  // passed by reference.
  SchemaValidationContext* statement_context_;

  // Editor used to modify the schema graph.
  std::unique_ptr<SchemaGraphEditor> editor_;

  // Manages global schema names to prevent and generate unique names.
  GlobalSchemaNames global_names_;

  // Assigns OIDs to database objects when dialect is POSTGRESQL. The assigner
  // is owned by the database and is shared across all schema changes.
  PgOidAssigner* pg_oid_assigner_;

  // Holds the database id for this schema updater.
  std::string database_id_;
};

absl::Status SchemaUpdaterImpl::Init() {
  for (const SchemaNode* node :
       latest_schema_->GetSchemaGraph()->GetSchemaNodes()) {
    if (auto name = node->GetSchemaNameInfo(); name && name.value().global) {
      ZETASQL_RETURN_IF_ERROR(
          global_names_.AddName(name.value().kind, name.value().name));
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AddNode(
    std::unique_ptr<const SchemaNode> node) {
  ZETASQL_RETURN_IF_ERROR(editor_->AddNode(std::move(node)));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::DropNode(const SchemaNode* node) {
  ZETASQL_RET_CHECK_NE(node, nullptr);
  ZETASQL_RETURN_IF_ERROR(editor_->DeleteNode(node));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterInNamedSchema(
    const std::string& object_name,
    const SchemaGraphEditor::EditCallback<NamedSchema>& alter_cb) {
  const absl::string_view schema_name =
      SDLObjectName::GetSchemaName(object_name);
  const NamedSchema* named_schema =
      latest_schema_->FindNamedSchema(std::string(schema_name));
  if (named_schema == nullptr) {
    return error::NamedSchemaNotFound(schema_name);
  }
  ZETASQL_RETURN_IF_ERROR(AlterNode<NamedSchema>(named_schema, alter_cb));

  return absl::OkStatus();
}

absl::Status ValidateDdlStatement(const ddl::DDLStatement& ddl,
                                  database_api::DatabaseDialect dialect) {
  if ((ddl.has_create_function() || ddl.has_drop_function()) &&
      !EmulatorFeatureFlags::instance().flags().enable_views) {
    return error::ViewsNotSupported(ddl.has_create_function() ? "CREATE"
                                                              : "DROP");
  }

  if ((ddl.has_create_sequence() || ddl.has_alter_sequence() ||
       ddl.has_drop_sequence()) &&
      dialect == database_api::DatabaseDialect::POSTGRESQL &&
      !EmulatorFeatureFlags::instance()
           .flags()
           .enable_bit_reversed_positive_sequences_postgresql) {
    return error::SequenceNotSupportedInPostgreSQL();
  }

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const Schema>>
SchemaUpdaterImpl::ApplyDDLStatement(
    absl::string_view statement, absl::string_view proto_descriptor_bytes,
    const database_api::DatabaseDialect& dialect) {
  if (statement.empty()) {
    return error::EmptyDDLStatement();
  }

  ZETASQL_RET_CHECK(!editor_->HasModifications());
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ddl::DDLStatement> ddl_statement,
                   ParseDDLByDialect(statement, dialect));
  ZETASQL_RETURN_IF_ERROR(ValidateDdlStatement(*ddl_statement, dialect));
  // Apply the statement to the schema graph.
  auto proto_bundle = latest_schema_->proto_bundle();
  switch (ddl_statement->statement_case()) {
    case ddl::DDLStatement::kCreateProtoBundle: {
      ZETASQL_ASSIGN_OR_RETURN(proto_bundle,
                       CreateProtoBundle(ddl_statement->create_proto_bundle(),
                                         proto_descriptor_bytes));
      break;
    }
    case ddl::DDLStatement::kCreateTable: {
      ZETASQL_RETURN_IF_ERROR(CreateTable(ddl_statement->create_table(), dialect));
      break;
    }
    case ddl::DDLStatement::kCreateChangeStream: {
      ZETASQL_RETURN_IF_ERROR(
          CreateChangeStream(ddl_statement->create_change_stream(), dialect)
              .status());
      break;
    }
    case ddl::DDLStatement::kCreateIndex: {
      if (global_names_.HasName(ddl_statement->create_index().index_name()) &&
          ddl_statement->create_index().existence_modifier() ==
              ddl::IF_NOT_EXISTS) {
        break;
      }
      ZETASQL_RETURN_IF_ERROR(CreateIndex(ddl_statement->create_index()).status());
      break;
    }
    case ddl::DDLStatement::kCreateFunction: {
      if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
        ddl::CreateFunction* create_function =
            ddl_statement->mutable_create_function();
        ZETASQL_RET_CHECK(create_function->has_sql_body_origin() &&
                  create_function->sql_body_origin().has_original_expression());
        ZETASQL_ASSIGN_OR_RETURN(
            ExpressionTranslateResult result,
            TranslatePostgreSqlQueryInView(absl::StripAsciiWhitespace(
                create_function->sql_body_origin().original_expression())));
        // Overwrite the original_expression to the deparsed (formalized) PG
        // expression which can be different from the user-input PG expression.
        create_function->mutable_sql_body_origin()->set_original_expression(
            result.original_postgresql_expression);
        create_function->set_sql_body(result.translated_googlesql_expression);
      }
      ZETASQL_RETURN_IF_ERROR(CreateFunction(ddl_statement->create_function()));
      break;
    }
    case ddl::DDLStatement::kCreateSequence: {
      const ddl::CreateSequence& create_sequence =
          ddl_statement->create_sequence();
      if (latest_schema_->FindSequence(create_sequence.sequence_name()) !=
              nullptr &&
          create_sequence.existence_modifier() == ddl::IF_NOT_EXISTS) {
        // A sequence with the same name already exists, and we have the
        // IF NOT EXISTS clause in the statement, so return it as a no-op which
        // would not cause any schema change.
        return nullptr;
      }
      ZETASQL_RETURN_IF_ERROR(
          CreateSequence(ddl_statement->create_sequence(), dialect).status());
      break;
    }
    case ddl::DDLStatement::kCreateSchema: {
      ZETASQL_RETURN_IF_ERROR(CreateNamedSchema(ddl_statement->create_schema()));
      break;
    }
    case ddl::DDLStatement::kCreateVectorIndex: {
      if (global_names_.HasName(
              ddl_statement->create_vector_index().index_name()) &&
          ddl_statement->create_vector_index().existence_modifier() ==
              ddl::IF_NOT_EXISTS) {
        break;
      }
      ZETASQL_RETURN_IF_ERROR(
          CreateVectorIndex(ddl_statement->create_vector_index()).status());
      break;
    }
    case ddl::DDLStatement::kCreateSearchIndex: {
      ZETASQL_RETURN_IF_ERROR(
          CreateSearchIndex(ddl_statement->create_search_index()).status());
      break;
    }
    case ddl::DDLStatement::kCreateLocalityGroup: {
      ZETASQL_RETURN_IF_ERROR(
          CreateLocalityGroup(ddl_statement->create_locality_group()));
      break;
    }
    case ddl::DDLStatement::kAlterDatabase: {
      ZETASQL_RETURN_IF_ERROR(AlterDatabase(ddl_statement->alter_database(), dialect));
      break;
    }
    case ddl::DDLStatement::kAlterTable: {
      ZETASQL_RETURN_IF_ERROR(AlterTable(ddl_statement->alter_table(), dialect));
      break;
    }
    case ddl::DDLStatement::kAlterChangeStream: {
      ZETASQL_RETURN_IF_ERROR(AlterChangeStream(ddl_statement->alter_change_stream()));
      break;
    }
    case ddl::DDLStatement::kAlterSequence: {
      const ddl::AlterSequence& alter_sequence =
          ddl_statement->alter_sequence();
      const Sequence* current_sequence =
          latest_schema_->FindSequence(alter_sequence.sequence_name(),
                                       /*exclude_internal=*/true);
      if (current_sequence == nullptr) {
        if (alter_sequence.existence_modifier() == ddl::IF_EXISTS) {
          // The sequence doesn't exist, but we have the IF EXISTS clause
          // in the statement, so return without error.
          break;
        }
        return error::SequenceNotFound(alter_sequence.sequence_name());
      }
      ZETASQL_RETURN_IF_ERROR(AlterSequence(alter_sequence, current_sequence));
      break;
    }
    case ddl::DDLStatement::kAlterIndex: {
      ZETASQL_RETURN_IF_ERROR(AlterIndex(ddl_statement->alter_index()));
      break;
    }
    case ddl::DDLStatement::kAlterVectorIndex: {
      ZETASQL_RETURN_IF_ERROR(AlterVectorIndex(ddl_statement->alter_vector_index()));
      break;
    }
    case ddl::DDLStatement::kAlterSchema: {
      ZETASQL_RETURN_IF_ERROR(AlterNamedSchema(ddl_statement->alter_schema()));
      break;
    }
    case ddl::DDLStatement::kAlterLocalityGroup: {
      ZETASQL_RETURN_IF_ERROR(
          AlterLocalityGroup(ddl_statement->alter_locality_group()));
      break;
    }
    case ddl::DDLStatement::kAlterProtoBundle: {
      ZETASQL_ASSIGN_OR_RETURN(proto_bundle,
                       AlterProtoBundle(ddl_statement->alter_proto_bundle(),
                                        proto_descriptor_bytes));
      break;
    }
    case ddl::DDLStatement::kDropTable: {
      ZETASQL_RETURN_IF_ERROR(DropTable(ddl_statement->drop_table()));
      break;
    }
    case ddl::DDLStatement::kDropIndex: {
      ZETASQL_RETURN_IF_ERROR(DropIndex(ddl_statement->drop_index()));
      break;
    }
    case ddl::DDLStatement::kDropSearchIndex: {
      ZETASQL_RETURN_IF_ERROR(DropSearchIndex(ddl_statement->drop_search_index()));
      break;
    }
    case ddl::DDLStatement::kDropVectorIndex: {
      ZETASQL_RETURN_IF_ERROR(DropVectorIndex(ddl_statement->drop_vector_index()));
      break;
    }
    case ddl::DDLStatement::kDropChangeStream: {
      ZETASQL_RETURN_IF_ERROR(DropChangeStream(ddl_statement->drop_change_stream()));
      break;
    }
    case ddl::DDLStatement::kDropFunction: {
      ZETASQL_RETURN_IF_ERROR(DropFunction(ddl_statement->drop_function()));
      break;
    }
    case ddl::DDLStatement::kDropSequence: {
      const ddl::DropSequence& drop_sequence = ddl_statement->drop_sequence();
      const Sequence* current_sequence =
          latest_schema_->FindSequence(drop_sequence.sequence_name(),
                                       /*exclude_internal=*/true);
      if (current_sequence == nullptr) {
        if (drop_sequence.existence_modifier() == ddl::IF_EXISTS) {
          // The sequence doesn't exist, but we have the IF EXISTS clause
          // in the statement, so return without error.
          break;
        }
        return error::SequenceNotFound(drop_sequence.sequence_name());
      }
      ZETASQL_RETURN_IF_ERROR(DropSequence(current_sequence));
      break;
    }
    case ddl::DDLStatement::kDropSchema: {
      ZETASQL_RETURN_IF_ERROR(DropNamedSchema(ddl_statement->drop_schema()));
      break;
    }
    case ddl::DDLStatement::kDropLocalityGroup: {
      ZETASQL_RETURN_IF_ERROR(DropLocalityGroup(ddl_statement->drop_locality_group()));
      break;
    }
    case ddl::DDLStatement::kDropProtoBundle: {
      ZETASQL_ASSIGN_OR_RETURN(proto_bundle, DropProtoBundle());
      break;
    }
    case ddl::DDLStatement::kAnalyze:
      // Intentionally no=op.
      break;
    case ddl::DDLStatement::kSetColumnOptions:
      ZETASQL_RETURN_IF_ERROR(ApplyImplSetColumnOptions(
          ddl_statement->set_column_options(), dialect));
      break;
    case ddl::DDLStatement::kCreateModel:
      ZETASQL_RETURN_IF_ERROR(CreateModel(ddl_statement->create_model(), dialect));
      break;
    case ddl::DDLStatement::kAlterModel:
      ZETASQL_RETURN_IF_ERROR(AlterModel(ddl_statement->alter_model()));
      break;
    case ddl::DDLStatement::kDropModel:
      ZETASQL_RETURN_IF_ERROR(DropModel(ddl_statement->drop_model()));
      break;
    case ddl::DDLStatement::kCreatePropertyGraph:
      ZETASQL_RETURN_IF_ERROR(
          CreatePropertyGraph(ddl_statement->create_property_graph(), dialect));
      break;
    case ddl::DDLStatement::kDropPropertyGraph:
      ZETASQL_RETURN_IF_ERROR(DropPropertyGraph(ddl_statement->drop_property_graph()));
      break;
    case ddl::DDLStatement::kCreatePlacement: {
      const Placement* placement = latest_schema_->FindPlacement(
          ddl_statement->create_placement().placement_name());
      if (placement != nullptr) {
        return error::SchemaObjectAlreadyExists(
            "Placement", ddl_statement->create_placement().placement_name());
      }
      Placement::Builder placement_builder;
      placement_builder.set_name(
          ddl_statement->create_placement().placement_name());

      const auto& set_options = ddl_statement->create_placement().set_options();
      if (!set_options.empty()) {
        ZETASQL_RETURN_IF_ERROR(AlterNode<Placement>(
            placement_builder.get(),
            [this, set_options](Placement::Editor* editor) -> absl::Status {
              // Set placement options
              return SetPlacementOptions(set_options, editor);
            }));
      }
      ZETASQL_RETURN_IF_ERROR(AddNode(placement_builder.build()));
      break;
    }
    case ddl::DDLStatement::kAlterPlacement: {
      const Placement* placement = latest_schema_->FindPlacement(
          ddl_statement->alter_placement().placement_name());
      if (placement == nullptr) {
        return error::PlacementNotFound(
            ddl_statement->alter_placement().placement_name());
      }
      const auto& set_options = ddl_statement->alter_placement().set_options();
      ddl_statement->create_placement().set_options();
      if (!set_options.empty()) {
        ZETASQL_RETURN_IF_ERROR(AlterNode<Placement>(
            placement,
            [this, set_options](Placement::Editor* editor) -> absl::Status {
              // Set change stream options
              return SetPlacementOptions(set_options, editor);
            }));
      }
      break;
    }
    case ddl::DDLStatement::kDropPlacement: {
      const Placement* placement = latest_schema_->FindPlacement(
          ddl_statement->drop_placement().placement_name());
      if (placement == nullptr) {
        return error::PlacementNotFound(
            ddl_statement->drop_placement().placement_name());
      }
      ZETASQL_RETURN_IF_ERROR(DropNode(placement));
      break;
    }
    case ddl::DDLStatement::kRenameTable: {
      if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
        return error::RenameTableNotSupportedInPostgreSQL();
      }
      ZETASQL_RETURN_IF_ERROR(RenameTable(ddl_statement->rename_table()));
      break;
    }
    default:
      ZETASQL_RET_CHECK(false) << "Unsupported ddl statement: "
                       << ddl_statement->statement_case();
  }
  // Since Proto bundle needs to be used for column validation (via
  // SchemaGraphEditor),this needs to be passed in statement context.
  // SchemaValidationContext is being used and updated in both editor and
  // updater and hence needs to be passed by reference.
  // Use this proto_bundle only during validation (before schema
  // generation). If there is a need to access proto_bundle after
  // validation, please use schema->proto_bundle().
  statement_context_->set_proto_bundle(proto_bundle);
  ZETASQL_ASSIGN_OR_RETURN(auto new_schema_graph, editor_->CanonicalizeGraph());
  return std::make_unique<const OwningSchema>(
      std::move(new_schema_graph), proto_bundle, dialect, database_id_);
}

absl::StatusOr<std::vector<SchemaValidationContext>>
SchemaUpdaterImpl::ApplyDDLStatements(
    const SchemaChangeOperation& schema_change_operation) {
  std::vector<SchemaValidationContext> pending_work;

  for (const auto& statement : schema_change_operation.statements) {
    ZETASQL_VLOG(2) << "Applying statement " << statement;

    // Set up the SchemaValidationContext before passing it to `editor_`. This
    // includes setting the old schema snapshot and a callback to construct
    // a temporary schema snapshot of the pending new schema. The temporary
    // schema snapshot does not own the new schema nodes but will remain alive
    // for the lifetime of `editor_` and `statement_context_`. The callback
    // mechanism is needed because 1) SchemaUpdater doesn't know at which point
    // SchemaGraphEditor will validate the new schema and 2) SchemaGraphEditor
    // or SchemaValidationContext cannot take a dependency on Schema.
    std::unique_ptr<const Schema> new_tmp_schema = nullptr;
    SchemaValidationContext statement_context{
        storage_, &global_names_, type_factory_, schema_change_timestamp_,
        schema_change_operation.database_dialect};
    statement_context_ = &statement_context;
    statement_context_->SetOldSchemaSnapshot(latest_schema_);
    statement_context_->SetTempNewSchemaSnapshotConstructor(
        [this,
         &new_tmp_schema](const SchemaGraph* unowned_graph) -> const Schema* {
          new_tmp_schema = std::make_unique<const Schema>(
              unowned_graph, latest_schema_->proto_bundle(),
              latest_schema_->dialect(), database_id_);
          return new_tmp_schema.get();
        });

    // Initialize the editor that will be used to stage the schema changes.
    editor_ = std::make_unique<SchemaGraphEditor>(
        latest_schema_->GetSchemaGraph(), statement_context_);

    // If there is a semantic validation error, then we return right away.
    ZETASQL_ASSIGN_OR_RETURN(
        auto new_schema,
        ApplyDDLStatement(statement,
                          schema_change_operation.proto_descriptor_bytes,
                          schema_change_operation.database_dialect));

    // This indicates that the statement was a no-op, e.g., a CREATE SEQUENCE IF
    // NOT EXISTS statement for an existent sequence.
    if (new_schema == nullptr) {
      continue;
    }

    // We save every schema snapshot as verifiers/backfillers from the
    // current/next statement may need to refer to the previous/current
    // schema snapshots.
    statement_context_->SetValidatedNewSchemaSnapshot(new_schema.get());
    latest_schema_ = new_schema.get();
    intermediate_schemas_.emplace_back(std::move(new_schema));
    pg_oid_assigner_->MarkNextPostgresqlOidForIntermediateSchema();

    // If everything was OK, make this the new schema snapshot for processing
    // the next statement and save the pending schema snapshot and backfill
    // work.
    pending_work.emplace_back(std::move(statement_context));
  }

  return pending_work;
}

template <typename Modifier>
absl::Status SchemaUpdaterImpl::ProcessLocalityGroupOption(
    const ddl::SetOption& option, Modifier* modifier) {
  if (option.has_string_value()) {
    ZETASQL_RETURN_IF_ERROR(AssignNewLocalityGroup(option.string_value(), modifier));
  } else {
    ZETASQL_RET_CHECK(false) << "Option " << ddl::kLocalityGroupOptionName
                     << " can only take string_value.";
  }
  return absl::OkStatus();
}

template <typename TableModifier>
absl::Status SchemaUpdaterImpl::SetTableOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    TableModifier* modifier) {
  for (const ddl::SetOption& option : set_options) {
    if (option.option_name() == ddl::kLocalityGroupOptionName) {
      ZETASQL_RETURN_IF_ERROR(ProcessLocalityGroupOption(option, modifier));
    } else {
      ZETASQL_RET_CHECK(false) << "Invalid table option: " << option.option_name();
    }
  }
  return absl::OkStatus();
}

template <typename IndexModifier>
absl::Status SchemaUpdaterImpl::SetIndexOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    IndexModifier* modifier, bool allow_other_options) {
  for (const ddl::SetOption& option : set_options) {
    if (option.option_name() == ddl::kLocalityGroupOptionName) {
      ZETASQL_RETURN_IF_ERROR(ProcessLocalityGroupOption(option, modifier));
    } else {
      ZETASQL_RET_CHECK(allow_other_options)
          << "Invalid index option: " << option.option_name();
    }
  }
  return absl::OkStatus();
}

template <typename ColumnModifier>
absl::Status SchemaUpdaterImpl::SetColumnOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    const database_api::DatabaseDialect& dialect, ColumnModifier* modifier) {
  std::optional<bool> allows_commit_timestamp = std::nullopt;
  std::string commit_timestamp_option_name = ddl::kCommitTimestampOptionName;
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    // PG uses spanner.commit_timestamp while in ZetaSQL, the default option
    // name is allow_commit_timestamp.
    commit_timestamp_option_name = ddl::kPGCommitTimestampOptionName;
  }
  for (const ddl::SetOption& option : set_options) {
    if (option.option_name() == commit_timestamp_option_name) {
      if (option.has_bool_value()) {
        allows_commit_timestamp = option.bool_value();
      } else if (option.has_null_value()) {
        allows_commit_timestamp = std::nullopt;
      } else {
        ZETASQL_RET_CHECK(false) << "Option " << commit_timestamp_option_name
                         << " can only take bool_value or null_value.";
      }
      modifier->set_allow_commit_timestamp(allows_commit_timestamp);
    } else if (option.option_name() == ddl::kLocalityGroupOptionName) {
      ZETASQL_RETURN_IF_ERROR(ProcessLocalityGroupOption(option, modifier));
    } else {
      ZETASQL_RET_CHECK(false) << "Invalid column option: " << option.option_name();
    }
  }
  return absl::OkStatus();
}

template <typename Modifier>
absl::Status SchemaUpdaterImpl::SetDatabaseOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    const database_api::DatabaseDialect& dialect, Modifier* modifier) {
  modifier->set_options(set_options);
  for (const ddl::SetOption& option : set_options) {
    if (absl::StripPrefix(option.option_name(), "spanner.internal.cloud_") ==
        ddl::kDefaultSequenceKindOptionName) {
      if (option.has_string_value()) {
        std::optional<std::string> default_sequence_kind =
            option.string_value();
        modifier->set_default_sequence_kind(default_sequence_kind);
      } else if (option.has_null_value()) {
        modifier->set_default_sequence_kind(std::nullopt);
      }
    }
    if (absl::StripPrefix(option.option_name(), "spanner.internal.cloud_") ==
        ddl::kDefaultTimeZoneOptionName) {
      if (option.has_string_value()) {
        std::optional<std::string> default_time_zone = option.string_value();
        modifier->set_default_time_zone(default_time_zone);
      } else if (option.has_null_value()) {
        modifier->set_default_time_zone(std::nullopt);
      }
    }
  }
  return absl::OkStatus();
}

template <typename ChangeStreamModifier>
absl::Status SchemaUpdaterImpl::SetChangeStreamOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    ChangeStreamModifier* modifier) {
  modifier->set_options(set_options);
  for (const ddl::SetOption& option : set_options) {
    if (option.has_null_value()) {
      continue;
    }
    if (option.option_name() == ddl::kChangeStreamRetentionPeriodOptionName) {
      std::optional<std::string> retention_period = option.string_value();
      modifier->set_retention_period(retention_period);
      modifier->set_parsed_retention_period(
          ParseSchemaTimeSpec(*retention_period));
    } else if (option.option_name() ==
               ddl::kChangeStreamValueCaptureTypeOptionName) {
      std::optional<std::string> value_capture_type = option.string_value();
      modifier->set_value_capture_type(value_capture_type);
    } else if (ddl::kChangeStreamBooleanOptions->contains(
                   option.option_name())) {
      modifier->set_boolean_option(option.option_name(), option.bool_value());
    }
  }
  return absl::OkStatus();
}

template <typename PlacementModifier>
absl::Status SchemaUpdaterImpl::SetPlacementOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    PlacementModifier* modifier) {
  modifier->set_options(set_options);
  for (const ddl::SetOption& option : set_options) {
    if (option.has_null_value()) {
      continue;
    }
    if (option.option_name() == ddl::kPlacementDefaultLeaderOptionName) {
      std::optional<std::string> default_leader = option.string_value();
      modifier->set_default_leader(default_leader);
    } else if (option.option_name() ==
               ddl::kPlacementInstancePartitionOptionName) {
      std::optional<std::string> instance_partition = option.string_value();
      modifier->set_instance_partition(instance_partition);
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::ValidateChangeStreamOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options) {
  // All options reach this step are already checked by ddl parser to ensure
  // they have valid option names and correct value types. Thus we can directly
  // validate the values.
  for (const ddl::SetOption& option : set_options) {
    // Retention Period Validation
    if (option.option_name() == ddl::kChangeStreamRetentionPeriodOptionName) {
      if (option.has_string_value()) {
        const int64_t retention_seconds =
            ParseSchemaTimeSpec(option.string_value());
        if (retention_seconds == -1) {
          return error::InvalidTimeDurationFormat(option.string_value());
        }
        if (!absl::GetFlag(
                FLAGS_cloud_spanner_emulator_disable_cs_retention_check) &&
            (retention_seconds < limits::kChangeStreamsMinRetention ||
             retention_seconds > limits::kChangeStreamsMaxRetention)) {
          return error::InvalidDataRetentionPeriod(option.string_value());
        }
      }
      continue;
    }
    // Value Capture Type Validation
    if (option.option_name() == ddl::kChangeStreamValueCaptureTypeOptionName) {
      if (option.has_string_value()) {
        const std::string& value_capture_type = option.string_value();
        const absl::flat_hash_set<absl::string_view> validCaptureTypes = {
            kChangeStreamValueCaptureTypeDefault,
            kChangeStreamValueCaptureTypeNewRow,
            kChangeStreamValueCaptureTypeNewValues,
            kChangeStreamValueCaptureTypeNewRowOldValues};

        if (validCaptureTypes.contains(value_capture_type)) {
          continue;
        } else {
          return error::InvalidValueCaptureType(value_capture_type);
        }
      }
    }
  }
  return absl::OkStatus();
}

template <typename Modifier>
absl::Status SchemaUpdaterImpl::AssignNewLocalityGroup(
    const std::string& locality_group_name, Modifier* modifier) {
  const LocalityGroup* locality_group =
      latest_schema_->FindLocalityGroup(locality_group_name);
  if (locality_group == nullptr) {
    return error::LocalityGroupNotFound(locality_group_name);
  }
  const LocalityGroup* old_locality_group = modifier->get()->locality_group();
  if (old_locality_group != nullptr) {
    ZETASQL_RETURN_IF_ERROR(AlterNode<LocalityGroup>(
        old_locality_group, [](LocalityGroup::Editor* editor) -> absl::Status {
          editor->decrement_use_count();
          return absl::OkStatus();
        }));
  }
  modifier->set_locality_group(locality_group);
  ZETASQL_RETURN_IF_ERROR(AlterNode<LocalityGroup>(
      locality_group, [](LocalityGroup::Editor* editor) -> absl::Status {
        editor->increment_use_count();
        return absl::OkStatus();
      }));

  return absl::OkStatus();
}

template <typename LocalityGroupModifier>
absl::Status SchemaUpdaterImpl::SetLocalityGroupOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    LocalityGroupModifier* modifier) {
  modifier->set_options(set_options);
  for (const ddl::SetOption& option : set_options) {
    if (option.has_null_value()) {
      continue;
    }
    if (option.option_name() == ddl::kInternalLocalityGroupStorageOptionName) {
      if (option.has_bool_value()) {
        modifier->set_inflash(option.bool_value());
      }
    } else if (option.option_name() ==
               ddl::kInternalLocalityGroupSpillTimeSpanOptionName) {
      modifier->set_ssd_to_hdd_spill_timespans(option.string_list_value());
    }
  }
  return absl::OkStatus();
}

// Construct the change stream tvf name for googlesql
std::string SchemaUpdaterImpl::MakeChangeStreamTvfName(
    std::string change_stream_name,
    const database_api::DatabaseDialect& dialect) {
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    return absl::StrCat(kChangeStreamTvfJsonPrefix, change_stream_name);
  }
  return absl::StrCat(kChangeStreamTvfStructPrefix, change_stream_name);
}

absl::Status SchemaUpdaterImpl::AlterColumnDefinition(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    const database_api::DatabaseDialect& dialect, Column::Editor* editor) {
  ZETASQL_RETURN_IF_ERROR(SetColumnDefinition(ddl_column, table,
                                      /*ddl_create_table=*/nullptr, dialect,
                                      /*is_alter=*/true, editor));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterColumnSetDropDefault(
    const ddl::AlterTable::AlterColumn& alter_column, const Table* table,
    const Column* column, const database_api::DatabaseDialect& dialect,
    Column::Editor* editor) {
  const ddl::AlterTable::AlterColumn::AlterColumnOp type =
      alter_column.operation();
  ZETASQL_RET_CHECK(type == ddl::AlterTable::AlterColumn::SET_DEFAULT ||
            type == ddl::AlterTable::AlterColumn::DROP_DEFAULT);

  const ddl::ColumnDefinition& new_column_def = alter_column.column();
  absl::flat_hash_set<const SchemaNode*> dependent_sequences;
  if (type == ddl::AlterTable::AlterColumn::SET_DEFAULT) {
    if (column->is_generated()) {
      return error::CannotSetDefaultValueOnGeneratedColumn(column->FullName());
    }

    if (column->is_identity_column() &&
        (new_column_def.has_column_default() ||
         new_column_def.has_generated_column())) {
      return error::CannotAlterIdentityColumnToGeneratedOrDefaultColumn(
          table->Name(), new_column_def.column_name());
    }

    std::string expression = new_column_def.column_default().expression();
    if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
      ZETASQL_ASSIGN_OR_RETURN(expression, TranslatePGExpression(
                                       new_column_def.column_default(), table,
                                       /*ddl_create_table=*/nullptr, *editor));
    }
    ZETASQL_RET_CHECK(new_column_def.has_column_default() && !expression.empty());
    absl::flat_hash_set<const SchemaNode*> udf_dependencies;
    absl::Status s = AnalyzeColumnDefaultValue(
        expression, column->Name(), column->GetType(), table,
        /*ddl_create_table=*/nullptr, &dependent_sequences, &udf_dependencies);
    if (!s.ok()) {
      return error::ColumnDefaultValueParseError(table->Name(), column->Name(),
                                                 s.message());
    }
    editor->set_postgresql_oid(pg_oid_assigner_->GetNextPostgresqlOid());
    editor->set_expression(expression);
    editor->set_has_default_value(true);
    editor->set_udf_dependencies(udf_dependencies);
    const Column* existing_column =
        table->FindColumn(alter_column.column().column_name());
    if (existing_column == nullptr) {
      return error::ColumnNotFound(table->Name(),
                                   alter_column.column().column_name());
    }
    absl::flat_hash_set<const SchemaNode*> deps;
    for (const auto& dep : existing_column->udf_dependencies()) {
      deps.insert(dep);
    }

    // Check for a recursive columns by analyzing the transitive set of
    // dependencies, i.e., if the view is a dependency of itself.
    auto transitive_deps = GatherTransitiveDependenciesForSchemaNode(deps);
    if (std::find_if(transitive_deps.begin(), transitive_deps.end(),
                     [existing_column](const SchemaNode* dep) {
                       return (dep->As<const Column>() != nullptr &&
                               dep->As<const Column>()->Name() ==
                                   existing_column->Name());
                     }) != transitive_deps.end()) {
      return error::ViewReplaceRecursive(existing_column->Name());
    }
  } else {
    if (!column->has_default_value() || column->is_identity_column()) {
      return absl::OkStatus();
    }
    editor->clear_expression();
    editor->clear_original_expression();
    editor->set_has_default_value(false);
    if (!column->is_generated()) {
      // Only default and generated columns need an OID. If the default is
      // dropped and the column is not generated then unassign the OID.
      editor->set_postgresql_oid(std::nullopt);
    }
  }
  // Clear all the old sequence dependencies and set the new ones
  editor->set_sequences_used(dependent_sequences);
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::InitColumnNameAndTypesFromTable(
    const Table* table, const ddl::CreateTable* ddl_create_table,
    std::vector<zetasql::SimpleTable::NameAndType>* name_and_types) {
  if (ddl_create_table != nullptr) {
    // We are processing a CREATE TABLE statement, so 'const Table* table' may
    // not have all the columns yet. Add all columns from the ddl.
    for (const ddl::ColumnDefinition& ddl_column : ddl_create_table->column()) {
      ZETASQL_ASSIGN_OR_RETURN(
          const zetasql::Type* type,
          DDLColumnTypeToGoogleSqlType(ddl_column, type_factory_,
                                       latest_schema_->proto_bundle().get()));
      name_and_types->emplace_back(ddl_column.column_name(), type);
    }
  } else {
    for (const Column* column : table->columns()) {
      name_and_types->emplace_back(column->Name(), column->GetType());
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AnalyzeGeneratedColumn(
    absl::string_view expression, const std::string& column_name,
    const zetasql::Type* column_type, const Table* table,
    const ddl::CreateTable* ddl_create_table,
    absl::flat_hash_set<std::string>* dependent_column_names,
    absl::flat_hash_set<const SchemaNode*>* udf_dependencies) {
  std::vector<zetasql::SimpleTable::NameAndType> name_and_types;
  ZETASQL_RETURN_IF_ERROR(InitColumnNameAndTypesFromTable(table, ddl_create_table,
                                                  &name_and_types));

  // For the case of adding a generated column in ALTER TABLE.
  if (ddl_create_table == nullptr &&
      table->FindColumn(column_name) == nullptr) {
    name_and_types.emplace_back(column_name, column_type);
  }

  return AnalyzeColumnExpression(
      expression, column_type, table, latest_schema_, type_factory_,
      name_and_types, "stored generated columns", dependent_column_names,
      /*dependent_sequences=*/nullptr,
      /*allow_volatile_expression=*/false, udf_dependencies);
}

absl::Status SchemaUpdaterImpl::AnalyzeColumnDefaultValue(
    absl::string_view expression, const std::string& column_name,
    const zetasql::Type* column_type, const Table* table,
    const ddl::CreateTable* ddl_create_table,
    absl::flat_hash_set<const SchemaNode*>* dependent_sequences,
    absl::flat_hash_set<const SchemaNode*>* udf_dependencies) {
  std::vector<zetasql::SimpleTable::NameAndType> name_and_types;
  ZETASQL_RETURN_IF_ERROR(InitColumnNameAndTypesFromTable(table, ddl_create_table,
                                                  &name_and_types));

  // For the case of adding a default column in ALTER TABLE.
  if (ddl_create_table == nullptr &&
      table->FindColumn(column_name) == nullptr) {
    name_and_types.emplace_back(column_name, column_type);
  }
  absl::flat_hash_set<std::string> dependent_column_names;
  ZETASQL_RETURN_IF_ERROR(AnalyzeColumnExpression(
      expression, column_type, table, latest_schema_, type_factory_,
      name_and_types, "column default", &dependent_column_names,
      dependent_sequences, /*allow_volatile_expression=*/true,
      udf_dependencies));
  if (!dependent_column_names.empty()) {
    return error::DefaultExpressionWithColumnDependency(column_name);
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AnalyzeCheckConstraint(
    absl::string_view expression, const Table* table,
    const ddl::CreateTable* ddl_create_table,
    absl::flat_hash_set<std::string>* dependent_column_names,
    CheckConstraint::Builder* builder,
    absl::flat_hash_set<const SchemaNode*>* udf_dependencies) {
  std::vector<zetasql::SimpleTable::NameAndType> name_and_types;
  ZETASQL_RETURN_IF_ERROR(InitColumnNameAndTypesFromTable(table, ddl_create_table,
                                                  &name_and_types));

  ZETASQL_RETURN_IF_ERROR(AnalyzeColumnExpression(
      expression, zetasql::types::BoolType(), table, latest_schema_,
      type_factory_, name_and_types, "check constraints",
      dependent_column_names,
      /*dependent_sequences=*/nullptr,
      /*allow_volatile_expression=*/false, udf_dependencies));

  for (const std::string& column_name : *dependent_column_names) {
    builder->add_dependent_column(table->FindColumn(column_name));
  }
  for (const SchemaNode* udf : *udf_dependencies) {
    builder->add_dependent_udf(udf);
  }

  return absl::OkStatus();
}

template <typename ColumnDef, typename ColumnDefModifer>
absl::StatusOr<std::string> SchemaUpdaterImpl::TranslatePGExpression(
    const ColumnDef& ddl_column, const Table* table,
    const ddl::CreateTable* ddl_create_table, ColumnDefModifer& modifier) {
  if (ddl_column.has_expression_origin()) {
    const std::string original_unformalized_expression =
        std::string(absl::StripAsciiWhitespace(
            ddl_column.expression_origin().original_expression()));
    ZETASQL_ASSIGN_OR_RETURN(
        ExpressionTranslateResult result,
        TranslatePostgreSqlExpression(table, ddl_create_table,
                                      original_unformalized_expression));
    // Overwrite the original_expression to the deparsed (formalized) PG
    // expression which can be different from the user-input PG expression.
    modifier.set_original_expression(result.original_postgresql_expression);
    return result.translated_googlesql_expression;

  } else {
    return error::Internal(absl::StrCat(
        "The field 'expression_origin' is empty: ", ddl_column.DebugString()));
  }
}

template <typename ColumnDefModifer>
absl::Status SchemaUpdaterImpl::SetColumnDefinition(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    const ddl::CreateTable* ddl_create_table,
    const database_api::DatabaseDialect& dialect, bool is_alter,
    ColumnDefModifer* modifier) {
  bool is_generated = false;
  bool has_default_value = false;
  bool is_identity_column = false;
  // Process any changes in column definition.
  ZETASQL_ASSIGN_OR_RETURN(
      const zetasql::Type* column_type,
      DDLColumnTypeToGoogleSqlType(ddl_column, type_factory_,
                                   latest_schema_->proto_bundle().get()));
  modifier->set_type(column_type);

  if (column_type->IsTokenList() && !ddl_column.hidden()) {
    return error::NonHiddenTokenlistColumn(table->Name(),
                                           ddl_column.column_name());
  }

  // For the case of removing a vector length param in ALTER TABLE ALTER COLUMN.
  if (ddl_create_table == nullptr) {
    const Column* column = table->FindColumn(ddl_column.column_name());
    if (column != nullptr && column->has_vector_length() &&
        !ddl_column.has_vector_length()) {
      return error::CannotAlterColumnToRemoveVectorLength(
          ddl_column.column_name());
    }
  }

  // Do not allow a column to convert to and stop being an identity column.
  const Column* old_column = table->FindColumn(ddl_column.column_name());
  if (old_column != nullptr &&
      old_column->is_identity_column() != ddl_column.has_identity_column()) {
    if (ddl_column.has_identity_column()) {
      return error::CannotAlterToIdentityColumn(table->Name(),
                                                ddl_column.column_name());
    } else {
      return error::CannotAlterColumnToDropIdentity(table->Name(),
                                                    ddl_column.column_name());
    }
  }

  absl::flat_hash_set<const SchemaNode*> udf_dependencies;
  absl::flat_hash_set<const SchemaNode*> dependent_sequences;
  if (ddl_column.has_column_default()) {
    std::string expression = ddl_column.column_default().expression();
    if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
      ZETASQL_ASSIGN_OR_RETURN(expression,
                       TranslatePGExpression(ddl_column.column_default(), table,
                                             ddl_create_table, *modifier));
    }
    has_default_value = true;
    modifier->set_expression(expression);

    absl::Status s = AnalyzeColumnDefaultValue(
        expression, ddl_column.column_name(), column_type, table,
        ddl_create_table, &dependent_sequences, &udf_dependencies);

    if (!s.ok()) {
      return error::ColumnDefaultValueParseError(
          table->Name(), ddl_column.column_name(), s.message());
    }
  } else if (ddl_column.has_identity_column()) {
    is_identity_column = true;
    // The default value is the expression
    // `GET_NEXT_SEQUENCE_VALUE(sequence_name)`
    has_default_value = true;
    std::vector<std::string> parts = absl::StrSplit(modifier->get()->id(), ':');
    ZETASQL_RET_CHECK_GE(parts.size(), 2);

    std::string sequence_name = absl::StrFormat(
        "_identity_seq_%s", absl::StrReplaceAll(parts[0], {{".", "__"}}));
    const Sequence* existing_sequence =
        latest_schema_->FindSequence(sequence_name);
    if (is_alter) {
      ZETASQL_RET_CHECK(existing_sequence != nullptr)
          << "sequence does not exist: " << sequence_name;
      ddl::AlterSequence alter_sequence;
      alter_sequence.set_sequence_name(sequence_name);
      SetSequenceOptionsForIdentityColumn(
          ddl_column.identity_column(),
          alter_sequence.mutable_set_options()->mutable_options());
      ZETASQL_RETURN_IF_ERROR(AlterSequence(alter_sequence, existing_sequence));
      dependent_sequences.insert(existing_sequence);
    } else {
      ZETASQL_RET_CHECK(existing_sequence == nullptr)
          << "sequence already exists: " << sequence_name;
      // Create the internal sequence.
      ddl::CreateSequence create_sequence;
      create_sequence.set_sequence_name(sequence_name);
      if (ddl_column.identity_column().has_type() &&
          ddl_column.identity_column().type() ==
              ddl::ColumnDefinition::IdentityColumnDefinition::
                  BIT_REVERSED_POSITIVE) {
        ddl::SetOption* sequence_kind = create_sequence.add_set_options();
        sequence_kind->set_option_name("sequence_kind");
        sequence_kind->set_string_value("bit_reversed_positive");
      } else if (!IsDefaultSequenceKindSet()) {
        return error::UnspecifiedIdentityColumnSequenceKind(
            ddl_column.column_name());
      }
      SetSequenceOptionsForIdentityColumn(
          ddl_column.identity_column(), create_sequence.mutable_set_options());
      ZETASQL_ASSIGN_OR_RETURN(
          const Sequence* sequence,
          CreateSequence(create_sequence, dialect, /*is_internal_use=*/true));

      std::string expression;
      if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
        expression =
            absl::StrFormat("(GET_NEXT_SEQUENCE_VALUE(\"%s\"))", sequence_name);
      } else {
        expression = absl::StrFormat("(GET_NEXT_SEQUENCE_VALUE(SEQUENCE %s))",
                                     sequence_name);
      }
      modifier->set_expression(expression);
      dependent_sequences.insert(sequence);
    }
  } else if (ddl_column.has_generated_column()) {
    std::string expression = ddl_column.generated_column().expression();

    if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
      ZETASQL_ASSIGN_OR_RETURN(expression, TranslatePGExpression(
                                       ddl_column.generated_column(), table,
                                       ddl_create_table, *modifier));
    }
    is_generated = true;
    modifier->set_expression(expression);
    absl::flat_hash_set<std::string> dependent_column_names;
    absl::Status s = AnalyzeGeneratedColumn(
        expression, ddl_column.column_name(), column_type, table,
        ddl_create_table, &dependent_column_names, &udf_dependencies);
    if (!s.ok()) {
      return error::GeneratedColumnDefinitionParseError(
          table->Name(), ddl_column.column_name(), s.message());
    }
    // Create a helper map to check if a column is generated.
    absl::flat_hash_set<std::string> generated_column_set;
    if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
      if (ddl_create_table != nullptr) {
        for (const ddl::ColumnDefinition& column_def :
             ddl_create_table->column()) {
          if (column_def.has_generated_column()) {
            generated_column_set.insert(column_def.column_name());
          }
        }
      } else {
        // This is for altering a table definition.
        for (const Column* column : table->columns()) {
          if (column->is_generated()) {
            generated_column_set.insert(column->Name());
          }
        }
      }
    }
    for (const std::string& column_name : dependent_column_names) {
      if (dialect == database_api::DatabaseDialect::POSTGRESQL &&
          generated_column_set.contains(column_name)) {
        // Check generated column does not reference to generated for
        // PostgreSQL schema.
        return error::DdlInvalidArgumentError(
            absl::Substitute("A generated column \"$0\" cannot reference "
                             "another generated column \"$1\".",
                             ddl_column.column_name(), column_name));
      }
      modifier->add_dependent_column_name(column_name);
    }
    modifier->set_stored(ddl_column.generated_column().stored());
  }

  if (!is_generated && !has_default_value) {
    // Altering a generated column to a non-generated column is disallowed. In
    // that case, the expression is cleared here and later validation at
    // column_validator.cc will block it.
    modifier->clear_expression();
  } else {
    ZETASQL_RET_CHECK(is_generated != has_default_value);
  }

  modifier->set_is_identity_column(is_identity_column);
  modifier->set_has_default_value(has_default_value);
  // Set the default values for nullability and length.
  modifier->set_nullable(!ddl_column.not_null());
  modifier->set_declared_max_length(std::nullopt);
  if (ddl_column.has_length()) {
    modifier->set_declared_max_length(ddl_column.length());
  } else if (ddl_column.type() == ddl::ColumnDefinition::ARRAY &&
             ddl_column.has_array_subtype() &&
             ddl_column.array_subtype().has_length()) {
    modifier->set_declared_max_length(ddl_column.array_subtype().length());
  }
  modifier->set_sequences_used(dependent_sequences);
  modifier->set_udf_dependencies(udf_dependencies);

  if (ddl_column.has_vector_length()) {
    // For the case of adding `vector_length` param in CREATE TABLE and ALTER
    // TABLE ADD COLUMN.
    if (ddl_create_table != nullptr ||
        (ddl_create_table == nullptr &&
         table->FindColumn(ddl_column.column_name()) == nullptr)) {
      modifier->set_vector_length(ddl_column.vector_length());
    } else {
      // For the case of adding or editing `vector_length` param in ALTER TABLE
      // ALTER COLUMN.
      return error::CannotAlterColumnToAddVectorLength(
          ddl_column.column_name());
    }
  }
  if (!ddl_column.set_options().empty()) {
    ZETASQL_RETURN_IF_ERROR(
        SetColumnOptions(ddl_column.set_options(), dialect, modifier));
  }

  if (is_alter) {
    const Column* existing_column = table->FindColumn(ddl_column.column_name());
    if (existing_column == nullptr) {
      return error::ColumnNotFound(table->Name(), ddl_column.column_name());
    }
    absl::flat_hash_set<const SchemaNode*> deps;
    for (const auto& dep : existing_column->udf_dependencies()) {
      deps.insert(dep);
    }
    // Check for a recursive columns by analyzing the transitive set of
    // dependencies, i.e., if the view is a dependency of itself.
    auto transitive_deps = GatherTransitiveDependenciesForSchemaNode(deps);
    if (std::find_if(transitive_deps.begin(), transitive_deps.end(),
                     [existing_column](const SchemaNode* dep) {
                       return (dep->As<const Column>() != nullptr &&
                               dep->As<const Column>()->Name() ==
                                   existing_column->Name());
                     }) != transitive_deps.end()) {
      return error::ViewReplaceRecursive(existing_column->Name());
    }
  }

  return absl::OkStatus();
}

absl::StatusOr<const Column*> SchemaUpdaterImpl::CreateColumn(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    const ddl::CreateTable* ddl_table,
    const database_api::DatabaseDialect& dialect) {
  const std::string& column_name = ddl_column.column_name();
  Column::Builder builder;
  builder
      .set_id(column_id_generator_->NextId(
          absl::StrCat(table->Name(), ".", column_name)))
      .set_name(column_name);
  ZETASQL_RETURN_IF_ERROR(SetColumnDefinition(ddl_column, table, ddl_table, dialect,
                                      /*is_alter=*/false, &builder));
  builder.set_hidden(ddl_column.has_hidden() && ddl_column.hidden());
  builder.set_is_placement_key(ddl_column.has_placement_key() &&
                               ddl_column.placement_key());
  const Column* column = builder.get();
  builder.set_table(table);
  if (column->is_generated() || column->has_default_value()) {
    statement_context_->AddAction(
        [column](const SchemaValidationContext* context) {
          return BackfillGeneratedColumnValue(column, context);
        });
    std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
    builder.set_postgresql_oid(oid);
    if (oid.has_value()) {
      ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " to column "
              << table->Name() << "." << column->Name();
    }
  }

  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return column;
}

// Check the number of change streams tracking an object and return error if the
// number exceed the limit.
absl::Status SchemaUpdaterImpl::ValidateChangeStreamLimits(
    const ddl::ChangeStreamForClause& change_stream_for_clause,
    absl::string_view change_stream_name) {
  if (change_stream_for_clause.all()) {
    int all_count = 1;
    for (const ChangeStream* change_stream : latest_schema_->change_streams()) {
      if (change_stream->Name() != change_stream_name &&
          (change_stream->for_clause() != nullptr &&
           change_stream->for_clause()->all())) {
        ++all_count;
        // Number of change streams tracking ALL should not exceed the limit.
        if (all_count > limits::kMaxChangeStreamsTrackingATableOrColumn) {
          return error::TooManyChangeStreamsTrackingSameObject(
              change_stream_name,
              limits::kMaxChangeStreamsTrackingATableOrColumn, "ALL");
        }
      }
    }
  }

  // Checks the number of change streams tracking the same table.
  auto validate_table = [&](const Table* table) {
    absl::flat_hash_set<std::string> all_change_streams;
    all_change_streams.reserve(table->change_streams().size());
    for (auto& change_stream : table->change_streams()) {
      all_change_streams.insert(change_stream->Name());
    }

    int change_stream_count = table->change_streams().size();
    if (!all_change_streams.contains(change_stream_name)) {
      ++change_stream_count;
    }
    if (change_stream_count > limits::kMaxChangeStreamsTrackingATableOrColumn) {
      return error::TooManyChangeStreamsTrackingSameObject(
          change_stream_name, limits::kMaxChangeStreamsTrackingATableOrColumn,
          table->Name());
    }
    return absl::OkStatus();
  };

  // Checks the number of change streams tracking the same column.
  auto validate_column = [&](const Column* column) {
    absl::flat_hash_set<std::string> all_change_streams;
    all_change_streams.reserve(column->change_streams().size());
    for (auto& change_stream : column->change_streams()) {
      all_change_streams.insert(change_stream->Name());
    }
    // No need to validate this limit for primary key columns because change
    // streams tracking pk columns only don't count towards the number limit (3)
    // of change streams per column.
    int change_stream_count = column->change_streams().size();
    if (!all_change_streams.contains(change_stream_name)) {
      ++change_stream_count;
    }
    if (change_stream_count > limits::kMaxChangeStreamsTrackingATableOrColumn) {
      return error::TooManyChangeStreamsTrackingSameObject(
          change_stream_name, limits::kMaxChangeStreamsTrackingATableOrColumn,
          column->Name());
    }
    return absl::OkStatus();
  };

  ZETASQL_RETURN_IF_ERROR(ValidateLimitsForTrackedObjects(
      change_stream_for_clause, validate_table, validate_column));

  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::RegisterTrackedObjects(
    const ChangeStream* change_stream,
    const ddl::ChangeStreamForClause& change_stream_for_clause,
    absl::FunctionRef<absl::Status(Table::Editor*)> table_cb,
    absl::FunctionRef<absl::Status(Column::Editor*)> column_cb) {
  if (change_stream_for_clause.all()) {
    for (const auto& table : latest_schema_->tables()) {
      if (!table->is_trackable_by_change_stream()) {
        continue;
      }
      ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(table, table_cb));
      for (const auto& column : table->columns()) {
        // Skip all key columns when registering/unregistering change stream
        // tracking columns to prevent modifications on key columns with child
        // tables. This is safe because primary key columns are guaranteed to be
        // populated in data change records no matter user specified or not.
        if (column->is_trackable_by_change_stream() &&
            !table->FindKeyColumn(column->Name())) {
          ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(column, column_cb));
        }
      }
    }
    return absl::OkStatus();
  }

  for (auto& entry : change_stream_for_clause.tracked_tables().table_entry()) {
    const Table* table = latest_schema_->FindTable(entry.table_name());
    ZETASQL_RET_CHECK(table != nullptr);
    if (!table->is_trackable_by_change_stream()) {
      return error::TrackUntrackableTables(entry.table_name());
    }
    ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
        table, [change_stream](Table::Editor* editor) -> absl::Status {
          editor->add_change_stream_explicitly_tracking_table(change_stream);
          return absl::OkStatus();
        }));
    if (entry.has_all_columns()) {
      ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(table, table_cb));
      for (auto& column : table->columns()) {
        if (column->is_trackable_by_change_stream() &&
            !table->FindKeyColumn(column->Name())) {
          ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(column, column_cb));
        }
      }
    } else if (entry.has_tracked_columns()) {
      for (const std::string& column_name :
           entry.tracked_columns().column_name()) {
        const Column* column = table->FindColumn(column_name);
        if (!column->is_trackable_by_change_stream()) {
          return error::TrackUntrackableColumns(column_name);
        }
        ZETASQL_RET_CHECK(column != nullptr);
        ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(column, column_cb));
        ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(
            column, [change_stream](Column::Editor* editor) -> absl::Status {
              editor->add_change_stream_explicitly_tracking_column(
                  change_stream);
              return absl::OkStatus();
            }));
      }
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::UnregisterChangeStreamFromTrackedObjects(
    const ChangeStream* change_stream,
    absl::FunctionRef<absl::Status(Table::Editor*)> table_cb,
    absl::FunctionRef<absl::Status(Column::Editor*)> column_cb) {
  for (auto& pair : change_stream->tracked_tables_columns()) {
    std::string table_name = pair.first;
    std::vector<std::string> column_name_list = pair.second;
    const Table* table = latest_schema_->FindTable(table_name);
    if (table->FindChangeStream(change_stream->Name())) {
      ZETASQL_RETURN_IF_ERROR(AlterNode(table, table_cb));
    }
    for (std::string& column_name : column_name_list) {
      const Column* column = table->FindColumn(column_name);
      if (!table->FindKeyColumn(column->Name())) {
        ZETASQL_RETURN_IF_ERROR(AlterNode(column, column_cb));
      }
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::RegisterTrackedObjects(
    const ddl::ChangeStreamForClause& change_stream_for_clause,
    const ChangeStream* change_stream) {
  // Register tracked objects.
  auto register_tracked_table = [change_stream](Table::Editor* table_editor) {
    table_editor->add_change_stream(change_stream);
    return absl::OkStatus();
  };
  auto register_tracked_column =
      [change_stream](Column::Editor* column_editor) {
        column_editor->add_change_stream(change_stream);
        return absl::OkStatus();
      };
  return RegisterTrackedObjects(change_stream, change_stream_for_clause,
                                register_tracked_table,
                                register_tracked_column);
}

absl::Status SchemaUpdaterImpl::UnregisterChangeStreamFromTrackedObjects(
    const ChangeStream* change_stream) {
  auto unregister_tracked_table = [change_stream](Table::Editor* table_editor) {
    table_editor->remove_change_stream(change_stream);
    return absl::OkStatus();
  };
  auto unregister_tracked_column =
      [change_stream](Column::Editor* column_editor) {
        column_editor->remove_change_stream(change_stream);
        return absl::OkStatus();
      };
  ZETASQL_RETURN_IF_ERROR(UnregisterChangeStreamFromTrackedObjects(
      change_stream, unregister_tracked_table, unregister_tracked_column));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::ValidateLimitsForTrackedObjects(
    const ddl::ChangeStreamForClause& change_stream_for_clause,
    absl::FunctionRef<absl::Status(const Table*)> table_cb,
    absl::FunctionRef<absl::Status(const Column*)> column_cb) {
  std::vector<std::string> tables_names_;
  if (change_stream_for_clause.all()) {
    for (const auto& table : latest_schema_->tables()) {
      ZETASQL_RETURN_IF_ERROR(table_cb(table));
      for (const auto& column : table->columns()) {
        if (column->is_trackable_by_change_stream() &&
            !table->FindKeyColumn(column->Name())) {
          ZETASQL_RETURN_IF_ERROR(column_cb(column));
        }
      }
    }
    return absl::OkStatus();
  }
  for (auto& entry : change_stream_for_clause.tracked_tables().table_entry()) {
    const Table* table = latest_schema_->FindTable(entry.table_name());
    ZETASQL_RET_CHECK(table != nullptr);

    if (entry.has_all_columns()) {
      ZETASQL_RETURN_IF_ERROR(table_cb(table));
      for (const auto& column : table->columns()) {
        if (column->is_trackable_by_change_stream() &&
            !table->FindKeyColumn(column->Name())) {
          ZETASQL_RETURN_IF_ERROR(column_cb(column));
        }
      }
    } else if (!entry.tracked_columns().column_name().empty()) {
      for (const std::string& column_name :
           entry.tracked_columns().column_name()) {
        const Column* column = table->FindColumn(column_name);
        ZETASQL_RET_CHECK(column != nullptr);
        ZETASQL_RETURN_IF_ERROR(column_cb(column));
      }
    } else {
      // Only key columns are tracked implicitly, which doesn't count toward
      // change streams number limit.
      continue;
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::ValidateChangeStreamForClause(
    const ddl::ChangeStreamForClause& change_stream_for_clause,
    absl::string_view change_stream_name) {
  if (change_stream_for_clause.has_all()) {
    return absl::OkStatus();
  }
  if (!change_stream_for_clause.has_tracked_tables()) {
    return error::CreateChangeStreamForClauseInvalidOneof(change_stream_name);
  }
  const ddl::ChangeStreamForClause::TrackedTables& tracked_tables =
      change_stream_for_clause.tracked_tables();
  if (tracked_tables.table_entry_size() == 0) {
    return error::CreateChangeStreamForClauseZeroEntriesInTrackedTables(
        change_stream_name);
  }

  absl::flat_hash_set<absl::string_view> tracked_tables_set;
  for (const ddl::ChangeStreamForClause::TrackedTables::Entry& entry :
       tracked_tables.table_entry()) {
    if (!entry.has_table_name()) {
      return error::
          CreateChangeStreamForClauseTrackedTablesEntryMissingTableName(
              change_stream_name);
    }

    const std::string& table_name = entry.table_name();
    // Cannot list the same table more than once.
    if (!tracked_tables_set.insert(table_name).second) {
      return error::ChangeStreamDuplicateTable(change_stream_name, table_name);
    }

    // Tracking a change stream is not supported.
    if (latest_schema_->FindChangeStream(table_name) != nullptr) {
      return error::InvalidTrackedObjectInChangeStream(
          change_stream_name, "Change Stream", table_name);
    }

    // Tracking an index is not supported.
    if (latest_schema_->FindIndex(table_name) != nullptr) {
      return error::InvalidTrackedObjectInChangeStream(change_stream_name,
                                                       "Index", table_name);
    }

    // TODO: Return error if the change stream is tracking a
    // function after function is supported.
    // TODO: The parser would have returned a parser error if the
    // user specified a FOR clause with table names starting with SPANNER_SYS.*
    // or INFORMATION_SCHEMA.*.

    const Table* table = latest_schema_->FindTable(table_name);
    if (table == nullptr) {
      return error::UnsupportedTrackedObjectOrNonExistentTableInChangeStream(
          change_stream_name, table_name);
    }

    // Validate the tracked columns of the change stream
    if (!entry.has_all_columns()) {
      if (!entry.has_tracked_columns()) {
        return error::CreateChangeStreamForClauseTrackedTablesEntryInvalidOneof(
            change_stream_name);
      }

      absl::flat_hash_set<absl::string_view> tracked_columns_set;
      // Note: entry.tracked_columns() can contain zero columns, indicating only
      // the primary key columns of the table are tracked.
      for (const std::string& column_name :
           entry.tracked_columns().column_name()) {
        // Cannot list the same column more than once.
        if (!tracked_columns_set.insert(column_name).second) {
          return error::ChangeStreamDuplicateColumn(change_stream_name,
                                                    column_name, table_name);
        }
        const Column* column = table->FindColumn(column_name);
        // Check that the column exists in the table
        if (column == nullptr) {
          return error::NonexistentTrackedColumnInChangeStream(
              change_stream_name, column_name, table_name);
        }
        const KeyColumn* key_column = table->FindKeyColumn(column_name);
        // Primary key column should not be listed in the FOR clause.
        if (key_column != nullptr) {
          return error::KeyColumnInChangeStreamForClause(
              change_stream_name, column_name, table_name);
        }
        if (column->is_generated()) {
          // Tracking a non-key Stored Generated column is not supported
          return error::InvalidTrackedObjectInChangeStream(
              change_stream_name, "non-key generated column",
              absl::StrCat(table_name, ".", column_name));
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::CreateInterleaveConstraintForTable(
    const ddl::InterleaveClause& interleave, Table::Builder* builder) {
  ZETASQL_ASSIGN_OR_RETURN(const Table* parent, GetInterleaveConstraintTable(
                                            interleave.table_name(), *builder));
  std::optional<Table::OnDeleteAction> on_delete =
      GetInterleaveConstraintOnDelete(interleave);
  Table::InterleaveType interleave_type = GetInterleaveType(interleave);
  if (interleave_type == Table::InterleaveType::kIn &&
      !EmulatorFeatureFlags::instance().flags().enable_interleave_in) {
    return error::InterleaveInNotSupported();
  }
  return CreateInterleaveConstraint(parent, on_delete, interleave_type,
                                    builder);
}

absl::Status SchemaUpdaterImpl::CreateInterleaveConstraint(
    const Table* parent, std::optional<Table::OnDeleteAction> on_delete,
    Table::InterleaveType interleave_type, Table::Builder* builder) {
  ZETASQL_RET_CHECK_EQ(builder->get()->parent(), nullptr);

  if (parent->row_deletion_policy().has_value() &&
      (interleave_type == Table::InterleaveType::kInParent &&
       (!on_delete.has_value() ||
        on_delete != Table::OnDeleteAction::kCascade))) {
    return error::RowDeletionPolicyOnAncestors(builder->get()->Name(),
                                               parent->Name());
  }

  ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
      parent, [builder](Table::Editor* parent_editor) -> absl::Status {
        parent_editor->add_child_table(builder->get());
        builder->set_parent_table(parent_editor->get());
        return absl::OkStatus();
      }));

  if (on_delete.has_value()) {
    builder->set_on_delete(on_delete.value());
  }
  builder->set_interleave_type(interleave_type);
  return absl::OkStatus();
}

absl::StatusOr<const Table*> SchemaUpdaterImpl::GetInterleaveConstraintTable(
    const std::string& interleave_in_table_name,
    const Table::Builder& builder) const {
  const auto* parent =
      latest_schema_->FindTableCaseSensitive(interleave_in_table_name);
  if (parent == nullptr) {
    const Table* table = builder.get();
    if (table->owner_index() == nullptr) {
      return error::TableNotFound(interleave_in_table_name);
    } else {
      return error::IndexInterleaveTableNotFound(table->owner_index()->Name(),
                                                 interleave_in_table_name);
    }
  }
  return parent;
}

std::optional<Table::OnDeleteAction>
SchemaUpdaterImpl::GetInterleaveConstraintOnDelete(
    const ddl::InterleaveClause& interleave) {
  if (!interleave.has_on_delete()) {
    return std::nullopt;
  }
  return interleave.on_delete() == ddl::InterleaveClause::CASCADE
             ? Table::OnDeleteAction::kCascade
             : Table::OnDeleteAction::kNoAction;
}

Table::InterleaveType SchemaUpdaterImpl::GetInterleaveType(
    const ddl::InterleaveClause& interleave) {
  return interleave.type() == ddl::InterleaveClause::IN_PARENT
             ? Table::InterleaveType::kInParent
             : Table::InterleaveType::kIn;
}

absl::Status SchemaUpdaterImpl::CreatePrimaryKeyConstraint(
    const ddl::KeyPartClause& ddl_key_part, Table::Builder* builder,
    bool with_oid) {
  KeyColumn::Builder key_col_builder;
  if (with_oid) {
    // Assign OID for PRIMARY KEY constraint.
    std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
    key_col_builder.set_postgresql_oid(oid);
    if (oid.has_value()) {
      ZETASQL_VLOG(2) << "Assigned oid " << oid.value()
              << " for PRIMARY KEY constraint on table "
              << builder->get()->Name();
    }
  }
  ZETASQL_ASSIGN_OR_RETURN(
      const KeyColumn* key_col,
      CreatePrimaryKeyColumn(ddl_key_part, builder->get(), &key_col_builder));
  builder->add_key_column(key_col);
  return absl::OkStatus();
}

absl::StatusOr<const KeyColumn*> SchemaUpdaterImpl::CreatePrimaryKeyColumn(
    const ddl::KeyPartClause& ddl_key_part, const Table* table,
    KeyColumn::Builder* builder) {
  const std::string& key_column_name = ddl_key_part.key_name();

  // References to columns in primary key clause are case-sensitive.
  const Column* column = table->FindColumnCaseSensitive(key_column_name);
  if (column == nullptr) {
    return error::NonExistentKeyColumn(
        OwningObjectType(table), OwningObjectName(table), key_column_name);
  }
  builder->set_column(column);
  // TODO: Specifying NULLS FIRST/LAST is unsupported in the
  // emulator. Currently, users cannot specify ASC_NULLS_LAST and
  // DESC_NULLS_FIRST.
  if (ddl_key_part.order() == ddl::KeyPartClause::ASC_NULLS_LAST) {
    builder->set_descending(false).set_nulls_last(true);
  } else if (ddl_key_part.order() == ddl::KeyPartClause::DESC_NULLS_FIRST) {
    builder->set_descending(true).set_nulls_last(false);
  } else {
    bool is_descending = (ddl_key_part.order() == ddl::KeyPartClause::DESC);
    builder->set_descending(is_descending);
    // Ascending direction with NULLs sorted first and descending direction
    // with NULLs sorted last.
    builder->set_nulls_last(is_descending);
  }
  const KeyColumn* key_col = builder->get();
  ZETASQL_RETURN_IF_ERROR(AddNode(builder->build()));
  return key_col;
}

absl::Status SchemaUpdaterImpl::CreateForeignKeyConstraint(
    const ddl::ForeignKey& ddl_foreign_key, const Table* referencing_table) {
  // Build and add the emulator foreign key before creating any of its backing
  // indexes. This ensures the foreign key is validated first which in turn
  // ensures better foreign key error messages instead of obscure index errors.

  // Not enforced foreign keys doesn't perform referential integrity
  // checks, so the index on the referencing table is not created. Only the
  // unique index on the referenced table is created, because the definition of
  // foreign keys requires them to refer to unique identities.
  ZETASQL_ASSIGN_OR_RETURN(
      const ForeignKey* foreign_key,
      BuildForeignKeyConstraint(ddl_foreign_key, referencing_table));

  auto index_column_names =
      [](const google::protobuf::RepeatedPtrField<std::string>& column_names,
         const std::vector<int>& column_order) {
        std::vector<std::string> names;
        names.reserve(column_names.size());
        for (int i = 0; i < column_names.size(); ++i) {
          names.push_back(column_names[column_order[i]]);
        }
        return names;
      };

  bool referenced_index_required = false;
  std::vector<int> index_column_order;
  ZETASQL_RETURN_IF_ERROR(EvaluateForeignKeyReferencedPrimaryKey(
      foreign_key->referenced_table(), ddl_foreign_key.referenced_column_name(),
      &index_column_order, &referenced_index_required));
  if (referenced_index_required) {
    std::vector<std::string> column_names;
    ZETASQL_ASSIGN_OR_RETURN(
        const Index* referenced_index,
        CreateForeignKeyIndex(
            foreign_key, foreign_key->referenced_table(),
            index_column_names(ddl_foreign_key.referenced_column_name(),
                               index_column_order),
            /*unique=*/true));
    ZETASQL_RETURN_IF_ERROR(
        AlterNode<ForeignKey>(foreign_key, [&](ForeignKey::Editor* editor) {
          editor->set_referenced_index(referenced_index);
          return absl::OkStatus();
        }));
  }

  if (!foreign_key->enforced()) {
    // Skip referencing index creation and fk data validation for unenforced
    // foreign keys.
    return absl::OkStatus();
  }

  bool referencing_index_required = false;
  ZETASQL_RETURN_IF_ERROR(EvaluateForeignKeyReferencingPrimaryKey(
      referencing_table, ddl_foreign_key.constrained_column_name(),
      index_column_order, &referencing_index_required));
  if (referencing_index_required) {
    ZETASQL_ASSIGN_OR_RETURN(
        const Index* referencing_index,
        CreateForeignKeyIndex(
            foreign_key, referencing_table,
            index_column_names(ddl_foreign_key.constrained_column_name(),
                               index_column_order),
            /*unique=*/false));
    ZETASQL_RETURN_IF_ERROR(
        AlterNode<ForeignKey>(foreign_key, [&](ForeignKey::Editor* editor) {
          editor->set_referencing_index(referencing_index);
          return absl::OkStatus();
        }));
  }

  // Validate any existing data. Skip new tables which have no data yet.
  if (latest_schema_->FindTableCaseSensitive(referencing_table->Name()) !=
      nullptr) {
    statement_context_->AddAction(
        [foreign_key](const SchemaValidationContext* context) {
          return VerifyForeignKeyData(foreign_key, context);
        });
  }

  return absl::OkStatus();
}

ForeignKey::Action GetForeignKeyOnDeleteAction(const ddl::ForeignKey& fk) {
  return fk.on_delete() == ddl::ForeignKey::CASCADE
             ? ForeignKey::Action::kCascade
             : ForeignKey::Action::kNoAction;
}

absl::StatusOr<const ForeignKey*> SchemaUpdaterImpl::BuildForeignKeyConstraint(
    const ddl::ForeignKey& ddl_foreign_key, const Table* referencing_table) {
  ForeignKey::Builder builder;
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " for FOREIGN KEY constraint "
            << ddl_foreign_key.constraint_name() << " on table "
            << referencing_table->Name();
  }

  ZETASQL_RETURN_IF_ERROR(
      AlterNode<Table>(referencing_table, [&](Table::Editor* editor) {
        editor->add_foreign_key(builder.get());
        return absl::OkStatus();
      }));
  builder.set_referencing_table(referencing_table);

  const Table* referenced_table = latest_schema_->FindTableCaseSensitive(
      ddl_foreign_key.referenced_table_name());
  if (referenced_table == nullptr) {
    if (ddl_foreign_key.referenced_table_name() != referencing_table->Name()) {
      return error::TableNotFound(ddl_foreign_key.referenced_table_name());
    }
    // Self-referencing foreign key.
    referenced_table = referencing_table;
  }
  ZETASQL_RETURN_IF_ERROR(
      AlterNode<Table>(referenced_table, [&](Table::Editor* editor) {
        editor->add_referencing_foreign_key(builder.get());
        return absl::OkStatus();
      }));
  builder.set_referenced_table(referenced_table);

  std::string foreign_key_name;
  if (ddl_foreign_key.has_constraint_name()) {
    foreign_key_name = ddl_foreign_key.constraint_name();
    ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Foreign Key", foreign_key_name));
    builder.set_constraint_name(foreign_key_name);
  } else {
    ZETASQL_ASSIGN_OR_RETURN(foreign_key_name,
                     global_names_.GenerateForeignKeyName(
                         referencing_table->Name(), referenced_table->Name()));
    builder.set_generated_name(foreign_key_name);
  }

  auto add_columns =
      [&](const Table* table,
          const google::protobuf::RepeatedPtrField<std::string>& column_names,
          std::function<void(const Column*)> add_column) {
        for (const std::string& column_name : column_names) {
          const Column* column = table->FindColumnCaseSensitive(column_name);
          if (column == nullptr) {
            return error::ForeignKeyColumnNotFound(column_name, table->Name(),
                                                   foreign_key_name);
          }
          add_column(column);
        }
        return absl::OkStatus();
      };
  ZETASQL_RETURN_IF_ERROR(add_columns(referencing_table,
                              ddl_foreign_key.constrained_column_name(),
                              [&builder](const Column* column) {
                                builder.add_referencing_column(column);
                              }));
  ZETASQL_RETURN_IF_ERROR(add_columns(referenced_table,
                              ddl_foreign_key.referenced_column_name(),
                              [&builder](const Column* column) {
                                builder.add_referenced_column(column);
                              }));
  if (ddl_foreign_key.has_on_delete()) {
    if (ddl_foreign_key.on_delete() != ddl::ForeignKey::ACTION_UNSPECIFIED &&
        ddl_foreign_key.on_delete() != ddl::ForeignKey::NO_ACTION &&
        !EmulatorFeatureFlags::instance()
             .flags()
             .enable_fk_delete_cascade_action) {
      return error::ForeignKeyOnDeleteActionUnsupported(
          ForeignKey::ActionName(GetForeignKeyOnDeleteAction(ddl_foreign_key)));
    }
    builder.set_delete_action(GetForeignKeyOnDeleteAction(ddl_foreign_key));
  }

  if (!ddl_foreign_key.enforced()) {
    if (!EmulatorFeatureFlags::instance()
             .flags()
             .enable_fk_enforcement_option) {
      return error::ForeignKeyEnforcementUnsupported();
    }
    builder.set_enforced(ddl_foreign_key.enforced());
  }

  const ForeignKey* foreign_key = builder.get();
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return foreign_key;
}

absl::Status SchemaUpdaterImpl::EvaluateForeignKeyReferencedPrimaryKey(
    const Table* table,
    const google::protobuf::RepeatedPtrField<std::string>& column_names,
    std::vector<int>* column_order, bool* index_required) const {
  column_order->reserve(column_names.size());
  *index_required = true;

  // Build a map of foreign key column names to their index positions.
  CaseInsensitiveStringMap<int> column_positions;
  for (int i = 0; i < column_names.size(); ++i) {
    column_positions[column_names[i]] = i;
  }

  // Evaluate the primary key columns.
  auto primary_key = table->primary_key();
  for (const KeyColumn* key_column : primary_key) {
    auto it = column_positions.find(key_column->column()->Name());
    if (it == column_positions.end()) {
      break;  // Foreign key doesn't use this key column.
    }
    column_order->push_back(it->second);
    if (column_order->size() == column_names.size()) {
      *index_required = primary_key.size() != column_names.size();
      return absl::OkStatus();
    }
  }

  // Match the remaining referenced columns in table column order.
  absl::flat_hash_set<int> assigned(column_order->begin(), column_order->end());
  for (const Column* column : table->columns()) {
    auto it = column_positions.find(column->Name());
    if (it != column_positions.end() && assigned.insert(it->second).second) {
      column_order->push_back(it->second);
      if (column_order->size() == column_names.size()) {
        break;  // Done ordering foreign key columns.
      }
    }
  }

  // Use the primary key if not all columns match. The validator will generate
  // an appropriate error message.
  *index_required = column_order->size() == column_names.size();
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::EvaluateForeignKeyReferencingPrimaryKey(
    const Table* table,
    const google::protobuf::RepeatedPtrField<std::string>& column_names,
    const std::vector<int>& column_order, bool* index_required) const {
  *index_required = true;

  // Skip creating an index if there is an error the validator will flag later.
  if (column_order.size() != column_names.size()) {
    *index_required = false;
    return absl::OkStatus();
  }

  // Foreign key columns must be a prefix of the primary key.
  auto primary_key = table->primary_key();
  if (column_names.size() > primary_key.size()) {
    return absl::OkStatus();
  }

  // Nullable columns require a null-filtered index.
  if (absl::c_any_of(column_names, [table](const std::string& column_name) {
        return table->FindColumn(column_name)->is_nullable();
      })) {
    return absl::OkStatus();
  }

  // Build a map of foreign key column names to their index positions.
  ZETASQL_RET_CHECK_EQ(column_order.size(), column_names.size());
  CaseInsensitiveStringMap<int> column_positions;
  for (int i = 0; i < column_names.size(); ++i) {
    column_positions[column_names[i]] = column_order[i];
  }

  // Evaluate the primary key columns to see if we can use the primary key.
  int matching_key_columns = 0;
  for (const KeyColumn* key_column : primary_key) {
    auto it = column_positions.find(key_column->column()->Name());
    if (it == column_positions.end() || it->second != matching_key_columns) {
      break;
    }
    ++matching_key_columns;
  }
  *index_required = matching_key_columns != column_names.size();
  return absl::OkStatus();
}

absl::StatusOr<const Index*> SchemaUpdaterImpl::CreateForeignKeyIndex(
    const ForeignKey* foreign_key, const Table* table,
    const std::vector<std::string>& column_names, bool unique) {
  bool null_filtered =
      absl::c_any_of(column_names, [table](const std::string& column_name) {
        return table->FindColumn(column_name)->is_nullable();
      });
  ZETASQL_ASSIGN_OR_RETURN(std::string index_name,
                   global_names_.GenerateManagedIndexName(
                       table->Name(), column_names, null_filtered, unique));
  const Index* index = latest_schema_->FindIndex(index_name);
  if (index == nullptr) {
    index = statement_context_->FindAddedNode<Index>(index_name);
  }
  if (index == nullptr) {
    ddl::CreateIndex ddl_index;
    ddl_index.set_index_name(index_name);
    ddl_index.set_index_base_name(table->Name());
    ddl_index.set_unique(unique);
    ddl_index.set_null_filtered(null_filtered);
    for (const std::string& column_name : column_names) {
      auto* key_part = ddl_index.add_key();
      key_part->set_key_name(column_name);
      const auto* key_column = table->FindKeyColumn(column_name);
      if (key_column != nullptr) {
        if (key_column->is_descending() && key_column->is_nulls_last()) {
          key_part->set_order(ddl::KeyPartClause::DESC);
        } else if (key_column->is_descending() &&
                   !key_column->is_nulls_last()) {
          key_part->set_order(ddl::KeyPartClause::DESC_NULLS_FIRST);
        } else if (!key_column->is_descending() &&
                   key_column->is_nulls_last()) {
          key_part->set_order(ddl::KeyPartClause::ASC_NULLS_LAST);
        }
      }
    }
    if (CanInterleaveForeignKeyIndex(table, column_names)) {
      ddl_index.set_interleave_in_table(table->Name());
    }
    ZETASQL_ASSIGN_OR_RETURN(index, CreateIndex(ddl_index, table));
  }
  ZETASQL_RETURN_IF_ERROR(AlterNode<Index>(index, [&](Index::Editor* index_editor) {
    index_editor->add_managing_node(foreign_key);
    return absl::OkStatus();
  }));
  return index;
}

bool SchemaUpdaterImpl::CanInterleaveForeignKeyIndex(
    const Table* table, const std::vector<std::string>& column_names) const {
  // The index can be interleaved into the indexed table if table's primary key
  // columns match a prefix of the index's columns.
  auto primary_key = table->primary_key();
  if (primary_key.size() > column_names.size()) {
    return false;  // Too many primary key columns to be a prefix.
  }
  for (int i = 0; i < primary_key.size(); ++i) {
    if (!absl::EqualsIgnoreCase(primary_key[i]->column()->Name(),
                                column_names[i])) {
      return false;  // Different columns.
    }
  }
  return true;  // Matching prefix.
}

absl::Status MapSpangresDDLErrorToSpannerError(const absl::Status& status) {
  if (status.ok()) return status;

  switch (status.code()) {
    // Used in DDL translator for unsupported pg sql feature.
    case absl::StatusCode::kFailedPrecondition:
    // Used in DDL direct printer for unimplemented pg sql printing.
    case absl::StatusCode::kUnimplemented:
    // Used in pg sql parser to indicate invalid syntax.
    case absl::StatusCode::kInvalidArgument:
    // Used in pg analyzer to indicate `NotFound` error, for example: table,
    // column not found.
    case absl::StatusCode::kNotFound:
    // Used in pg analyzer to indicate some kind of statement is not supported,
    // for example: too many arguments.
    case absl::StatusCode::kOutOfRange: {
      return error::DdlInvalidArgumentError(status.message());
    }
    default: {
      return error::DdlUnavailableError();
    }
  }
}

// Translates Spangres SQL expression stored as a string in
// `original_expression` field and returns a translated ZetaSQL expression.
// When we first translate a Spangres DDL statement which contains an
// expression, like check constraints, generated column, and column default,
// we do not translate the expression. We translate it later when we have more
// table information.
absl::StatusOr<ExpressionTranslateResult>
SchemaUpdaterImpl::TranslatePostgreSqlExpression(
    const Table* table, const ddl::CreateTable* ddl_create_table,
    absl::string_view expression) {
  std::vector<zetasql::SimpleTable::NameAndType> name_and_types;
  ZETASQL_RETURN_IF_ERROR(InitColumnNameAndTypesFromTable(table, ddl_create_table,
                                                  &name_and_types));
  zetasql::SimpleTable simple_table(table->Name(), name_and_types);
  // Setup a catalog for PostgreSQL analyzer needed to resolve provided AST.
  zetasql::SimpleCatalog catalog("pg simple catalog");
  catalog.AddTable(&simple_table);

  // Setup zetasql::AnalyzerOptions needed for translation from PostgreSQL to
  // ZetaSQL.
  zetasql::AnalyzerOptions analyzer_options =
      MakeGoogleSqlAnalyzerOptions(GetTimeZone());
  analyzer_options.CreateDefaultArenasIfNotSet();

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
      postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));
  ZETASQL_ASSIGN_OR_RETURN(
      ExpressionTranslateResult result,
      postgres_translator::spangres::TranslateTableLevelExpression(
          expression, simple_table.Name(), catalog, analyzer_options,
          type_factory_,
          std::make_unique<FunctionCatalog>(
              type_factory_,
              /*catalog_name=*/kCloudSpannerEmulatorFunctionCatalogName,
              /*schema=*/latest_schema_)),
      _.With(MapSpangresDDLErrorToSpannerError));
  return result;
}

absl::StatusOr<ExpressionTranslateResult>
SchemaUpdaterImpl::TranslatePostgreSqlQueryInView(absl::string_view query) {
  zetasql::AnalyzerOptions analyzer_options =
      MakeGoogleSqlAnalyzerOptionsForViewsAndFunctions(
          GetTimeZone(), admin::database::v1::POSTGRESQL);
  analyzer_options.CreateDefaultArenasIfNotSet();
  FunctionCatalog function_catalog(type_factory_);
  function_catalog.SetLatestSchema(latest_schema_);
  Catalog catalog(latest_schema_, &function_catalog, type_factory_,
                  analyzer_options);

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
      postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));
  ZETASQL_ASSIGN_OR_RETURN(
      ExpressionTranslateResult result,
      postgres_translator::spangres::TranslateQueryInView(
          query, catalog, analyzer_options, type_factory_,
          std::make_unique<FunctionCatalog>(
              type_factory_,
              /*catalog_name=*/kCloudSpannerEmulatorFunctionCatalogName,
              /*schema=*/latest_schema_)),
      _.With(MapSpangresDDLErrorToSpannerError));
  return result;
}

absl::Status SchemaUpdaterImpl::CreateCheckConstraint(
    const ddl::CheckConstraint& ddl_check_constraint, const Table* table,
    const ddl::CreateTable* ddl_create_table) {
  CheckConstraint::Builder builder;
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " for CHECK CONSTRAINT "
            << ddl_check_constraint.name() << " on table " << table->Name();
  }

  ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(table, [&](Table::Editor* editor) {
    editor->add_check_constraint(builder.get());
    return absl::OkStatus();
  }));
  builder.set_table(table);

  std::string check_constraint_name;
  bool is_generated_name = false;
  if (ddl_check_constraint.has_name()) {
    check_constraint_name = ddl_check_constraint.name();
    ZETASQL_RETURN_IF_ERROR(
        global_names_.AddName("Check Constraint", check_constraint_name));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(check_constraint_name,
                     global_names_.GenerateCheckConstraintName(table->Name()));
    is_generated_name = true;
  }
  builder.set_constraint_name(check_constraint_name);
  builder.has_generated_name(is_generated_name);
  builder.set_expression(ddl_check_constraint.expression());
  if (ddl_check_constraint.has_expression_origin() &&
      !ddl_check_constraint.expression_origin().original_expression().empty()) {
    builder.set_original_expression(std::string(absl::StripAsciiWhitespace(
        ddl_check_constraint.expression_origin().original_expression())));
  }
  absl::flat_hash_set<std::string> dependent_column_names;
  absl::flat_hash_set<const SchemaNode*> udf_dependencies;
  absl::Status s = AnalyzeCheckConstraint(
      ddl_check_constraint.expression(), table, ddl_create_table,
      &dependent_column_names, &builder, &udf_dependencies);

  if (!s.ok()) {
    const std::string display_name =
        is_generated_name ? "<unnamed>" : check_constraint_name;
    return error::CheckConstraintExpressionParseError(
        table->Name(), ddl_check_constraint.expression(), display_name,
        s.message());
  }

  const CheckConstraint* check_constraint = builder.get();
  statement_context_->AddAction(
      [check_constraint](const SchemaValidationContext* context) {
        return VerifyCheckConstraintData(check_constraint, context);
      });
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::CreateRowDeletionPolicy(
    const ddl::RowDeletionPolicy& row_deletion_policy,
    Table::Builder* builder) {
  builder->set_row_deletion_policy(row_deletion_policy);
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<const ProtoBundle>>
SchemaUpdaterImpl::CreateProtoBundle(
    const ddl::CreateProtoBundle& ddl_proto_bundle,
    absl::string_view proto_descriptor_bytes) {
  ZETASQL_ASSIGN_OR_RETURN(auto proto_bundle_builder,
                   ProtoBundle::Builder::New(proto_descriptor_bytes));
  auto insert_types = ddl_proto_bundle.insert_type();
  std::vector<std::string> insert_type_names;
  insert_type_names.reserve(insert_types.size());
  for (int i = 0; i < insert_types.size(); ++i) {
    insert_type_names.push_back(insert_types.at(i).source_name());
  }
  ZETASQL_RETURN_IF_ERROR(proto_bundle_builder->InsertTypes(insert_type_names));
  return proto_bundle_builder->Build();
}

absl::Status SchemaUpdaterImpl::CreateTable(
    const ddl::CreateTable& ddl_table,
    const database_api::DatabaseDialect& dialect) {
  if (latest_schema_->tables().size() >= limits::kMaxTablesPerDatabase) {
    return error::TooManyTablesPerDatabase(ddl_table.table_name(),
                                           limits::kMaxTablesPerDatabase);
  }

  if (global_names_.HasName(ddl_table.table_name()) &&
      ddl_table.existence_modifier() == ddl::IF_NOT_EXISTS) {
    return absl::OkStatus();
  }

  ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Table", ddl_table.table_name()));

  Table::Builder builder;
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " to table "
            << ddl_table.table_name();
  }
  builder.set_id(table_id_generator_->NextId(ddl_table.table_name()))
      .set_name(ddl_table.table_name());

  for (const ddl::ColumnDefinition& ddl_column : ddl_table.column()) {
    ZETASQL_ASSIGN_OR_RETURN(
        const Column* column,
        CreateColumn(ddl_column, builder.get(), &ddl_table, dialect));
    builder.add_column(column);
  }

  for (const Column* column : builder.get()->columns()) {
    if (column->is_generated()) {
      const_cast<Column*>(column)->PopulateDependentColumns();
    }
  }

  // Some constraints have a dependency on the primary key, so create it first.
  for (const ddl::KeyPartClause& ddl_key_part : ddl_table.primary_key()) {
    ZETASQL_RETURN_IF_ERROR(CreatePrimaryKeyConstraint(ddl_key_part, &builder,
                                               /*with_oid=*/true));
  }
  // Assign OID for PRIMARY KEY index.
  oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_primary_key_index_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value()
            << " for PRIMARY KEY index on table " << ddl_table.table_name();
  }

  for (const ddl::ForeignKey& ddl_foreign_key : ddl_table.foreign_key()) {
    ZETASQL_RETURN_IF_ERROR(CreateForeignKeyConstraint(ddl_foreign_key, builder.get()));
  }
  if (ddl_table.has_interleave_clause()) {
    ZETASQL_RETURN_IF_ERROR(CreateInterleaveConstraintForTable(
        ddl_table.interleave_clause(), &builder));
    if (ddl_table.interleave_clause().type() ==
        ddl::InterleaveClause::IN_PARENT) {
      oid = pg_oid_assigner_->GetNextPostgresqlOid();
      builder.set_interleave_in_parent_postgresql_oid(oid);
      if (oid.has_value()) {
        ZETASQL_VLOG(2) << "Assigned oid " << oid.value()
                << " for IN_PARENT interleave on table "
                << ddl_table.table_name();
      }
    }
  }
  for (const ddl::CheckConstraint& ddl_check_constraint :
       ddl_table.check_constraint()) {
    const Table* table = builder.get();
    if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
      ddl::CheckConstraint mutable_check_constraint = ddl_check_constraint;
      ZETASQL_RET_CHECK(mutable_check_constraint.has_expression_origin());
      ZETASQL_ASSIGN_OR_RETURN(ExpressionTranslateResult result,
                       TranslatePostgreSqlExpression(
                           table, &ddl_table,
                           mutable_check_constraint.expression_origin()
                               .original_expression()));
      mutable_check_constraint.mutable_expression_origin()
          ->set_original_expression(result.original_postgresql_expression);
      mutable_check_constraint.set_expression(
          result.translated_googlesql_expression);
      ZETASQL_RETURN_IF_ERROR(
          CreateCheckConstraint(mutable_check_constraint, table, &ddl_table));
    } else {
      ZETASQL_RETURN_IF_ERROR(
          CreateCheckConstraint(ddl_check_constraint, table, &ddl_table));
    }
  }

  if (ddl_table.has_row_deletion_policy()) {
    ZETASQL_RETURN_IF_ERROR(
        CreateRowDeletionPolicy(ddl_table.row_deletion_policy(), &builder));
  }
  if (ddl_table.has_synonym()) {
    ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Table", ddl_table.synonym()));
    builder.set_synonym(ddl_table.synonym());
  }
  if (builder.get()->is_trackable_by_change_stream()) {
    // If change streams implicitly tracking the entire database, newly added
    // tables should be automatically watched by those change streams.
    for (const ChangeStream* change_stream : latest_schema_->change_streams()) {
      if (change_stream->track_all()) {
        std::vector<std::string> columns = builder.get()->trackable_columns();
        for (const Column* column : builder.get()->columns()) {
          if (column->is_trackable_by_change_stream() &&
              !column->table()->FindKeyColumn(column->Name())) {
            // Register the trackable columns of the newly added table to the
            // list of change streams implicitly tracking the entire database
            ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(
                column,
                [change_stream](Column::Editor* editor) -> absl::Status {
                  editor->add_change_stream(change_stream);
                  return absl::OkStatus();
                }));
          }
        }
        // Register the newly added table to the list of change streams
        // implicitly tracking the entire database
        ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
            change_stream,
            [ddl_table, columns](
                ChangeStream::Editor* change_stream_editor) -> absl::Status {
              change_stream_editor->add_tracked_tables_columns(
                  ddl_table.table_name(), columns);
              return absl::OkStatus();
            }));
        // Register the change stream tracking the entire database to the newly
        // added table
        builder.add_change_stream(change_stream);
      }
    }
  }

  if (!ddl_table.set_options().empty()) {
    ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
        builder.get(),
        [this, &ddl_table](Table::Editor* editor) -> absl::Status {
          return SetTableOptions(ddl_table.set_options(), editor);
        }));
  }

  if (SDLObjectName::IsFullyQualifiedName(ddl_table.table_name())) {
    const absl::string_view schema_name =
        SDLObjectName::GetSchemaName(ddl_table.table_name());
    const NamedSchema* named_schema =
        latest_schema_->FindNamedSchema(std::string(schema_name));
    if (named_schema == nullptr) {
      return error::NamedSchemaNotFound(schema_name);
    }

    ZETASQL_RETURN_IF_ERROR(AlterNode<NamedSchema>(
        named_schema, [&](NamedSchema::Editor* editor) -> absl::Status {
          editor->add_table(builder.get());
          if (ddl_table.has_synonym()) {
            editor->add_synonym(builder.get());
          }
          return absl::OkStatus();
        }));
  }

  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return absl::OkStatus();
}

absl::StatusOr<const Column*> SchemaUpdaterImpl::CreateIndexDataTableColumn(
    const Table* indexed_table, const std::string& source_column_name,
    const Table* index_data_table, bool is_null_filtered) {
  const Column* source_column =
      indexed_table->FindColumnCaseSensitive(source_column_name);
  if (source_column == nullptr) {
    return error::IndexRefsNonExistentColumn(
        index_data_table->owner_index()->Name(), source_column_name);
  }

  Column::Builder builder;
  builder.set_name(source_column->Name())
      .set_id(column_id_generator_->NextId(
          absl::StrCat(index_data_table->Name(), ".", source_column->Name())))
      .set_source_column(source_column)
      .set_table(index_data_table);

  if (is_null_filtered) {
    builder.set_nullable(false);
  } else {
    builder.set_nullable(source_column->is_nullable());
  }

  const Column* column = builder.get();
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return column;
}

absl::Status SchemaUpdaterImpl::AddIndexColumnsByName(
    const std::string& column_name, const Table* indexed_table,
    bool is_null_filtered, std::vector<const Column*>& columns,
    Table::Builder& builder) {
  const Column* column = builder.get()->FindColumn(column_name);
  // Skip already added columns
  if (column == nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        column, CreateIndexDataTableColumn(indexed_table, column_name,
                                           builder.get(), is_null_filtered));
    builder.add_column(column);
  }
  columns.push_back(column);

  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AddSearchIndexColumns(
    const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>& key_parts,
    const Table* indexed_table, bool is_null_filtered,
    std::vector<const Column*>& columns, Table::Builder& builder) {
  for (const ddl::KeyPartClause& ddl_key_part : key_parts) {
    const std::string& column_name = ddl_key_part.key_name();
    ZETASQL_RETURN_IF_ERROR(AddIndexColumnsByName(column_name, indexed_table,
                                          is_null_filtered, columns, builder));
  }

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const Table>>
SchemaUpdaterImpl::CreateChangeStreamPartitionTable(
    const ChangeStream* change_stream) {
  std::string partition_table_name =
      absl::StrCat(kChangeStreamPartitionTablePrefix, change_stream->Name());
  Table::Builder builder;
  builder.set_name(partition_table_name)
      .set_id(table_id_generator_->NextId(partition_table_name))
      .set_owner_change_stream(change_stream);
  // Create columns in table _ChangeStream_Partition_${ChangeStreamName}
  ZETASQL_ASSIGN_OR_RETURN(const Column* column, CreateChangeStreamTableColumn(
                                             "partition_token", builder.get(),
                                             type_factory_->get_string()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("start_time", builder.get(),
                                            type_factory_->get_timestamp()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("end_time", builder.get(),
                                            type_factory_->get_timestamp()));
  builder.add_column(column);
  const zetasql::Type* updated_string_array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(type_factory_->get_string(),
                                               &updated_string_array_type));
  ZETASQL_ASSIGN_OR_RETURN(column,
                   CreateChangeStreamTableColumn("parents", builder.get(),
                                                 updated_string_array_type));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column,
                   CreateChangeStreamTableColumn("children", builder.get(),
                                                 updated_string_array_type));
  builder.add_column(column);

  ZETASQL_ASSIGN_OR_RETURN(column,
                   CreateChangeStreamTableColumn("next_churn", builder.get(),
                                                 type_factory_->get_string()));
  builder.add_column(column);

  // Set the partition_token as primary key column
  ZETASQL_RETURN_IF_ERROR(
      CreateChangeStreamTablePKConstraint("partition_token", &builder));
  return builder.build();
}

absl::StatusOr<std::unique_ptr<const Table>>
SchemaUpdaterImpl::CreateChangeStreamDataTable(
    const ChangeStream* change_stream,
    const Table* change_stream_partition_table) {
  std::string data_table_name =
      absl::StrCat(kChangeStreamDataTablePrefix, change_stream->Name());
  Table::Builder builder;
  builder.set_name(data_table_name)
      .set_id(table_id_generator_->NextId(data_table_name))
      .set_owner_change_stream(change_stream);
  // Create columns in table _ChangeStream_Data_${ChangeStreamName}
  ZETASQL_ASSIGN_OR_RETURN(const Column* column, CreateChangeStreamTableColumn(
                                             "partition_token", builder.get(),
                                             type_factory_->get_string()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("commit_timestamp", builder.get(),
                                            type_factory_->get_timestamp()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column, CreateChangeStreamTableColumn(
                               "server_transaction_id", builder.get(),
                               type_factory_->get_string()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("record_sequence", builder.get(),
                                            type_factory_->get_string()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column, CreateChangeStreamTableColumn(
                               "is_last_record_in_transaction_in_partition",
                               builder.get(), type_factory_->get_bool()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column,
                   CreateChangeStreamTableColumn("table_name", builder.get(),
                                                 type_factory_->get_string()));
  builder.add_column(column);
  const zetasql::Type* updated_string_array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(type_factory_->get_string(),
                                               &updated_string_array_type));
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("column_types_name", builder.get(),
                                            updated_string_array_type));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("column_types_type", builder.get(),
                                            updated_string_array_type));
  builder.add_column(column);
  const zetasql::Type* updated_bool_array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(type_factory_->get_bool(),
                                               &updated_bool_array_type));
  ZETASQL_ASSIGN_OR_RETURN(column, CreateChangeStreamTableColumn(
                               "column_types_is_primary_key", builder.get(),
                               updated_bool_array_type));
  builder.add_column(column);
  const zetasql::Type* updated_int64_array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(type_factory_->get_int64(),
                                               &updated_int64_array_type));
  ZETASQL_ASSIGN_OR_RETURN(column, CreateChangeStreamTableColumn(
                               "column_types_ordinal_position", builder.get(),
                               updated_int64_array_type));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column,
                   CreateChangeStreamTableColumn("mods_keys", builder.get(),
                                                 updated_string_array_type));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("mods_new_values", builder.get(),
                                            updated_string_array_type));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("mods_old_values", builder.get(),
                                            updated_string_array_type));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column,
                   CreateChangeStreamTableColumn("mod_type", builder.get(),
                                                 type_factory_->get_string()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("value_capture_type", builder.get(),
                                            type_factory_->get_string()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column, CreateChangeStreamTableColumn(
                               "number_of_records_in_transaction",
                               builder.get(), type_factory_->get_int64()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column, CreateChangeStreamTableColumn(
                               "number_of_partitions_in_transaction",
                               builder.get(), type_factory_->get_int64()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(
      column, CreateChangeStreamTableColumn("transaction_tag", builder.get(),
                                            type_factory_->get_string()));
  builder.add_column(column);
  ZETASQL_ASSIGN_OR_RETURN(column, CreateChangeStreamTableColumn(
                               "is_system_transaction", builder.get(),
                               type_factory_->get_bool()));
  builder.add_column(column);
  // Set primary key columns
  ZETASQL_RETURN_IF_ERROR(
      CreateChangeStreamTablePKConstraint("partition_token", &builder));
  ZETASQL_RETURN_IF_ERROR(
      CreateChangeStreamTablePKConstraint("commit_timestamp", &builder));
  ZETASQL_RETURN_IF_ERROR(
      CreateChangeStreamTablePKConstraint("server_transaction_id", &builder));
  ZETASQL_RETURN_IF_ERROR(
      CreateChangeStreamTablePKConstraint("record_sequence", &builder));
  // Set _ChangeStream_Partition_${ChangeStreamName} as interleaved parent table
  ZETASQL_RETURN_IF_ERROR(CreateInterleaveConstraint(
      change_stream_partition_table,
      /*on_delete=*/Table::OnDeleteAction::kNoAction,
      /*interleave_type=*/Table::InterleaveType::kInParent, &builder));
  return builder.build();
}

absl::StatusOr<const Column*> SchemaUpdaterImpl::CreateChangeStreamTableColumn(
    const std::string& column_name, const Table* change_stream_table,
    const zetasql::Type* type) {
  Column::Builder builder;
  builder.set_name(column_name)
      .set_id(column_id_generator_->NextId(
          absl::StrCat(change_stream_table->Name(), ".", column_name)))
      .set_table(change_stream_table)
      .set_type(type);
  if (column_name == "end_time" || column_name == "start_time" ||
      column_name == "commit_timestamp") {
    builder.set_allow_commit_timestamp(true);
  }
  const Column* column = builder.get();
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return column;
}

absl::StatusOr<const KeyColumn*>
SchemaUpdaterImpl::CreateChangeStreamTablePKColumn(
    const std::string& pk_column_name, const Table* change_stream_table) {
  KeyColumn::Builder builder;
  const bool is_descending = false;

  // References to columns in primary key clause are case-sensitive.
  const Column* column =
      change_stream_table->FindColumnCaseSensitive(pk_column_name);
  if (column == nullptr) {
    return error::NonExistentKeyColumn(OwningObjectType(change_stream_table),
                                       OwningObjectName(change_stream_table),
                                       pk_column_name);
  }
  builder.set_column(column).set_descending(is_descending);
  const KeyColumn* key_col = builder.get();
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return key_col;
}

absl::Status SchemaUpdaterImpl::CreateChangeStreamTablePKConstraint(
    const std::string& pk_column_name, Table::Builder* builder) {
  ZETASQL_ASSIGN_OR_RETURN(
      const KeyColumn* key_col,
      CreateChangeStreamTablePKColumn(pk_column_name, builder->get()));
  builder->add_key_column(key_col);
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const Table>>
SchemaUpdaterImpl::CreateIndexDataTable(
    absl::string_view index_name,
    const std::vector<ddl::KeyPartClause>& index_pk,
    const std::string* interleave_in_table,
    const ::google::protobuf::RepeatedPtrField<ddl::StoredColumnDefinition>&
        stored_columns,
    const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>* partition_by,
    const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>* order_by,
    const ::google::protobuf::RepeatedPtrField<std::string>* null_filtered_columns,
    const Index* index, const Table* indexed_table,
    ColumnsUsedByIndex* columns_used_by_index) {
  std::string table_name = absl::StrCat(kIndexDataTablePrefix, index_name);
  Table::Builder builder;
  builder.set_name(table_name)
      .set_id(table_id_generator_->NextId(table_name))
      .set_owner_index(index);
  absl::flat_hash_set<std::string> null_filtered_columns_set;
  // Add null filtered columns to index data table.
  if (null_filtered_columns != nullptr) {
    for (const std::string& column_name : *null_filtered_columns) {
      null_filtered_columns_set.insert(column_name);
    }
  }

  // Add indexed columns to the index_data_table's columns and primary key.
  if (!index_pk.empty()) {
    // The primary key is a combination of (index_keys,indexed_table_keys)
    std::vector<ddl::KeyPartClause> data_table_pk;
    data_table_pk.reserve(index_pk.size());
    // First create columns for the specified primary key.

    for (const ddl::KeyPartClause& ddl_key_part : index_pk) {
      data_table_pk.push_back(ddl_key_part);

      const std::string& column_name = ddl_key_part.key_name();
      ZETASQL_ASSIGN_OR_RETURN(
          const Column* column,
          CreateIndexDataTableColumn(
              indexed_table, column_name, builder.get(),
              index->is_null_filtered() ||
                  null_filtered_columns_set.contains(column_name)));
      builder.add_column(column);
    }

    // Next, create columns for the indexed table's primary key.
    for (const KeyColumn* key_col : indexed_table->primary_key()) {
      if (builder.get()->FindColumn(key_col->column()->Name()) != nullptr) {
        // Skip already added columns
        continue;
      }
      std::string key_col_name = key_col->column()->Name();
      ZETASQL_ASSIGN_OR_RETURN(
          const Column* column,
          CreateIndexDataTableColumn(
              indexed_table, key_col_name, builder.get(),
              index->is_null_filtered() ||
                  null_filtered_columns_set.contains(key_col_name)));
      builder.add_column(column);

      // Add to the PK specification.
      ddl::KeyPartClause key_part;
      key_part.set_key_name(key_col_name);
      if (key_col->is_descending() && key_col->is_nulls_last()) {
        key_part.set_order(ddl::KeyPartClause::DESC);
      } else if (key_col->is_descending() && !key_col->is_nulls_last()) {
        key_part.set_order(ddl::KeyPartClause::DESC_NULLS_FIRST);
      } else if (!key_col->is_descending() && key_col->is_nulls_last()) {
        key_part.set_order(ddl::KeyPartClause::ASC_NULLS_LAST);
      }
      data_table_pk.push_back(key_part);
    }

    for (const ddl::KeyPartClause& ddl_key_part : data_table_pk) {
      // The data table is a hidden table so don't assign oids to to the PKs.
      ZETASQL_RETURN_IF_ERROR(CreatePrimaryKeyConstraint(ddl_key_part, &builder,
                                                 /*with_oid=*/false));
    }
    int num_declared_keys = index_pk.size();
    auto data_table_key_cols = builder.get()->primary_key();
    for (int i = 0; i < num_declared_keys; ++i) {
      columns_used_by_index->index_key_columns.push_back(
          data_table_key_cols[i]);
    }
  }
  if (interleave_in_table != nullptr) {
    const Table* parent_table = indexed_table;
    if (!absl::EqualsIgnoreCase(parent_table->Name(), *interleave_in_table)) {
      ZETASQL_ASSIGN_OR_RETURN(parent_table, GetInterleaveConstraintTable(
                                         *interleave_in_table, builder));
    }
    ZETASQL_RETURN_IF_ERROR(CreateInterleaveConstraint(
        parent_table,
        /*on_delete=*/Table::OnDeleteAction::kCascade,
        /*interleave_type=*/Table::InterleaveType::kInParent, &builder));
  }

  // Add stored columns to index data table.
  for (const ddl::StoredColumnDefinition& ddl_column : stored_columns) {
    const std::string& column_name = ddl_column.name();
    ZETASQL_ASSIGN_OR_RETURN(const Column* column,
                     CreateIndexDataTableColumn(
                         indexed_table, column_name, builder.get(),
                         /*is_null_filtered=*/
                         null_filtered_columns_set.contains(column_name)));
    builder.add_column(column);
    columns_used_by_index->stored_columns.push_back(column);
  }

  // Add null filtered columns to index data table.
  if (null_filtered_columns != nullptr) {
    // Add null filtered columns to index data table.
    for (const std::string& column_name : *null_filtered_columns) {
      ZETASQL_RETURN_IF_ERROR(AddIndexColumnsByName(
          column_name, indexed_table, /*is_null_filtered=*/true,
          columns_used_by_index->null_filtered_columns, builder));
    }
  }

  if (partition_by != nullptr) {
    // Add partition by columns to index data table
    ZETASQL_RETURN_IF_ERROR(AddSearchIndexColumns(
        *partition_by, indexed_table, index->is_null_filtered(),
        columns_used_by_index->partition_by_columns, builder));
  }

  if (order_by != nullptr) {
    for (const ddl::KeyPartClause& ddl_key_part : *order_by) {
      const std::string& column_name = ddl_key_part.key_name();
      std::vector<const Column*> columns;
      // Add column to index data table.
      ZETASQL_RETURN_IF_ERROR(AddIndexColumnsByName(column_name, indexed_table,
                                            index->is_null_filtered(), columns,
                                            builder));

      // ORDER BY columns cannot be unsupported key column types.
      if (!IsSupportedKeyColumnType(columns[0]->GetType(),
                                    index->is_vector_index())) {
        return error::SearchIndexOrderByMustBeIntegerType(
            index->Name(), column_name, columns[0]->GetType()->DebugString());
      }

      // Create KeyColumn to preserve ordering metadata.
      KeyColumn::Builder key_builder;
      ZETASQL_ASSIGN_OR_RETURN(
          const KeyColumn* order_by_column,
          CreatePrimaryKeyColumn(ddl_key_part, builder.get(), &key_builder));
      columns_used_by_index->order_by_columns.push_back(order_by_column);
    }
  }

  return builder.build();
}

absl::StatusOr<const Index*> SchemaUpdaterImpl::CreateIndex(
    const ddl::CreateIndex& ddl_index, const Table* indexed_table) {
  const std::string* interleave_in_table =
      ddl_index.has_interleave_in_table() ? &ddl_index.interleave_in_table()
                                          : nullptr;
  const auto& index_pk = std::vector<ddl::KeyPartClause>(
      ddl_index.key().begin(), ddl_index.key().end());
  bool is_unique = ddl_index.unique();
  bool is_null_filtered = ddl_index.null_filtered();

  const Index* index =
      latest_schema_->FindIndexCaseSensitive(ddl_index.index_name());
  if (index != nullptr &&
      ddl_index.existence_modifier() == ddl::IF_NOT_EXISTS) {
    return index;
  }

  const ::google::protobuf::RepeatedPtrField<ddl::SetOption>* set_options = nullptr;

  set_options = &ddl_index.set_options();

  return CreateIndexHelper(
      ddl_index.index_name(), ddl_index.index_base_name(), is_unique,
      is_null_filtered, interleave_in_table, index_pk,
      ddl_index.stored_column_definition(),
      /*is_search_index=*/false,
      /*is_vector_index=*/false,
      /*partition_by=*/nullptr,
      /*order_by=*/nullptr,
      /*null_filtered_columns=*/&ddl_index.null_filtered_column(),
      /*set_options=*/set_options, indexed_table);
}

const google::protobuf::FieldDescriptor* GetFieldDescriptor(absl::string_view sdl_type,
                                                  absl::string_view sdl_name,
                                                  absl::string_view option_name,
                                                  google::protobuf::Message* proto,
                                                  std::string* error) {
  error->clear();
  const google::protobuf::FieldDescriptor* field =
      proto->GetDescriptor()->FindFieldByName(option_name);

  if (field == nullptr) {
    *error = absl::StrCat("Unable to set ", option_name, " option on ",
                          sdl_type, " ", sdl_name, ".  Unknown option name.");
    return nullptr;
  }
  return field;
}

bool SetInt64Option(absl::string_view sdl_type, absl::string_view sdl_name,
                    absl::string_view option_name, int64_t option_value,
                    google::protobuf::Message* proto, std::string* error) {
  const google::protobuf::FieldDescriptor* field =
      GetFieldDescriptor(sdl_type, sdl_name, option_name, proto, error);
  if (field == nullptr) {
    return false;
  }
  const google::protobuf::Reflection* reflection = proto->GetReflection();
  reflection->SetInt64(proto, field, option_value);
  return true;
}

bool SetStringOption(absl::string_view sdl_type, absl::string_view sdl_name,
                     absl::string_view option_name,
                     absl::string_view option_value, google::protobuf::Message* proto,
                     std::string* error) {
  const google::protobuf::FieldDescriptor* field =
      GetFieldDescriptor(sdl_type, sdl_name, option_name, proto, error);
  if (field == nullptr) {
    return false;
  }
  const google::protobuf::Reflection* reflection = proto->GetReflection();
  reflection->SetString(proto, field, std::string(option_value));
  return true;
}

bool ApplyOptions(absl::string_view sdl_type, absl::string_view sdl_name,
                  const OptionList& input, google::protobuf::Message* proto,
                  std::string* error) {
  for (const ddl::SetOption& option : input) {
    if (option.has_int64_value()) {
      if (!SetInt64Option(sdl_type, sdl_name, option.option_name(),
                          option.int64_value(), proto, error)) {
        return false;
      }
    } else if (option.has_string_value()) {
      if (!SetStringOption(sdl_type, sdl_name, option.option_name(),
                           option.string_value(), proto, error)) {
        return false;
      }
    } else {
      *error =
          absl::StrCat("Unable to set ", option.option_name(), " option on ",
                       sdl_type, " ", sdl_name, ".  No value to set.");
      return false;
    }
  }
  return true;
}

absl::Status SetVectorIndexOptions(
    std::string_view sdl_name,
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    Index::Builder* modifier) {
  std::string error;
  ddl::VectorIndexOptionsProto vector_index_options;
  if (!ApplyOptions("Vector Index", sdl_name, set_options,
                    &vector_index_options, &error)) {
    return error::OptionsError(error);
  }
  int tree_depth = vector_index_options.tree_depth();
  if (tree_depth != 2 && tree_depth != 3) {
    return error::OptionsError("vector index tree depth must be 2 or 3.");
  }
  if (vector_index_options.has_num_leaves() &&
      vector_index_options.num_leaves() <= 0) {
    return error::OptionsError("vector index num_leaves must be > 0.");
  }
  if (vector_index_options.has_num_branches() &&
      vector_index_options.num_branches() <= 0) {
    return error::OptionsError("vector index num_branches must be > 0.");
  }
  if (vector_index_options.has_num_branches() &&
      vector_index_options.has_num_leaves()) {
    if (vector_index_options.num_branches() >
        vector_index_options.num_leaves()) {
      return error::OptionsError(
          "num_leaves cannot be fewer than num_branches.");
    }
  }
  if (vector_index_options.has_distance_type()) {
    ddl::VectorIndexOptionsProto::DistanceType distance_type;
    if (!ddl::VectorIndexOptionsProto::DistanceType_Parse(
            vector_index_options.distance_type(), &distance_type) ||
        distance_type ==
            ddl::VectorIndexOptionsProto::DISTANCE_TYPE_UNSPECIFIED) {
      return error::OptionsError(
          absl::StrCat("The distance_type of ", sdl_name, " is invalid."));
    }
  }
  if (vector_index_options.has_leaf_scatter_factor()) {
    if (vector_index_options.leaf_scatter_factor() < 0) {
      return error::OptionsError(
          "vector index leaf_scatter_factor must be >= 0.");
    }
    if (vector_index_options.leaf_scatter_factor() > 32) {
      return error::OptionsError(
          "vector index leaf_scatter_factor must be <= 32.");
    }
  }
  if (vector_index_options.has_min_branch_splits()) {
    if (vector_index_options.min_branch_splits() <= 0) {
      return error::OptionsError("vector index min_branch_splits must be > 0.");
    }
  }
  if (vector_index_options.has_min_leaf_splits()) {
    if (vector_index_options.min_leaf_splits() <= 0) {
      return error::OptionsError("vector index min_leaf_splits must be > 0.");
    }
  }

  modifier->set_vector_index_options(vector_index_options);
  return absl::OkStatus();
}

absl::StatusOr<const Index*> SchemaUpdaterImpl::CreateVectorIndex(
    const ddl::CreateVectorIndex& ddl_index, const Table* indexed_table) {
  if (ddl_index.partition_by_size() > 0) {
    return error::VectorIndexPartitionByUnsupported(ddl_index.index_name());
  }
  std::vector<ddl::KeyPartClause> index_pk;
  index_pk.push_back(ddl_index.key());

  const Index* index =
      latest_schema_->FindIndexCaseSensitive(ddl_index.index_name());
  if (index != nullptr &&
      ddl_index.existence_modifier() == ddl::IF_NOT_EXISTS) {
    return index;
  }

  return CreateIndexHelper(
      ddl_index.index_name(), ddl_index.index_base_name(), false, false,
      nullptr, index_pk, ddl_index.stored_column_definition(),
      /*is_search_index=*/false,
      /*is_vector_index=*/true,
      /*partition_by=*/nullptr,
      /*order_by=*/nullptr,
      /*null_filtered_columns=*/&ddl_index.null_filtered_column(),
      /*set_options=*/&ddl_index.set_options(), indexed_table);
}

absl::StatusOr<const Index*> SchemaUpdaterImpl::CreateSearchIndex(
    const ddl::CreateSearchIndex& ddl_index, const Table* indexed_table) {
  const std::string* interleave_in_table =
      ddl_index.has_interleave_in_table() ? &ddl_index.interleave_in_table()
                                          : nullptr;
  std::vector<ddl::KeyPartClause> table_pk;
  table_pk.reserve(ddl_index.token_column_definition().size());
  for (const ddl::TokenColumnDefinition& token_column_def :
       ddl_index.token_column_definition()) {
    const ddl::KeyPartClause& key = token_column_def.token_column();
    if (key.has_order()) {
      return error::SearchIndexTokenlistKeyOrderUnsupported(
          key.key_name(), ddl_index.index_name());
    }

    table_pk.push_back(token_column_def.token_column());
  }
  bool is_null_filtered = ddl_index.null_filtered();
  return CreateIndexHelper(
      ddl_index.index_name(), ddl_index.index_base_name(),
      /*is_unique=*/false, is_null_filtered, interleave_in_table, table_pk,
      ddl_index.stored_column_definition(),
      /*is_search_index=*/true, false, &ddl_index.partition_by(),
      &ddl_index.order_by(), &ddl_index.null_filtered_column(),
      /*set_options=*/nullptr, indexed_table);
}

absl::StatusOr<const ChangeStream*> SchemaUpdaterImpl::CreateChangeStream(
    const ddl::CreateChangeStream& ddl_change_stream,
    const database_api::DatabaseDialect& dialect) {
  if (latest_schema_->change_streams().size() >=
      limits::kMaxChangeStreamsPerDatabase) {
    return error::TooManyChangeStreamsPerDatabase(
        ddl_change_stream.change_stream_name(),
        limits::kMaxChangeStreamsPerDatabase);
  }

  // Validate the change stream name in global_names_
  ZETASQL_RETURN_IF_ERROR(global_names_.AddName(
      "Change Stream", ddl_change_stream.change_stream_name()));

  ChangeStream::Builder builder;
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " on change stream "
            << ddl_change_stream.change_stream_name();
  }
  builder.set_name(ddl_change_stream.change_stream_name());
  const ChangeStream* change_stream = builder.get();
  const std::string tvf_name =
      MakeChangeStreamTvfName(ddl_change_stream.change_stream_name(), dialect);
  builder.set_tvf_name(tvf_name);
  std::optional<uint32_t> tvf_oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_tvf_postgresql_oid(tvf_oid);
  if (tvf_oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << tvf_oid.value() << " to TVF " << tvf_name
            << " on change stream " << ddl_change_stream.change_stream_name();
  }
  // Validate the change stream tvf name in global_names_
  ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Change Stream", tvf_name));
  // Set up _ChangeStream_Partition_${ChangeStreamName}
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const Table> change_stream_partition_table,
                   CreateChangeStreamPartitionTable(change_stream));
  builder.set_change_stream_partition_table(
      change_stream_partition_table.get());
  // Register a backfill action for the change stream partition table.
  statement_context_->AddAction(
      [change_stream](const SchemaValidationContext* context) {
        return BackfillChangeStream(change_stream, context);
      });
  // Set up _ChangeStream_Data_${ChangeStreamName}
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const Table> change_stream_data_table,
                   CreateChangeStreamDataTable(
                       change_stream, change_stream_partition_table.get()));
  builder.set_change_stream_data_table(change_stream_data_table.get());
  // Set up for clause
  if (ddl_change_stream.has_for_clause()) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateChangeStreamForClause(ddl_change_stream.for_clause(),
                                      ddl_change_stream.change_stream_name()));
    builder.set_for_clause(ddl_change_stream.for_clause());
    builder.set_track_all(ddl_change_stream.for_clause().all());
    ZETASQL_RETURN_IF_ERROR(
        ValidateChangeStreamLimits(ddl_change_stream.for_clause(),
                                   ddl_change_stream.change_stream_name()));
  }

  // Validate and set change stream options.
  const auto& set_options = ddl_change_stream.set_options();
  if (!set_options.empty()) {
    ZETASQL_RETURN_IF_ERROR(ValidateChangeStreamOptions(set_options));
    ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
        change_stream,
        [this, set_options](ChangeStream::Editor* editor) -> absl::Status {
          // Set change stream options
          return SetChangeStreamOptions(set_options, editor);
        }));
  }
  // Add the current change stream to the change stream lists of tracked table
  // and column.
  if (ddl_change_stream.has_for_clause()) {
    ZETASQL_RETURN_IF_ERROR(
        RegisterTrackedObjects(ddl_change_stream.for_clause(), change_stream));
    ZETASQL_RETURN_IF_ERROR(BuildChangeStreamTrackedObjects(
        ddl_change_stream.for_clause(), change_stream, &builder));
  }
  // Set the creation time to now
  builder.set_creation_time(absl::Now());

  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  ZETASQL_RETURN_IF_ERROR(AddNode(std::move(change_stream_partition_table)));
  ZETASQL_RETURN_IF_ERROR(AddNode(std::move(change_stream_data_table)));
  return change_stream;
}

absl::Status SchemaUpdaterImpl::BuildChangeStreamTrackedObjects(
    const ddl::ChangeStreamForClause& change_stream_for_clause,
    const ChangeStream* change_stream, ChangeStream::Builder* builder) {
  if (change_stream_for_clause.has_tracked_tables()) {
    for (const ddl::ChangeStreamForClause::TrackedTables::Entry& entry :
         change_stream_for_clause.tracked_tables().table_entry()) {
      if (!latest_schema_->FindTable(entry.table_name())
               ->is_trackable_by_change_stream()) {
        return error::TrackUntrackableTables(entry.table_name());
      }
      std::vector<std::string> columns;
      if (entry.has_tracked_columns()) {
        for (const std::string& column_name :
             entry.tracked_columns().column_name()) {
          columns.push_back(column_name);
        }
        builder->add_tracked_tables_columns(entry.table_name(), columns);
      } else if (entry.has_all_columns()) {
        const Table* table = latest_schema_->FindTable(entry.table_name());
        for (const Column* column :
             latest_schema_->FindTable(entry.table_name())->columns()) {
          if (!column->is_trackable_by_change_stream() &&
              !table->FindKeyColumn(column->Name())) {
            continue;
          }
          columns.push_back(column->Name());
        }
        builder->add_tracked_tables_columns(entry.table_name(), columns);
      } else {
        return error::UnsetTrackedObject(change_stream->Name(),
                                         entry.table_name());
      }
    }
  } else if (change_stream_for_clause.all()) {
    for (const Table* table : latest_schema_->tables()) {
      if (!table->is_trackable_by_change_stream()) {
        continue;
      }
      builder->add_tracked_tables_columns(table->Name(),
                                          table->trackable_columns());
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::EditChangeStreamTrackedObjects(
    const ddl::ChangeStreamForClause& change_stream_for_clause,
    const ChangeStream* change_stream) {
  if (change_stream_for_clause.all()) {
    for (const auto& table : latest_schema_->tables()) {
      if (!table->is_trackable_by_change_stream()) {
        continue;
      }
      ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
          change_stream, [table](ChangeStream::Editor* editor) -> absl::Status {
            editor->add_tracked_tables_columns(table->Name(),
                                               table->trackable_columns());
            return absl::OkStatus();
          }));
    }
    return absl::OkStatus();
  }
  if (!change_stream_for_clause.has_tracked_tables()) {
    return absl::OkStatus();
  }
  ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
      change_stream, [](ChangeStream::Editor* editor) -> absl::Status {
        editor->clear_tracked_tables_columns();
        return absl::OkStatus();
      }));
  for (const ddl::ChangeStreamForClause::TrackedTables::Entry& entry :
       change_stream_for_clause.tracked_tables().table_entry()) {
    std::vector<std::string> columns;
    if (!entry.has_all_columns()) {
      for (const std::string& column_name :
           entry.tracked_columns().column_name()) {
        if (latest_schema_->FindTable(entry.table_name())
                ->is_trackable_by_change_stream()) {
          columns.push_back(column_name);
        } else {
          return error::TrackUntrackableColumns(column_name);
        }
      }
    } else {
      for (const std::string& column_name :
           latest_schema_->FindTable(entry.table_name())->trackable_columns()) {
        columns.push_back(column_name);
      }
    }
    ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
        change_stream,
        [&, entry, columns](ChangeStream::Editor* editor) -> absl::Status {
          editor->add_tracked_tables_columns(entry.table_name(), columns);
          return absl::OkStatus();
        }));
  }
  return absl::OkStatus();
}

absl::StatusOr<const Index*> SchemaUpdaterImpl::CreateIndexHelper(
    const std::string& index_name, const std::string& index_base_name,
    bool is_unique, bool is_null_filtered,
    const std::string* interleave_in_table,
    const std::vector<ddl::KeyPartClause>& table_pk,
    const ::google::protobuf::RepeatedPtrField<ddl::StoredColumnDefinition>&
        stored_columns,
    bool is_search_index, bool is_vector_index,
    const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>* partition_by,
    const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>* order_by,
    const ::google::protobuf::RepeatedPtrField<std::string>* null_filtered_columns,
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>* set_options,
    const Table* indexed_table) {
  if (indexed_table == nullptr) {
    indexed_table = latest_schema_->FindTableCaseSensitive(index_base_name);
    if (indexed_table == nullptr) {
      return error::TableNotFound(index_base_name);
    }
  }
  if (latest_schema_->num_index() >= limits::kMaxIndexesPerDatabase) {
    return error::TooManyIndicesPerDatabase(index_name,
                                            limits::kMaxIndexesPerDatabase);
  }

  // Tables and indexes share a namespace.
  ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Index", index_name));

  Index::Builder builder;
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " to index " << index_name;
  }
  builder.set_name(index_name);
  builder.set_unique(is_unique);
  builder.set_null_filtered(is_null_filtered);
  ColumnsUsedByIndex columns_used_by_index;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const Table> data_table,
      CreateIndexDataTable(index_name, table_pk, interleave_in_table,
                           stored_columns, partition_by, order_by,
                           null_filtered_columns, builder.get(), indexed_table,
                           &columns_used_by_index));

  builder.set_index_data_table(data_table.get());

  for (const KeyColumn* key_col : columns_used_by_index.index_key_columns) {
    builder.add_key_column(key_col);
  }

  for (const Column* col : columns_used_by_index.stored_columns) {
    builder.add_stored_column(col);
  }
  for (const Column* col : columns_used_by_index.null_filtered_columns) {
    builder.add_null_filtered_column(col);
  }
  if (is_search_index) {
    builder.set_index_type(is_search_index);

    for (const Column* col : columns_used_by_index.partition_by_columns) {
      builder.add_partition_by_column(col);
    }

    for (const KeyColumn* col : columns_used_by_index.order_by_columns) {
      builder.add_order_by_column(col);
    }
  }
  if (is_vector_index) {
    builder.set_vector_index_type(is_vector_index);
    ZETASQL_RETURN_IF_ERROR(SetVectorIndexOptions(index_name, *set_options, &builder));
  }

  ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
      indexed_table, [&builder](Table::Editor* table_editor) -> absl::Status {
        table_editor->add_index(builder.get());
        builder.set_indexed_table(table_editor->get());
        return absl::OkStatus();
      }));
  // Register a backfill action for the index.
  const Index* index = builder.get();

  if (!is_search_index && !is_vector_index) {
    statement_context_->AddAction(
        [index](const SchemaValidationContext* context) {
          return BackfillIndex(index, context);
        });
  }

  if (SDLObjectName::IsFullyQualifiedName(index_name)) {
    ZETASQL_RETURN_IF_ERROR(AlterInNamedSchema(
        index_name, [&builder](NamedSchema::Editor* editor) -> absl::Status {
          editor->add_index(builder.get());
          return absl::OkStatus();
        }));
  }

  if (set_options != nullptr && !set_options->empty()) {
    ZETASQL_RETURN_IF_ERROR(SetIndexOptions(*set_options, &builder, is_vector_index));
  }

  // The data table must be added after the index for correct order of
  // validation.
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  ZETASQL_RETURN_IF_ERROR(AddNode(std::move(data_table)));
  return index;
}

absl::flat_hash_set<const SchemaNode*>
SchemaUpdaterImpl::GatherTransitiveDependenciesForSchemaNode(
    const absl::flat_hash_set<const SchemaNode*>& initial_set) {
  absl::flat_hash_set<const SchemaNode*> transitive;
  std::stack<const SchemaNode*> explore;
  for (const auto& dep : initial_set) {
    transitive.insert(dep);
    explore.push(dep);
  }

  while (!explore.empty()) {
    auto dep = explore.top();
    explore.pop();
    // Only explore dependencies that are views, columns, or udfs. (Leave out
    // sequences).
    if (auto view = dynamic_cast<const View*>(dep); view != nullptr) {
      for (const auto& dependency : view->dependencies()) {
        if (transitive.insert(dependency).second) explore.push(dependency);
      }
    }
    if (auto udf = dynamic_cast<const Udf*>(dep); udf != nullptr) {
      for (const auto& dependency : udf->dependencies()) {
        if (transitive.insert(dependency).second) explore.push(dependency);
      }
    }
    if (auto column = dynamic_cast<const Column*>(dep); column != nullptr) {
      for (const auto& dependency : column->udf_dependencies()) {
        if (transitive.insert(dependency).second) explore.push(dependency);
      }
    }
  }
  return transitive;
}

bool SchemaUpdaterImpl::IsBuiltInFunction(const std::string& function_name) {
  // Do not include the schema to avoid populating the schema's Udfs.
  FunctionCatalog function_catalog(type_factory_);
  const zetasql::Function* function = nullptr;
  function_catalog.GetFunction({function_name}, &function);
  return function != nullptr;
}

bool SchemaUpdaterImpl::CanFunctionReplaceTakenName(
    const ddl::CreateFunction& ddl_function) {
  const View* existing_view =
      latest_schema_->FindView(ddl_function.function_name());
  const Udf* existing_udf =
      latest_schema_->FindUdf(ddl_function.function_name());

  if (existing_view != nullptr &&
      ddl_function.function_kind() == ddl::Function::VIEW &&
      ddl_function.is_or_replace()) {
    return true;
  }
  if (existing_udf != nullptr &&
      ddl_function.function_kind() == ddl::Function::FUNCTION &&
      ddl_function.is_or_replace()) {
    return true;
  }
  return false;
}

std::string SchemaUpdaterImpl::GetFunctionKindAsString(
    const ddl::CreateFunction& ddl_function) {
  if (ddl_function.function_kind() == ddl::Function::FUNCTION) {
    return "Function";
  }
  if (ddl_function.function_kind() == ddl::Function::VIEW) {
    return "View";
  }
  return "Unknown";
}

absl::Status SchemaUpdaterImpl::AnalyzeFunctionDefinition(
    const ddl::CreateFunction& ddl_function, bool replace,
    absl::flat_hash_set<const SchemaNode*>* dependencies,
    std::unique_ptr<zetasql::FunctionSignature>* function_signature,
    Udf::Determinism* determinism_level) {
  std::string param_list = "";
  for (int i = 0; i < ddl_function.param_size(); i++) {
    param_list += ddl_function.param(i).name() + " " +
                  ddl_function.param(i).param_typename();
    if (ddl_function.param(i).has_default_value()) {
      param_list += " DEFAULT " + ddl_function.param(i).default_value();
    }
    if (i < ddl_function.param_size() - 1) {
      param_list += ", ";
    }
  }
  auto status = AnalyzeUdfDefinition(ddl_function.function_name(), param_list,
                                     ddl_function.sql_body(), latest_schema_,
                                     type_factory_, dependencies,
                                     function_signature, determinism_level);

  if (!status.ok()) {
    return replace ? error::FunctionReplaceError(ddl_function.function_name(),
                                                 status.message())
                   : error::FunctionBodyAnalysisError(
                         ddl_function.function_name(), status.message());
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AnalyzeFunctionDefinition(
    const ddl::CreateFunction& ddl_function, bool replace,
    std::vector<View::Column>* output_columns,
    absl::flat_hash_set<const SchemaNode*>* dependencies) {
  auto status = AnalyzeViewDefinition(
      ddl_function.function_name(), ddl_function.sql_body(), latest_schema_,
      type_factory_, output_columns, dependencies);
  if (!status.ok()) {
    return replace ? error::ViewReplaceError(ddl_function.function_name(),
                                             status.message())
                   : error::ViewBodyAnalysisError(ddl_function.function_name(),
                                                  status.message());
  }
  return absl::OkStatus();
}

absl::StatusOr<Udf::Builder> SchemaUpdaterImpl::CreateFunctionBuilder(
    const ddl::CreateFunction& ddl_function,
    std::unique_ptr<zetasql::FunctionSignature> function_signature,
    Udf::Determinism determinism_level,
    absl::flat_hash_set<const SchemaNode*> dependencies) {
  Udf::Builder builder;
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " to function "
            << ddl_function.function_name();
  }
  builder.set_name(ddl_function.function_name())
      .set_sql_body(ddl_function.sql_body())
      .set_sql_security([&]() {
        switch (ddl_function.sql_security()) {
          case ddl::Function::UNSPECIFIED_SQL_SECURITY:
            return Udf::SqlSecurity::SQL_SECURITY_UNSPECIFIED;
          case ddl::Function::INVOKER:
            return Udf::SqlSecurity::INVOKER;
        }
      }());
  if (ddl_function.has_sql_body_origin() &&
      !ddl_function.sql_body_origin().original_expression().empty()) {
    builder.set_body_origin(absl::StripAsciiWhitespace(
        ddl_function.sql_body_origin().original_expression()));
  }

  if (ddl_function.has_return_typename() &&
      ddl_function.return_typename() !=
          function_signature->result_type().type()->TypeName(
              zetasql::PRODUCT_EXTERNAL)) {
    return error::FunctionTypeMismatch(
        ddl_function.function_name(), ddl_function.return_typename(),
        function_signature->result_type().type()->TypeName(
            zetasql::PRODUCT_EXTERNAL));
  }
  builder.set_signature(std::move(function_signature));

  builder.set_determinism_level(determinism_level);

  for (auto dependency : dependencies) {
    builder.add_dependency(dependency);
  }

  return builder;
}

absl::StatusOr<View::Builder> SchemaUpdaterImpl::CreateFunctionBuilder(
    const ddl::CreateFunction& ddl_function,
    std::vector<View::Column> output_columns,
    absl::flat_hash_set<const SchemaNode*> dependencies) {
  View::Builder builder;
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " to view "
            << ddl_function.function_name();
  }
  builder.set_name(ddl_function.function_name())
      .set_sql_security([&]() {
        switch (ddl_function.sql_security()) {
          case ddl::Function::UNSPECIFIED_SQL_SECURITY:
            return View::SqlSecurity::UNSPECIFIED;
          case ddl::Function::INVOKER:
            return View::SqlSecurity::INVOKER;
        }
      }())
      .set_sql_body(ddl_function.sql_body());

  if (ddl_function.has_sql_body_origin() &&
      !ddl_function.sql_body_origin().original_expression().empty()) {
    builder.set_sql_body_origin(absl::StripAsciiWhitespace(
        ddl_function.sql_body_origin().original_expression()));
  }

  for (auto output_column : output_columns) {
    builder.add_column(std::move(output_column));
  }

  for (const auto& dependency : dependencies) {
    builder.add_dependency(dependency);
  }
  return builder;
}

absl::Status SchemaUpdaterImpl::CreateFunction(
    const ddl::CreateFunction& ddl_function) {
  if (ddl_function.function_kind() == ddl::Function::VIEW &&
      latest_schema_->views().size() >= limits::kMaxViewsPerDatabase) {
    return error::TooManyViewsPerDatabase(ddl_function.function_name(),
                                          limits::kMaxViewsPerDatabase);
  }

  if (!ddl_function.has_sql_security() ||
      ddl_function.sql_security() != ddl::Function::INVOKER) {
    return ddl_function.function_kind() == ddl::Function::FUNCTION
               ? error::FunctionRequiresInvokerSecurity(
                     ddl_function.function_name())
               : error::ViewRequiresInvokerSecurity(
                     ddl_function.function_name());
  }

  bool replace = false;
  if (global_names_.HasName(ddl_function.function_name())) {
    replace = CanFunctionReplaceTakenName(ddl_function);
    if (!replace) {
      return error::SchemaObjectAlreadyExists(
          GetFunctionKindAsString(ddl_function), ddl_function.function_name());
    }
  }

  if (!replace) {
    ZETASQL_RETURN_IF_ERROR(global_names_.AddName(GetFunctionKindAsString(ddl_function),
                                          ddl_function.function_name()));
  }

  if (IsBuiltInFunction(ddl_function.function_name())) {
    return error::ReplacingBuiltInFunction(
        ddl_function.is_or_replace() ? "create or replace" : "create",
        GetFunctionKindAsString(ddl_function), ddl_function.function_name());
  }

  absl::Status error;
  absl::flat_hash_set<const SchemaNode*> dependencies;
  if (ddl_function.function_kind() == ddl::Function::FUNCTION) {
    std::unique_ptr<zetasql::FunctionSignature> function_signature;
    Udf::Determinism determinism_level = Udf::Determinism::DETERMINISTIC;
    ZETASQL_RETURN_IF_ERROR(
        AnalyzeFunctionDefinition(ddl_function, replace, &dependencies,
                                  &function_signature, &determinism_level));
    ZETASQL_ASSIGN_OR_RETURN(
        Udf::Builder builder,
        CreateFunctionBuilder(ddl_function, std::move(function_signature),
                              determinism_level, dependencies));
    if (replace) {
      const Udf* existing_udf =
          latest_schema_->FindUdf(ddl_function.function_name());
      // Check for a recursive view by analyzing the transitive set of
      // dependencies, i.e., if the view is a dependency of itself.
      auto transitive_deps =
          GatherTransitiveDependenciesForSchemaNode(dependencies);
      if (std::find_if(transitive_deps.begin(), transitive_deps.end(),
                       [existing_udf](const SchemaNode* dep) {
                         return (dep->As<const Udf>() != nullptr &&
                                 dep->As<const Udf>()->Name() ==
                                     existing_udf->Name());
                       }) != transitive_deps.end()) {
        return error::ViewReplaceRecursive(existing_udf->Name());
      }
      return AlterNode<Udf>(existing_udf,
                            [&](Udf::Editor* editor) -> absl::Status {
                              // Just replace the udf definition completely.
                              // The temp instance inside builder will be
                              // cleaned up when the builder goes out of scope.
                              editor->copy_from(builder.get());
                              return absl::OkStatus();
                            });
    }

    if (SDLObjectName::IsFullyQualifiedName(ddl_function.function_name())) {
      ZETASQL_RETURN_IF_ERROR(AlterInNamedSchema(
          ddl_function.function_name(),
          [&builder](NamedSchema::Editor* editor) -> absl::Status {
            editor->add_udf(builder.get());
            return absl::OkStatus();
          }));
    }

    return AddNode(builder.build());
  } else if (ddl_function.function_kind() == ddl::Function::VIEW) {
    std::vector<View::Column> output_columns;
    ZETASQL_RETURN_IF_ERROR(AnalyzeFunctionDefinition(ddl_function, replace,
                                              &output_columns, &dependencies));
    ZETASQL_ASSIGN_OR_RETURN(
        View::Builder builder,
        CreateFunctionBuilder(ddl_function, std::move(output_columns),
                              dependencies));

    if (replace) {
      const View* existing_view =
          latest_schema_->FindView(ddl_function.function_name());
      // Check for a recursive view by analyzing the transitive set of
      // dependencies, i.e., if the view is a dependency of itself.
      auto transitive_deps =
          GatherTransitiveDependenciesForSchemaNode(dependencies);
      if (std::find_if(transitive_deps.begin(), transitive_deps.end(),
                       [existing_view](const SchemaNode* dep) {
                         return (dep->As<const View>() != nullptr &&
                                 dep->As<const View>()->Name() ==
                                     existing_view->Name());
                       }) != transitive_deps.end()) {
        return error::ViewReplaceRecursive(existing_view->Name());
      }
      return AlterNode<View>(existing_view,
                             [&](View::Editor* editor) -> absl::Status {
                               // Just replace the view definition completely.
                               // The temp instance inside builder will be
                               // cleaned up when the builder goes out of scope.
                               editor->copy_from(builder.get());
                               return absl::OkStatus();
                             });
    }
    // No need to account for the named schema in the replace case.
    if (SDLObjectName::IsFullyQualifiedName(ddl_function.function_name())) {
      ZETASQL_RETURN_IF_ERROR(AlterInNamedSchema(
          ddl_function.function_name(),
          [&builder](NamedSchema::Editor* editor) -> absl::Status {
            editor->add_view(builder.get());
            return absl::OkStatus();
          }));
    }

    return AddNode(builder.build());
  }
  return absl::OkStatus();  // ERROR FOR UNSUPPORTED VIEW TYPE
}

template <typename SequenceModifier>
absl::Status SchemaUpdaterImpl::SetSequenceOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    SequenceModifier* modifier) {
  // We are only setting the sequence options as we see them here. We validate
  // the options in the function `ValidateSequenceOptions`.
  for (const ddl::SetOption& option : set_options) {
    if (option.option_name() == kSequenceKindOptionName) {
      modifier->set_sequence_kind(Sequence::BIT_REVERSED_POSITIVE);
    } else if (option.option_name() == kSequenceStartWithCounterOptionName) {
      if (option.has_int64_value()) {
        modifier->set_start_with_counter(option.int64_value());
      } else {
        modifier->clear_start_with_counter();
      }
    } else if (option.option_name() == kSequenceSkipRangeMinOptionName) {
      if (option.has_int64_value()) {
        modifier->set_skip_range_min(option.int64_value());
      } else {
        modifier->clear_skip_range_min();
      }
    } else if (option.option_name() == kSequenceSkipRangeMaxOptionName) {
      if (option.has_int64_value()) {
        modifier->set_skip_range_max(option.int64_value());
      } else {
        modifier->clear_skip_range_max();
      }
    } else {
      return error::UnsupportedSequenceOption(option.option_name());
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::ValidateSequenceOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    const Sequence* current_sequence) {
  absl::flat_hash_map<std::string, std::optional<int64_t>> int64_options;
  int64_options.reserve(3);
  for (const ddl::SetOption& option : set_options) {
    if (option.option_name() == kSequenceKindOptionName) {
      if (!option.has_string_value()) {
        return error::InvalidSequenceOptionValue(kSequenceKindOptionName,
                                                 "string");
      }
      if (option.string_value() != kSequenceKindBitReversedPositive) {
        return error::UnsupportedSequenceKind(option.string_value());
      }
    } else if (option.option_name() == kSequenceStartWithCounterOptionName ||
               option.option_name() == kSequenceSkipRangeMinOptionName ||
               option.option_name() == kSequenceSkipRangeMaxOptionName) {
      // In this block, we're only assigning values to the temporary
      // `int64_options` map as we see them. We check for their validity
      // after this for loop.

      // Checking for null value here is necessary, since a sequence can be
      // altered to clear the option.
      if (option.has_null_value()) {
        int64_options[option.option_name()] = std::nullopt;
      } else {
        if (!option.has_int64_value()) {
          return error::InvalidSequenceOptionValue(option.option_name(),
                                                   "integer");
        }
        int64_options[option.option_name()] = option.int64_value();
      }
    } else {
      return error::UnsupportedSequenceOption(option.option_name());
    }
  }

  if (int64_options.contains(kSequenceStartWithCounterOptionName)) {
    if (int64_options[kSequenceStartWithCounterOptionName].has_value() &&
        int64_options[kSequenceStartWithCounterOptionName].value() < 1) {
      return error::InvalidSequenceStartWithCounterValue();
    }
  }

  std::optional<int64_t> skip_range_min, skip_range_max;
  if (current_sequence != nullptr) {
    // We are altering the sequence to set options. So we're loading the
    // current values of these options to compare against the new values we're
    // seeing.
    skip_range_min = current_sequence->skip_range_min();
    skip_range_max = current_sequence->skip_range_max();
  }
  // In this condition and below, if skip_range_min (or skip_range_max) is
  // explicitly set to null, we are overwriting its current value here.
  if (int64_options.contains(kSequenceSkipRangeMinOptionName)) {
    skip_range_min = int64_options[kSequenceSkipRangeMinOptionName];
  }
  if (int64_options.contains(kSequenceSkipRangeMaxOptionName)) {
    skip_range_max = int64_options[kSequenceSkipRangeMaxOptionName];
  }

  if (skip_range_min.has_value() != skip_range_max.has_value()) {
    return error::SequenceSkipRangeMinMaxNotSetTogether();
  }
  if (!skip_range_min.has_value()) {
    // Skipped range is not set, safely return.
    return absl::OkStatus();
  }
  if (skip_range_min.value() > skip_range_max.value()) {
    return error::SequenceSkipRangeMinLargerThanMax();
  }
  if (skip_range_max.value() < 1) {
    return error::SequenceSkippedRangeHasAtleastOnePositiveNumber();
  }

  return absl::OkStatus();
}

bool SchemaUpdaterImpl::IsDefaultSequenceKindSet() {
  const DatabaseOptions* db_options = latest_schema_->options();
  return db_options != nullptr &&
         db_options->default_sequence_kind().has_value() &&
         db_options->default_sequence_kind().value() ==
             kSequenceKindBitReversedPositive;
}

void SchemaUpdaterImpl::SetOptionsForSequenceClauses(
    const ddl::CreateSequence& create_sequence,
    ::google::protobuf::RepeatedPtrField<ddl::SetOption>* set_options) {
  ddl::SetOption* option;
  if (create_sequence.has_type() &&
      create_sequence.type() == ddl::CreateSequence::BIT_REVERSED_POSITIVE) {
    option = set_options->Add();
    option->set_option_name(kSequenceKindOptionName);
    option->set_string_value(kSequenceKindBitReversedPositive);
  }
  if (create_sequence.has_start_with_counter()) {
    option = set_options->Add();
    option->set_option_name(kSequenceStartWithCounterOptionName);
    option->set_int64_value(create_sequence.start_with_counter());
  }
  if (create_sequence.has_skip_range_min()) {
    option = set_options->Add();
    option->set_option_name(kSequenceSkipRangeMinOptionName);
    option->set_int64_value(create_sequence.skip_range_min());
    option = set_options->Add();
    option->set_option_name(kSequenceSkipRangeMaxOptionName);
    option->set_int64_value(create_sequence.skip_range_max());
  }
}

absl::StatusOr<const Sequence*> SchemaUpdaterImpl::CreateSequence(
    const ddl::CreateSequence& create_sequence,
    const database_api::DatabaseDialect& dialect, bool is_internal_use) {
  // Validate the sequence name in global_names_
  ZETASQL_RETURN_IF_ERROR(
      global_names_.AddName("Sequence", create_sequence.sequence_name()));

  Sequence::Builder builder;
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " to sequence "
            << create_sequence.sequence_name();
  }
  builder.set_name(create_sequence.sequence_name());
  absl::BitGen bitgen;
  builder.set_id(absl::StrCat(
      "seq_",
      zetasql::functions::GenerateUuid(bitgen)
      ));
  ::google::protobuf::RepeatedPtrField<ddl::SetOption> clause_options;
  if (dialect == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    bool created_from_syntax = create_sequence.has_type() ||
                               create_sequence.has_start_with_counter() ||
                               create_sequence.has_skip_range_min();

    if (created_from_syntax && !create_sequence.set_options().empty()) {
      return error::CannotSetSequenceClauseAndOptionTogether(
          create_sequence.sequence_name());
    }
    if (created_from_syntax) {
      SetOptionsForSequenceClauses(create_sequence, &clause_options);
      builder.set_created_from_syntax();
    }
  }

  if (!create_sequence.set_options().empty()) {
    builder.set_created_from_options();
  }

  if (is_internal_use) {
    builder.set_internal_use();
  }
  const Sequence* sequence = builder.get();

  // Validate and set sequence options.
  const auto& set_options =
      clause_options.empty() ? create_sequence.set_options() : clause_options;
  ZETASQL_RETURN_IF_ERROR(ValidateSequenceOptions(set_options,
                                          /*current_sequence=*/nullptr));
  ZETASQL_RETURN_IF_ERROR(AlterNode<Sequence>(
      sequence, [this, set_options](Sequence::Editor* editor) -> absl::Status {
        bool has_sequence_kind_option = false;
        for (const ddl::SetOption& option : set_options) {
          if (option.option_name() == kSequenceKindOptionName) {
            has_sequence_kind_option = true;
            break;
          }
        }
        if (!has_sequence_kind_option) {
          if (IsDefaultSequenceKindSet()) {
            editor->set_sequence_kind(Sequence::BIT_REVERSED_POSITIVE);
            editor->set_use_default_sequence_kind_option();
          } else {
            return error::UnspecifiedSequenceKind();
          }
        }
        // Set sequence options
        return SetSequenceOptions(set_options, editor);
      }));

  if (SDLObjectName::IsFullyQualifiedName(create_sequence.sequence_name())) {
    ZETASQL_RETURN_IF_ERROR(AlterInNamedSchema(
        create_sequence.sequence_name(),
        [&sequence](NamedSchema::Editor* editor) -> absl::Status {
          editor->add_sequence(sequence);
          return absl::OkStatus();
        }));
  }

  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return sequence;
}

absl::Status SchemaUpdaterImpl::CreateNamedSchema(
    const ddl::CreateSchema& create_schema) {
  if (latest_schema_->FindNamedSchema(create_schema.schema_name()) != nullptr &&
      create_schema.existence_modifier() == ddl::IF_NOT_EXISTS) {
    return absl::OkStatus();
  }
  // Validate the named schema name in global_names_
  ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Schema", create_schema.schema_name()));

  NamedSchema::Builder builder;
  builder.set_name(create_schema.schema_name());
  std::optional<uint32_t> oid = pg_oid_assigner_->GetNextPostgresqlOid();
  builder.set_postgresql_oid(oid);
  if (oid.has_value()) {
    ZETASQL_VLOG(2) << "Assigned oid " << oid.value() << " to named schema "
            << create_schema.schema_name();
  }
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::CreateLocalityGroup(
    const ddl::CreateLocalityGroup& create_locality_group) {
  if (global_names_.HasName(create_locality_group.locality_group_name()) &&
      create_locality_group.existence_modifier() == ddl::IF_NOT_EXISTS) {
    return absl::OkStatus();
  }
  if (create_locality_group.locality_group_name() ==
      ddl::kDefaultLocalityGroupName) {
    return error::CreatingDefaultLocalityGroup();
  }
  if (absl::StartsWith(create_locality_group.locality_group_name(), "_")) {
    return error::InvalidLocalityGroupName(
        create_locality_group.locality_group_name());
  }

  // Validate the locality group name in global_names_
  ZETASQL_RETURN_IF_ERROR(global_names_.AddName(
      "LocalityGroup", create_locality_group.locality_group_name()));

  LocalityGroup::Builder builder;

  builder.set_name(create_locality_group.locality_group_name());
  const LocalityGroup* locality_group = builder.get();

  // Validate and set locality group options.
  const auto& set_options = create_locality_group.set_options();
  if (!set_options.empty()) {
    ZETASQL_RETURN_IF_ERROR(AlterNode<LocalityGroup>(
        locality_group,
        [this, set_options](LocalityGroup::Editor* editor) -> absl::Status {
          // Set locality group options
          return SetLocalityGroupOptions(set_options, editor);
        }));
  }
  // Add the locality group to the schema.
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));

  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterRowDeletionPolicy(
    std::optional<ddl::RowDeletionPolicy> row_deletion_policy,
    const Table* table) {
  return AlterNode(table, [&](Table::Editor* editor) {
    editor->set_row_deletion_policy(row_deletion_policy);
    return absl::OkStatus();
  });
}

absl::Status SchemaUpdaterImpl::ValidateAlterDatabaseOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options) {
  for (const ddl::SetOption& option : set_options) {
    absl::string_view option_name =
        absl::StripPrefix(option.option_name(), "spanner.internal.cloud_");
    if (option_name == ddl::kWitnessLocationOptionName) {
      if (option.has_string_value()) {
        continue;
      } else if (option.has_null_value()) {
        return error::NullValueAlterDatabaseOption();
      }
    } else if (option_name == ddl::kDefaultLeaderOptionName) {
      if (option.has_string_value()) {
        continue;
      } else if (option.has_null_value()) {
        return error::NullValueAlterDatabaseOption();
      }
    } else if (option_name == ddl::kDefaultSequenceKindOptionName) {
      if (option.has_string_value() || option.has_null_value()) {
        if (latest_schema_->options() != nullptr &&
            latest_schema_->options()->default_sequence_kind().has_value()) {
          return error::DefaultSequenceKindAlreadySet();
        }
        if (option.has_string_value() &&
            option.string_value() != kSequenceKindBitReversedPositive) {
          return error::UnsupportedSequenceKind(option.string_value());
        }
      } else {
        return error::UnsupportedDefaultSequenceKindOptionValues();
      }
    } else if (option_name == ddl::kDefaultTimeZoneOptionName) {
      if (option.has_string_value() || option.has_null_value()) {
        if (!latest_schema_->tables().empty()) {
          return error::ChangeDefaultTimeZoneOnNonEmptyDatabase();
        }
        absl::TimeZone time_zone;
        if (option.has_string_value() &&
            !absl::LoadTimeZone(option.string_value(), &time_zone)) {
          return error::InvalidDefaultTimeZoneOption(option.string_value());
        }
      } else {
        return error::UnsupportedDefaultTimeZoneOptionValues();
      }
    } else if (option_name == ddl::kVersionRetentionPeriodOptionName) {
      if (option.has_string_value() || option.has_null_value()) {
        continue;
      }

      return error::UnsupportedVersionRetentionPeriodOptionValues();
    } else {
      return error::UnsupportedAlterDatabaseOption(option_name);
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::RenameTo(
    const ddl::AlterTable::RenameTo& rename_to, const Table* table) {
  ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Table", rename_to.name()));
  global_names_.RemoveName(table->Name());

  const Table* updated_table = nullptr;
  ZETASQL_RETURN_IF_ERROR(AlterNode(table, [&](Table::Editor* editor) {
    editor->set_name(rename_to.name());
    updated_table = editor->get();
    return absl::OkStatus();
  }));

  // Handle synonyms.
  if (!rename_to.synonym().empty()) {
    ZETASQL_RETURN_IF_ERROR(AddSynonym(rename_to.synonym(), updated_table));
  }

  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::RenameTable(
    const ddl::RenameTable& rename_table) {
  for (const ddl::RenameTable::RenameOp& op : rename_table.rename_op()) {
    const std::string& from_name = op.from_name();
    const Table* from_table = latest_schema_->FindTableCaseSensitive(from_name);
    if (from_table == nullptr) {
      return error::TableNotFound(from_name);
    }
    const std::string& to_name = op.to_name();
    ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Table", to_name));
    global_names_.RemoveName(from_name);

    const Table* updated_table = nullptr;
    ZETASQL_RETURN_IF_ERROR(AlterNode(from_table, [&](Table::Editor* editor) {
      editor->set_name(to_name);
      updated_table = editor->get();
      return absl::OkStatus();
    }));
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AddSynonym(const std::string& synonym,
                                           const Table* table) {
  ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Table", synonym));
  const Table* updated_table = nullptr;
  ZETASQL_RETURN_IF_ERROR(AlterNode(table, [&](Table::Editor* editor) {
    editor->set_synonym(synonym);
    updated_table = editor->get();
    return absl::OkStatus();
  }));
  if (SDLObjectName::IsFullyQualifiedName(synonym)) {
    ZETASQL_RETURN_IF_ERROR(AlterInNamedSchema(
        synonym, [&updated_table](NamedSchema::Editor* editor) -> absl::Status {
          editor->add_synonym(updated_table);
          return absl::OkStatus();
        }));
  }

  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::DropSynonym(
    const ddl::AlterTable::DropSynonym& drop_synonym, const Table* table) {
  if (SDLObjectName::IsFullyQualifiedName(drop_synonym.synonym())) {
    ZETASQL_RETURN_IF_ERROR(AlterInNamedSchema(
        drop_synonym.synonym(),
        [&table](NamedSchema::Editor* editor) -> absl::Status {
          editor->drop_synonym(table);
          return absl::OkStatus();
        }));
  }

  global_names_.RemoveName(drop_synonym.synonym());
  return AlterNode(table, [&](Table::Editor* editor) {
    editor->drop_synonym(drop_synonym.synonym());
    return absl::OkStatus();
  });
}

absl::Status SchemaUpdaterImpl::AlterDatabase(
    const ddl::AlterDatabase& alter_database,
    const database_api::DatabaseDialect& dialect) {
  const auto& set_options = alter_database.set_options();
  if (!set_options.options().empty()) {
    ZETASQL_RETURN_IF_ERROR(ValidateAlterDatabaseOptions(set_options.options()));
    const DatabaseOptions* database_options = latest_schema_->options();
    if (database_options == nullptr) {
      DatabaseOptions::Builder builder;
      builder.set_db_name(alter_database.db_name());
      ZETASQL_RETURN_IF_ERROR(
          SetDatabaseOptions(set_options.options(), dialect, &builder));
      ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
    } else {
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& repeated_set_options =
          set_options.options();
      ZETASQL_RETURN_IF_ERROR(AlterNode<DatabaseOptions>(
          database_options,
          [this, repeated_set_options,
           dialect](DatabaseOptions::Editor* editor) -> absl::Status {
            // Set database options
            return SetDatabaseOptions(repeated_set_options, dialect, editor);
          }));
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterChangeStream(
    const ddl::AlterChangeStream& alter_change_stream) {
  const ChangeStream* change_stream = latest_schema_->FindChangeStream(
      alter_change_stream.change_stream_name());
  if (change_stream == nullptr) {
    return error::ChangeStreamNotFound(
        alter_change_stream.change_stream_name());
  }
  std::string change_stream_name = change_stream->Name();
  switch (alter_change_stream.alter_type_case()) {
    case ddl::AlterChangeStream::kSetForClause: {
      ZETASQL_RETURN_IF_ERROR(AlterChangeStreamForClause(
          alter_change_stream.set_for_clause(), change_stream));
      return absl::OkStatus();
    }
    case ddl::AlterChangeStream::kSetOptions: {
      const auto& set_options = alter_change_stream.set_options();
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& repeated_set_options =
          set_options.options();
      ZETASQL_RETURN_IF_ERROR(ValidateChangeStreamOptions(repeated_set_options));
      ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
          change_stream,
          [this,
           repeated_set_options](ChangeStream::Editor* editor) -> absl::Status {
            // Set change stream options
            return SetChangeStreamOptions(repeated_set_options, editor);
          }));
      return absl::OkStatus();
    }
    case ddl::AlterChangeStream::kDropForClause: {
      ZETASQL_RET_CHECK(alter_change_stream.drop_for_clause().all());
      if (!change_stream->for_clause()) {
        return error::AlterChangeStreamDropNonexistentForClause(
            change_stream->Name());
      }
      ZETASQL_RETURN_IF_ERROR(UnregisterChangeStreamFromTrackedObjects(change_stream));
      ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
          change_stream, [](ChangeStream::Editor* editor) -> absl::Status {
            editor->clear_for_clause();
            editor->clear_tracked_tables_columns();
            return absl::OkStatus();
          }));
      return absl::OkStatus();
    }
    default:
      return error::Internal(
          absl::StrCat("Unsupported alter change stream type: ",
                       alter_change_stream.DebugString()));
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterChangeStreamForClause(
    const ddl::ChangeStreamForClause& ddl_change_stream_for_clause,
    const ChangeStream* change_stream) {
  ZETASQL_RETURN_IF_ERROR(ValidateChangeStreamForClause(ddl_change_stream_for_clause,
                                                change_stream->Name()));
  ZETASQL_RETURN_IF_ERROR(ValidateChangeStreamLimits(ddl_change_stream_for_clause,
                                             change_stream->Name()));
  ZETASQL_RETURN_IF_ERROR(UnregisterChangeStreamFromTrackedObjects(change_stream));
  ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
      change_stream, [&](ChangeStream::Editor* editor) -> absl::Status {
        editor->set_for_clause(ddl_change_stream_for_clause);
        editor->set_track_all(ddl_change_stream_for_clause.all());
        return absl::OkStatus();
      }));

  ZETASQL_RETURN_IF_ERROR(
      RegisterTrackedObjects(ddl_change_stream_for_clause, change_stream));
  ZETASQL_RETURN_IF_ERROR(EditChangeStreamTrackedObjects(ddl_change_stream_for_clause,
                                                 change_stream));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterSequence(
    const ddl::AlterSequence& alter_sequence,
    const Sequence* current_sequence) {
  std::string sequence_name = current_sequence->Name();
  if (alter_sequence.alter_type_case() != ddl::AlterSequence::kSetOptions &&
      alter_sequence.alter_type_case() != ddl::AlterSequence::kSetSkipRange &&
      alter_sequence.alter_type_case() !=
          ddl::AlterSequence::kSetStartWithCounter) {
    return error::Internal(
        absl::StrCat("Unsupported alter sequence statement: ",
                     alter_sequence.DebugString()));
  }

  ::google::protobuf::RepeatedPtrField<ddl::SetOption> repeated_set_options;
  bool set_from_syntax = false;
  if (alter_sequence.alter_type_case() == ddl::AlterSequence::kSetOptions) {
    if (current_sequence->created_from_syntax()) {
      return error::CannotSetSequenceClauseAndOptionTogether(sequence_name);
    }
    repeated_set_options = alter_sequence.set_options().options();
  } else if (alter_sequence.alter_type_case() ==
             ddl::AlterSequence::kSetSkipRange) {
    if (current_sequence->created_from_options()) {
      return error::CannotSetSequenceClauseAndOptionTogether(sequence_name);
    }
    ddl::SetOption* skip_range_min = repeated_set_options.Add();
    ddl::SetOption* skip_range_max = repeated_set_options.Add();
    skip_range_min->set_option_name(kSequenceSkipRangeMinOptionName);
    skip_range_max->set_option_name(kSequenceSkipRangeMaxOptionName);
    if (alter_sequence.set_skip_range().has_min_value()) {
      skip_range_min->set_int64_value(
          alter_sequence.set_skip_range().min_value());
      skip_range_max->set_int64_value(
          alter_sequence.set_skip_range().max_value());
    } else {
      skip_range_min->set_null_value(true);
      skip_range_max->set_null_value(true);
    }
    set_from_syntax = true;
  } else if (alter_sequence.alter_type_case() ==
             ddl::AlterSequence::kSetStartWithCounter) {
    if (current_sequence->created_from_options()) {
      return error::CannotSetSequenceClauseAndOptionTogether(sequence_name);
    }
    ddl::SetOption* start_with_counter = repeated_set_options.Add();
    start_with_counter->set_option_name(kSequenceStartWithCounterOptionName);
    start_with_counter->set_int64_value(
        alter_sequence.set_start_with_counter().counter_value());
    set_from_syntax = true;
  }
  ZETASQL_RETURN_IF_ERROR(
      ValidateSequenceOptions(repeated_set_options, current_sequence));
  ZETASQL_RETURN_IF_ERROR(AlterNode<Sequence>(
      current_sequence,
      [this, set_from_syntax,
       repeated_set_options](Sequence::Editor* editor) -> absl::Status {
        if (set_from_syntax) {
          editor->set_created_from_syntax();
        } else {
          editor->set_created_from_options();
        }
        // Set sequence options
        return SetSequenceOptions(repeated_set_options, editor);
      }));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterNamedSchema(
    const ddl::AlterSchema& alter_schema) {
  const NamedSchema* current_named_schema =
      latest_schema_->FindNamedSchema(alter_schema.schema_name());
  if (current_named_schema == nullptr) {
    if (alter_schema.if_exists() == true) {
      return absl::OkStatus();
    }
    return error::NamedSchemaNotFound(alter_schema.schema_name());
  }
  return error::AlterNamedSchemaNotSupported();
}

absl::Status SchemaUpdaterImpl::AlterTable(
    const ddl::AlterTable& alter_table,
    const database_api::DatabaseDialect& dialect) {
  const Table* table =
      latest_schema_->FindTableCaseSensitive(alter_table.table_name());
  if (table == nullptr) {
    return error::TableNotFound(alter_table.table_name());
  }

  switch (alter_table.alter_type_case()) {
    case ddl::AlterTable::kSetInterleaveClause: {
      return AlterSetInterleave(alter_table.set_interleave_clause(), table);
    }
    case ddl::AlterTable::kSetOnDelete: {
      return AlterInterleaveAction(alter_table.set_on_delete().action(), table);
    }
    case ddl::AlterTable::kAddCheckConstraint: {
      if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
        ddl::CheckConstraint mutable_check_constraint =
            alter_table.add_check_constraint().check_constraint();
        ZETASQL_RET_CHECK(mutable_check_constraint.has_expression_origin());
        ZETASQL_ASSIGN_OR_RETURN(ExpressionTranslateResult result,
                         TranslatePostgreSqlExpression(
                             table, /*ddl_create_table=*/nullptr,
                             mutable_check_constraint.expression_origin()
                                 .original_expression()));
        mutable_check_constraint.mutable_expression_origin()
            ->set_original_expression(result.original_postgresql_expression);
        mutable_check_constraint.set_expression(
            result.translated_googlesql_expression);
        return AddCheckConstraint(mutable_check_constraint, table);
      }
      return AddCheckConstraint(
          alter_table.add_check_constraint().check_constraint(), table);
    }
    case ddl::AlterTable::kAddForeignKey: {
      return AddForeignKey(alter_table.add_foreign_key().foreign_key(), table);
    }
    case ddl::AlterTable::kDropConstraint: {
      return DropConstraint(alter_table.drop_constraint().name(), table);
    }
    case ddl::AlterTable::kAddColumn: {
      const auto& column_def = alter_table.add_column().column();
      // If the column exists but IF_NOT_EXISTS is set then we're fine.
      if (table->FindColumn(column_def.column_name()) != nullptr &&
          alter_table.add_column().existence_modifier() == ddl::IF_NOT_EXISTS) {
        return absl::OkStatus();
      }
      ZETASQL_ASSIGN_OR_RETURN(const Column* new_column,
                       CreateColumn(column_def, table, /*ddl_table=*/
                                    nullptr, dialect));
      if (new_column->is_generated()) {
        const_cast<Column*>(new_column)->PopulateDependentColumns();
      }

      if (new_column->is_trackable_by_change_stream()) {
        // Add the newly added column to tracking objects map for each change
        // stream implicitly/explicitly tracking the entire table this column
        // belongs to
        for (const ChangeStream* change_stream : table->change_streams()) {
          ZETASQL_RETURN_IF_ERROR(AlterNode<ChangeStream>(
              change_stream,
              [table, new_column](
                  ChangeStream::Editor* change_stream_editor) -> absl::Status {
                change_stream_editor->add_tracked_table_column(
                    table->Name(), new_column->Name());
                return absl::OkStatus();
              }));
        }
        // Populate the list of change streams tracking this column
        ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(
            new_column, [table](Column::Editor* column_editor) -> absl::Status {
              for (const ChangeStream* change_stream :
                   table->change_streams()) {
                column_editor->add_change_stream(change_stream);
              }
              return absl::OkStatus();
            }));
      }

      ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
          table, [new_column](Table::Editor* editor) -> absl::Status {
            editor->add_column(new_column);
            return absl::OkStatus();
          }));
      return absl::OkStatus();
    }
    case ddl::AlterTable::kAlterColumn: {
      const std::string& column_name =
          alter_table.alter_column().column().column_name();
      const Column* column = table->FindColumn(column_name);
      if (column == nullptr) {
        return error::ColumnNotFound(table->Name(), column_name);
      }
      const auto& alter_column = alter_table.alter_column();
      if (alter_column.has_operation()) {
        if (alter_column.operation() ==
            ddl::AlterTable::AlterColumn::ALTER_IDENTITY) {
          if (!column->is_identity_column()) {
            return error::ColumnIsNotIdentityColumn(table->Name(), column_name);
          }
          ZETASQL_RET_CHECK(column->sequences_used().size() == 1);
          const Sequence* sequence =
              static_cast<const Sequence*>(column->sequences_used().at(0));
          const auto& column_def = alter_column.column();
          ddl::AlterSequence alter_sequence;
          alter_sequence.set_sequence_name(sequence->Name());
          if (alter_column.identity_alter_start_with_counter()) {
            ddl::SetOption* start_with_counter =
                alter_sequence.mutable_set_options()->add_options();
            start_with_counter->set_option_name("start_with_counter");
            start_with_counter->set_int64_value(
                column_def.identity_column().start_with_counter());
          }
          if (alter_column.identity_alter_skip_range()) {
            ddl::SetOption* skip_range_min =
                alter_sequence.mutable_set_options()->add_options();
            ddl::SetOption* skip_range_max =
                alter_sequence.mutable_set_options()->add_options();
            skip_range_min->set_option_name("skip_range_min");
            skip_range_max->set_option_name("skip_range_max");
            if (column_def.identity_column().has_skip_range_min()) {
              skip_range_min->set_int64_value(
                  column_def.identity_column().skip_range_min());
              skip_range_max->set_int64_value(
                  column_def.identity_column().skip_range_max());
            } else {
              skip_range_min->set_null_value(true);
              skip_range_max->set_null_value(true);
            }
          }
          ZETASQL_RETURN_IF_ERROR(AlterSequence(alter_sequence, sequence));
        } else {
          ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(
              column,
              [this, &alter_column, &column, &table,
               &dialect](Column::Editor* editor) -> absl::Status {
                return AlterColumnSetDropDefault(alter_column, table, column,
                                                 dialect, editor);
              }));
        }
      } else {
        const auto& column_def = alter_column.column();
        ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(
            column,
            [this, &column_def, &table,
             dialect](Column::Editor* editor) -> absl::Status {
              return AlterColumnDefinition(column_def, table, dialect, editor);
            }));
      }
      return absl::OkStatus();
    }
    case ddl::AlterTable::kDropColumn: {
      const Column* column = table->FindColumn(alter_table.drop_column());
      if (column == nullptr) {
        return error::ColumnNotFound(table->Name(), alter_table.drop_column());
      }
      if (!column->change_streams_explicitly_tracking_column().empty()) {
        std::vector<std::string> change_stream_names_list;
        for (const ChangeStream* const change_stream :
             column->change_streams()) {
          change_stream_names_list.push_back(change_stream->Name());
        }
        std::string change_stream_names =
            absl::StrJoin(change_stream_names_list, ",");
        return error::DropColumnWithChangeStream(
            table->Name(), column->Name(), change_stream_names_list.size(),
            change_stream_names);
      }
      ZETASQL_RETURN_IF_ERROR(DropNode(column));
      if (column->is_identity_column()) {
        ZETASQL_RET_CHECK(column->sequences_used().size() == 1);
        const Sequence* sequence =
            static_cast<const Sequence*>(column->sequences_used().at(0));
        ZETASQL_RETURN_IF_ERROR(DropSequence(sequence));
      }

      if (column->locality_group() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(AlterNode<LocalityGroup>(
            column->locality_group(),
            [](LocalityGroup::Editor* editor) -> absl::Status {
              editor->decrement_use_count();
              return absl::OkStatus();
            }));
      }

      return absl::OkStatus();
    }
    case ddl::AlterTable::kAddRowDeletionPolicy: {
      const auto& policy = alter_table.add_row_deletion_policy();
      if (!table->row_deletion_policy().has_value()) {
        return AlterRowDeletionPolicy(policy, table);
      } else {
        return error::RowDeletionPolicyAlreadyExists(policy.column_name(),
                                                     table->Name());
      }
    }
    case ddl::AlterTable::kAlterRowDeletionPolicy: {
      const auto& policy = alter_table.alter_row_deletion_policy();
      if (table->row_deletion_policy().has_value()) {
        return AlterRowDeletionPolicy(policy, table);
      } else {
        return error::RowDeletionPolicyDoesNotExist(table->Name());
      }
    }
    case ddl::AlterTable::kDropRowDeletionPolicy: {
      if (table->row_deletion_policy().has_value()) {
        return AlterRowDeletionPolicy(std::nullopt, table);
      } else {
        return error::RowDeletionPolicyDoesNotExist(table->Name());
      }
    }
    case ddl::AlterTable::kRenameTo: {
      const auto& rename_to = alter_table.rename_to();
      return RenameTo(rename_to, table);
    }
    case ddl::AlterTable::kAddSynonym: {
      const auto& add_synonym = alter_table.add_synonym();
      if (table->synonym().empty()) {
        return AddSynonym(add_synonym.synonym(), table);
      } else {
        return error::SynonymAlreadyExists(table->synonym(), table->Name());
      }
    }
    case ddl::AlterTable::kDropSynonym: {
      const auto& drop_synonym = alter_table.drop_synonym();
      if (drop_synonym.synonym() == table->synonym()) {
        return DropSynonym(drop_synonym, table);
      } else {
        return error::SynonymDoesNotExist(drop_synonym.synonym(),
                                          table->Name());
      }
    }
    case ddl::AlterTable::kSetOptions: {
      ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
          table, [this, &alter_table](Table::Editor* editor) -> absl::Status {
            return SetTableOptions(alter_table.set_options().options(), editor);
          }));
      return absl::OkStatus();
    }
    default:
      return error::Internal(absl::StrCat("Unsupported alter table type: ",
                                          alter_table.DebugString()));
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterIndex(const ddl::AlterIndex& alter_index) {
  const Index* index = latest_schema_->FindIndex(alter_index.index_name());

  if (index == nullptr) {
    return error::IndexNotFound(alter_index.index_name());
  }

  const Table* indexed_table = index->indexed_table();
  const Table* indexed_data_table = index->index_data_table();
  ZETASQL_RET_CHECK(indexed_table != nullptr)
      << "No indexed table found for the index ";
  ZETASQL_RET_CHECK(indexed_data_table != nullptr)
      << "No index data table found for the index ";

  switch (alter_index.alter_type_case()) {
    case ddl::AlterIndex::kAddStoredColumn: {
      const std::string& column_name =
          alter_index.add_stored_column().column_name();

      if (indexed_table->FindColumn(column_name) == nullptr) {
        return error::ColumnNotFound(indexed_table->Name(), column_name);
      }

      if (indexed_data_table->FindColumn(column_name) != nullptr) {
        return error::ColumnInIndexAlreadyExists(index->Name(), column_name);
      }

      ZETASQL_ASSIGN_OR_RETURN(const Column* new_index_data_table_column,
                       CreateIndexDataTableColumn(indexed_table, column_name,
                                                  indexed_data_table, false));

      ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
          indexed_data_table,
          [new_index_data_table_column](Table::Editor* editor) -> absl::Status {
            editor->add_column(new_index_data_table_column);
            return absl::OkStatus();
          }));
      ZETASQL_RETURN_IF_ERROR(AlterNode<Index>(
          index,
          [new_index_data_table_column](Index::Editor* editor) -> absl::Status {
            editor->add_stored_column(new_index_data_table_column);
            return absl::OkStatus();
          }));

      statement_context_->AddAction(
          [index, new_index_data_table_column](
              const SchemaValidationContext* context) {
            return BackfillIndexAddedColumn(index, new_index_data_table_column,
                                            context);
          });
      break;
    }
    case ddl::AlterIndex::kDropStoredColumn: {
      const std::string& column_name = alter_index.drop_stored_column();
      auto stored_columns = index->stored_columns();
      if (!absl::c_any_of(stored_columns, [column_name](const Column* c) {
            return c->Name() == column_name;
          })) {
        return error::ColumnNotFoundInIndex(alter_index.index_name(),
                                            column_name);
      }

      const Column* drop_column = indexed_data_table->FindColumn(column_name);
      return DropNode(drop_column);
      break;
    }
    case ddl::AlterIndex::kSetOptions: {
      ZETASQL_RETURN_IF_ERROR(AlterNode<Index>(
          index, [this, &alter_index](Index::Editor* editor) -> absl::Status {
            return SetIndexOptions(alter_index.set_options().options(), editor);
          }));
      return absl::OkStatus();
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Invalid alter index type: "
                       << absl::StrCat(alter_index);
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterVectorIndex(
    const ddl::AlterVectorIndex& alter_index) {
  const Index* index = latest_schema_->FindIndex(alter_index.index_name());

  if (index == nullptr || !index->is_vector_index()) {
    return error::IndexNotFound(alter_index.index_name());
  }

  const Table* indexed_table = index->indexed_table();
  const Table* indexed_data_table = index->index_data_table();
  ZETASQL_RET_CHECK(indexed_table != nullptr)
      << "No indexed table found for the index ";
  ZETASQL_RET_CHECK(indexed_data_table != nullptr)
      << "No index data table found for the index ";

  switch (alter_index.alter_type_case()) {
    case ddl::AlterVectorIndex::kAddStoredColumn: {
      const std::string& column_name =
          alter_index.add_stored_column().column().name();

      if (indexed_table->FindColumn(column_name) == nullptr) {
        return error::VectorIndexStoredColumnNotFound(alter_index.index_name(),
                                                      column_name);
      }
      for (const Column* column : index->stored_columns()) {
        if (column->Name() == column_name) {
          return error::VectorIndexStoredColumnAlreadyExists(
              alter_index.index_name(), column_name);
        }
      }
      if (indexed_table->FindKeyColumn(column_name) != nullptr) {
        return error::VectorIndexStoredColumnIsKey(
            alter_index.index_name(), column_name, indexed_table->Name());
      }
      if (indexed_data_table->FindColumn(column_name) != nullptr) {
        return error::VectorIndexStoredColumnAlreadyPrimaryKey(
            alter_index.index_name(), column_name);
      }

      ZETASQL_ASSIGN_OR_RETURN(const Column* new_index_data_table_column,
                       CreateIndexDataTableColumn(indexed_table, column_name,
                                                  indexed_data_table, false));

      ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
          indexed_data_table,
          [new_index_data_table_column](Table::Editor* editor) -> absl::Status {
            editor->add_column(new_index_data_table_column);
            return absl::OkStatus();
          }));
      ZETASQL_RETURN_IF_ERROR(AlterNode<Index>(
          index,
          [new_index_data_table_column](Index::Editor* editor) -> absl::Status {
            editor->add_stored_column(new_index_data_table_column);
            return absl::OkStatus();
          }));
      break;
    }
    case ddl::AlterVectorIndex::kDropStoredColumn: {
      const std::string& column_name = alter_index.drop_stored_column();
      if (indexed_data_table->FindColumn(column_name) == nullptr) {
        return error::ColumnNotFound(index->Name(), column_name);
      }
      auto stored_columns = index->stored_columns();
      bool is_stored_column = absl::c_any_of(
          stored_columns,
          [column_name](const Column* c) { return c->Name() == column_name; });
      if (!is_stored_column) {
        if (!absl::c_any_of(index->key_columns(),
                            [column_name](const KeyColumn* c) {
                              return c->column()->Name() == column_name;
                            })) {
          return error::ColumnNotFound(index->Name(), column_name);
        }
        return error::VectorIndexNotStoredColumn(alter_index.index_name(),
                                                 column_name);
      }

      const Column* drop_column = indexed_data_table->FindColumn(column_name);
      return DropNode(drop_column);
      break;
    }
    case ddl::AlterVectorIndex::kSetOptions: {
      return error::AlterVectorIndexSetOptionsUnsupported();
    }
    case ddl::AlterVectorIndex::kAlterStoredColumn: {
      return error::AlterVectorIndexStoredColumnUnsupported();
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Invalid alter index type: "
                       << absl::StrCat(alter_index);
  }
  return absl::OkStatus();
}

// Returns whether the given ON DELETE `action` would be valid for the given SDL
// element interleaved in `parent`.
static absl::Status IsOnDeleteValid(const std::string& name,
                                    const Table* parent,
                                    ddl::InterleaveClause interleave_clause) {
  if (!interleave_clause.has_on_delete() ||
      interleave_clause.on_delete() == ddl::InterleaveClause::NO_ACTION) {
    for (const Table* ancestor = parent; ancestor != nullptr;
         ancestor = ancestor->parent()) {
      if (ancestor->row_deletion_policy().has_value()) {
        return error::RowDeletionPolicyOnAncestors(name, ancestor->Name());
      }
      if (ancestor->interleave_type().has_value() &&
          ancestor->interleave_type().value() == Table::InterleaveType::kIn) {
        // If the ancestor is an INTERLEAVE IN table, we don't need to check
        // its ancestors for Row Deletion Policies.
        break;
      }
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterSetInterleave(
    const ddl::AlterTable::SetInterleaveClause& set_interleave_clause,
    const Table* table) {
  if (table->parent() == nullptr ||
      !set_interleave_clause.has_interleave_clause()) {
    // Error if the table is not already interleaved, or the ALTER statement
    // does not have an INTERLEAVE clause.
    return error::ChangeInterleavingNotAllowed(table->Name());
  }

  const ddl::InterleaveClause& new_interleave_clause =
      set_interleave_clause.interleave_clause();

  if (new_interleave_clause.type() == ddl::InterleaveClause::IN &&
      !EmulatorFeatureFlags::instance().flags().enable_interleave_in) {
    return error::InterleaveInNotSupported();
  }
  if (!absl::EqualsIgnoreCase(table->parent()->Name(),
                              new_interleave_clause.table_name())) {
    // Cannot change the interleaving parent.
    return error::ChangeInterleavingTableNotAllowed(table->Name());
  }
  const Table* parent = table->parent();
  if (new_interleave_clause.type() == ddl::InterleaveClause::IN_PARENT) {
    ZETASQL_RETURN_IF_ERROR(
        IsOnDeleteValid(table->Name(), parent, new_interleave_clause));
  }

  if (table->interleave_type().value() == Table::InterleaveType::kIn &&
      new_interleave_clause.type() == ddl::InterleaveClause::IN_PARENT) {
    if (new_interleave_clause.on_delete() == ddl::InterleaveClause::CASCADE) {
      // Direct migration from INTERLEAVE IN to INTERLEAVE IN PARENT ON DELETE
      // CASCADE is not supported.
      return error::InterleaveInToInParentOnDeleteCascadeUnsupported(
          table->Name());
    }
    statement_context_->AddAction(
        [parent, table](const SchemaValidationContext* context) {
          return VerifyInterleaveInParentTableRowsExist(parent, table, context);
        });
  }

  return AlterNode<Table>(table, [&](Table::Editor* editor) {
    if (new_interleave_clause.type() == ddl::InterleaveClause::IN) {
      editor->set_interleave_type(Table::InterleaveType::kIn);
      editor->clear_on_delete();
    } else {
      editor->set_interleave_type(Table::InterleaveType::kInParent);
    }

    if (new_interleave_clause.has_on_delete()) {
      if (new_interleave_clause.on_delete() == ddl::InterleaveClause::CASCADE) {
        editor->set_on_delete(Table::OnDeleteAction::kCascade);
      } else {
        editor->set_on_delete(Table::OnDeleteAction::kNoAction);
      }
    } else {
      editor->clear_on_delete();
    }
    return absl::OkStatus();
  });
}

absl::Status SchemaUpdaterImpl::AlterInterleaveAction(
    const ddl::InterleaveClause::Action& ddl_interleave_action,
    const Table* table) {
  /*if (table->interleave_type() == Table::InterleaveType::kIn) {
    // ON DELETE actions cannot be set on INTERLEAVE IN tables.
    return error::SetOnDeleteOnInterleaveInTables(table->Name());
  }*/

  return AlterNode<Table>(table, [&](Table::Editor* editor) {
    if (ddl_interleave_action == ddl::InterleaveClause::CASCADE) {
      editor->set_on_delete(Table::OnDeleteAction::kCascade);
    } else {
      editor->set_on_delete(Table::OnDeleteAction::kNoAction);
    }
    return absl::OkStatus();
  });
}

absl::StatusOr<const zetasql::Type*>
SchemaUpdaterImpl::GetProtoTypeFromBundle(const zetasql::Type* type,
                                          const ProtoBundle* proto_bundle) {
  ZETASQL_RET_CHECK(type->IsProto() || type->IsEnum());
  auto ddl_type = GoogleSqlTypeToDDLColumnType(type);
  return DDLColumnTypeToGoogleSqlType(ddl_type, type_factory_, proto_bundle);
}

absl::Status SchemaUpdaterImpl::AlterProtoColumnType(
    const Column* column, const ProtoBundle* proto_bundle,
    Column::Editor* editor) {
  const zetasql::Type* type = column->GetType();
  ZETASQL_RET_CHECK(proto_bundle != nullptr &&
            (type->IsProto() || type->IsEnum() || type->IsArray()));

  if (type->IsArray()) {
    const zetasql::Type* element_type = type->AsArray()->element_type();
    ZETASQL_RET_CHECK(element_type->IsProto() || element_type->IsEnum());
    ZETASQL_ASSIGN_OR_RETURN(auto updated_element_type,
                     GetProtoTypeFromBundle(element_type, proto_bundle));
    const zetasql::Type* updated_array_type;
    ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(updated_element_type,
                                                 &updated_array_type));
    editor->set_type(updated_array_type);
    return absl::OkStatus();
  }

  ZETASQL_ASSIGN_OR_RETURN(auto updated_type,
                   GetProtoTypeFromBundle(type, proto_bundle));
  editor->set_type(updated_type);
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterProtoColumnTypes(
    const ProtoBundle* proto_bundle,
    const ddl::AlterProtoBundle& ddl_alter_proto_bundle) {
  ZETASQL_RET_CHECK_NE(proto_bundle, nullptr);
  // Return if no types are being updated
  if (ddl_alter_proto_bundle.update_type().empty()) return absl::OkStatus();
  absl::flat_hash_set<std::string> update_types;
  for (const auto& type : ddl_alter_proto_bundle.update_type()) {
    update_types.insert(type.source_name());
  }
  const auto& tables = latest_schema_->tables();
  for (const auto& table : tables) {
    auto columns = table->columns();
    for (const Column* column : columns) {
      const zetasql::Type* column_type = column->GetType();
      if (column_type->IsArray()) {
        column_type = column_type->AsArray()->element_type();
      }
      if (!column_type->IsProto() && !column_type->IsEnum()) continue;
      // Types not in alter proto bundle do not require edits
      absl::string_view type_name =
          column_type->IsProto()
              ? column_type->AsProto()->descriptor()->full_name()
              : column_type->AsEnum()->enum_descriptor()->full_name();
      if (!update_types.contains(type_name)) {
        continue;
      }
      ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(
          column,
          [this, &column,
           &proto_bundle](Column::Editor* editor) -> absl::Status {
            return AlterProtoColumnType(column, proto_bundle, editor);
          }));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<const ProtoBundle>>
SchemaUpdaterImpl::AlterProtoBundle(
    const ddl::AlterProtoBundle& ddl_alter_proto_bundle,
    absl::string_view proto_descriptor_bytes) {
  if (latest_schema_->proto_bundle()->empty()) {
    return absl::FailedPreconditionError(
        "Proto bundle does not yet exist; cannot alter it");
  }

  ZETASQL_ASSIGN_OR_RETURN(
      auto proto_bundle_builder,
      ProtoBundle::Builder::New(proto_descriptor_bytes,
                                latest_schema_->proto_bundle().get()));
  auto insert_types = ddl_alter_proto_bundle.insert_type();
  std::vector<std::string> insert_type_names;
  insert_type_names.reserve(insert_types.size());
  for (int i = 0; i < insert_types.size(); ++i) {
    insert_type_names.push_back(insert_types.at(i).source_name());
  }
  ZETASQL_RETURN_IF_ERROR(proto_bundle_builder->InsertTypes(insert_type_names));

  auto update_types = ddl_alter_proto_bundle.update_type();
  std::vector<std::string> update_type_names;
  update_type_names.reserve(update_types.size());
  for (int i = 0; i < update_types.size(); ++i) {
    update_type_names.push_back(update_types.at(i).source_name());
  }
  ZETASQL_RETURN_IF_ERROR(proto_bundle_builder->UpdateTypes(update_type_names));

  auto delete_types = ddl_alter_proto_bundle.delete_type();
  ZETASQL_RETURN_IF_ERROR(proto_bundle_builder->DeleteTypes(
      std::vector(delete_types.begin(), delete_types.end())));

  ZETASQL_ASSIGN_OR_RETURN(auto proto_bundle, proto_bundle_builder->Build());
  ZETASQL_RETURN_IF_ERROR(
      AlterProtoColumnTypes(proto_bundle.get(), ddl_alter_proto_bundle));
  return proto_bundle;
}

absl::Status SchemaUpdaterImpl::AlterLocalityGroup(
    const ddl::AlterLocalityGroup& alter_locality_group) {
  const LocalityGroup* locality_group = latest_schema_->FindLocalityGroup(
      alter_locality_group.locality_group_name());
  LocalityGroup::Builder builder;
  if (locality_group == nullptr) {
    // Create default locality group if it does not exist.
    if (alter_locality_group.locality_group_name() ==
        ddl::kDefaultLocalityGroupName) {
      builder.set_name(ddl::kDefaultLocalityGroupName);
      locality_group = builder.get();
      ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
    } else if (alter_locality_group.existence_modifier() == ddl::IF_EXISTS) {
      return absl::OkStatus();
    } else {
      return error::LocalityGroupNotFound(
          alter_locality_group.locality_group_name());
    }
  }

  // Validate and set locality group options.
  const auto& set_options = alter_locality_group.set_options();
  if (!set_options.options().empty()) {
    ZETASQL_RETURN_IF_ERROR(AlterNode<LocalityGroup>(
        locality_group,
        [this, &set_options](LocalityGroup::Editor* editor) -> absl::Status {
          // Set locality group options
          return SetLocalityGroupOptions(set_options.options(), editor);
        }));
  } else {
    return error::AlterLocalityGroupWithoutOptions();
  }

  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AddCheckConstraint(
    const ddl::CheckConstraint& ddl_check_constraint, const Table* table) {
  return AlterNode<Table>(table, [&](Table::Editor* editor) -> absl::Status {
    return CreateCheckConstraint(ddl_check_constraint, table,
                                 /*ddl_create_table=*/nullptr);
  });
}

absl::Status SchemaUpdaterImpl::AddForeignKey(
    const ddl::ForeignKey& ddl_foreign_key, const Table* table) {
  return AlterNode<Table>(table, [&](Table::Editor* editor) -> absl::Status {
    return CreateForeignKeyConstraint(ddl_foreign_key, table);
  });
}

absl::Status SchemaUpdaterImpl::DropConstraint(
    const std::string& constraint_name, const Table* table) {
  // Try each type of constraint supported by ALTER TABLE DROP CONSTRAINT.
  const ForeignKey* foreign_key = table->FindForeignKey(constraint_name);
  if (foreign_key != nullptr) {
    return DropNode(foreign_key);
  }
  const CheckConstraint* check_constraint =
      table->FindCheckConstraint(constraint_name);
  if (check_constraint != nullptr) {
    return DropNode(check_constraint);
  }
  return error::ConstraintNotFound(constraint_name, table->Name());
}

absl::Status SchemaUpdaterImpl::DropTable(const ddl::DropTable& drop_table) {
  const Table* table =
      latest_schema_->FindTableCaseSensitive(drop_table.table_name());
  if (table == nullptr) {
    if (drop_table.existence_modifier() == ddl::IF_EXISTS) {
      return absl::OkStatus();
    }
    return error::TableNotFound(drop_table.table_name());
  }
  if (!table->change_streams_explicitly_tracking_table().empty()) {
    absl::Span<const ChangeStream* const>
        change_streams_explicitly_tracking_table =
            table->change_streams_explicitly_tracking_table();
    std::string change_stream_names;
    for (auto change_stream = change_streams_explicitly_tracking_table.begin();
         change_stream != change_streams_explicitly_tracking_table.end();
         ++change_stream) {
      if (change_stream != change_streams_explicitly_tracking_table.begin()) {
        change_stream_names += ",";
      }
      change_stream_names += (*change_stream)->Name();
    }
    return error::DropTableWithChangeStream(
        table->Name(), change_streams_explicitly_tracking_table.size(),
        change_stream_names);
  }

  if (table->locality_group() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(AlterNode<LocalityGroup>(
        table->locality_group(),
        [](LocalityGroup::Editor* editor) -> absl::Status {
          editor->decrement_use_count();
          return absl::OkStatus();
        }));
  }

  // TODO : Error if any view depends on this table.
  absl::Status status = DropNode(table);
  if (status.ok()) {
    // Drop dependent sequences.
    for (const Column* column : table->columns()) {
      if (column->is_identity_column()) {
        ZETASQL_RET_CHECK(column->sequences_used().size() == 1);
        const Sequence* sequence =
            static_cast<const Sequence*>(column->sequences_used().at(0));
        ZETASQL_RETURN_IF_ERROR(DropSequence(sequence));
      }

      if (column->locality_group() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(AlterNode<LocalityGroup>(
            column->locality_group(),
            [](LocalityGroup::Editor* editor) -> absl::Status {
              editor->decrement_use_count();
              return absl::OkStatus();
            }));
      }
    }
  }
  return status;
}

absl::Status SchemaUpdaterImpl::DropIndex(const ddl::DropIndex& drop_index) {
  const Index* index =
      latest_schema_->FindIndexCaseSensitive(drop_index.index_name());
  if (index == nullptr) {
    if (drop_index.existence_modifier() == ddl::IF_EXISTS) {
      return absl::OkStatus();
    }
    return error::IndexNotFound(drop_index.index_name());
  }

  if (index->locality_group() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(AlterNode<LocalityGroup>(
        index->locality_group(),
        [](LocalityGroup::Editor* editor) -> absl::Status {
          editor->decrement_use_count();
          return absl::OkStatus();
        }));
  }
  return DropNode(index);
}

absl::Status SchemaUpdaterImpl::DropSearchIndex(
    const ddl::DropSearchIndex& drop_search_index) {
  const Index* index =
      latest_schema_->FindIndexCaseSensitive(drop_search_index.index_name());
  if (index == nullptr) {
    if (drop_search_index.existence_modifier() == ddl::IF_EXISTS) {
      return absl::OkStatus();
    }
    return error::IndexNotFound(drop_search_index.index_name());
  }
  if (!index->is_search_index()) {
    return error::IndexNotFound(drop_search_index.index_name());
  }
  return DropNode(index);
}

absl::Status SchemaUpdaterImpl::DropVectorIndex(
    const ddl::DropVectorIndex& drop_vector_index) {
  const Index* index =
      latest_schema_->FindIndexCaseSensitive(drop_vector_index.index_name());
  if (index == nullptr) {
    if (drop_vector_index.existence_modifier() == ddl::IF_EXISTS) {
      return absl::OkStatus();
    }
    return error::IndexNotFound(drop_vector_index.index_name());
  }
  if (!index->is_vector_index()) {
    return error::IndexNotFound(drop_vector_index.index_name());
  }
  return DropNode(index);
}

absl::Status SchemaUpdaterImpl::DropChangeStream(
    const ddl::DropChangeStream& drop_change_stream) {
  const ChangeStream* change_stream =
      latest_schema_->FindChangeStream(drop_change_stream.change_stream_name());
  if (change_stream == nullptr) {
    return error::ChangeStreamNotFound(drop_change_stream.change_stream_name());
  }
  global_names_.RemoveName(change_stream->tvf_name());
  return DropNode(change_stream);
}

absl::Status SchemaUpdaterImpl::DropSequence(const Sequence* drop_sequence) {
  global_names_.RemoveName(drop_sequence->Name());
  drop_sequence->RemoveSequenceFromLastValuesMap();
  ZETASQL_RETURN_IF_ERROR(DropNode(drop_sequence));
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::DropNamedSchema(
    const ddl::DropSchema& drop_schema) {
  const NamedSchema* current_named_schema =
      latest_schema_->FindNamedSchema(drop_schema.schema_name());
  if (current_named_schema == nullptr) {
    if (drop_schema.if_exists() == true) {
      return absl::OkStatus();
    }
    return error::NamedSchemaNotFound(drop_schema.schema_name());
  }
  global_names_.RemoveName(drop_schema.schema_name());
  return DropNode(current_named_schema);
}

absl::Status SchemaUpdaterImpl::DropLocalityGroup(
    const ddl::DropLocalityGroup& drop_locality_group) {
  if (drop_locality_group.locality_group_name() ==
      ddl::kDefaultLocalityGroupName) {
    return error::DroppingDefaultLocalityGroup();
  }
  const LocalityGroup* locality_group = latest_schema_->FindLocalityGroup(
      drop_locality_group.locality_group_name());
  if (locality_group == nullptr) {
    if (drop_locality_group.existence_modifier() == ddl::IF_EXISTS) {
      return absl::OkStatus();
    }
    return error::LocalityGroupNotFound(
        drop_locality_group.locality_group_name());
  }

  if (locality_group->use_count() > 0) {
    return error::DroppingLocalityGroupWithAssignedTableColumn(
        locality_group->Name());
  }

  return DropNode(locality_group);
}

absl::Status SchemaUpdaterImpl::ApplyImplSetColumnOptions(
    const ddl::SetColumnOptions& set_column_options,
    const database_api::DatabaseDialect& dialect) {
  for (const ddl::SetColumnOptions::ColumnPath& path :
       set_column_options.column_path()) {
    const Table* table =
        latest_schema_->FindTableCaseSensitive(path.table_name());
    if (table == nullptr) {
      return error::TableNotFound(path.table_name());
    }
    const Column* column = table->FindColumn(path.column_name());
    if (column == nullptr) {
      return error::ColumnNotFound(path.table_name(), path.column_name());
    }
    ZETASQL_RETURN_IF_ERROR(
        AlterNode<Column>(column,
                          [this, &set_column_options,
                           dialect](Column::Editor* editor) -> absl::Status {
                            return SetColumnOptions(
                                set_column_options.options(), dialect, editor);
                          }));
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::DropFunction(
    const ddl::DropFunction& drop_function) {
  const SchemaNode* node = nullptr;
  if (drop_function.function_kind() == ddl::Function::VIEW) {
    node = latest_schema_->FindViewCaseSensitive(drop_function.function_name());
    if (node == nullptr) {
      if (drop_function.has_existence_modifier() &&
          drop_function.existence_modifier() == ddl::IF_EXISTS) {
        return absl::OkStatus();
      }
      return error::ViewNotFound(drop_function.function_name());
    }
  } else if (drop_function.function_kind() == ddl::Function::FUNCTION) {
    node = latest_schema_->FindUdfCaseSensitive(drop_function.function_name());
    if (node == nullptr) {
      if (drop_function.has_existence_modifier() &&
          drop_function.existence_modifier() == ddl::IF_EXISTS) {
        return absl::OkStatus();
      }
      return error::FunctionNotFound(drop_function.function_name());
    }
  }
  return DropNode(node);
}

absl::StatusOr<Model::ModelColumn> SchemaUpdaterImpl::CreateModelColumn(
    const ddl::ColumnDefinition& ddl_column, const Model* model,
    const ddl::CreateModel* ddl_model,
    const database_api::DatabaseDialect& dialect) {
  const std::string& column_name = ddl_column.column_name();
  ZETASQL_ASSIGN_OR_RETURN(
      const zetasql::Type* column_type,
      DDLColumnTypeToGoogleSqlType(ddl_column, type_factory_,
                                   latest_schema_->proto_bundle().get()));
  if (ddl_column.not_null()) {
    return error::ModelColumnNotNull(model->Name(), column_name);
  }
  if (ddl_column.hidden()) {
    return error::ModelColumnHidden(model->Name(), column_name);
  }
  if (ddl_column.has_length()) {
    return error::ModelColumnLength(model->Name(), column_name);
  }
  if (ddl_column.has_generated_column()) {
    return error::ModelColumnGenerated(model->Name(), column_name);
  }
  if (ddl_column.has_column_default()) {
    return error::ModelColumnDefault(model->Name(), column_name);
  }

  std::optional<bool> is_required;
  for (const auto& option : ddl_column.set_options()) {
    if (option.option_name() == ddl::kModelColumnRequiredOptionName) {
      if (option.has_null_value()) {
        is_required = std::nullopt;
      } else {
        ZETASQL_RET_CHECK(option.has_bool_value())
            << "Option " << ddl::kModelColumnRequiredOptionName
            << " can only take bool value";
        is_required = option.bool_value();
      }
    } else {
      ZETASQL_RET_CHECK_FAIL() << "Invalid column option: " << option.option_name();
    }
  }

  return Model::ModelColumn{
      .name = column_name,
      .type = column_type,
      // Auto discovery doesn't work in Emulator.
      .is_explicit = true,
      .is_required = is_required,
  };
}

absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
SchemaUpdaterImpl::AnalyzeCreatePropertyGraph(
    const ddl::CreatePropertyGraph& ddl_create_property_graph,
    const zetasql::AnalyzerOptions& analyzer_options, Catalog* catalog) {
  std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output;

  // Delegate to ZetaSQL for DDL parsing and analysis.
  ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeStatement(
      ddl_create_property_graph.ddl_body(), analyzer_options, catalog,
      type_factory_, &analyzer_output));

  // Confirm that the analyzed statement is a valid CREATE PROPERTY GRAPH
  // statement.
  const zetasql::ResolvedStatement* stmt =
      analyzer_output->resolved_statement();
  ZETASQL_RET_CHECK(stmt->Is<zetasql::ResolvedCreatePropertyGraphStmt>())
      << "Failed to analyze DDL. Expects a CREATE [OR REPLACE] PROPERTY "
         "GRAPH statement, actual is: "
      << stmt->DebugString();
  return analyzer_output;
}

std::string SchemaUpdaterImpl::GetTimeZone() const {
  std::string time_zone = kDefaultTimeZone;
  if (latest_schema_ != nullptr) {
    time_zone = latest_schema_->default_time_zone();
  }
  return time_zone;
}

absl::Status SchemaUpdaterImpl::CreatePropertyGraph(
    const ddl::CreatePropertyGraph& ddl_create_property_graph,
    const database_api::DatabaseDialect& dialect) {
  const PropertyGraph* existing_graph =
      latest_schema_->FindPropertyGraph(ddl_create_property_graph.name());

  bool replace = false;
  if (existing_graph &&
      ddl_create_property_graph.existence_modifier() == ddl::IF_NOT_EXISTS) {
    return absl::OkStatus();
  } else if (existing_graph && ddl_create_property_graph.existence_modifier() ==
                                   ddl::OR_REPLACE) {
    replace = true;
  } else {
    if (latest_schema_->property_graphs().size() >=
        limits::kMaxPropertyGraphsPerDatabase) {
      return error::TooManyPropertyGraphsPerDatabase(
          ddl_create_property_graph.name(),
          limits::kMaxPropertyGraphsPerDatabase);
    }

    ZETASQL_RETURN_IF_ERROR(global_names_.AddName("PropertyGraph",
                                          ddl_create_property_graph.name()));
  }

  zetasql::AnalyzerOptions analyzer_options =
      MakeGoogleSqlAnalyzerOptions(GetTimeZone());
  analyzer_options.mutable_language()->set_name_resolution_mode(
      zetasql::NAME_RESOLUTION_DEFAULT);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      zetasql::FEATURE_V_1_4_SQL_GRAPH);
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      zetasql::RESOLVED_CREATE_PROPERTY_GRAPH_STMT);
  FunctionCatalog function_catalog(type_factory_);
  function_catalog.SetLatestSchema(latest_schema_);

  // Create a catalog based on 'latest_schema_'for property graph DDL parsing
  // and analysis.
  PreparePropertyGraphCatalog catalog(latest_schema_, &function_catalog,
                                      type_factory_, analyzer_options);
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const zetasql::AnalyzerOutput> analyzer_output,
      AnalyzeCreatePropertyGraph(ddl_create_property_graph, analyzer_options,
                                 &catalog));

  const zetasql::ResolvedCreatePropertyGraphStmt* graph_stmt =
      analyzer_output->resolved_statement()
          ->GetAs<zetasql::ResolvedCreatePropertyGraphStmt>();

  PropertyGraph::Builder property_graph_builder;
  ZETASQL_RETURN_IF_ERROR(PopulatePropertyGraph(ddl_create_property_graph, graph_stmt,
                                        &property_graph_builder));

  const PropertyGraph* graph = property_graph_builder.get();
  if (replace) {
    return AlterNode<PropertyGraph>(
        existing_graph, [&](PropertyGraph::Editor* editor) -> absl::Status {
          editor->copy_from(graph);
          return absl::OkStatus();
        });
  } else {
    return AddNode(property_graph_builder.build());
  }
}

absl::StatusOr<std::string> GetColumnNameFromExpr(
    const zetasql::ResolvedExpr* expr) {
  if (expr->Is<zetasql::ResolvedColumnRef>()) {
    return expr->GetAs<zetasql::ResolvedColumnRef>()->column().name();
  }
  if (expr->Is<zetasql::ResolvedCatalogColumnRef>()) {
    return expr->GetAs<zetasql::ResolvedCatalogColumnRef>()->column()->Name();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Expected ResolvedColumnRef or ResolvedCatalogColumnRef."
                   " Found ",
                   expr->DebugString()));
}

absl::Status SetNodeReference(
    const zetasql::ResolvedGraphNodeTableReference* node_reference,
    bool is_source, PropertyGraph::GraphElementTable* new_element_table) {
  std::vector<std::string> node_table_column_names;
  std::vector<std::string> edge_table_column_names;
  for (int i = 0; i < node_reference->edge_table_column_list_size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::string edge_column_name,
        GetColumnNameFromExpr(node_reference->edge_table_column_list(i)));
    ZETASQL_ASSIGN_OR_RETURN(
        std::string node_column_name,
        GetColumnNameFromExpr(node_reference->node_table_column_list(i)));
    edge_table_column_names.push_back(std::move(edge_column_name));
    node_table_column_names.push_back(std::move(node_column_name));
  }
  if (is_source) {
    new_element_table->set_source_node_reference(
        node_reference->node_table_identifier(), node_table_column_names,
        edge_table_column_names);
  } else {
    new_element_table->set_target_node_reference(
        node_reference->node_table_identifier(), node_table_column_names,
        edge_table_column_names);
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AddGraphElementTable(
    const zetasql::ResolvedGraphElementTable* element, bool is_node,
    PropertyGraph::Builder* graph_builder) {
  const zetasql::ResolvedTableScan* table_scan =
      element->input_scan()->GetAs<zetasql::ResolvedTableScan>();
  PropertyGraph::GraphElementTable new_element_table;
  new_element_table.set_name(table_scan->table()->Name());
  new_element_table.set_alias(element->alias());

  for (const auto& key : element->key_list()) {
    ZETASQL_ASSIGN_OR_RETURN(std::string key_column_name,
                     GetColumnNameFromExpr(key.get()));
    new_element_table.add_key_clause_column(std::move(key_column_name));
  }
  for (const auto& label : element->label_name_list()) {
    new_element_table.add_label_name(label);
  }
  for (const auto& property_definition : element->property_definition_list()) {
    new_element_table.add_property_definition(
        property_definition->property_declaration_name(),
        property_definition->sql());
  }

  if (is_node) {
    new_element_table.set_element_kind(PropertyGraph::GraphElementKind::NODE);
    graph_builder->add_node_table(new_element_table);
    return absl::OkStatus();
  }

  new_element_table.set_element_kind(PropertyGraph::GraphElementKind::EDGE);
  ZETASQL_RETURN_IF_ERROR(SetNodeReference(element->source_node_reference(),
                                   /*is_source=*/true, &new_element_table));
  ZETASQL_RETURN_IF_ERROR(SetNodeReference(element->dest_node_reference(),
                                   /*is_source=*/false, &new_element_table));

  graph_builder->add_edge_table(new_element_table);
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::PopulatePropertyGraph(
    const ddl::CreatePropertyGraph& ddl_create_property_graph,
    const zetasql::ResolvedCreatePropertyGraphStmt* const graph_stmt,
    PropertyGraph::Builder* graph_builder) {
  // Property graph name and ddl body.
  graph_builder->set_name(ddl_create_property_graph.name());
  graph_builder->set_ddl_body(ddl_create_property_graph.ddl_body());

  // Property declarations.
  for (const auto& property : graph_stmt->property_declaration_list()) {
    PropertyGraph::PropertyDeclaration property_declaration;
    property_declaration.name = property->name();
    property_declaration.type = property->type()->TypeName(
        zetasql::PRODUCT_EXTERNAL, /*use_external_float32_unused=*/true);
    graph_builder->add_property_declaration(property_declaration);
  }

  // Labels.
  for (const auto& label : graph_stmt->label_list()) {
    PropertyGraph::Label label_declaration;
    label_declaration.name = label->name();
    for (const auto& property_declaration_name :
         label->property_declaration_name_list()) {
      label_declaration.property_names.insert(property_declaration_name);
    }
    graph_builder->add_label(label_declaration);
  }
  // Node tables.
  for (const auto& node_table : graph_stmt->node_table_list()) {
    ZETASQL_RETURN_IF_ERROR(AddGraphElementTable(node_table.get(), /*is_node=*/true,
                                         graph_builder));
  }
  // Edge tables.
  for (const auto& edge_table : graph_stmt->edge_table_list()) {
    ZETASQL_RETURN_IF_ERROR(AddGraphElementTable(edge_table.get(), /*is_node=*/false,
                                         graph_builder));
  }

  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::CreateModel(
    const ddl::CreateModel& ddl_model,
    const database_api::DatabaseDialect& dialect) {
  const Model* existing_model =
      latest_schema_->FindModel(ddl_model.model_name());

  bool replace = false;
  if (existing_model && ddl_model.existence_modifier() == ddl::IF_NOT_EXISTS) {
    return absl::OkStatus();
  } else if (existing_model &&
             ddl_model.existence_modifier() == ddl::OR_REPLACE) {
    replace = true;
  } else {
    if (latest_schema_->models().size() >= limits::kMaxModelsPerDatabase) {
      return error::TooManyModelsPerDatabase(ddl_model.model_name(),
                                             limits::kMaxModelsPerDatabase);
    }

    ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Model", ddl_model.model_name()));
  }

  Model::Builder builder;
  builder.set_name(ddl_model.model_name());
  for (const ddl::ColumnDefinition& input_column : ddl_model.input()) {
    ZETASQL_ASSIGN_OR_RETURN(
        Model::ModelColumn column,
        CreateModelColumn(input_column, builder.get(), &ddl_model, dialect));
    builder.add_input_column(column);
  }

  for (const ddl::ColumnDefinition& output_column : ddl_model.output()) {
    ZETASQL_ASSIGN_OR_RETURN(
        Model::ModelColumn column,
        CreateModelColumn(output_column, builder.get(), &ddl_model, dialect));
    builder.add_output_column(column);
  }
  builder.set_remote(ddl_model.remote());
  ZETASQL_RETURN_IF_ERROR(SetModelOptions(ddl_model.set_options(), &builder));
  const Model* model = builder.get();

  if (replace) {
    return AlterNode<Model>(existing_model,
                            [&](Model::Editor* editor) -> absl::Status {
                              editor->copy_from(model);
                              return absl::OkStatus();
                            });
  } else {
    return AddNode(builder.build());
  }
}

absl::Status SchemaUpdaterImpl::AlterModel(const ddl::AlterModel& alter_model) {
  const Model* model = latest_schema_->FindModel(alter_model.model_name());
  if (model == nullptr) {
    if (alter_model.if_exists()) {
      return absl::OkStatus();
    }
    return error::ModelNotFound(alter_model.model_name());
  }

  const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& options =
      alter_model.set_options().options();
  ZETASQL_RETURN_IF_ERROR(AlterNode<Model>(
      model, [this, options](Model::Editor* editor) -> absl::Status {
        return SetModelOptions(options, editor);
      }));
  return absl::OkStatus();
}

template <typename ModelModifier>
absl::Status SchemaUpdaterImpl::SetModelOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    ModelModifier* modifier) {
  for (const ddl::SetOption& option : set_options) {
    if (option.option_name() == ddl::kModelDefaultBatchSizeOptionName) {
      if (option.has_null_value()) {
        modifier->set_default_batch_size(std::nullopt);
      } else {
        ZETASQL_RET_CHECK(option.has_int64_value())
            << "Option " << ddl::kModelDefaultBatchSizeOptionName
            << " can only take int64_t value";
        modifier->set_default_batch_size(option.int64_value());
      }
    } else if (option.option_name() == ddl::kModelEndpointOptionName) {
      if (option.has_null_value()) {
        modifier->set_endpoint(std::nullopt);
      } else {
        ZETASQL_RET_CHECK(option.has_string_value())
            << "Option " << ddl::kModelEndpointOptionName
            << " can only take string value";
        modifier->set_endpoint(option.string_value());
      }
    } else if (option.option_name() == ddl::kModelEndpointsOptionName) {
      if (option.has_null_value()) {
        modifier->set_endpoints({});
      } else {
        ZETASQL_RET_CHECK(!option.string_list_value().empty())
            << "Option " << ddl::kModelEndpointsOptionName
            << " can only take string list value";
        modifier->set_endpoints({option.string_list_value().begin(),
                                 option.string_list_value().end()});
      }
    } else {
      ZETASQL_RET_CHECK_FAIL() << "Invalid column option: " << option.option_name();
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::DropModel(const ddl::DropModel& drop_model) {
  const Model* model = latest_schema_->FindModel(drop_model.model_name());
  if (model == nullptr) {
    if (drop_model.if_exists()) {
      return absl::OkStatus();
    }
    return error::ModelNotFound(drop_model.model_name());
  }
  return DropNode(model);
}

absl::Status SchemaUpdaterImpl::DropPropertyGraph(
    const ddl::DropPropertyGraph& ddl_drop_property_graph) {
  const PropertyGraph* graph =
      latest_schema_->FindPropertyGraph(ddl_drop_property_graph.name());
  if (graph == nullptr) {
    if (ddl_drop_property_graph.existence_modifier() == ddl::IF_EXISTS) {
      return absl::OkStatus();
    }
    return error::PropertyGraphNotFound(ddl_drop_property_graph.name());
  }
  return DropNode(graph);
}

absl::StatusOr<std::shared_ptr<const ProtoBundle>>
SchemaUpdaterImpl::DropProtoBundle() {
  if (latest_schema_->proto_bundle()->empty()) {
    return absl::FailedPreconditionError(
        "Proto bundle does not yet exist; cannot drop it");
  }
  return ProtoBundle::CreateEmpty();
}

const Schema* EmptySchema(std::string_view database_id,
                          database_api::DatabaseDialect dialect) {
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    static const Schema* empty_pg_schema =
        new Schema(SchemaGraph::CreateEmpty(), ProtoBundle::CreateEmpty(),
                   dialect, database_id);
    return empty_pg_schema;
  }
  static const Schema* empty_gsql_schema = new Schema;
  return empty_gsql_schema;
}

}  // namespace

absl::StatusOr<std::unique_ptr<const Schema>>
SchemaUpdater::ValidateSchemaFromDDL(
    const SchemaChangeOperation& schema_change_operation,
    const SchemaChangeContext& context, const Schema* existing_schema) {
  if (existing_schema == nullptr) {
    existing_schema = EmptySchema(context.database_id,
                                  schema_change_operation.database_dialect);
  }
  ZETASQL_ASSIGN_OR_RETURN(SchemaUpdaterImpl updater,
                   SchemaUpdaterImpl::Build(
                       context.type_factory, context.table_id_generator,
                       context.column_id_generator, context.storage,
                       context.schema_change_timestamp, context.pg_oid_assigner,
                       existing_schema, context.database_id));
  context.pg_oid_assigner->BeginAssignment();
  ZETASQL_ASSIGN_OR_RETURN(pending_work_,
                   updater.ApplyDDLStatements(schema_change_operation));
  intermediate_schemas_ = updater.GetIntermediateSchemas();

  std::unique_ptr<const Schema> new_schema = nullptr;
  if (!intermediate_schemas_.empty()) {
    new_schema = std::move(*intermediate_schemas_.rbegin());
    ZETASQL_RETURN_IF_ERROR(context.pg_oid_assigner->EndAssignment());
  }
  pending_work_.clear();
  intermediate_schemas_.clear();
  return new_schema;
}

// TODO : These should run in a ReadWriteTransaction with rollback
// capability so that changes to the database can be reversed.
absl::Status SchemaUpdater::RunPendingActions(int* num_succesful) {
  for (const auto& pending_statement : pending_work_) {
    ZETASQL_RETURN_IF_ERROR(pending_statement.RunSchemaChangeActions());
    ++(*num_succesful);
  }
  return absl::OkStatus();
}

absl::StatusOr<SchemaChangeResult> SchemaUpdater::UpdateSchemaFromDDL(
    const Schema* existing_schema,
    const SchemaChangeOperation& schema_change_operation,
    const SchemaChangeContext& context) {
  ZETASQL_ASSIGN_OR_RETURN(SchemaUpdaterImpl updater,
                   SchemaUpdaterImpl::Build(
                       context.type_factory, context.table_id_generator,
                       context.column_id_generator, context.storage,
                       context.schema_change_timestamp, context.pg_oid_assigner,
                       existing_schema, context.database_id));
  context.pg_oid_assigner->BeginAssignment();
  ZETASQL_ASSIGN_OR_RETURN(pending_work_,
                   updater.ApplyDDLStatements(schema_change_operation));
  intermediate_schemas_ = updater.GetIntermediateSchemas();

  // Use the schema snapshot for the last succesful statement.
  int num_successful = 0;
  std::unique_ptr<const Schema> new_schema = nullptr;

  absl::Status backfill_status = RunPendingActions(&num_successful);
  if (num_successful > 0) {
    new_schema = std::move(intermediate_schemas_[num_successful - 1]);
    ZETASQL_RETURN_IF_ERROR(context.pg_oid_assigner->EndAssignmentAtIntermediateSchema(
        num_successful - 1));
  }
  ZETASQL_RET_CHECK_LE(num_successful, intermediate_schemas_.size());
  return SchemaChangeResult{
      .num_successful_statements = num_successful,
      .updated_schema = std::move(new_schema),
      .backfill_status = backfill_status,
  };
}

absl::StatusOr<std::unique_ptr<const Schema>>
SchemaUpdater::CreateSchemaFromDDL(
    const SchemaChangeOperation& schema_change_operation,
    const SchemaChangeContext& context) {
  ZETASQL_ASSIGN_OR_RETURN(
      SchemaChangeResult result,
      UpdateSchemaFromDDL(EmptySchema(context.database_id,
                                      schema_change_operation.database_dialect),
                          schema_change_operation, context));
  ZETASQL_RETURN_IF_ERROR(result.backfill_status);
  return std::move(result.updated_schema);
}

absl::StatusOr<std::unique_ptr<ddl::DDLStatement>> ParseDDLByDialect(
    absl::string_view statement, database_api::DatabaseDialect dialect) {
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> arena,
        postgres_translator::spangres::MemoryContextPGArena::Init(nullptr));
    ZETASQL_ASSIGN_OR_RETURN(
        postgres_translator::interfaces::ParserOutput parser_output,
        postgres_translator::CheckedPgRawParserFullOutput(
            std::string(statement).c_str()),
        _ << "failed to parse the DDL statements.");
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<
            postgres_translator::spangres::PostgreSQLToSpannerDDLTranslator>
            translator,
        postgres_translator::spangres::
            CreatePostgreSQLToSpannerDDLTranslator());
    const postgres_translator::spangres::TranslationOptions options{
        .enable_nulls_ordering = true,
        .enable_interleave_in = true,
        .enable_generated_column = true,
        .enable_column_default = true,
        .enable_identity_column =
            EmulatorFeatureFlags::instance().flags().enable_identity_columns,
        .enable_default_sequence_kind =
            EmulatorFeatureFlags::instance().flags().enable_identity_columns,
        .enable_default_time_zone =
            EmulatorFeatureFlags::instance().flags().enable_default_time_zone,
        .enable_jsonb_type = true,
        .enable_array_jsonb_type = true,
        .enable_create_view = true,
        // This enables Spangres ddl translator to record the original
        // expression in PG.
        .enable_expression_string = true,
        .enable_if_not_exists = true,
        .enable_change_streams = true,
        .enable_change_streams_mod_type_filter_options = true,
        .enable_change_streams_ttl_deletes_filter_option = true,
        .enable_search_index =
            EmulatorFeatureFlags::instance().flags().enable_search_index,
        .enable_sequence = true,
        .enable_virtual_generated_column = true,
        .enable_hidden_column =
            EmulatorFeatureFlags::instance().flags().enable_hidden_column,
        .enable_serial_types = EmulatorFeatureFlags::instance()
                                   .flags()
                                   .enable_serial_auto_increment,
    };
    ZETASQL_ASSIGN_OR_RETURN(ddl::DDLStatementList ddl_statement_list,
                     translator->TranslateForEmulator(parser_output, options));
    ZETASQL_RET_CHECK_EQ(ddl_statement_list.statement_size(), 1);
    return std::make_unique<ddl::DDLStatement>(ddl_statement_list.statement(0));
  } else {
    auto ddl_statement = std::make_unique<ddl::DDLStatement>();
    ZETASQL_RETURN_IF_ERROR(ddl::ParseDDLStatement(statement, ddl_statement.get()));
    return std::move(ddl_statement);
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
