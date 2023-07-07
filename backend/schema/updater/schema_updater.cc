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
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/repeated_field.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/log.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/query_engine_options.h"
#include "backend/query/query_validator.h"
#include "backend/schema/backfills/column_value_backfill.h"
#include "backend/schema/backfills/index_backfill.h"
#include "backend/schema/builders/change_stream_builder.h"
#include "backend/schema/builders/check_constraint_builder.h"
#include "backend/schema/builders/column_builder.h"
#include "backend/schema/builders/foreign_key_builder.h"
#include "backend/schema/builders/index_builder.h"
#include "backend/schema/builders/table_builder.h"
#include "backend/schema/builders/view_builder.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/parser/ddl_parser.h"
#include "backend/schema/updater/ddl_type_conversion.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/updater/sql_expression_validators.h"
#include "backend/schema/validators/view_validator.h"
#include "backend/schema/verifiers/check_constraint_verifiers.h"
#include "backend/schema/verifiers/foreign_key_verifiers.h"
#include "common/errors.h"
#include "common/feature_flags.h"
#include "common/limits.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// A struct that defines the columns used by an index.
struct ColumnsUsedByIndex {
  std::vector<const KeyColumn*> index_key_columns;
  std::vector<const Column*> stored_columns;
};

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
      absl::Time schema_change_ts, const Schema* existing_schema) {
    SchemaUpdaterImpl impl(type_factory, table_id_generator,
                           column_id_generator, storage, schema_change_ts,
                           existing_schema);
    ZETASQL_RETURN_IF_ERROR(impl.Init());
    return impl;
  }

  // Apply DDL statements returning the SchemaValidationContext containing
  // the schema change actions resulting from each statement.
  absl::StatusOr<std::vector<SchemaValidationContext>> ApplyDDLStatements(
      const SchemaChangeOperation& schema_change_operation);

  std::vector<std::unique_ptr<const Schema>> GetIntermediateSchemas() {
    return std::move(intermediate_schemas_);
  }

 private:
  SchemaUpdaterImpl(zetasql::TypeFactory* type_factory,
                    TableIDGenerator* table_id_generator,
                    ColumnIDGenerator* column_id_generator, Storage* storage,
                    absl::Time schema_change_ts, const Schema* existing_schema)
      : type_factory_(type_factory),
        table_id_generator_(table_id_generator),
        column_id_generator_(column_id_generator),
        storage_(storage),
        schema_change_timestamp_(schema_change_ts),
        latest_schema_(existing_schema),
        editor_(nullptr) {}

  // Initializes potentially failing components after construction.
  absl::Status Init();

  // Applies the given `statement` on to `latest_schema_`.
  absl::StatusOr<std::unique_ptr<const Schema>> ApplyDDLStatement(
      absl::string_view statement
  );

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
      absl::flat_hash_set<std::string>* dependent_column_names);

  absl::Status AnalyzeColumnDefaultValue(
      absl::string_view expression, const std::string& column_name,
      const zetasql::Type* column_type, const Table* table,
      const ddl::CreateTable* ddl_create_table);

  absl::Status AnalyzeCheckConstraint(
      absl::string_view expression, const Table* table,
      const ddl::CreateTable* ddl_create_table,
      absl::flat_hash_set<std::string>* dependent_column_names,
      CheckConstraint::Builder* builder);

  template <typename ColumnModifier>
  absl::Status SetColumnOptions(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
      ColumnModifier* modifier);

  template <typename ColumnDefModifier>
  absl::Status SetColumnDefinition(const ddl::ColumnDefinition& ddl_column,
                                   const Table* table,
                                   const ddl::CreateTable* ddl_create_table,
                                   ColumnDefModifier* modifier);

  absl::Status AlterColumnDefinition(
      const ddl::ColumnDefinition& ddl_column, const Table* table,
      Column::Editor* editor);

  absl::Status AlterColumnSetDropDefault(
      const ddl::AlterTable::AlterColumn& alter_column, const Table* table,
      const Column* column,
      Column::Editor* editor);

  absl::StatusOr<const Column*> CreateColumn(
      const ddl::ColumnDefinition& ddl_column, const Table* table,
      const ddl::CreateTable* ddl_table
  );

  absl::StatusOr<const KeyColumn*> CreatePrimaryKeyColumn(
      const ddl::KeyPartClause& ddl_key_part, const Table* table);

  absl::Status CreatePrimaryKeyConstraint(
      const ddl::KeyPartClause& ddl_key_part, Table::Builder* builder);

  absl::Status CreateInterleaveConstraint(
      const ddl::InterleaveClause& interleave, Table::Builder* builder);
  absl::Status CreateInterleaveConstraint(const Table* parent,
                                          Table::OnDeleteAction on_delete,
                                          Table::Builder* builder);
  absl::StatusOr<const Table*> GetInterleaveConstraintTable(
      const std::string& interleave_in_table_name,
      const Table::Builder& builder) const;
  static Table::OnDeleteAction GetInterleaveConstraintOnDelete(
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

  absl::Status CreateCheckConstraint(
      const ddl::CheckConstraint& ddl_check_constraint, const Table* table,
      const ddl::CreateTable* ddl_create_table);
  absl::Status CreateRowDeletionPolicy(
      const ddl::RowDeletionPolicy& row_deletion_policy,
      Table::Builder* builder);
  absl::Status CreateTable(const ddl::CreateTable& ddl_table
  );

  absl::StatusOr<const Column*> CreateIndexDataTableColumn(
      const Table* indexed_table, const std::string& source_column_name,
      const Table* index_data_table, bool null_filtered_key_column);

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
      const Index* index, const Table* indexed_table,
      ColumnsUsedByIndex* columns_used_by_index);

  absl::StatusOr<const Index*> CreateIndex(
      const ddl::CreateIndex& ddl_index, const Table* indexed_table = nullptr);

  absl::StatusOr<const ChangeStream*> CreateChangeStream(
      const ddl::CreateChangeStream& ddl_change_stream);
  absl::StatusOr<const Index*> CreateIndexHelper(
      const std::string& index_name, const std::string& index_base_name,
      bool is_unique, bool is_null_filtered,
      const std::string* interleave_in_table,
      const std::vector<ddl::KeyPartClause>& table_pk,
      const ::google::protobuf::RepeatedPtrField<ddl::StoredColumnDefinition>&
          stored_columns,
      const Table* indexed_table);

  absl::Status CreateFunction(const ddl::CreateFunction& ddl_function);

  absl::Status AlterRowDeletionPolicy(
      std::optional<ddl::RowDeletionPolicy> row_deletion_policy,
      const Table* table);
  absl::Status AlterTable(const ddl::AlterTable& alter_table
  );
  absl::Status AlterChangeStream(
      const ddl::AlterChangeStream& alter_change_stream);
  absl::Status AlterInterleaveAction(
      const ddl::InterleaveClause::Action& ddl_interleave_action,
      const Table* table);
  absl::Status AddCheckConstraint(
      const ddl::CheckConstraint& ddl_check_constraint, const Table* table);
  absl::Status AddForeignKey(const ddl::ForeignKey& ddl_foreign_key,
                             const Table* table);
  absl::Status DropConstraint(const std::string& constraint_name,
                              const Table* table);

  absl::Status DropTable(const ddl::DropTable& drop_table);

  absl::Status DropIndex(const ddl::DropIndex& drop_index);

  absl::Status DropChangeStream(
      const ddl::DropChangeStream& drop_change_stream);

  absl::Status ApplyImplSetColumnOptions(
      const ddl::SetColumnOptions& set_column_options
  );

  absl::Status DropFunction(const ddl::DropFunction& drop_function);

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

absl::Status ValidateDdlStatement(const ddl::DDLStatement& ddl) {
  auto check_generated_column = [&](const ddl::ColumnDefinition& col) {
    if (col.has_generated_column()) {
      if (!col.generated_column().stored()) {
        return error::NonStoredGeneratedColumnUnsupported(col.column_name());
      }
    }
    return absl::OkStatus();
  };

  if (ddl.has_create_table()) {
    for (const auto& col : ddl.create_table().column()) {
      ZETASQL_RETURN_IF_ERROR(check_generated_column(col));
    }
  }
  if (ddl.has_alter_table()) {
    if (ddl.alter_table().has_add_column()) {
      ZETASQL_RETURN_IF_ERROR(
          check_generated_column(ddl.alter_table().add_column().column()));
    } else if (ddl.alter_table().has_alter_column()) {
      ZETASQL_RETURN_IF_ERROR(
          check_generated_column(ddl.alter_table().alter_column().column()));
    }
  }

  if ((ddl.has_create_function() || ddl.has_drop_function()) &&
      !EmulatorFeatureFlags::instance().flags().enable_views) {
    return error::ViewsNotSupported(ddl.has_create_function() ? "CREATE"
                                                              : "DROP");
  }

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const Schema>>
SchemaUpdaterImpl::ApplyDDLStatement(
    absl::string_view statement
) {
  if (statement.empty()) {
    return error::EmptyDDLStatement();
  }

  ZETASQL_RET_CHECK(!editor_->HasModifications());
  ZETASQL_ASSIGN_OR_RETURN(ddl::DDLStatement ddl_statement,
                   ParseDDLByDialect(statement
                                     ));
  ZETASQL_RETURN_IF_ERROR(ValidateDdlStatement(ddl_statement));

  // Apply the statement to the schema graph.
  switch (ddl_statement.statement_case()) {
    case ddl::DDLStatement::kCreateTable: {
      ZETASQL_RETURN_IF_ERROR(CreateTable(ddl_statement.create_table()
                                  ));
      break;
    }
    case ddl::DDLStatement::kCreateChangeStream: {
      ZETASQL_RETURN_IF_ERROR(
          CreateChangeStream(ddl_statement.create_change_stream()).status());
      break;
    }
    case ddl::DDLStatement::kCreateIndex: {
      if (global_names_.HasName(ddl_statement.create_index().index_name()) &&
          ddl_statement.create_index().existence_modifier() ==
              ddl::IF_NOT_EXISTS) {
        break;
      }
      ZETASQL_RETURN_IF_ERROR(CreateIndex(ddl_statement.create_index()).status());
      break;
    }
    case ddl::DDLStatement::kCreateFunction: {
      ZETASQL_RETURN_IF_ERROR(CreateFunction(ddl_statement.create_function()));
      break;
    }
    case ddl::DDLStatement::kAlterTable: {
      ZETASQL_RETURN_IF_ERROR(AlterTable(ddl_statement.alter_table()
                                 ));
      break;
    }
    case ddl::DDLStatement::kAlterChangeStream: {
      ZETASQL_RETURN_IF_ERROR(AlterChangeStream(ddl_statement.alter_change_stream()));
      break;
    }
    case ddl::DDLStatement::kDropTable: {
      ZETASQL_RETURN_IF_ERROR(DropTable(ddl_statement.drop_table()));
      break;
    }
    case ddl::DDLStatement::kDropIndex: {
      ZETASQL_RETURN_IF_ERROR(DropIndex(ddl_statement.drop_index()));
      break;
    }
    case ddl::DDLStatement::kDropChangeStream: {
      ZETASQL_RETURN_IF_ERROR(DropChangeStream(ddl_statement.drop_change_stream()));
      break;
    }
    case ddl::DDLStatement::kAnalyze:
      // Intentionally no=op.
      break;
    case ddl::DDLStatement::kSetColumnOptions:
      ZETASQL_RETURN_IF_ERROR(ApplyImplSetColumnOptions(
          ddl_statement.set_column_options()
          ));
      break;
    case ddl::DDLStatement::kDropFunction:
      ZETASQL_RETURN_IF_ERROR(DropFunction(ddl_statement.drop_function()));
      break;
    default:
      ZETASQL_RET_CHECK(false) << "Unsupported ddl statement: "
                       << ddl_statement.statement_case();
  }
  ZETASQL_ASSIGN_OR_RETURN(auto new_schema_graph, editor_->CanonicalizeGraph());
  return std::make_unique<const OwningSchema>(
      std::move(new_schema_graph)
  );
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
        storage_, &global_names_, type_factory_, schema_change_timestamp_};
    statement_context_ = &statement_context;
    statement_context_->SetOldSchemaSnapshot(latest_schema_);
    statement_context_->SetTempNewSchemaSnapshotConstructor(
        [this,
         &new_tmp_schema](const SchemaGraph* unowned_graph) -> const Schema* {
          new_tmp_schema = std::make_unique<const Schema>(
              unowned_graph
          );
          return new_tmp_schema.get();
        });

    // Initialize the editor that will be used to stage the schema changes.
    editor_ = std::make_unique<SchemaGraphEditor>(
        latest_schema_->GetSchemaGraph(), statement_context_);

    // If there is a semantic validation error, then we return right away.
    ZETASQL_ASSIGN_OR_RETURN(
        auto new_schema,
        ApplyDDLStatement(statement
                          ));

    // We save every schema snapshot as verifiers/backfillers from the
    // current/next statement may need to refer to the previous/current
    // schema snapshots.
    statement_context_->SetValidatedNewSchemaSnapshot(new_schema.get());
    latest_schema_ = new_schema.get();
    intermediate_schemas_.emplace_back(std::move(new_schema));

    // If everything was OK, make this the new schema snapshot for processing
    // the next statement and save the pending schema snapshot and backfill
    // work.
    pending_work.emplace_back(std::move(statement_context));
  }

  return pending_work;
}

template <typename ColumnModifier>
absl::Status SchemaUpdaterImpl::SetColumnOptions(
    const ::google::protobuf::RepeatedPtrField<ddl::SetOption>& set_options,
    ColumnModifier* modifier) {
  std::optional<bool> allows_commit_timestamp = std::nullopt;
  std::string commit_timestamp_option_name = ddl::kCommitTimestampOptionName;
  for (const ddl::SetOption& option : set_options) {
    ZETASQL_RET_CHECK_EQ(option.option_name(), commit_timestamp_option_name)
        << "Invalid column option: " << option.option_name();
    if (option.has_bool_value()) {
      allows_commit_timestamp = option.bool_value();
    } else if (option.has_null_value()) {
      allows_commit_timestamp = std::nullopt;
    } else {
      ZETASQL_RET_CHECK(false) << "Option " << commit_timestamp_option_name
                       << " can only take bool_value or null_value.";
    }
  }
  modifier->set_allow_commit_timestamp(allows_commit_timestamp);
  return absl::OkStatus();
}
absl::Status SchemaUpdaterImpl::AlterColumnDefinition(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    Column::Editor* editor) {
  return SetColumnDefinition(ddl_column, table, /*ddl_create_table=*/nullptr,
                             editor);
}

absl::Status SchemaUpdaterImpl::AlterColumnSetDropDefault(
    const ddl::AlterTable::AlterColumn& alter_column, const Table* table,
    const Column* column,
    Column::Editor* editor) {
  const ddl::AlterTable::AlterColumn::AlterColumnOp type =
      alter_column.operation();
  ZETASQL_RET_CHECK(type == ddl::AlterTable::AlterColumn::SET_DEFAULT ||
            type == ddl::AlterTable::AlterColumn::DROP_DEFAULT);

  const ddl::ColumnDefinition& new_column_def = alter_column.column();
  if (type == ddl::AlterTable::AlterColumn::SET_DEFAULT) {
    if (column->is_generated()) {
      return error::CannotSetDefaultValueOnGeneratedColumn(column->FullName());
    }

    std::string expression = new_column_def.column_default().expression();
    ZETASQL_RET_CHECK(new_column_def.has_column_default() && !expression.empty());
    absl::Status s = AnalyzeColumnDefaultValue(expression, column->Name(),
                                               column->GetType(), table,
                                               /*ddl_create_table=*/nullptr);
    if (!s.ok()) {
      return error::ColumnDefaultValueParseError(table->Name(), column->Name(),
                                                 s.message());
    }
    editor->set_expression(expression);
    editor->set_has_default_value(true);
  } else {
    if (!column->has_default_value()) {
      return absl::OkStatus();
    }
    editor->clear_expression();
    editor->set_has_default_value(false);
  }

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
          DDLColumnTypeToGoogleSqlType(ddl_column,
                                       type_factory_
                                       ));
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
    absl::flat_hash_set<std::string>* dependent_column_names) {
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
      /*allow_volatile_expression=*/false);
}

absl::Status SchemaUpdaterImpl::AnalyzeColumnDefaultValue(
    absl::string_view expression, const std::string& column_name,
    const zetasql::Type* column_type, const Table* table,
    const ddl::CreateTable* ddl_create_table) {
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
      /*allow_volatile_expression=*/true));
  if (!dependent_column_names.empty()) {
    return error::DefaultExpressionWithColumnDependency(column_name);
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AnalyzeCheckConstraint(
    absl::string_view expression, const Table* table,
    const ddl::CreateTable* ddl_create_table,
    absl::flat_hash_set<std::string>* dependent_column_names,
    CheckConstraint::Builder* builder) {
  std::vector<zetasql::SimpleTable::NameAndType> name_and_types;
  ZETASQL_RETURN_IF_ERROR(InitColumnNameAndTypesFromTable(table, ddl_create_table,
                                                  &name_and_types));

  ZETASQL_RETURN_IF_ERROR(
      AnalyzeColumnExpression(expression, zetasql::types::BoolType(), table,
                              latest_schema_, type_factory_, name_and_types,
                              "check constraints", dependent_column_names,
                              /*allow_volatile_expression=*/false));

  for (const std::string& column_name : *dependent_column_names) {
    builder->add_dependent_column(table->FindColumn(column_name));
  }
  return absl::OkStatus();
}

template <typename ColumnDefModifer>
absl::Status SchemaUpdaterImpl::SetColumnDefinition(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    const ddl::CreateTable* ddl_create_table,
    ColumnDefModifer* modifier) {
  bool is_generated = false;
  bool has_default_value = false;
  // Process any changes in column definition.
  ZETASQL_ASSIGN_OR_RETURN(
      const zetasql::Type* column_type,
      DDLColumnTypeToGoogleSqlType(ddl_column,
                                   type_factory_
                                   ));
  modifier->set_type(column_type);

  if (ddl_column.has_column_default()) {
    std::string expression = ddl_column.column_default().expression();
    has_default_value = true;
    modifier->set_expression(expression);
    absl::Status s =
        AnalyzeColumnDefaultValue(expression, ddl_column.column_name(),
                                  column_type, table, ddl_create_table);
    if (!s.ok()) {
      return error::ColumnDefaultValueParseError(
          table->Name(), ddl_column.column_name(), s.message());
    }
  } else if (ddl_column.has_generated_column()) {
    if (!ddl_column.generated_column().stored()) {
      return error::NonStoredGeneratedColumnUnsupported(
          ddl_column.column_name());
    }

    std::string expression = ddl_column.generated_column().expression();
    is_generated = true;
    modifier->set_expression(expression);
    absl::flat_hash_set<std::string> dependent_column_names;
    absl::Status s = AnalyzeGeneratedColumn(
        expression, ddl_column.column_name(), column_type, table,
        ddl_create_table, &dependent_column_names);
    if (!s.ok()) {
      return error::GeneratedColumnDefinitionParseError(
          table->Name(), ddl_column.column_name(), s.message());
    }
    for (const std::string& column_name : dependent_column_names) {
      modifier->add_dependent_column_name(column_name);
    }
  }

  if (!is_generated && !has_default_value) {
    // Altering a generated column to a non-generated column is disallowed. In
    // that case, the expression is cleared here and later validation at
    // column_validator.cc will block it.
    modifier->clear_expression();
  } else {
    ZETASQL_RET_CHECK(is_generated != has_default_value);
  }

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
  if (!ddl_column.set_options().empty()) {
    ZETASQL_RETURN_IF_ERROR(SetColumnOptions(ddl_column.set_options(),
                                     modifier));
  }

  return absl::OkStatus();
}

absl::StatusOr<const Column*> SchemaUpdaterImpl::CreateColumn(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    const ddl::CreateTable* ddl_table
) {
  const std::string& column_name = ddl_column.column_name();
  Column::Builder builder;
  builder
      .set_id(column_id_generator_->NextId(
          absl::StrCat(table->Name(), ".", column_name)))
      .set_name(column_name);
  ZETASQL_RETURN_IF_ERROR(SetColumnDefinition(ddl_column, table, ddl_table,
                                      &builder));
  builder.set_hidden(ddl_column.has_hidden() && ddl_column.hidden());
  const Column* column = builder.get();
  builder.set_table(table);
  if (column->is_generated() || column->has_default_value()) {
    statement_context_->AddAction(
        [column](const SchemaValidationContext* context) {
          return BackfillGeneratedColumnValue(column, context);
        });
  }
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return column;
}

absl::Status SchemaUpdaterImpl::CreateInterleaveConstraint(
    const ddl::InterleaveClause& interleave, Table::Builder* builder) {
  ZETASQL_ASSIGN_OR_RETURN(const Table* parent, GetInterleaveConstraintTable(
                                            interleave.table_name(), *builder));
  Table::OnDeleteAction on_delete = GetInterleaveConstraintOnDelete(interleave);
  return CreateInterleaveConstraint(parent, on_delete, builder);
}

absl::Status SchemaUpdaterImpl::CreateInterleaveConstraint(
    const Table* parent, Table::OnDeleteAction on_delete,
    Table::Builder* builder) {
  ZETASQL_RET_CHECK_EQ(builder->get()->parent(), nullptr);

  if (parent->row_deletion_policy().has_value() &&
      on_delete != Table::OnDeleteAction::kCascade) {
    return error::RowDeletionPolicyOnAncestors(builder->get()->Name(),
                                               parent->Name());
  }

  ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
      parent, [builder](Table::Editor* parent_editor) -> absl::Status {
        parent_editor->add_child_table(builder->get());
        builder->set_parent_table(parent_editor->get());
        return absl::OkStatus();
      }));

  builder->set_on_delete(on_delete);
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

Table::OnDeleteAction SchemaUpdaterImpl::GetInterleaveConstraintOnDelete(
    const ddl::InterleaveClause& interleave) {
  return interleave.on_delete() == ddl::InterleaveClause::CASCADE
             ? Table::OnDeleteAction::kCascade
             : Table::OnDeleteAction::kNoAction;
}

absl::Status SchemaUpdaterImpl::CreatePrimaryKeyConstraint(
    const ddl::KeyPartClause& ddl_key_part, Table::Builder* builder) {
  ZETASQL_ASSIGN_OR_RETURN(const KeyColumn* key_col,
                   CreatePrimaryKeyColumn(ddl_key_part, builder->get()));
  builder->add_key_column(key_col);
  return absl::OkStatus();
}

absl::StatusOr<const KeyColumn*> SchemaUpdaterImpl::CreatePrimaryKeyColumn(
    const ddl::KeyPartClause& ddl_key_part, const Table* table) {
  KeyColumn::Builder builder;
  const std::string& key_column_name = ddl_key_part.key_name();
  bool is_descending = (ddl_key_part.order() == ddl::KeyPartClause::DESC);

  // References to columns in primary key clause are case-sensitive.
  const Column* column = table->FindColumnCaseSensitive(key_column_name);
  if (column == nullptr) {
    return error::NonExistentKeyColumn(
        OwningObjectType(table), OwningObjectName(table), key_column_name);
  }
  builder.set_column(column).set_descending(is_descending);
  const KeyColumn* key_col = builder.get();
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return key_col;
}

absl::Status SchemaUpdaterImpl::CreateForeignKeyConstraint(
    const ddl::ForeignKey& ddl_foreign_key, const Table* referencing_table) {
  // Build and add the emulator foreign key before creating any of its backing
  // indexes. This ensures the foreign key is validated first which in turn
  // ensures better foreign key error messages instead of obscure index errors.
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

absl::StatusOr<const ForeignKey*> SchemaUpdaterImpl::BuildForeignKeyConstraint(
    const ddl::ForeignKey& ddl_foreign_key, const Table* referencing_table) {
  ForeignKey::Builder builder;

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
      if (key_column != nullptr && key_column->is_descending()) {
        key_part->set_order(ddl::KeyPartClause::DESC);
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

absl::Status SchemaUpdaterImpl::CreateCheckConstraint(
    const ddl::CheckConstraint& ddl_check_constraint, const Table* table,
    const ddl::CreateTable* ddl_create_table) {
  CheckConstraint::Builder builder;

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
  absl::flat_hash_set<std::string> dependent_column_names;
  absl::Status s = AnalyzeCheckConstraint(ddl_check_constraint.expression(),
                                          table, ddl_create_table,
                                          &dependent_column_names, &builder);

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

absl::Status SchemaUpdaterImpl::CreateTable(
    const ddl::CreateTable& ddl_table
) {
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
  builder.set_id(table_id_generator_->NextId(ddl_table.table_name()))
      .set_name(ddl_table.table_name());

  for (const ddl::ColumnDefinition& ddl_column : ddl_table.column()) {
    ZETASQL_ASSIGN_OR_RETURN(const Column* column,
                     CreateColumn(ddl_column, builder.get(),
                                  &ddl_table
                                  ));
    builder.add_column(column);
  }

  for (const Column* column : builder.get()->columns()) {
    if (column->is_generated()) {
      const_cast<Column*>(column)->PopulateDependentColumns();
    }
  }

  // Some constraints have a dependency on the primary key, so create it first.
  for (const ddl::KeyPartClause& ddl_key_part : ddl_table.primary_key()) {
    ZETASQL_RETURN_IF_ERROR(CreatePrimaryKeyConstraint(ddl_key_part, &builder));
  }

  for (const ddl::ForeignKey& ddl_foreign_key : ddl_table.foreign_key()) {
    ZETASQL_RETURN_IF_ERROR(CreateForeignKeyConstraint(ddl_foreign_key, builder.get()));
  }
  if (ddl_table.has_interleave_clause()) {
    ZETASQL_RETURN_IF_ERROR(
        CreateInterleaveConstraint(ddl_table.interleave_clause(), &builder));
  }
  for (const ddl::CheckConstraint& ddl_check_constraint :
       ddl_table.check_constraint()) {
    const Table* table = builder.get();
      ZETASQL_RETURN_IF_ERROR(
          CreateCheckConstraint(ddl_check_constraint, table, &ddl_table));
  }

  if (ddl_table.has_row_deletion_policy()) {
    ZETASQL_RETURN_IF_ERROR(
        CreateRowDeletionPolicy(ddl_table.row_deletion_policy(), &builder));
  }
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return absl::OkStatus();
}

absl::StatusOr<const Column*> SchemaUpdaterImpl::CreateIndexDataTableColumn(
    const Table* indexed_table, const std::string& source_column_name,
    const Table* index_data_table, bool null_filtered_key_column) {
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

  if (null_filtered_key_column) {
    builder.set_nullable(false);
  } else {
    builder.set_nullable(source_column->is_nullable());
  }

  const Column* column = builder.get();
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return column;
}

absl::Status SchemaUpdaterImpl::AddSearchIndexColumns(
    const ::google::protobuf::RepeatedPtrField<ddl::KeyPartClause>& key_parts,
    const Table* indexed_table, bool is_null_filtered,
    std::vector<const Column*>& columns, Table::Builder& builder) {
  for (const ddl::KeyPartClause& ddl_key_part : key_parts) {
    const std::string& column_name = ddl_key_part.key_name();
    const Column* column = builder.get()->FindColumn(column_name);
    // Skip already added columns
    if (column == nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(
          column, CreateIndexDataTableColumn(indexed_table, column_name,
                                             builder.get(), is_null_filtered));
      builder.add_column(column);
    }
    columns.push_back(column);
  }

  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const Table>>
SchemaUpdaterImpl::CreateIndexDataTable(
    absl::string_view index_name,
    const std::vector<ddl::KeyPartClause>& index_pk,
    const std::string* interleave_in_table,
    const ::google::protobuf::RepeatedPtrField<ddl::StoredColumnDefinition>&
        stored_columns,
    const Index* index, const Table* indexed_table,
    ColumnsUsedByIndex* columns_used_by_index) {
  std::string table_name = absl::StrCat(kIndexDataTablePrefix, index_name);
  Table::Builder builder;
  builder.set_name(table_name)
      .set_id(table_id_generator_->NextId(table_name))
      .set_owner_index(index);

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
          CreateIndexDataTableColumn(indexed_table, column_name, builder.get(),
                                     index->is_null_filtered()));
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
          CreateIndexDataTableColumn(indexed_table, key_col_name, builder.get(),
                                     index->is_null_filtered()));
      builder.add_column(column);

      // Add to the PK specification.
      ddl::KeyPartClause key_part;
      key_part.set_key_name(key_col_name);
      if (key_col->is_descending()) {
        key_part.set_order(ddl::KeyPartClause::DESC);
      }
      data_table_pk.push_back(key_part);
    }

    for (const ddl::KeyPartClause& ddl_key_part : data_table_pk) {
      ZETASQL_RETURN_IF_ERROR(CreatePrimaryKeyConstraint(ddl_key_part, &builder));
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
        parent_table, Table::OnDeleteAction::kCascade, &builder));
  }

  // Add stored columns to index data table.
  for (const ddl::StoredColumnDefinition& ddl_column : stored_columns) {
    const std::string& column_name = ddl_column.name();
    ZETASQL_ASSIGN_OR_RETURN(
        const Column* column,
        CreateIndexDataTableColumn(indexed_table, column_name, builder.get(),
                                   /*null_filtered_key_column=*/false));
    builder.add_column(column);
    columns_used_by_index->stored_columns.push_back(column);
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

  return CreateIndexHelper(ddl_index.index_name(), ddl_index.index_base_name(),
                           is_unique, is_null_filtered, interleave_in_table,
                           index_pk, ddl_index.stored_column_definition(),
                           indexed_table);
}

absl::StatusOr<const ChangeStream*> SchemaUpdaterImpl::CreateChangeStream(
    const ddl::CreateChangeStream& ddl_change_stream) {
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
  builder.set_name(ddl_change_stream.change_stream_name());
  const ChangeStream* change_stream = builder.get();
  return change_stream;
}

absl::StatusOr<const Index*> SchemaUpdaterImpl::CreateIndexHelper(
    const std::string& index_name, const std::string& index_base_name,
    bool is_unique, bool is_null_filtered,
    const std::string* interleave_in_table,
    const std::vector<ddl::KeyPartClause>& table_pk,
    const ::google::protobuf::RepeatedPtrField<ddl::StoredColumnDefinition>&
        stored_columns,
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
  builder.set_name(index_name);
  builder.set_unique(is_unique);
  builder.set_null_filtered(is_null_filtered);
  ColumnsUsedByIndex columns_used_by_index;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const Table> data_table,
      CreateIndexDataTable(
          index_name, table_pk, interleave_in_table, stored_columns,
          builder.get(), indexed_table, &columns_used_by_index));

  builder.set_index_data_table(data_table.get());

  for (const KeyColumn* key_col : columns_used_by_index.index_key_columns) {
    builder.add_key_column(key_col);
  }

  for (const Column* col : columns_used_by_index.stored_columns) {
    builder.add_stored_column(col);
  }

  ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
      indexed_table, [&builder](Table::Editor* table_editor) -> absl::Status {
        table_editor->add_index(builder.get());
        builder.set_indexed_table(table_editor->get());
        return absl::OkStatus();
      }));

  // Register a backfill action for the index.
  const Index* index = builder.get();

    statement_context_->AddAction(
        [index](const SchemaValidationContext* context) {
          return BackfillIndex(index, context);
        });

  // The data table must be added after the index for correct order of
  // validation.
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  ZETASQL_RETURN_IF_ERROR(AddNode(std::move(data_table)));
  return index;
}

absl::Status SchemaUpdaterImpl::CreateFunction(
    const ddl::CreateFunction& ddl_function) {
  if (latest_schema_->views().size() >= limits::kMaxViewsPerDatabase) {
    return error::TooManyViewsPerDatabase(ddl_function.function_name(),
                                          limits::kMaxViewsPerDatabase);
  }

  if (!ddl_function.has_sql_security() ||
      ddl_function.sql_security() != ddl::Function::INVOKER) {
    return error::ViewRequiresInvokerSecurity(ddl_function.function_name());
  }

  // Name lookup is case insensitive.
  const View* existing_view =
      latest_schema_->FindView(ddl_function.function_name());
  const bool replace = existing_view && ddl_function.has_is_or_replace() &&
                       ddl_function.is_or_replace();
  if (!replace) {
    ZETASQL_RETURN_IF_ERROR(
        global_names_.AddName("View", ddl_function.function_name()));
  }

  // Analyze the view definition.
  absl::Status error;
  absl::flat_hash_set<const SchemaNode*> dependencies;
  std::vector<View::Column> output_columns;
  auto status = AnalyzeViewDefinition(
      ddl_function.function_name(), ddl_function.sql_body(), latest_schema_,
      type_factory_, &output_columns, &dependencies);
  if (!status.ok()) {
    return replace ? error::ViewReplaceError(ddl_function.function_name(),
                                             status.message())
                   : error::ViewBodyAnalysisError(ddl_function.function_name(),
                                                  status.message());
  }

  View::Builder builder;
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

  for (auto output_column : output_columns) {
    builder.add_column(std::move(output_column));
  }

  for (const auto& dependency : dependencies) {
    builder.add_dependency(dependency);
  }

  if (replace) {
    // Check for a recursive view by analyzing the transitive set of
    // dependencies, i.e., if the view is a dependency of itself.
    auto transitive_deps = GatherTransitiveDependenciesForView(dependencies);
    if (std::find_if(transitive_deps.begin(), transitive_deps.end(),
                     [existing_view](const SchemaNode* dep) {
                       auto view_dep = dep->As<const View>();
                       return view_dep != nullptr &&
                              view_dep->Name() == existing_view->Name();
                     }) != transitive_deps.end()) {
      return error::ViewReplaceRecursive(existing_view->Name());
    }
    return AlterNode<View>(existing_view,
                           [&](View::Editor* editor) -> absl::Status {
                             // Just replace the view definition completely. The
                             // temp instance inside builder will be cleaned up
                             // when the builder goes out of scope.
                             editor->copy_from(builder.get());
                             return absl::OkStatus();
                           });
  } else {
    return AddNode(builder.build());
  }
}

absl::Status SchemaUpdaterImpl::AlterRowDeletionPolicy(
    std::optional<ddl::RowDeletionPolicy> row_deletion_policy,
    const Table* table) {
  return AlterNode(table, [&](Table::Editor* editor) {
    editor->set_row_deletion_policy(row_deletion_policy);
    return absl::OkStatus();
  });
}

absl::Status SchemaUpdaterImpl::AlterChangeStream(
    const ddl::AlterChangeStream& alter_change_stream) {
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterTable(
    const ddl::AlterTable& alter_table
) {
  const Table* table =
      latest_schema_->FindTableCaseSensitive(alter_table.table_name());
  if (table == nullptr) {
    return error::TableNotFound(alter_table.table_name());
  }

  switch (alter_table.alter_type_case()) {
    case ddl::AlterTable::kSetInterleaveClause: {
      return AlterInterleaveAction(
          alter_table.set_interleave_clause().interleave_clause().on_delete(),
          table);
    }
    case ddl::AlterTable::kSetOnDelete: {
      return AlterInterleaveAction(alter_table.set_on_delete().action(), table);
    }
    case ddl::AlterTable::kAddCheckConstraint: {
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
                                    nullptr
                                    ));
      if (new_column->is_generated()) {
        const_cast<Column*>(new_column)->PopulateDependentColumns();
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
        ZETASQL_RETURN_IF_ERROR(
            AlterNode<Column>(column,
                              [this, &alter_column, &column,
                               &table
        ](Column::Editor* editor) -> absl::Status {
                                return AlterColumnSetDropDefault(
                                    alter_column, table, column,
                                    editor);
                              }));
      } else {
        const auto& column_def = alter_column.column();
        ZETASQL_RETURN_IF_ERROR(
            AlterNode<Column>(column,
                              [this, &column_def,
                               &table
        ](Column::Editor* editor) -> absl::Status {
                                return AlterColumnDefinition(
                                    column_def, table,
                                    editor);
                              }));
      }
      return absl::OkStatus();
    }
    case ddl::AlterTable::kDropColumn: {
      const Column* column = table->FindColumn(alter_table.drop_column());
      if (column == nullptr) {
        return error::ColumnNotFound(table->Name(), alter_table.drop_column());
      }
      ZETASQL_RETURN_IF_ERROR(DropNode(column));
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
    default:
      return error::Internal(absl::StrCat("Unsupported alter table type: ",
                                          alter_table.DebugString()));
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterInterleaveAction(
    const ddl::InterleaveClause::Action& ddl_interleave_action,
    const Table* table) {
  return AlterNode<Table>(table, [&](Table::Editor* editor) {
    if (ddl_interleave_action == ddl::InterleaveClause::CASCADE) {
      editor->set_on_delete(Table::OnDeleteAction::kCascade);
    } else {
      editor->set_on_delete(Table::OnDeleteAction::kNoAction);
    }
    return absl::OkStatus();
  });
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

  // TODO : Error if any view depends on this table.
  return DropNode(table);
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
  return DropNode(index);
}

absl::Status SchemaUpdaterImpl::DropChangeStream(
    const ddl::DropChangeStream& drop_change_stream) {
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::ApplyImplSetColumnOptions(
    const ddl::SetColumnOptions& set_column_options
) {
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
    ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(
        column,
        [this,
         &set_column_options
    ](Column::Editor* editor) -> absl::Status {
          return SetColumnOptions(set_column_options.options(),
                                  editor);
        }));
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::DropFunction(
    const ddl::DropFunction& drop_function) {
  const View* view =
      latest_schema_->FindViewCaseSensitive(drop_function.function_name());
  if (view == nullptr) {
    return error::ViewNotFound(drop_function.function_name());
  }
  return DropNode(view);
}

const Schema* EmptySchema() {
  static const Schema* empty_schema = new Schema;
  return empty_schema;
}

}  // namespace

absl::StatusOr<std::unique_ptr<const Schema>>
SchemaUpdater::ValidateSchemaFromDDL(
    const SchemaChangeOperation& schema_change_operation,
    const SchemaChangeContext& context, const Schema* existing_schema) {
  if (existing_schema == nullptr) {
    existing_schema = EmptySchema();
  }
  ZETASQL_ASSIGN_OR_RETURN(SchemaUpdaterImpl updater,
                   SchemaUpdaterImpl::Build(
                       context.type_factory, context.table_id_generator,
                       context.column_id_generator, context.storage,
                       context.schema_change_timestamp, existing_schema));
  ZETASQL_ASSIGN_OR_RETURN(pending_work_,
                   updater.ApplyDDLStatements(schema_change_operation));
  intermediate_schemas_ = updater.GetIntermediateSchemas();

  std::unique_ptr<const Schema> new_schema = nullptr;
  if (!intermediate_schemas_.empty()) {
    new_schema = std::move(*intermediate_schemas_.rbegin());
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
                       context.schema_change_timestamp, existing_schema));
  ZETASQL_ASSIGN_OR_RETURN(pending_work_,
                   updater.ApplyDDLStatements(schema_change_operation));
  intermediate_schemas_ = updater.GetIntermediateSchemas();

  // Use the schema snapshot for the last succesful statement.
  int num_successful = 0;
  std::unique_ptr<const Schema> new_schema = nullptr;

  absl::Status backfill_status = RunPendingActions(&num_successful);
  if (num_successful > 0) {
    new_schema = std::move(intermediate_schemas_[num_successful - 1]);
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
      UpdateSchemaFromDDL(EmptySchema(), schema_change_operation, context));
  ZETASQL_RETURN_IF_ERROR(result.backfill_status);
  return std::move(result.updated_schema);
}

absl::StatusOr<ddl::DDLStatement> ParseDDLByDialect(
    absl::string_view statement
) {
  ddl::DDLStatement ddl_statement;
  ZETASQL_RETURN_IF_ERROR(ddl::ParseDDLStatement(statement, &ddl_statement));
  return ddl_statement;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
