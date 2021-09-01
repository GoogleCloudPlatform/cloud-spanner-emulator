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
#include <memory>

#include "google/protobuf/repeated_field.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/match.h"
#include "absl/types/optional.h"
#include "backend/datamodel/types.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/query_engine_options.h"
#include "backend/query/query_validator.h"
#include "backend/schema/backfills/column_value_backfill.h"
#include "backend/schema/backfills/index_backfill.h"
#include "backend/schema/builders/check_constraint_builder.h"
#include "backend/schema/builders/column_builder.h"
#include "backend/schema/builders/foreign_key_builder.h"
#include "backend/schema/builders/index_builder.h"
#include "backend/schema/builders/table_builder.h"
#include "backend/schema/catalog/check_constraint.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/parser/DDLParser.h"
#include "backend/schema/parser/ddl_parser.h"
#include "backend/schema/updater/ddl_type_conversion.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/verifiers/check_constraint_verifiers.h"
#include "backend/schema/verifiers/foreign_key_verifiers.h"
#include "common/constants.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

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

  zetasql_base::StatusOr<SchemaUpdaterImpl> static Build(
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
  zetasql_base::StatusOr<std::vector<SchemaValidationContext>> ApplyDDLStatements(
      absl::Span<const std::string> statements);

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
  zetasql_base::StatusOr<std::unique_ptr<const Schema>> ApplyDDLStatement(
      absl::string_view statement);

  // Run any pending schema actions resulting from the schema change statements.
  absl::Status RunPendingActions(
      const std::vector<SchemaValidationContext>& pending_work,
      int* num_succesful);

  absl::Status InitColumnNameAndTypesFromTable(
      const Table* table, const ddl::CreateTable* ddl_create_table,
      std::vector<zetasql::SimpleTable::NameAndType>* name_and_types);

  absl::Status AnalyzeColumnExpression(
      absl::string_view expression, const zetasql::Type* target_type,
      const Table* table,
      const std::vector<zetasql::SimpleTable::NameAndType>& name_and_types,
      absl::string_view expression_use,
      absl::flat_hash_set<std::string>* dependent_column_names);

  absl::Status AnalyzeGeneratedColumn(
      absl::string_view expression, const std::string& column_name,
      const zetasql::Type* column_type, const Table* table,
      const ddl::CreateTable* ddl_create_table,
      absl::flat_hash_set<std::string>* dependent_column_names);

  absl::Status AnalyzeCheckConstraint(
      absl::string_view expression, const Table* table,
      const ddl::CreateTable* ddl_create_table,
      absl::flat_hash_set<std::string>* dependent_column_names,
      CheckConstraint::Builder* builder);

  template <typename ColumnModifier>
  absl::Status SetColumnOptions(const ddl::Options& options,
                                ColumnModifier* modifier);

  template <typename ColumnDefModifier>
  absl::Status SetColumnDefinition(const ddl::ColumnDefinition& ddl_column,
                                   const Table* table,
                                   const ddl::CreateTable* ddl_create_table,
                                   ColumnDefModifier* modifier);

  absl::Status AlterColumnDefinition(const ddl::ColumnDefinition& ddl_column,
                                     const Table* table,
                                     Column::Editor* editor);

  zetasql_base::StatusOr<const Column*> CreateColumn(
      const ddl::ColumnDefinition& ddl_column, const Table* table,
      const ddl::CreateTable* ddl_table);

  zetasql_base::StatusOr<const KeyColumn*> CreatePrimaryKeyColumn(
      const ddl::PrimaryKeyConstraint::KeyPart& ddl_key_part,
      const Table* table);

  absl::Status CreatePrimaryKeyConstraint(
      const ddl::PrimaryKeyConstraint& ddl_primary_key,
      Table::Builder* builder);

  absl::Status CreateInterleaveConstraint(
      const ddl::InterleaveConstraint& interleave, Table::Builder* builder);
  absl::Status CreateInterleaveConstraint(const Table* parent,
                                          Table::OnDeleteAction on_delete,
                                          Table::Builder* builder);
  zetasql_base::StatusOr<const Table*> GetInterleaveConstraintTable(
      const ddl::InterleaveConstraint& interleave,
      const Table::Builder& builder) const;
  static Table::OnDeleteAction GetInterleaveConstraintOnDelete(
      const ddl::InterleaveConstraint& interleave);

  absl::Status CreateForeignKeyConstraint(
      const ddl::ForeignKeyConstraint& ddl_foreign_key,
      const Table* referencing_table);
  zetasql_base::StatusOr<const ForeignKey*> BuildForeignKeyConstraint(
      const ddl::ForeignKeyConstraint& ddl_foreign_key,
      const Table* referencing_table);
  absl::Status EvaluateForeignKeyReferencedPrimaryKey(
      const Table* table,
      const google::protobuf::RepeatedPtrField<std::string>& column_names,
      std::vector<int>* column_order, bool* index_required) const;
  absl::Status EvaluateForeignKeyReferencingPrimaryKey(
      const Table* table,
      const google::protobuf::RepeatedPtrField<std::string>& column_names,
      const std::vector<int>& column_order, bool* index_required) const;
  zetasql_base::StatusOr<const Index*> CreateForeignKeyIndex(
      const ForeignKey* foreign_key, const Table* table,
      const std::vector<std::string>& column_names, bool unique);
  bool CanInterleaveForeignKeyIndex(
      const Table* table, const std::vector<std::string>& column_names) const;

  absl::Status CreateCheckConstraint(
      const ddl::CheckConstraint& ddl_check_constraint, const Table* table,
      const ddl::CreateTable* ddl_create_table);
  absl::Status CreateTable(const ddl::CreateTable& ddl_table);

  zetasql_base::StatusOr<const Column*> CreateIndexDataTableColumn(
      const Table* indexed_table, const std::string& source_column_name,
      const Table* index_data_table, bool null_filtered_key_column);

  zetasql_base::StatusOr<std::unique_ptr<const Table>> CreateIndexDataTable(
      const ddl::CreateIndex& ddl_index, const Index* index,
      const Table* indexed_table,
      std::vector<const KeyColumn*>* index_key_columns,
      std::vector<const Column*>* stored_columns);

  zetasql_base::StatusOr<const Index*> CreateIndex(
      const ddl::CreateIndex& ddl_index, const Table* indexed_table = nullptr);

  absl::Status AlterTable(const ddl::AlterTable& alter_table);
  absl::Status AlterInterleave(const ddl::InterleaveConstraint& ddl_interleave,
                               const Table* table);
  absl::Status AddCheckConstraint(
      const ddl::CheckConstraint& ddl_check_constraint, const Table* table);
  absl::Status AddForeignKey(const ddl::ForeignKeyConstraint& ddl_foreign_key,
                             const Table* table);
  absl::Status DropConstraint(const std::string& constraint_name,
                              const Table* table);

  absl::Status DropTable(const ddl::DropTable& drop_table);

  absl::Status DropIndex(const ddl::DropIndex& drop_index);

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

zetasql_base::StatusOr<std::unique_ptr<const Schema>>
SchemaUpdaterImpl::ApplyDDLStatement(absl::string_view statement) {
  if (statement.empty()) {
    return error::EmptyDDLStatement();
  }

  // Apply the statement to the schema graph.
  ZETASQL_RET_CHECK(!editor_->HasModifications());
  ZETASQL_ASSIGN_OR_RETURN(const auto& ddl_statement,
                   ddl::ParseDDLStatement(statement));

  switch (ddl_statement.kind_case()) {
    case ddl::DDLStatement::kCreateTable: {
      ZETASQL_RETURN_IF_ERROR(CreateTable(ddl_statement.create_table()));
      break;
    }
    case ddl::DDLStatement::kCreateIndex: {
      ZETASQL_RETURN_IF_ERROR(CreateIndex(ddl_statement.create_index()).status());
      break;
    }
    case ddl::DDLStatement::kAlterTable: {
      ZETASQL_RETURN_IF_ERROR(AlterTable(ddl_statement.alter_table()));
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
    default:
      ZETASQL_RET_CHECK(false) << "Unsupported ddl statement: "
                       << ddl_statement.kind_case();
  }
  ZETASQL_ASSIGN_OR_RETURN(auto new_schema_graph, editor_->CanonicalizeGraph());
  return absl::make_unique<const Schema>(std::move(new_schema_graph));
}

zetasql_base::StatusOr<std::vector<SchemaValidationContext>>
SchemaUpdaterImpl::ApplyDDLStatements(
    absl::Span<const std::string> statements) {
  std::vector<SchemaValidationContext> pending_work;

  for (const auto& statement : statements) {
    ZETASQL_VLOG(2) << "Applying statement " << statement;
    SchemaValidationContext statement_context{
        storage_, &global_names_, type_factory_, schema_change_timestamp_};
    statement_context_ = &statement_context;
    editor_ = absl::make_unique<SchemaGraphEditor>(
        latest_schema_->GetSchemaGraph(), statement_context_);

    // If there is a semantic validation error, then we return right away.
    ZETASQL_ASSIGN_OR_RETURN(auto new_schema, ApplyDDLStatement(statement));

    // We save every schema snapshot as verifiers/backfillers from the
    // current/next statement may need to refer to the previous/current
    // schema snapshots.
    statement_context.SetOldSchemaSnapshot(latest_schema_);
    statement_context.SetNewSchemaSnapshot(new_schema.get());
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
absl::Status SchemaUpdaterImpl::SetColumnOptions(const ddl::Options& options,
                                                 ColumnModifier* modifier) {
  absl::optional<bool> allows_commit_timestamp = absl::nullopt;
  for (const ddl::Options::Option& option : options.option_val()) {
    ZETASQL_RET_CHECK_EQ(option.name(), ddl::kCommitTimestampOptionName)
        << "Invalid column option: " << option.name();
    switch (option.kind_case()) {
      case ddl::Options_Option::kBoolValue:
        allows_commit_timestamp = option.bool_value();
        break;
      case ddl::Options_Option::kNullValue:
        allows_commit_timestamp = absl::nullopt;
        break;
      default:
        ZETASQL_RET_CHECK(false) << "Option " << ddl::kCommitTimestampOptionName
                         << " can only take bool_value or null_value.";
    }
  }
  modifier->set_allow_commit_timestamp(allows_commit_timestamp);
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterColumnDefinition(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    Column::Editor* editor) {
  if (ddl_column.has_options()) {
    // Column options are specified using the SET OPTIONS syntax and
    // cannot be combined with changes to column definitions so we
    // return from here.
    return SetColumnOptions(ddl_column.options(), editor);
  }
  return SetColumnDefinition(ddl_column, table, /*ddl_table=*/nullptr, editor);
}

class ColumnExpressionValidator : public QueryValidator {
 public:
  ColumnExpressionValidator(
      const Schema* schema, const zetasql::Table* table,
      QueryEngineOptions* options, absl::string_view expression_use,
      absl::flat_hash_set<std::string>* dependent_column_names)
      : QueryValidator(schema, options),
        table_(table),
        expression_use_(expression_use),
        dependent_column_names_(dependent_column_names) {}

  absl::Status DefaultVisit(const zetasql::ResolvedNode* node) override {
    if (node->IsScan() ||
        node->node_kind() == zetasql::RESOLVED_SUBQUERY_EXPR) {
      return error::NonScalarExpressionInColumnExpression(expression_use_);
    }
    if (node->node_kind() == zetasql::RESOLVED_EXPRESSION_COLUMN) {
      std::string column_name =
          node->GetAs<zetasql::ResolvedExpressionColumn>()->name();
      const zetasql::Column* column = table_->FindColumnByName(column_name);
      ZETASQL_RET_CHECK_NE(column, nullptr);
      dependent_column_names_->insert(column->Name());
    }
    return QueryValidator::DefaultVisit(node);
  }

 protected:
  absl::Status VisitResolvedFunctionCall(
      const zetasql::ResolvedFunctionCall* node) override {
    // The validation order matters here.
    // Need to invoke the parent visitor first since some higher level
    // validation should precede the deterministic function check. For example,
    // using pending_commit_timestamp() in generated column at CREATE TABLE
    // should return error due to that function only being allowed in INSERT or
    // UPDATE.
    ZETASQL_RETURN_IF_ERROR(QueryValidator::VisitResolvedFunctionCall(node));
    if (node->function()->function_options().volatility !=
        zetasql::FunctionEnums::IMMUTABLE) {
      return error::NonDeterministicFunctionInColumnExpression(
          node->function()->SQLName(), expression_use_);
    }
    return absl::OkStatus();
  }

 private:
  const zetasql::Table* table_;
  absl::string_view expression_use_;
  absl::flat_hash_set<std::string>* dependent_column_names_;
};

absl::Status SchemaUpdaterImpl::InitColumnNameAndTypesFromTable(
    const Table* table, const ddl::CreateTable* ddl_create_table,
    std::vector<zetasql::SimpleTable::NameAndType>* name_and_types) {
  if (ddl_create_table != nullptr) {
    // We are processing a CREATE TABLE statement, so 'const Table* table' may
    // not have all the columns yet. Add all columns from the ddl.
    for (const ddl::ColumnDefinition& ddl_column :
         ddl_create_table->columns()) {
      ZETASQL_ASSIGN_OR_RETURN(
          const zetasql::Type* type,
          DDLColumnTypeToGoogleSqlType(ddl_column.properties().column_type(),
                                       type_factory_));
      name_and_types->emplace_back(ddl_column.column_name(), type);
    }
  } else {
    for (const Column* column : table->columns()) {
      name_and_types->emplace_back(column->Name(), column->GetType());
    }
  }
  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AnalyzeColumnExpression(
    absl::string_view expression, const zetasql::Type* target_type,
    const Table* table,
    const std::vector<zetasql::SimpleTable::NameAndType>& name_and_types,
    absl::string_view expression_use,
    absl::flat_hash_set<std::string>* dependent_column_names) {
  zetasql::SimpleTable simple_table(table->Name(), name_and_types);
  zetasql::AnalyzerOptions options = MakeGoogleSqlAnalyzerOptions();
  for (const auto& name_and_type : name_and_types) {
    ZETASQL_RETURN_IF_ERROR(
        options.AddExpressionColumn(name_and_type.first, name_and_type.second));
  }
  std::unique_ptr<const zetasql::AnalyzerOutput> output;
  FunctionCatalog function_catalog(type_factory_);
  Catalog catalog(latest_schema_, &function_catalog);

  ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeExpressionForAssignmentToType(
      expression, options, &catalog, type_factory_, target_type, &output));

  QueryEngineOptions query_engine_options;
  ColumnExpressionValidator validator(latest_schema_, &simple_table,
                                      &query_engine_options, expression_use,
                                      dependent_column_names);
  ZETASQL_RETURN_IF_ERROR(output->resolved_expr()->Accept(&validator));
  if (output->resolved_expr()->GetTreeDepth() >
      limits::kColumnExpressionMaxDepth) {
    return error::ColumnExpressionMaxDepthExceeded(
        output->resolved_expr()->GetTreeDepth(),
        limits::kColumnExpressionMaxDepth);
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

  return AnalyzeColumnExpression(expression, column_type, table, name_and_types,
                                 "stored generated columns",
                                 dependent_column_names);
}

absl::Status SchemaUpdaterImpl::AnalyzeCheckConstraint(
    absl::string_view expression, const Table* table,
    const ddl::CreateTable* ddl_create_table,
    absl::flat_hash_set<std::string>* dependent_column_names,
    CheckConstraint::Builder* builder) {
  std::vector<zetasql::SimpleTable::NameAndType> name_and_types;
  ZETASQL_RETURN_IF_ERROR(InitColumnNameAndTypesFromTable(table, ddl_create_table,
                                                  &name_and_types));

  ZETASQL_RETURN_IF_ERROR(AnalyzeColumnExpression(
      expression, zetasql::types::BoolType(), table, name_and_types,
      "check constraints", dependent_column_names));

  for (const std::string& column_name : *dependent_column_names) {
    builder->add_dependent_column(table->FindColumn(column_name));
  }
  return absl::OkStatus();
}

template <typename ColumnDefModifer>
absl::Status SchemaUpdaterImpl::SetColumnDefinition(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    const ddl::CreateTable* ddl_create_table, ColumnDefModifer* modifier) {
  bool is_generated = false;
  // Process any changes in column definition.
  if (ddl_column.has_properties() &&
      ddl_column.properties().has_column_type()) {
    ZETASQL_ASSIGN_OR_RETURN(const zetasql::Type* column_type,
                     DDLColumnTypeToGoogleSqlType(
                         ddl_column.properties().column_type(), type_factory_));
    modifier->set_type(column_type);

    if (ddl_column.properties().has_expression()) {
      is_generated = true;
      modifier->set_expression(ddl_column.properties().expression());
      absl::flat_hash_set<std::string> dependent_column_names;
      absl::Status s = AnalyzeGeneratedColumn(
          ddl_column.properties().expression(), ddl_column.column_name(),
          column_type, table, ddl_create_table, &dependent_column_names);
      if (!s.ok()) {
        return error::GeneratedColumnDefinitionParseError(
            table->Name(), ddl_column.column_name(), s.message());
      }
      for (const std::string& column_name : dependent_column_names) {
        modifier->add_dependent_column_name(column_name);
      }
    }
  }

  if (!is_generated) {
    // Altering a generated column to a non-generated column is disallowed. In
    // that case, the expression is cleared here and later validation at
    // column_validator.cc will block it.
    modifier->clear_expression();
  }

  // Set the default values for nullability and length.
  modifier->set_nullable(true);
  modifier->set_declared_max_length(absl::nullopt);
  for (const ddl::Constraint& ddl_constraint : ddl_column.constraints()) {
    switch (ddl_constraint.kind_case()) {
      case ddl::Constraint::kNotNull: {
        modifier->set_nullable(ddl_constraint.not_null().nullable());
        break;
      }
      case ddl::Constraint::kColumnLength: {
        modifier->set_declared_max_length(
            ddl_constraint.column_length().max_length());
        break;
      }
      default:
        ZETASQL_RET_CHECK(false) << "Unexpected constraint: "
                         << ddl_constraint.kind_case()
                         << " for column: " << ddl_column.column_name();
    }
  }

  if (ddl_column.has_options()) {
    ZETASQL_RETURN_IF_ERROR(SetColumnOptions(ddl_column.options(), modifier));
  }

  return absl::OkStatus();
}

zetasql_base::StatusOr<const Column*> SchemaUpdaterImpl::CreateColumn(
    const ddl::ColumnDefinition& ddl_column, const Table* table,
    const ddl::CreateTable* ddl_table) {
  const std::string& column_name = ddl_column.column_name();
  Column::Builder builder;
  builder
      .set_id(column_id_generator_->NextId(
          absl::StrCat(table->Name(), ".", column_name)))
      .set_name(column_name);
  ZETASQL_RETURN_IF_ERROR(SetColumnDefinition(ddl_column, table, ddl_table, &builder));
  const Column* column = builder.get();
  builder.set_table(table);
  if (column->is_generated()) {
    statement_context_->AddAction(
        [column](const SchemaValidationContext* context) {
          return BackfillGeneratedColumnValue(column, context);
        });
  }
  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return column;
}

absl::Status SchemaUpdaterImpl::CreateInterleaveConstraint(
    const ddl::InterleaveConstraint& interleave, Table::Builder* builder) {
  ZETASQL_ASSIGN_OR_RETURN(const Table* parent,
                   GetInterleaveConstraintTable(interleave, *builder));
  Table::OnDeleteAction on_delete = GetInterleaveConstraintOnDelete(interleave);
  return CreateInterleaveConstraint(parent, on_delete, builder);
}

absl::Status SchemaUpdaterImpl::CreateInterleaveConstraint(
    const Table* parent, Table::OnDeleteAction on_delete,
    Table::Builder* builder) {
  ZETASQL_RET_CHECK_EQ(builder->get()->parent(), nullptr);

  ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
      parent, [builder](Table::Editor* parent_editor) -> absl::Status {
        parent_editor->add_child_table(builder->get());
        builder->set_parent_table(parent_editor->get());
        return absl::OkStatus();
      }));

  builder->set_on_delete(on_delete);
  return absl::OkStatus();
}

zetasql_base::StatusOr<const Table*> SchemaUpdaterImpl::GetInterleaveConstraintTable(
    const ddl::InterleaveConstraint& interleave,
    const Table::Builder& builder) const {
  const auto* parent = latest_schema_->FindTable(interleave.parent());
  if (parent == nullptr) {
    const Table* table = builder.get();
    if (table->owner_index() == nullptr) {
      return error::TableNotFound(interleave.parent());
    } else {
      return error::IndexInterleaveTableNotFound(table->owner_index()->Name(),
                                                 interleave.parent());
    }
  }
  return parent;
}

Table::OnDeleteAction SchemaUpdaterImpl::GetInterleaveConstraintOnDelete(
    const ddl::InterleaveConstraint& interleave) {
  return interleave.on_delete().action() == ddl::OnDeleteAction::CASCADE
             ? Table::OnDeleteAction::kCascade
             : Table::OnDeleteAction::kNoAction;
}

absl::Status SchemaUpdaterImpl::CreatePrimaryKeyConstraint(
    const ddl::PrimaryKeyConstraint& ddl_primary_key, Table::Builder* builder) {
  for (const ddl::PrimaryKeyConstraint::KeyPart& ddl_key_part :
       ddl_primary_key.key_part()) {
    ZETASQL_ASSIGN_OR_RETURN(const KeyColumn* key_col,
                     CreatePrimaryKeyColumn(ddl_key_part, builder->get()));
    builder->add_key_column(key_col);
  }
  return absl::OkStatus();
}

zetasql_base::StatusOr<const KeyColumn*> SchemaUpdaterImpl::CreatePrimaryKeyColumn(
    const ddl::PrimaryKeyConstraint::KeyPart& ddl_key_part,
    const Table* table) {
  KeyColumn::Builder builder;
  const std::string& key_column_name = ddl_key_part.key_column_name();
  bool is_descending =
      (ddl_key_part.order() == ddl::PrimaryKeyConstraint::DESC);

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
    const ddl::ForeignKeyConstraint& ddl_foreign_key,
    const Table* referencing_table) {
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
      referencing_table, ddl_foreign_key.referencing_column_name(),
      index_column_order, &referencing_index_required));
  if (referencing_index_required) {
    ZETASQL_ASSIGN_OR_RETURN(
        const Index* referencing_index,
        CreateForeignKeyIndex(
            foreign_key, referencing_table,
            index_column_names(ddl_foreign_key.referencing_column_name(),
                               index_column_order),
            /*unique=*/false));
    ZETASQL_RETURN_IF_ERROR(
        AlterNode<ForeignKey>(foreign_key, [&](ForeignKey::Editor* editor) {
          editor->set_referencing_index(referencing_index);
          return absl::OkStatus();
        }));
  }

  // Validate any existing data. Skip new tables which have no data yet.
  if (latest_schema_->FindTable(referencing_table->Name()) != nullptr) {
    statement_context_->AddAction(
        [foreign_key](const SchemaValidationContext* context) {
          return VerifyForeignKeyData(foreign_key, context);
        });
  }

  return absl::OkStatus();
}

zetasql_base::StatusOr<const ForeignKey*> SchemaUpdaterImpl::BuildForeignKeyConstraint(
    const ddl::ForeignKeyConstraint& ddl_foreign_key,
    const Table* referencing_table) {
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
                              ddl_foreign_key.referencing_column_name(),
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

zetasql_base::StatusOr<const Index*> SchemaUpdaterImpl::CreateForeignKeyIndex(
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
    ddl_index.set_table_name(table->Name());
    ddl_index.mutable_properties()->set_unique(unique);
    ddl_index.mutable_properties()->set_null_filtered(null_filtered);
    ddl::PrimaryKeyConstraint* primary_key =
        ddl_index.add_constraints()->mutable_primary_key();
    for (const std::string& column_name : column_names) {
      auto* key_part = primary_key->add_key_part();
      key_part->set_key_column_name(column_name);
      const auto* key_column = table->FindKeyColumn(column_name);
      if (key_column != nullptr && key_column->is_descending()) {
        key_part->set_order(ddl::PrimaryKeyConstraint::DESC);
      }
    }
    if (CanInterleaveForeignKeyIndex(table, column_names)) {
      ddl_index.add_constraints()->mutable_interleave()->set_parent(
          table->Name());
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
  if (ddl_check_constraint.has_constraint_name() &&
      !ddl_check_constraint.constraint_name().empty()) {
    check_constraint_name = ddl_check_constraint.constraint_name();
    ZETASQL_RETURN_IF_ERROR(
        global_names_.AddName("Check Constraint", check_constraint_name));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(check_constraint_name,
                     global_names_.GenerateCheckConstraintName(table->Name()));
    is_generated_name = true;
  }
  builder.set_constraint_name(check_constraint_name);
  builder.has_generated_name(is_generated_name);
  builder.set_expression(ddl_check_constraint.sql_expression());

  absl::flat_hash_set<std::string> dependent_column_names;
  absl::Status s = AnalyzeCheckConstraint(ddl_check_constraint.sql_expression(),
                                          table, ddl_create_table,
                                          &dependent_column_names, &builder);

  if (!s.ok()) {
    const std::string display_name =
        is_generated_name ? "<unnamed>" : check_constraint_name;
    return error::CheckConstraintExpressionParseError(
        table->Name(), ddl_check_constraint.sql_expression(), display_name,
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

absl::Status SchemaUpdaterImpl::CreateTable(const ddl::CreateTable& ddl_table) {
  if (latest_schema_->tables().size() >= limits::kMaxTablesPerDatabase) {
    return error::TooManyTablesPerDatabase(ddl_table.table_name(),
                                           limits::kMaxTablesPerDatabase);
  }

  ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Table", ddl_table.table_name()));

  Table::Builder builder;
  builder.set_id(table_id_generator_->NextId(ddl_table.table_name()))
      .set_name(ddl_table.table_name());

  for (const ddl::ColumnDefinition& ddl_column : ddl_table.columns()) {
    ZETASQL_ASSIGN_OR_RETURN(const Column* column,
                     CreateColumn(ddl_column, builder.get(), &ddl_table));
    builder.add_column(column);
  }

  for (const Column* column : builder.get()->columns()) {
    if (column->is_generated()) {
      const_cast<Column*>(column)->PopulateDependentColumns();
    }
  }

  // Some constraints have a dependency on the primary key, so create it first.
  for (const ddl::Constraint& ddl_constraint : ddl_table.constraints()) {
    if (ddl_constraint.kind_case() == ddl::Constraint::kPrimaryKey) {
      ZETASQL_RETURN_IF_ERROR(
          CreatePrimaryKeyConstraint(ddl_constraint.primary_key(), &builder));
      break;
    }
  }

  for (const ddl::Constraint& ddl_constraint : ddl_table.constraints()) {
    switch (ddl_constraint.kind_case()) {
      case ddl::Constraint::kInterleave: {
        ZETASQL_RETURN_IF_ERROR(
            CreateInterleaveConstraint(ddl_constraint.interleave(), &builder));
        break;
      }
      case ddl::Constraint::kForeignKey: {
        ZETASQL_RETURN_IF_ERROR(CreateForeignKeyConstraint(ddl_constraint.foreign_key(),
                                                   builder.get()));
        break;
      }
      case ddl::Constraint::kPrimaryKey:
        break;
      case ddl::Constraint::kCheck:
        ZETASQL_RETURN_IF_ERROR(CreateCheckConstraint(ddl_constraint.check(),
                                              builder.get(), &ddl_table));
        break;
      default:
        ZETASQL_RET_CHECK(false) << "Unsupported constraint type: "
                         << ddl_constraint.DebugString();
    }
  }

  ZETASQL_RETURN_IF_ERROR(AddNode(builder.build()));
  return absl::OkStatus();
}

zetasql_base::StatusOr<const Column*> SchemaUpdaterImpl::CreateIndexDataTableColumn(
    const Table* indexed_table, const std::string& source_column_name,
    const Table* index_data_table, bool null_filtered_key_column) {
  const Column* source_column = indexed_table->FindColumn(source_column_name);
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

zetasql_base::StatusOr<std::unique_ptr<const Table>>
SchemaUpdaterImpl::CreateIndexDataTable(
    const ddl::CreateIndex& ddl_index, const Index* index,
    const Table* indexed_table,
    std::vector<const KeyColumn*>* index_key_columns,
    std::vector<const Column*>* stored_columns) {
  std::string table_name =
      absl::StrCat(kIndexDataTablePrefix, ddl_index.index_name());
  Table::Builder builder;
  builder.set_name(table_name)
      .set_id(table_id_generator_->NextId(table_name))
      .set_owner_index(index);

  // Add indexed columns to the index_data_table's columns and primary key.
  for (const ddl::Constraint& ddl_constraint : ddl_index.constraints()) {
    switch (ddl_constraint.kind_case()) {
      case ddl::Constraint::kPrimaryKey: {
        const ddl::PrimaryKeyConstraint ddl_primary_key =
            ddl_constraint.primary_key();
        // The primary key is a combination of (index_keys,indexed_table_keys)
        ddl::PrimaryKeyConstraint data_table_pk = ddl_primary_key;

        // First create columns for the specified primary key.
        for (const ddl::PrimaryKeyConstraint::KeyPart& ddl_key_part :
             ddl_primary_key.key_part()) {
          const std::string& column_name = ddl_key_part.key_column_name();
          ZETASQL_ASSIGN_OR_RETURN(const Column* column,
                           CreateIndexDataTableColumn(
                               indexed_table, column_name, builder.get(),
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
          ZETASQL_ASSIGN_OR_RETURN(const Column* column,
                           CreateIndexDataTableColumn(
                               indexed_table, key_col_name, builder.get(),
                               index->is_null_filtered()));
          builder.add_column(column);

          // Add to the PK specification.
          ddl::PrimaryKeyConstraint::KeyPart* key_part =
              data_table_pk.add_key_part();
          key_part->set_key_column_name(key_col_name);
          if (key_col->is_descending()) {
            key_part->set_order(ddl::PrimaryKeyConstraint::DESC);
          }
        }

        ZETASQL_RETURN_IF_ERROR(CreatePrimaryKeyConstraint(data_table_pk, &builder));
        int num_declared_keys = ddl_primary_key.key_part_size();
        auto data_table_key_cols = builder.get()->primary_key();
        for (int i = 0; i < num_declared_keys; ++i) {
          index_key_columns->push_back(data_table_key_cols[i]);
        }
        break;
      }
      case ddl::Constraint::kInterleave: {
        const auto& ddl_interleave = ddl_constraint.interleave();
        const Table* parent_table = indexed_table;
        if (!absl::EqualsIgnoreCase(parent_table->Name(),
                                    ddl_interleave.parent())) {
          ZETASQL_ASSIGN_OR_RETURN(parent_table, GetInterleaveConstraintTable(
                                             ddl_interleave, builder));
        }
        ZETASQL_RETURN_IF_ERROR(CreateInterleaveConstraint(
            parent_table, Table::OnDeleteAction::kCascade, &builder));
        break;
      }
      default:
        ZETASQL_RET_CHECK(false) << "Unsupported constraint type: "
                         << ddl_constraint.DebugString();
    }
  }

  // Add stored columns to index data table.
  for (const ddl::ColumnDefinition& ddl_column : ddl_index.columns()) {
    ZETASQL_RET_CHECK(ddl_column.has_properties() &&
              ddl_column.properties().has_stored() &&
              ddl_column.properties().stored() == ddl_column.column_name())
        << "Invalid stored column specification for index: "
        << ddl_index.DebugString() << " " << ddl_column.DebugString();
    const std::string& column_name = ddl_column.column_name();
    ZETASQL_ASSIGN_OR_RETURN(
        const Column* column,
        CreateIndexDataTableColumn(indexed_table, column_name, builder.get(),
                                   /*null_filtered_key_column=*/false));
    builder.add_column(column);
    stored_columns->push_back(column);
  }

  return builder.build();
}

zetasql_base::StatusOr<const Index*> SchemaUpdaterImpl::CreateIndex(
    const ddl::CreateIndex& ddl_index, const Table* indexed_table) {
  if (indexed_table == nullptr) {
    indexed_table = latest_schema_->FindTable(ddl_index.table_name());
    if (indexed_table == nullptr) {
      return error::TableNotFound(ddl_index.table_name());
    }
  }
  if (latest_schema_->num_index() >= limits::kMaxIndexesPerDatabase) {
    return error::TooManyIndicesPerDatabase(ddl_index.index_name(),
                                            limits::kMaxIndexesPerDatabase);
  }

  // Tables and indexes share a namespace.
  ZETASQL_RETURN_IF_ERROR(global_names_.AddName("Index", ddl_index.index_name()));

  Index::Builder builder;
  builder.set_name(ddl_index.index_name())
      .set_unique(ddl_index.properties().unique())
      .set_null_filtered(ddl_index.properties().null_filtered());

  std::vector<const KeyColumn*> key_columns;
  std::vector<const Column*> stored_columns;
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const Table> data_table,
                   CreateIndexDataTable(ddl_index, builder.get(), indexed_table,
                                        &key_columns, &stored_columns));
  builder.set_index_data_table(data_table.get());

  for (const KeyColumn* key_col : key_columns) {
    builder.add_key_column(key_col);
  }

  for (const Column* col : stored_columns) {
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

absl::Status SchemaUpdaterImpl::AlterTable(const ddl::AlterTable& alter_table) {
  const Table* table = latest_schema_->FindTable(alter_table.table_name());
  if (table == nullptr) {
    return error::TableNotFound(alter_table.table_name());
  }

  ZETASQL_RET_CHECK(alter_table.has_alter_column() ||
            alter_table.has_alter_constraint());

  if (alter_table.has_alter_constraint()) {
    const auto& alter_constraint = alter_table.alter_constraint();
    auto alter_type = alter_constraint.type();
    if (alter_constraint.constraint().has_interleave() &&
        alter_type == ddl::AlterConstraint::ALTER) {
      return AlterInterleave(alter_constraint.constraint().interleave(), table);
    }
    if (alter_constraint.constraint().has_check() &&
        alter_type == ddl::AlterConstraint::ADD) {
      return AddCheckConstraint(alter_constraint.constraint().check(), table);
    }
    if (alter_constraint.constraint().has_foreign_key() &&
        alter_type == ddl::AlterConstraint::ADD) {
      return AddForeignKey(alter_constraint.constraint().foreign_key(), table);
    }
    if (!alter_constraint.has_constraint() &&
        alter_constraint.has_constraint_name() &&
        alter_type == ddl::AlterConstraint::DROP) {
      return DropConstraint(alter_constraint.constraint_name(), table);
    }
    return error::Internal(
        absl::StrCat("Invalid alter table constraint operation: ",
                     alter_table.DebugString()));
  }

  if (alter_table.has_alter_column()) {
    const auto& alter_column = alter_table.alter_column();
    switch (alter_column.type()) {
      case ddl::AlterColumn::ADD: {
        const auto& column_def = alter_column.column();
        ZETASQL_ASSIGN_OR_RETURN(
            const Column* new_column,
            CreateColumn(column_def, table, /*ddl_table=*/nullptr));
        if (new_column->is_generated()) {
          const_cast<Column*>(new_column)->PopulateDependentColumns();
        }
        ZETASQL_RETURN_IF_ERROR(AlterNode<Table>(
            table, [new_column](Table::Editor* editor) -> absl::Status {
              editor->add_column(new_column);
              return absl::OkStatus();
            }));
        break;
      }
      case ddl::AlterColumn::ALTER: {
        const Column* column = table->FindColumn(alter_column.column_name());
        if (column == nullptr) {
          return error::ColumnNotFound(table->Name(),
                                       alter_column.column_name());
        }
        const auto& column_def = alter_column.column();
        ZETASQL_RETURN_IF_ERROR(AlterNode<Column>(
            column,
            [this, &column_def,
             &table](Column::Editor* editor) -> absl::Status {
              return AlterColumnDefinition(column_def, table, editor);
            }));
        break;
      }
      case ddl::AlterColumn::DROP: {
        const Column* column = table->FindColumn(alter_column.column_name());
        if (column == nullptr) {
          return error::ColumnNotFound(table->Name(),
                                       alter_column.column_name());
        }
        ZETASQL_RETURN_IF_ERROR(DropNode(column));
        break;
      }
      default:
        ZETASQL_RET_CHECK(false) << "Invalid alter column specification: "
                         << alter_column.DebugString();
    }
    return absl::OkStatus();
  }

  return absl::OkStatus();
}

absl::Status SchemaUpdaterImpl::AlterInterleave(
    const ddl::InterleaveConstraint& ddl_interleave, const Table* table) {
  return AlterNode<Table>(table, [&](Table::Editor* editor) {
    if (ddl_interleave.on_delete().action() == ddl::OnDeleteAction::CASCADE) {
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
    const ddl::ForeignKeyConstraint& ddl_foreign_key, const Table* table) {
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
  const Table* table = latest_schema_->FindTable(drop_table.table_name());
  if (table == nullptr) {
    return error::TableNotFound(drop_table.table_name());
  }
  return DropNode(table);
}

absl::Status SchemaUpdaterImpl::DropIndex(const ddl::DropIndex& drop_index) {
  const Index* index = latest_schema_->FindIndex(drop_index.index_name());
  if (index == nullptr) {
    return error::IndexNotFound(drop_index.index_name());
  }
  return DropNode(index);
}

}  // namespace

const Schema* SchemaUpdater::EmptySchema() {
  static const Schema* empty_schema = new Schema;
  return empty_schema;
}

zetasql_base::StatusOr<std::unique_ptr<const Schema>>
SchemaUpdater::ValidateSchemaFromDDL(absl::Span<const std::string> statements,
                                     const SchemaChangeContext& context,
                                     const Schema* existing_schema) {
  if (existing_schema == nullptr) {
    existing_schema = EmptySchema();
  }
  ZETASQL_ASSIGN_OR_RETURN(SchemaUpdaterImpl updater,
                   SchemaUpdaterImpl::Build(
                       context.type_factory, context.table_id_generator,
                       context.column_id_generator, context.storage,
                       context.schema_change_timestamp, existing_schema));
  ZETASQL_ASSIGN_OR_RETURN(pending_work_, updater.ApplyDDLStatements(statements));
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

zetasql_base::StatusOr<SchemaChangeResult> SchemaUpdater::UpdateSchemaFromDDL(
    const Schema* existing_schema, absl::Span<const std::string> statements,
    const SchemaChangeContext& context) {
  ZETASQL_ASSIGN_OR_RETURN(SchemaUpdaterImpl updater,
                   SchemaUpdaterImpl::Build(
                       context.type_factory, context.table_id_generator,
                       context.column_id_generator, context.storage,
                       context.schema_change_timestamp, existing_schema));
  ZETASQL_ASSIGN_OR_RETURN(pending_work_, updater.ApplyDDLStatements(statements));
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

zetasql_base::StatusOr<std::unique_ptr<const Schema>>
SchemaUpdater::CreateSchemaFromDDL(absl::Span<const std::string> statements,
                                   const SchemaChangeContext& context) {
  ZETASQL_ASSIGN_OR_RETURN(SchemaChangeResult result,
                   UpdateSchemaFromDDL(EmptySchema(), statements, context));
  ZETASQL_RETURN_IF_ERROR(result.backfill_status);
  return std::move(result.updated_schema);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
