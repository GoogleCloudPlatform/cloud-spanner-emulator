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

#include "backend/query/catalog.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/function.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/property_graph.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/query/change_stream/queryable_change_stream_tvf.h"
#include "backend/query/function_catalog.h"
#include "backend/query/information_schema_catalog.h"
#include "backend/query/pg_catalog.h"
#include "backend/query/queryable_model.h"
#include "backend/query/queryable_named_schema.h"
#include "backend/query/queryable_property_graph.h"
#include "backend/query/queryable_sequence.h"
#include "backend/query/queryable_table.h"
#include "backend/query/queryable_udf.h"
#include "backend/query/queryable_view.h"
#include "backend/query/spanner_sys_catalog.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "common/errors.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"
#include "third_party/spanner_pg/datatypes/extended/conversion_finder.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "google/protobuf/descriptor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using postgres_translator::spangres::datatypes::GetPgJsonbType;
using postgres_translator::spangres::datatypes::GetPgNumericType;
using postgres_translator::spangres::datatypes::GetPgOidType;

// A sub-catalog used for resolving NET function lookups.
class NetCatalog : public googlesql::Catalog {
 public:
  explicit NetCatalog(backend::Catalog* root_catalog)
      : root_catalog_(root_catalog) {}

  static constexpr char kName[] = "NET";

  std::string FullName() const final {
    std::string name = root_catalog_->FullName();
    if (name.empty()) {
      return kName;
    }
    absl::StrAppend(&name, kName);
    return name;
  }

  // Implementation of the googlesql::Catalog interface.
  absl::Status GetFunction(const std::string& name,
                           const googlesql::Function** function,
                           const FindOptions& options) final {
    // The list of all functions is maintained in the root catalog in the
    // form of their fully-qualified names. Just prefix the function name
    // with the name of 'this' catalog and delegate the request to parent.
    return root_catalog_->GetFunction(absl::StrJoin({FullName(), name}, "."),
                                      function, options);
  }

 private:
  backend::Catalog* root_catalog_;
};

// A sub-catalog used for resolving AI function lookups.
class AiCatalog : public googlesql::Catalog {
 public:
  explicit AiCatalog(backend::Catalog* root_catalog)
      : root_catalog_(root_catalog) {}
  static constexpr char kName[] = "AI";

  std::string FullName() const final {
    std::string name = root_catalog_->FullName();
    if (name.empty()) {
      return kName;
    }
    absl::StrAppend(&name, kName);
    return name;
  }

  // Implementation of the googlesql::Catalog interface.
  absl::Status GetFunction(const std::string& name,
                           const googlesql::Function** function,
                           const FindOptions& options) final {
    // The list of all functions is maintained in the root catalog in the
    // form of their fully-qualified names. Just prefix the function name
    // with the name of 'this' catalog and delegate the request to parent.
    return root_catalog_->GetFunction(absl::StrJoin({FullName(), name}, "."),
                                      function, options);
  }

 private:
  backend::Catalog* root_catalog_;
};

// A sub-catalog used for resolving PG function lookups from GSQL queries.
// Required for supporting check constraints as PG queries are translated to
// GSQL queries before storing in the DDL statement.
class PGFunctionCatalog : public googlesql::Catalog {
 public:
  explicit PGFunctionCatalog(backend::Catalog* root_catalog)
      : root_catalog_(root_catalog) {}

  static constexpr char kName[] = "PG";
  static constexpr char kPgNumericTypeName[] = "PG.NUMERIC";
  static constexpr char kPgJsonbTypeName[] = "PG.JSONB";
  static constexpr char kPgOidTypeName[] = "PG.OID";

  std::string FullName() const final {
    std::string name = root_catalog_->FullName();
    if (name.empty()) {
      return kName;
    }
    absl::StrAppend(&name, kName);
    return name;
  }

  // Implementation of the googlesql::Catalog interface.
  absl::Status GetFunction(const std::string& name,
                           const googlesql::Function** function,
                           const FindOptions& options) final {
    // The list of all functions is maintained in the root catalog in the
    // form of their fully-qualified names. Just prefix the function name
    // with the name of 'this' catalog and delegate the request to parent.
    return root_catalog_->GetFunction(absl::StrJoin({FullName(), name}, "."),
                                      function, options);
  }

  // Similar to GetFunction. Types are maintained in the root catalog in the
  // form of their fully-qualified names, such as PG.NUMERIC and PG.JSONB.
  // Redirect the request to the parent.
  absl::Status GetType(const std::string& name, const googlesql::Type** type,
                       const FindOptions& options) final {
    std::string full_name = absl::StrJoin({FullName(), name}, ".");
    if (root_catalog_->schema_ != nullptr &&
        root_catalog_->schema_->dialect() ==
            database_api::DatabaseDialect::POSTGRESQL) {
      if (absl::EqualsIgnoreCase(full_name, kPgNumericTypeName)) {
        *type = GetPgNumericType();
        return absl::OkStatus();
      } else if (absl::EqualsIgnoreCase(full_name, kPgJsonbTypeName)) {
        *type = GetPgJsonbType();
        return absl::OkStatus();
      } else if (absl::EqualsIgnoreCase(full_name, kPgOidTypeName)) {
        *type = GetPgOidType();
        return absl::OkStatus();
      }
    }
    return root_catalog_->GetType(full_name, type, options);
  }

 private:
  backend::Catalog* root_catalog_;
};

Catalog::Catalog(
    const Schema* schema, const FunctionCatalog* function_catalog,
    googlesql::TypeFactory* type_factory,
    const googlesql::AnalyzerOptions& options, RowReader* reader,
    QueryEvaluator* query_evaluator,
    std::optional<std::string> change_stream_internal_lookup,
    const absl::flat_hash_map<std::string, google::protobuf::Value>&
        secure_context)
    : schema_(schema),
      function_catalog_(function_catalog),
      type_factory_(type_factory),
      secure_context_(secure_context) {
  // SECURE_CONTEXT is query-specific because it accesses query-level
  // secure parameters. We construct it here using the query-specific
  // secure_context_ map and store it in the Catalog. We do not register
  // it in the global FunctionCatalog to ensure it is only accessible
  // when a query context is available.
  secure_context_function_ =
      CreateSecureContextFunction("Spanner", secure_context_);
  for (const auto* named_schema : schema->named_schemas()) {
    named_schemas_[named_schema->Name()] =
        std::make_unique<QueryableNamedSchema>(named_schema);
  }

  for (const auto* udf : schema->udfs()) {
    std::string udf_name = udf->Name();
    auto queryable_udf = QueryableUdf::Create(udf, schema->default_time_zone(),
                                              this, type_factory);
    if (!queryable_udf.ok()) {
      ABSL_LOG(ERROR) << "Skipping UDF: " << udf_name
                 << " due to a failure: " << queryable_udf.status().message();
      continue;
    }

    if (SDLObjectName::IsFullyQualifiedName(udf_name)) {
      const auto [schema_part, name_part] =
          SDLObjectName::SplitSchemaName(udf_name);
      absl::Status status = AddObjectToNamedSchema(std::string(schema_part),
                                                   *std::move(queryable_udf));
      LOG_IF(ERROR, !status.ok()) << status.message();
    } else {
      udfs_[udf->Name()] = *std::move(queryable_udf);
    }
  }

  // Pass the sequences to the catalog. This step has to be before the below
  // schema objects, because sequences may be used by table columns and views.
  for (const backend::Sequence* sequence : schema->sequences()) {
    std::string name = sequence->Name();
    if (SDLObjectName::IsFullyQualifiedName(name)) {
      absl::Status status = AddObjectToNamedSchema(
          std::string(SDLObjectName::GetSchemaName(name)),
          std::make_unique<QueryableSequence>(sequence));
      LOG_IF(ERROR, !status.ok()) << status.message();
    } else {
      sequences_[sequence->Name()] =
          std::make_unique<QueryableSequence>(sequence);
    }
  }

  // Pass the reader to tables.
  for (const auto* table : schema->tables()) {
    std::string name = table->Name();
    if (SDLObjectName::IsFullyQualifiedName(name)) {
      absl::Status status = AddObjectToNamedSchema(
          std::string(SDLObjectName::GetSchemaName(name)),
          std::make_unique<QueryableTable>(table, reader, options, this,
                                           type_factory));
      LOG_IF(ERROR, !status.ok()) << status.message();
    } else {
      tables_[table->Name()] = std::make_unique<QueryableTable>(
          table, reader, options, this, type_factory);
    }

    std::string synonym_name = table->synonym();
    if (!synonym_name.empty()) {
      if (SDLObjectName::IsFullyQualifiedName(synonym_name)) {
        absl::Status status = AddObjectToNamedSchema(
            std::string(SDLObjectName::GetSchemaName(synonym_name)),
            std::make_unique<QueryableTable>(table, reader, options, this,
                                             type_factory,
                                             /*is_synonym=*/true));
        LOG_IF(ERROR, !status.ok()) << status.message();
      } else {
        tables_[synonym_name] = std::make_unique<QueryableTable>(
            table, reader, options, this, type_factory, /*is_synonym=*/true);
      }
    }
  }

  for (const auto* model : schema->models()) {
    std::string name = model->Name();
    if (SDLObjectName::IsFullyQualifiedName(name)) {
      absl::Status status = AddObjectToNamedSchema(
          std::string(SDLObjectName::GetSchemaName(name)),
          std::make_unique<QueryableModel>(model));
      LOG_IF(ERROR, !status.ok()) << status.message();
    } else {
      models_[model->Name()] = std::make_unique<QueryableModel>(model);
    }
  }

  // Pass the query_evaluator to views.
  for (const auto* view : schema->views()) {
    std::string name = view->Name();
    if (SDLObjectName::IsFullyQualifiedName(name)) {
      absl::Status status = AddObjectToNamedSchema(
          std::string(SDLObjectName::GetSchemaName(name)),
          std::make_unique<QueryableView>(view, query_evaluator));
      LOG_IF(ERROR, !status.ok()) << status.message();
    } else {
      views_[view->Name()] =
          std::make_unique<QueryableView>(view, query_evaluator);
    }
  }

  for (const auto* graph : schema->property_graphs()) {
    std::string name = graph->Name();
    // Confirm that the property graph is not in a named schema as that is not
    // supported yet.
    if (SDLObjectName::IsFullyQualifiedName(name)) {
      ABSL_LOG(FATAL) << "PropertyGraph not supported in named schemas. " << name;
    } else {
      for (const auto& node_table : graph->NodeTables()) {
        std::string node_table_name = node_table.name();
        if (SDLObjectName::IsFullyQualifiedName(node_table_name)) {
          ABSL_LOG(FATAL) << "PropertyGraph not supported in named schemas. "
                     << "Element table is within a named schema: "
                     << node_table_name;
        }
      }
      for (const auto& edge_table : graph->EdgeTables()) {
        std::string edge_table_name = edge_table.name();
        if (SDLObjectName::IsFullyQualifiedName(edge_table_name)) {
          ABSL_LOG(FATAL) << "PropertyGraph not supported in named schemas. "
                     << "Element table is within a named schema: "
                     << edge_table_name;
        }
      }
      property_graphs_[graph->Name()] =
          std::make_unique<QueryablePropertyGraph>(this, type_factory, graph);
    }
  }

  if (change_stream_internal_lookup.has_value()) {
    auto change_stream =
        schema->FindChangeStream(change_stream_internal_lookup.value());
    auto partition_table = change_stream->change_stream_partition_table();
    auto data_table = change_stream->change_stream_data_table();
    tables_[partition_table->Name()] = std::make_unique<QueryableTable>(
        partition_table, reader, options, this, type_factory);
    tables_[data_table->Name()] = std::make_unique<QueryableTable>(
        data_table, reader, options, this, type_factory);
  }
  // Register a table valued function for each active change stream
  for (const auto* change_stream : schema->change_streams()) {
    tvfs_[change_stream->tvf_name()] =
        std::move(*QueryableChangeStreamTvf::Create(
            change_stream->tvf_name(), options, this, type_factory,
            /*is_pg=*/schema->dialect() ==
                database_api::DatabaseDialect::POSTGRESQL,
            /*is_mutable_key_range=*/change_stream->partition_mode() ==
                kChangeStreamPartitionModeMutableKeyRange));
  }

  // Read types.
  for (const auto& tablepair : tables_) {
    const QueryableTable* table = tablepair.second.get();
    for (int i = 0; i < table->NumColumns(); ++i) {
      std::string type_name = table->GetColumn(i)->GetType()->TypeName(
          googlesql::PRODUCT_EXTERNAL, /*use_external_float32=*/true);
      types_[type_name] = table->GetColumn(i)->GetType();
    }
  }

  if (absl::Status status = PopulateSystemProcedureMap(); !status.ok()) {
    // Not an error that requires us to exit the emulator.
    ABSL_LOG(ERROR) << "Failed to populate system procedure map: ";
  }
}

absl::Status Catalog::PopulateSystemProcedureMap() {
  // Context id is required if we have to hold and pass on some context for
  // the implementation to map the signature back to an evaluator. Not
  // required here.
  auto [_, inserted] = procedures_.emplace(
      "cancel_query",
      std::make_unique<googlesql::Procedure>(
          std::vector<std::string>{"cancel_query"},
          googlesql::FunctionSignature(
              googlesql::types::BoolType(),
              {googlesql::FunctionArgumentType(googlesql::types::StringType())},
              /*context_id=*/-1)));
  if (!inserted) {
    return absl::InternalError("Unable to populate system procedure map.");
  }
  return absl::OkStatus();
}

absl::Status Catalog::GetCatalog(const std::string& name,
                                 googlesql::Catalog** catalog,
                                 const FindOptions& options) {
  if (absl::EqualsIgnoreCase(name, InformationSchemaCatalog::kName)) {
    *catalog = GetInformationSchemaCatalog();
  } else if (absl::EqualsIgnoreCase(name, InformationSchemaCatalog::kPGName)) {
    *catalog = GetPGInformationSchemaCatalog();
  } else if (absl::EqualsIgnoreCase(name, SpannerSysCatalog::kName)) {
    *catalog = GetSpannerSysCatalog();
  } else if (absl::EqualsIgnoreCase(name, NetCatalog::kName)) {
    *catalog = GetNetFunctionsCatalog();
  } else if (absl::EqualsIgnoreCase(name, AiCatalog::kName)) {
    *catalog = GetAiFunctionsCatalog();
  } else if (absl::EqualsIgnoreCase(name, PGFunctionCatalog::kName)) {
    *catalog = GetPGFunctionsCatalog();
  } else if (absl::EqualsIgnoreCase(name,
                                    postgres_translator::PGCatalog::kName)) {
    *catalog = GetPGCatalog();
  } else {
    if (QueryableNamedSchema* named_schema = GetNamedSchema(name);
        named_schema) {
      *catalog = named_schema;
    }
  }
  return absl::OkStatus();
}

absl::Status Catalog::GetTable(const std::string& name,
                               const googlesql::Table** table,
                               const FindOptions& options) {
  *table = nullptr;
  if (auto it = views_.find(name); it != views_.end()) {
    *table = it->second.get();
    return absl::OkStatus();
  }

  if (auto it = tables_.find(name); it != tables_.end()) {
    *table = it->second.get();
    return absl::OkStatus();
  }

  return error::TableNotFound(name);
}

absl::Status Catalog::GetModel(const std::string& name,
                               const googlesql::Model** model,
                               const FindOptions& options) {
  *model = nullptr;
  if (auto it = models_.find(name); it != models_.end()) {
    *model = it->second.get();
    return absl::OkStatus();
  }
  return absl::OkStatus();
}

absl::Status Catalog::FindConversion(
    const googlesql::Type* from_type, const googlesql::Type* to_type,
    const googlesql::Catalog::FindConversionOptions& options,
    googlesql::Conversion* conversion) {
  GOOGLESQL_ASSIGN_OR_RETURN(
      *conversion,
      postgres_translator::spangres::datatypes::FindExtendedTypeConversion(
          from_type, to_type, options));
  return absl::OkStatus();
}

absl::Status Catalog::GetPropertyGraph(std::string_view name,
                                       const googlesql::PropertyGraph*& graph,
                                       const FindOptions& options) {
  if (auto it = property_graphs_.find(std::string(name));
      it != property_graphs_.end()) {
    graph = it->second.get();
    return absl::OkStatus();
  }
  return error::PropertyGraphNotFound(name);
}

absl::Status Catalog::FindTableValuedFunction(
    const absl::Span<const std::string>& path,
    const googlesql::TableValuedFunction** function,
    const FindOptions& options) {
  *function = nullptr;
  std::string name = absl::StrJoin(path, ".");
  if (auto it = tvfs_.find(name); it != tvfs_.end()) {
    *function = it->second.get();
    return absl::OkStatus();
  }

  function_catalog_->GetTableValuedFunction(name, function);
  if (*function == nullptr) {
    return TableValuedFunctionNotFoundError(path);
  }

  return absl::OkStatus();
}

absl::Status Catalog::GetFunction(const std::string& name,
                                  const googlesql::Function** function,
                                  const FindOptions& options) {
  if (absl::EqualsIgnoreCase(name, "SECURE_CONTEXT")) {
    *function = secure_context_function_.get();
    return absl::OkStatus();
  }

  auto it = udfs_.find(name);
  if (it != udfs_.end()) {
    *function = it->second.get();
    return absl::OkStatus();
  }

  function_catalog_->GetFunction(name, function);
  return absl::OkStatus();
}

absl::Status Catalog::GetProcedure(const std::string& full_name,
                                   const googlesql::Procedure** procedure,
                                   const FindOptions& options) {
  auto it = procedures_.find(absl::AsciiStrToLower(full_name));
  if (it == procedures_.end()) {
    return error::UnsupportedProcedure(full_name);
  }
  *procedure = it->second.get();
  return absl::OkStatus();
}

absl::Status Catalog::GetSequence(const std::string& name,
                                  const googlesql::Sequence** sequence,
                                  const FindOptions& options) {
  *sequence = nullptr;
  if (auto it = sequences_.find(name); it != sequences_.end()) {
    *sequence = it->second.get();
    return absl::OkStatus();
  }
  return error::SequenceNotFound(name);
}

absl::Status Catalog::GetCatalogs(
    absl::flat_hash_set<const googlesql::Catalog*>* output) const {
  output->insert(GetInformationSchemaCatalog());
  output->insert(GetSpannerSysCatalog());
  output->insert(GetNetFunctionsCatalog());
  output->insert(GetAiFunctionsCatalog());
  GOOGLESQL_RETURN_IF_ERROR(GetNamedSchemas(output));

  return absl::OkStatus();
}

absl::Status Catalog::GetTables(
    absl::flat_hash_set<const googlesql::Table*>* output) const {
  for (auto iter = tables_.begin(); iter != tables_.end(); ++iter) {
    output->insert(iter->second.get());
  }
  for (auto iter = views_.begin(); iter != views_.end(); ++iter) {
    output->insert(iter->second.get());
  }
  return absl::OkStatus();
}

absl::Status Catalog::GetTypes(
    absl::flat_hash_set<const googlesql::Type*>* output) const {
  for (const auto& [unused_name, type] : types_) {
    output->insert(type);
  }
  return absl::OkStatus();
}

absl::Status Catalog::GetType(const std::string& name,
                              const googlesql::Type** type,
                              const FindOptions& options) {
  *type = nullptr;
  if (auto it = types_.find(name); it != types_.end()) {
    *type = it->second;
    return absl::OkStatus();
  }
  absl::StatusOr<const google::protobuf::Descriptor*> descriptor =
      schema_->proto_bundle()->GetTypeDescriptor(name);
  if (descriptor.ok()) {
    GOOGLESQL_RETURN_IF_ERROR(type_factory_->MakeProtoType(descriptor.value(), type));
    return absl::OkStatus();
  } else {
    auto enum_descriptor = schema_->proto_bundle()->GetEnumTypeDescriptor(name);
    if (enum_descriptor.ok()) {
      GOOGLESQL_RETURN_IF_ERROR(
          type_factory_->MakeEnumType(enum_descriptor.value(), type));
      return absl::OkStatus();
    }
    return descriptor.status();
  }
  return error::TypeNotFound(name);
}

absl::Status Catalog::GetFunctions(
    absl::flat_hash_set<const googlesql::Function*>* output) const {
  for (const auto& [unused_name, function] : udfs_) {
    output->insert(function.get());
  }
  GOOGLESQL_RET_CHECK(secure_context_function_ != nullptr)
      << "SECURE_CONTEXT function must be initialized";
  output->insert(secure_context_function_.get());

  function_catalog_->GetFunctions(output);
  return absl::OkStatus();
}

absl::Status Catalog::GetTableValuedFunctions(
    absl::flat_hash_set<const googlesql::TableValuedFunction*>* output) const {
  for (const auto& [unused_name, function] : tvfs_) {
    output->insert(function.get());
  }
  return absl::OkStatus();
}

googlesql::Catalog* Catalog::GetInformationSchemaCatalog() const {
  absl::MutexLock lock(mu_);
  auto spanner_sys_catalog = GetSpannerSysCatalogWithoutLocks();
  if (!information_schema_catalog_) {
    information_schema_catalog_ = std::make_unique<InformationSchemaCatalog>(
        InformationSchemaCatalog::kName, schema_, spanner_sys_catalog);
  }
  return information_schema_catalog_.get();
}

SpannerSysCatalog* Catalog::GetSpannerSysCatalog() const {
  absl::MutexLock lock(mu_);
  return GetSpannerSysCatalogWithoutLocks();
}

SpannerSysCatalog* Catalog::GetSpannerSysCatalogWithoutLocks() const {
  if (!spanner_sys_catalog_) {
    spanner_sys_catalog_ = std::make_unique<SpannerSysCatalog>();
  }
  return spanner_sys_catalog_.get();
}

googlesql::Catalog* Catalog::GetPGInformationSchemaCatalog() const {
  absl::MutexLock lock(mu_);
  auto spanner_sys_catalog = GetSpannerSysCatalogWithoutLocks();
  if (!pg_information_schema_catalog_) {
    pg_information_schema_catalog_ = std::make_unique<InformationSchemaCatalog>(
        InformationSchemaCatalog::kPGName, schema_, spanner_sys_catalog);
  }
  return pg_information_schema_catalog_.get();
}

googlesql::Catalog* Catalog::GetNetFunctionsCatalog() const {
  absl::MutexLock lock(mu_);
  if (!net_catalog_) {
    net_catalog_ = std::make_unique<NetCatalog>(const_cast<Catalog*>(this));
  }
  return net_catalog_.get();
}

googlesql::Catalog* Catalog::GetAiFunctionsCatalog() const {
  absl::MutexLock lock(mu_);
  if (!ai_catalog_) {
    ai_catalog_ = std::make_unique<AiCatalog>(const_cast<Catalog*>(this));
  }
  return ai_catalog_.get();
}

googlesql::Catalog* Catalog::GetPGFunctionsCatalog() const {
  absl::MutexLock lock(mu_);
  if (!pg_function_catalog_) {
    pg_function_catalog_ =
        std::make_unique<PGFunctionCatalog>(const_cast<Catalog*>(this));
  }
  return pg_function_catalog_.get();
}

googlesql::Catalog* Catalog::GetPGCatalog() const {
  absl::MutexLock lock(mu_);
  if (schema_->dialect() == database_api::DatabaseDialect::POSTGRESQL) {
    if (!pg_catalog_) {
      pg_catalog_ =
          std::make_unique<postgres_translator::PGCatalog>(this, schema_);
    }
    return pg_catalog_.get();
  }
  return nullptr;
}

QueryableNamedSchema* Catalog::GetNamedSchema(const std::string& name) {
  if (auto it = named_schemas_.find(name); it != named_schemas_.end()) {
    return it->second.get();
  }
  return nullptr;
}

absl::Status Catalog::GetNamedSchemas(
    absl::flat_hash_set<const googlesql::Catalog*>* output) const {
  for (auto iter = named_schemas_.begin(); iter != named_schemas_.end();
       ++iter) {
    output->insert(iter->second.get());
  }
  return absl::OkStatus();
}

template <typename T>
absl::Status Catalog::AddObjectToNamedSchema(
    const std::string& named_schema_name, T object) {
  QueryableNamedSchema* named_schema = GetNamedSchema(named_schema_name);
  if (named_schema == nullptr) {
    return error::NamedSchemaNotFound(named_schema_name);
  }
  named_schema->AddObject(std::move(object));
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
