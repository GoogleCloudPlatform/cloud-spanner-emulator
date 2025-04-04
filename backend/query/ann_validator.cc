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

#include "backend/query/ann_validator.h"

#include <string>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "backend/query/ann_functions_rewriter.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/ddl/operations.pb.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

ddl::VectorIndexOptionsProto::DistanceType ANNFunctionsToDistanceType(
    const std::string& function_name) {
  if (function_name == "approx_cosine_distance") {
    return ddl::VectorIndexOptionsProto::COSINE;
  } else if (function_name == "approx_dot_product") {
    return ddl::VectorIndexOptionsProto::DOT_PRODUCT;
  } else if (function_name == "approx_euclidean_distance") {
    return ddl::VectorIndexOptionsProto::EUCLIDEAN;
  }
  ABSL_LOG(FATAL) << "Invalid ANN function: " << function_name;  // Crash OK
}

absl::Status GetAnnFunctionCall(
    const zetasql::ResolvedLimitOffsetScan* node,
    const zetasql::ResolvedFunctionCall*& ann_func,
    std::vector<const zetasql::ResolvedNode*>& child_nodes) {
  if (!node->input_scan()->Is<zetasql::ResolvedOrderByScan>()) {
    return absl::InvalidArgumentError("No order by clause.");
  }
  const zetasql::ResolvedOrderByScan* orderby_scan =
      node->input_scan()->GetAs<zetasql::ResolvedOrderByScan>();
  if (orderby_scan->order_by_item_list().size() != 1 ||
      !orderby_scan->order_by_item_list()[0]
           ->Is<zetasql::ResolvedOrderByItem>()) {
    return absl::InvalidArgumentError("Invalid order by clause.");
  }
  zetasql::ResolvedColumn order_by_column =
      orderby_scan->order_by_item_list()[0]
          ->GetAs<zetasql::ResolvedOrderByItem>()
          ->column_ref()
          ->column();

  if (!orderby_scan->input_scan()->Is<zetasql::ResolvedProjectScan>()) {
    return absl::InvalidArgumentError("Input scan is not a project scan.");
  }
  const zetasql::ResolvedProjectScan* project_scan =
      node->input_scan()
          ->GetAs<zetasql::ResolvedOrderByScan>()
          ->input_scan()
          ->GetAs<zetasql::ResolvedProjectScan>();
  project_scan->GetChildNodes(&child_nodes);
  for (auto child : child_nodes) {
    if (child->Is<zetasql::ResolvedComputedColumn>()) {
      const zetasql::ResolvedComputedColumn* cc =
          child->GetAs<zetasql::ResolvedComputedColumn>();
      if (order_by_column != cc->column()) {
        return absl::InvalidArgumentError("Invalid order by clause.");
      }
      std::vector<const zetasql::ResolvedNode*> computed_children;
      cc->GetChildNodes(&computed_children);
      for (auto computed_child : computed_children) {
        if (computed_child->Is<zetasql::ResolvedFunctionCall>()) {
          const zetasql::ResolvedFunctionCall* func =
              computed_child->GetAs<zetasql::ResolvedFunctionCall>();
          ZETASQL_RET_CHECK(func->function() != nullptr);
          if (IsANNFunction(func->function()->Name())) {
            ZETASQL_RET_CHECK(ann_func == nullptr);
            ann_func = func;
          }
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status GetNotNullColumns(
    std::vector<const zetasql::ResolvedNode*>& child_nodes,
    const zetasql::ResolvedScan*& scan,
    std::vector<zetasql::ResolvedColumn>& not_null_columns) {
  for (auto child : child_nodes) {
    if (child->Is<zetasql::ResolvedScan>()) {
      scan = child->GetAs<zetasql::ResolvedScan>();
      if (scan->Is<zetasql::ResolvedFilterScan>()) {
        const zetasql::ResolvedFilterScan* filter_scan =
            scan->GetAs<zetasql::ResolvedFilterScan>();
        scan = filter_scan->input_scan();
        if (filter_scan->filter_expr() != nullptr &&
            filter_scan->filter_expr()->Is<zetasql::ResolvedFunctionCall>()) {
          const zetasql::ResolvedFunctionCall* func =
              filter_scan->filter_expr()
                  ->GetAs<zetasql::ResolvedFunctionCall>();
          if (func->function()->Name() == "$and") {
            for (const auto& arg : func->argument_list()) {
              if (arg->Is<zetasql::ResolvedFunctionCall>()) {
                const zetasql::ResolvedFunctionCall* not_func =
                    arg->GetAs<zetasql::ResolvedFunctionCall>();
                if (not_func->function()->Name() == "$not" &&
                    not_func->argument_list_size() == 1 &&
                    not_func->argument_list(0)
                        ->Is<zetasql::ResolvedFunctionCall>() &&
                    not_func->argument_list(0)
                            ->GetAs<zetasql::ResolvedFunctionCall>()
                            ->function()
                            ->Name() == "$is_null") {
                  const zetasql::ResolvedFunctionCall* is_null_func =
                      not_func->argument_list(0)
                          ->GetAs<zetasql::ResolvedFunctionCall>();
                  if (is_null_func->argument_list_size() == 1 &&
                      is_null_func->argument_list(0)
                          ->Is<zetasql::ResolvedColumnRef>()) {
                    not_null_columns.push_back(
                        is_null_func->argument_list(0)
                            ->GetAs<zetasql::ResolvedColumnRef>()
                            ->column());
                  }
                }
              }
            }
          } else if (func->function()->Name() == "$not") {
            if (func->argument_list_size() == 1 &&
                func->argument_list(0)->Is<zetasql::ResolvedFunctionCall>() &&
                func->argument_list(0)
                        ->GetAs<zetasql::ResolvedFunctionCall>()
                        ->function()
                        ->Name() == "$is_null") {
              const zetasql::ResolvedFunctionCall* is_null_func =
                  func->argument_list(0)
                      ->GetAs<zetasql::ResolvedFunctionCall>();
              if (is_null_func->argument_list_size() == 1 &&
                  is_null_func->argument_list(0)
                      ->Is<zetasql::ResolvedColumnRef>()) {
                not_null_columns.push_back(
                    is_null_func->argument_list(0)
                        ->GetAs<zetasql::ResolvedColumnRef>()
                        ->column());
              }
            }
          }
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status GetANNFunctionArguments(
    const zetasql::ResolvedFunctionCall* last_ann_func,
    zetasql::ResolvedColumn& ann_func_column,
    zetasql::Value& ann_func_value) {
  if (last_ann_func->argument_list(0)->Is<zetasql::ResolvedColumnRef>()) {
    ann_func_column = last_ann_func->argument_list()[0]
                          ->GetAs<zetasql::ResolvedColumnRef>()
                          ->column();
    if (last_ann_func->argument_list(1)->Is<zetasql::ResolvedLiteral>()) {
      ann_func_value = last_ann_func->argument_list()[1]
                           ->GetAs<zetasql::ResolvedLiteral>()
                           ->value();
    } else if (!last_ann_func->argument_list(1)
                    ->Is<zetasql::ResolvedParameter>()) {
      return error::ApproxDistanceInvalidShape(
          last_ann_func->function()->Name());
    }
  } else if (last_ann_func->argument_list(1)
                 ->Is<zetasql::ResolvedColumnRef>()) {
    ann_func_column = last_ann_func->argument_list()[1]
                          ->GetAs<zetasql::ResolvedColumnRef>()
                          ->column();
    if (last_ann_func->argument_list(0)->Is<zetasql::ResolvedLiteral>()) {
      ann_func_value = last_ann_func->argument_list()[0]
                           ->GetAs<zetasql::ResolvedLiteral>()
                           ->value();
    } else if (!last_ann_func->argument_list(0)
                    ->Is<zetasql::ResolvedParameter>()) {
      return error::ApproxDistanceInvalidShape(
          last_ann_func->function()->Name());
    }
  } else {
    return error::ApproxDistanceInvalidShape(last_ann_func->function()->Name());
  }
  return absl::OkStatus();
}

absl::Status ValidateFunctionArguments(
    const zetasql::Value& ann_func_value,
    const zetasql::ResolvedFunctionCall* last_ann_func) {
  if (ann_func_value.is_valid()) {
    if (ann_func_value.is_null() || !ann_func_value.type()->IsArray()) {
      return error::ApproxDistanceInvalidShape(
          last_ann_func->function()->Name());
    }
    std::vector<zetasql::Value> elements = ann_func_value.elements();
    bool is_all_zero = true;
    for (const auto& element : elements) {
      if (element.is_null() ||
          (!element.type()->IsDouble() && !element.type()->IsFloat())) {
        return error::ApproxDistanceInvalidShape(
            last_ann_func->function()->Name());
      }
      double value = element.ToDouble();
      if (value != 0) {
        is_all_zero = false;
      }
    }
    if (is_all_zero &&
        last_ann_func->function()->Name() == "approx_cosine_distance") {
      return absl::InvalidArgumentError(
          "Cannot compute cosine distance against zero vector.");
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<const Index*> FindVectorIndex(
    const std::vector<const Index*>& indexes,
    const zetasql::ResolvedColumn& ann_func_column,
    const zetasql::Value& ann_func_value,
    const zetasql::ResolvedFunctionCall* last_ann_func, bool is_force_index) {
  ddl::VectorIndexOptionsProto::DistanceType distance_type =
      ANNFunctionsToDistanceType(last_ann_func->function()->Name());
  int i = 0;
  bool found_column = false;
  for (; i < indexes.size(); ++i) {
    const Index* index = indexes[i];
    ZETASQL_RET_CHECK(index->key_columns().size() == 1);
    const Column* key_column = index->key_columns()[0]->column();
    if (index->indexed_table()->Name() == ann_func_column.table_name() &&
        key_column->Name() == ann_func_column.name()) {
      found_column = true;
      if (!key_column->has_vector_length()) {
        return error::ApproxDistanceInvalidShape(
            last_ann_func->function()->Name());
      }
      if (ann_func_value.is_valid()) {
        int vector_length = *key_column->vector_length();
        if (vector_length != ann_func_value.elements().size()) {
          return error::ApproxDistanceLengthMismatch(
              last_ann_func->function()->Name(),
              ann_func_value.elements().size(), vector_length);
        }
      }
      ddl::VectorIndexOptionsProto::DistanceType index_distance_type;
      if (!index->vector_index_options().has_distance_type() ||
          !ddl::VectorIndexOptionsProto::DistanceType_Parse(
              index->vector_index_options().distance_type(),
              &index_distance_type) ||
          index_distance_type ==
              ddl::VectorIndexOptionsProto::DISTANCE_TYPE_UNSPECIFIED ||
          index_distance_type == distance_type) {
        break;
      }
    }
  }
  if (i == indexes.size()) {
    if (is_force_index) {
      if (found_column) {
        return error::VectorIndexesUnusableForceIndexWrongDistanceType(
            indexes[0]->Name(),
            indexes[0]->vector_index_options().distance_type(),
            last_ann_func->function()->Name(), ann_func_column.name());
      } else {
        return error::VectorIndexesUnusableForceIndexWrongColumn(
            indexes[0]->Name(), last_ann_func->function()->Name(),
            ann_func_column.name());
      }
    }
    return error::VectorIndexesUnusable(
        ddl::VectorIndexOptionsProto::DistanceType_Name(distance_type),
        ann_func_column.name(), last_ann_func->function()->Name());
  }
  return indexes[i];
}

absl::Status ANNValidator::VisitResolvedLimitOffsetScan(
    const zetasql::ResolvedLimitOffsetScan* node) {
  std::vector<const zetasql::ResolvedNode*> child_nodes;
  const zetasql::ResolvedFunctionCall* last_ann_func = nullptr;
  if (!GetAnnFunctionCall(node, last_ann_func, child_nodes).ok()) {
    return zetasql::ResolvedASTVisitor::DefaultVisit(node);
  }
  ann_functions_.insert(last_ann_func);

  std::vector<const Index*> indexes;
  std::vector<zetasql::ResolvedColumn> not_null_columns;
  bool is_force_index = false;
  const zetasql::ResolvedScan* scan = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetNotNullColumns(child_nodes, scan, not_null_columns));

  if (scan->Is<zetasql::ResolvedJoinScan>() && last_ann_func != nullptr) {
    return error::ApproxDistanceInvalidShape(last_ann_func->function()->Name());
  }
  if (scan->Is<zetasql::ResolvedTableScan>() &&
      !scan->GetAs<zetasql::ResolvedTableScan>()->hint_list().empty()) {
    for (const auto& hint :
         scan->GetAs<zetasql::ResolvedTableScan>()->hint_list()) {
      if (absl::EqualsIgnoreCase(hint->name(), "force_index")) {
        ZETASQL_RET_CHECK(hint->value()->Is<zetasql::ResolvedLiteral>());
        indexes = schema_->FindIndexesUnderName(
            hint->value()
                ->GetAs<zetasql::ResolvedLiteral>()
                ->value()
                .string_value());
        ZETASQL_RET_CHECK(indexes.size() == 1);
        is_force_index = true;
        if (!indexes[0]->is_vector_index() && last_ann_func != nullptr) {
          return error::NotVectorIndexes(indexes[0]->Name());
        }
      }
    }
  }

  if (last_ann_func == nullptr) {
    return zetasql::ResolvedASTVisitor::DefaultVisit(node);
  }
  zetasql::ResolvedColumn ann_func_column;
  zetasql::Value ann_func_value;
  ZETASQL_RETURN_IF_ERROR(
      GetANNFunctionArguments(last_ann_func, ann_func_column, ann_func_value));
  ZETASQL_RETURN_IF_ERROR(ValidateFunctionArguments(ann_func_value, last_ann_func));

  if (indexes.empty()) {
    indexes = schema_->vector_indexes();
  }
  ZETASQL_ASSIGN_OR_RETURN(const Index* vec_index,
                   FindVectorIndex(indexes, ann_func_column, ann_func_value,
                                   last_ann_func, is_force_index));

  ZETASQL_RET_CHECK(vec_index->key_columns().size() == 1);
  const KeyColumn* key_column = vec_index->key_columns()[0];
  bool is_key_null_filtered = false;
  for (const auto* column : vec_index->null_filtered_columns()) {
    if (key_column->column()->Name() == column->Name()) {
      is_key_null_filtered = true;
      break;
    }
  }
  if (is_key_null_filtered) {
    bool is_not_null_column_found = false;
    for (const auto& not_null_column : not_null_columns) {
      if (vec_index->indexed_table()->Name() == not_null_column.table_name() &&
          key_column->column()->Name() == not_null_column.name()) {
        is_not_null_column_found = true;
        break;
      }
    }
    if (!is_not_null_column_found) {
      return error::VectorIndexesUnusableNotNullFiltered(
          vec_index->Name(), key_column->column()->Name());
    }
  }

  return zetasql::ResolvedASTVisitor::DefaultVisit(node);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
