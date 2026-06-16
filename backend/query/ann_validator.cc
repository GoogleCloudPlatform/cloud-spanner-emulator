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

#include "googlesql/public/function.h"
#include "googlesql/public/value.h"
#include "googlesql/resolved_ast/resolved_ast.h"
#include "googlesql/resolved_ast/resolved_column.h"
#include "googlesql/resolved_ast/resolved_node.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "backend/query/ann_functions_rewriter.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/ddl/operations.pb.h"
#include "common/errors.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

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
    const googlesql::ResolvedLimitOffsetScan* node,
    const googlesql::ResolvedFunctionCall*& ann_func,
    std::vector<const googlesql::ResolvedNode*>& child_nodes) {
  if (!node->input_scan()->Is<googlesql::ResolvedOrderByScan>()) {
    return absl::InvalidArgumentError("No order by clause.");
  }
  const googlesql::ResolvedOrderByScan* orderby_scan =
      node->input_scan()->GetAs<googlesql::ResolvedOrderByScan>();
  if (orderby_scan->order_by_item_list().size() != 1 ||
      !orderby_scan->order_by_item_list()[0]
           ->Is<googlesql::ResolvedOrderByItem>()) {
    return absl::InvalidArgumentError("Invalid order by clause.");
  }
  googlesql::ResolvedColumn order_by_column =
      orderby_scan->order_by_item_list()[0]
          ->GetAs<googlesql::ResolvedOrderByItem>()
          ->column_ref()
          ->column();

  if (!orderby_scan->input_scan()->Is<googlesql::ResolvedProjectScan>()) {
    return absl::InvalidArgumentError("Input scan is not a project scan.");
  }
  const googlesql::ResolvedProjectScan* project_scan =
      node->input_scan()
          ->GetAs<googlesql::ResolvedOrderByScan>()
          ->input_scan()
          ->GetAs<googlesql::ResolvedProjectScan>();
  project_scan->GetChildNodes(&child_nodes);
  for (auto child : child_nodes) {
    if (child->Is<googlesql::ResolvedComputedColumn>()) {
      const googlesql::ResolvedComputedColumn* cc =
          child->GetAs<googlesql::ResolvedComputedColumn>();
      if (order_by_column != cc->column()) {
        return absl::InvalidArgumentError("Invalid order by clause.");
      }
      std::vector<const googlesql::ResolvedNode*> computed_children;
      cc->GetChildNodes(&computed_children);
      for (auto computed_child : computed_children) {
        if (computed_child->Is<googlesql::ResolvedFunctionCall>()) {
          const googlesql::ResolvedFunctionCall* func =
              computed_child->GetAs<googlesql::ResolvedFunctionCall>();
          GOOGLESQL_RET_CHECK(func->function() != nullptr);
          if (IsANNFunction(func->function()->Name())) {
            GOOGLESQL_RET_CHECK(ann_func == nullptr);
            ann_func = func;
          }
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::Status GetNotNullColumns(
    std::vector<const googlesql::ResolvedNode*>& child_nodes,
    const googlesql::ResolvedScan*& scan,
    std::vector<googlesql::ResolvedColumn>& not_null_columns) {
  for (auto child : child_nodes) {
    if (child->Is<googlesql::ResolvedScan>()) {
      scan = child->GetAs<googlesql::ResolvedScan>();
      if (scan->Is<googlesql::ResolvedFilterScan>()) {
        const googlesql::ResolvedFilterScan* filter_scan =
            scan->GetAs<googlesql::ResolvedFilterScan>();
        scan = filter_scan->input_scan();
        if (filter_scan->filter_expr() != nullptr &&
            filter_scan->filter_expr()->Is<googlesql::ResolvedFunctionCall>()) {
          const googlesql::ResolvedFunctionCall* func =
              filter_scan->filter_expr()
                  ->GetAs<googlesql::ResolvedFunctionCall>();
          if (func->function()->Name() == "$and") {
            for (const auto& arg : func->argument_list()) {
              if (arg->Is<googlesql::ResolvedFunctionCall>()) {
                const googlesql::ResolvedFunctionCall* not_func =
                    arg->GetAs<googlesql::ResolvedFunctionCall>();
                if (not_func->function()->Name() == "$not" &&
                    not_func->argument_list_size() == 1 &&
                    not_func->argument_list(0)
                        ->Is<googlesql::ResolvedFunctionCall>() &&
                    not_func->argument_list(0)
                            ->GetAs<googlesql::ResolvedFunctionCall>()
                            ->function()
                            ->Name() == "$is_null") {
                  const googlesql::ResolvedFunctionCall* is_null_func =
                      not_func->argument_list(0)
                          ->GetAs<googlesql::ResolvedFunctionCall>();
                  if (is_null_func->argument_list_size() == 1 &&
                      is_null_func->argument_list(0)
                          ->Is<googlesql::ResolvedColumnRef>()) {
                    not_null_columns.push_back(
                        is_null_func->argument_list(0)
                            ->GetAs<googlesql::ResolvedColumnRef>()
                            ->column());
                  }
                }
              }
            }
          } else if (func->function()->Name() == "$not") {
            if (func->argument_list_size() == 1 &&
                func->argument_list(0)->Is<googlesql::ResolvedFunctionCall>() &&
                func->argument_list(0)
                        ->GetAs<googlesql::ResolvedFunctionCall>()
                        ->function()
                        ->Name() == "$is_null") {
              const googlesql::ResolvedFunctionCall* is_null_func =
                  func->argument_list(0)
                      ->GetAs<googlesql::ResolvedFunctionCall>();
              if (is_null_func->argument_list_size() == 1 &&
                  is_null_func->argument_list(0)
                      ->Is<googlesql::ResolvedColumnRef>()) {
                not_null_columns.push_back(
                    is_null_func->argument_list(0)
                        ->GetAs<googlesql::ResolvedColumnRef>()
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
    const googlesql::ResolvedFunctionCall* last_ann_func,
    googlesql::ResolvedColumn& ann_func_column,
    googlesql::Value& ann_func_value) {
  if (last_ann_func->argument_list(0)->Is<googlesql::ResolvedColumnRef>()) {
    ann_func_column = last_ann_func->argument_list()[0]
                          ->GetAs<googlesql::ResolvedColumnRef>()
                          ->column();
    if (last_ann_func->argument_list(1)->Is<googlesql::ResolvedLiteral>()) {
      ann_func_value = last_ann_func->argument_list()[1]
                           ->GetAs<googlesql::ResolvedLiteral>()
                           ->value();
    } else if (!last_ann_func->argument_list(1)
                    ->Is<googlesql::ResolvedParameter>()) {
      return error::ApproxDistanceInvalidShape(
          last_ann_func->function()->Name());
    }
  } else if (last_ann_func->argument_list(1)
                 ->Is<googlesql::ResolvedColumnRef>()) {
    ann_func_column = last_ann_func->argument_list()[1]
                          ->GetAs<googlesql::ResolvedColumnRef>()
                          ->column();
    if (last_ann_func->argument_list(0)->Is<googlesql::ResolvedLiteral>()) {
      ann_func_value = last_ann_func->argument_list()[0]
                           ->GetAs<googlesql::ResolvedLiteral>()
                           ->value();
    } else if (!last_ann_func->argument_list(0)
                    ->Is<googlesql::ResolvedParameter>()) {
      return error::ApproxDistanceInvalidShape(
          last_ann_func->function()->Name());
    }
  } else {
    return error::ApproxDistanceInvalidShape(last_ann_func->function()->Name());
  }
  return absl::OkStatus();
}

absl::Status ValidateFunctionArguments(
    const googlesql::Value& ann_func_value,
    const googlesql::ResolvedFunctionCall* last_ann_func) {
  if (ann_func_value.is_valid()) {
    if (ann_func_value.is_null() || !ann_func_value.type()->IsArray()) {
      return error::ApproxDistanceInvalidShape(
          last_ann_func->function()->Name());
    }
    std::vector<googlesql::Value> elements = ann_func_value.elements();
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
    const googlesql::ResolvedColumn& ann_func_column,
    const googlesql::Value& ann_func_value,
    const googlesql::ResolvedFunctionCall* last_ann_func, bool is_force_index) {
  ddl::VectorIndexOptionsProto::DistanceType distance_type =
      ANNFunctionsToDistanceType(last_ann_func->function()->Name());
  int i = 0;
  bool found_column = false;
  for (; i < indexes.size(); ++i) {
    const Index* index = indexes[i];
    GOOGLESQL_RET_CHECK(index->key_columns().size() == 1);
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
    const googlesql::ResolvedLimitOffsetScan* node) {
  std::vector<const googlesql::ResolvedNode*> child_nodes;
  const googlesql::ResolvedFunctionCall* last_ann_func = nullptr;
  if (!GetAnnFunctionCall(node, last_ann_func, child_nodes).ok()) {
    return googlesql::ResolvedASTVisitor::DefaultVisit(node);
  }
  ann_functions_.insert(last_ann_func);

  std::vector<const Index*> indexes;
  std::vector<googlesql::ResolvedColumn> not_null_columns;
  bool is_force_index = false;
  const googlesql::ResolvedScan* scan = nullptr;
  GOOGLESQL_RETURN_IF_ERROR(GetNotNullColumns(child_nodes, scan, not_null_columns));

  if (scan->Is<googlesql::ResolvedJoinScan>() && last_ann_func != nullptr) {
    return error::ApproxDistanceInvalidShape(last_ann_func->function()->Name());
  }
  if (scan->Is<googlesql::ResolvedTableScan>() &&
      !scan->GetAs<googlesql::ResolvedTableScan>()->hint_list().empty()) {
    for (const auto& hint :
         scan->GetAs<googlesql::ResolvedTableScan>()->hint_list()) {
      if (absl::EqualsIgnoreCase(hint->name(), "force_index")) {
        GOOGLESQL_RET_CHECK(hint->value()->Is<googlesql::ResolvedLiteral>());
        indexes = schema_->FindIndexesUnderName(
            hint->value()
                ->GetAs<googlesql::ResolvedLiteral>()
                ->value()
                .string_value());
        GOOGLESQL_RET_CHECK(indexes.size() == 1);
        is_force_index = true;
        if (!indexes[0]->is_vector_index() && last_ann_func != nullptr) {
          return error::NotVectorIndexes(indexes[0]->Name());
        }
      }
    }
  }

  if (last_ann_func == nullptr) {
    return googlesql::ResolvedASTVisitor::DefaultVisit(node);
  }
  googlesql::ResolvedColumn ann_func_column;
  googlesql::Value ann_func_value;
  GOOGLESQL_RETURN_IF_ERROR(
      GetANNFunctionArguments(last_ann_func, ann_func_column, ann_func_value));
  GOOGLESQL_RETURN_IF_ERROR(ValidateFunctionArguments(ann_func_value, last_ann_func));

  if (indexes.empty()) {
    indexes = schema_->vector_indexes();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(const Index* vec_index,
                   FindVectorIndex(indexes, ann_func_column, ann_func_value,
                                   last_ann_func, is_force_index));

  GOOGLESQL_RET_CHECK(vec_index->key_columns().size() == 1);
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

  return googlesql::ResolvedASTVisitor::DefaultVisit(node);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
