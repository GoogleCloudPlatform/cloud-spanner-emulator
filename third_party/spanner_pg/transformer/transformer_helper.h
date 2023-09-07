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

#ifndef TRANSFORMER_TRANSFORMER_HELPER_H_
#define TRANSFORMER_TRANSFORMER_HELPER_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/analyzer/expr_matching_helpers.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "zetasql/base/map_util.h"

namespace postgres_translator {

// Converting a Postgres positional parameter (an integer)
// to a ZetaSQL named parameter (a string) by prepending the prefix.
inline constexpr absl::string_view kParameterPrefix = "p";

inline constexpr absl::string_view kAnonymousQuery = "$query";
inline constexpr absl::string_view kAnonymousExprSubquery = "$expr_subquery";
inline constexpr absl::string_view kAnonymousSubqueryPrefix = "$subquery";
inline constexpr absl::string_view kAnonymousSubqueryPrefixPattern =
    R"(\$subquery[1-9]\d*)";
// i.e. "$subquery"
inline constexpr absl::string_view kAnonymousAggregateQuery = "$aggregate";

inline constexpr absl::string_view kUnnamedJoin = "unnamed_join";
inline constexpr absl::string_view kOrderByColumn = "$orderbycol";
inline constexpr absl::string_view kOrderByPrefix = "$orderby";
inline constexpr absl::string_view kGroupByColumn = "$groupbycol";
inline constexpr absl::string_view kGroupByPrefix = "$groupby";
inline constexpr absl::string_view kPreGroupByPrefix = "$pre_groupby";
inline constexpr absl::string_view kDistinctId = "$distinct";

// kEqualFunction is the ZetaSQL function for equal. This maps to the
// PostgreSQL "=" operators.
inline constexpr absl::string_view kEqualFunction = "$equal";

// kFullJoin is the ZetaSQL placeholder name for columns corresponding to a
// FULL JOIN with USING.
inline constexpr absl::string_view kFullJoin = "$full_join";

// Pushes back an empty element to the vector on initialization and
// pops it on destruction.
template <typename Element>
class TemporaryVectorElement {
 public:
  TemporaryVectorElement(std::vector<Element>& vector)
      : vector_(vector), size_on_initialization_(vector.size() + 1) {
    vector.push_back({});
  }
  ~TemporaryVectorElement() {
    ABSL_LOG_IF(FATAL, vector_.size() != size_on_initialization_)
        << "Dismatched vector size, current vs. initialization = "
        << vector_.size() << " vs. " << size_on_initialization_;
    vector_.pop_back();
  }

 private:
  std::vector<Element>& vector_;
  int size_on_initialization_;
};

// Template deduction guide so that we can save the boilerplate of
// specifying the template argument.
template <typename Element>
TemporaryVectorElement(std::vector<Element>& vector)
    -> TemporaryVectorElement<Element>;

struct NamedColumn {
 public:
  NamedColumn(zetasql::IdString name_in,
              const zetasql::ResolvedColumn& column_in)
      : name(name_in), column(column_in) {}

  zetasql::IdString name;
  zetasql::ResolvedColumn column;
};

struct NamedSubquery {
  NamedSubquery(absl::string_view subquery_name_in,
                const std::vector<zetasql::ResolvedColumn>& column_list_in,
                const std::vector<NamedColumn>& column_aliases_in)
      : subquery_name(subquery_name_in),
        column_list(column_list_in),
        column_aliases(column_aliases_in) {}

  std::string subquery_name;
  std::vector<zetasql::ResolvedColumn> column_list;
  std::vector<NamedColumn> column_aliases;
};

// TransformerGroupByAndAggregateInfo is used (and mutated) to store info
// related to grouping/distinct and aggregation analysis for a single SELECT
// query block. Originally modeled after ZetaSQL's
// QueryGroupByAndAggregateInfo.
struct TransformerGroupByAndAggregateInfo {
  TransformerGroupByAndAggregateInfo() {}

  // Identifies whether or not group by or aggregation is present in this
  // (sub)query.
  bool has_group_by = false;
  bool has_aggregation = false;

  // Map from an aggregate function Expr to the related
  // ResolvedComputedColumn. Populated during the first pass resolution of
  // expressions. Second pass resolution of expressions will use these
  // computed columns for the given aggregate expression.
  // Not owned.
  // The ResolvedComputedColumns are owned by <aggregate_columns_>.
  std::map<const Expr*, const zetasql::ResolvedComputedColumn*>
      aggregate_expr_map;

  // Group by expressions that must be computed.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      group_by_columns_to_compute;

  // Aggregate function calls that must be computed.
  // This is built up as expressions are resolved.  During expression
  // resolution, aggregate functions are moved into <aggregate_columns_> and
  // replaced by a ResolvedColumnRef pointing at the ResolvedColumn created
  // here.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      aggregate_columns_to_compute;

  // Map of GROUP BY Vars to the GROUP BY zetasql::ResolvedColumn
  std::unordered_map<const Var*, zetasql::ResolvedColumn> group_by_map;

  bool is_post_distinct = false;

  // Resets all fields to their initial values, and empties all maps and
  // lists.
  void Reset() {
    has_group_by = false;
    has_aggregation = false;
    aggregate_expr_map.clear();
    group_by_columns_to_compute.clear();
    aggregate_columns_to_compute.clear();
    group_by_map.clear();
    is_post_distinct = false;
  }
};

// SelectColumnTransformState contains state related to an expression in the
// select-list of a query, while it is being transformed. This is used and
// mutated in multiple passes while transforming the SELECT-list and GROUP BY.
// Originally modeled after ZetaSQL's SelectColumnState.
struct SelectColumnTransformState {
  explicit SelectColumnTransformState(
      const TargetEntry* entry, zetasql::IdString alias_in,
      bool has_aggregation_in,
      std::unique_ptr<const zetasql::ResolvedExpr> resolved_expr_in)
      : target_entry(entry),
        alias(alias_in),
        resolved_expr(std::move(resolved_expr_in)),
        has_aggregation(has_aggregation_in) {}

  SelectColumnTransformState(const SelectColumnTransformState&) = delete;
  SelectColumnTransformState& operator=(const SelectColumnTransformState&) =
      delete;

  const zetasql::Type* GetType() const {
    if (resolved_select_column.IsInitialized()) {
      return resolved_select_column.type();
    }
    if (resolved_expr != nullptr) {
      return resolved_expr->type();
    }
    return nullptr;
  }

  // The PostgreSQL TargetEntry for this column. Not owned
  const TargetEntry* target_entry;

  // The alias for this column.
  const zetasql::IdString alias;

  // Owned ResolvedExpr for this SELECT list column. If we need a
  // ResolvedComputedColumn for this SELECT column, then ownership of
  // this <resolved_expr> will be transferred to that ResolvedComputedColumn
  // and <resolved_expr> will be set to NULL.
  std::unique_ptr<const zetasql::ResolvedExpr> resolved_expr;

  // References the related ResolvedComputedColumn for this SELECT list column,
  // if one is needed.  Otherwise it is NULL.  The referenced
  // ResolvedComputedColumn is owned by a column list in TransformerInfo.
  // The reference here is required to allow us to maintain the relationship
  // between this SELECT list column and its related expression for
  // subsequent HAVING and ORDER BY expression analysis.
  // Not owned.
  const zetasql::ResolvedComputedColumn* resolved_computed_column = nullptr;

  // True if this expression includes aggregation.  Select-list expressions
  // that use aggregation cannot be referenced in GROUP BY.
  bool has_aggregation = false;

  // If true, this expression is used as a GROUP BY key.
  bool is_group_by_column = false;

  // The output column of this select list item. It is projected by a scan
  // that computes the related expression. After the SELECT list has
  // been fully transformed, <resolved_select_column> will be initialized.
  // After it is set, it is used in subsequent expression transformation (SELECT
  // list ordinal references and SELECT list alias references).
  zetasql::ResolvedColumn resolved_select_column;

  // If set, indicates the pre-GROUP BY version of the column.  Will only
  // be set if the column must be computed before the AggregateScan (so
  // it will not necessarily always be set if is_group_by_column is true).
  zetasql::ResolvedColumn resolved_pre_group_by_select_column;
};

// This class contains a SelectColumnTransformState for each column in the
// SELECT list and maps the ordinal references to the SELECT-list column.
class SelectListTransformState {
 public:
  SelectListTransformState() {}
  SelectListTransformState(const SelectListTransformState&) = delete;
  SelectListTransformState& operator=(const SelectListTransformState&) = delete;

  // Creates and returns a SelectColumnTransformState for a new SELECT-list
  // column.
  SelectColumnTransformState* AddSelectColumn(
      const TargetEntry* target_entry, zetasql::IdString alias,
      bool has_aggregation,
      std::unique_ptr<const zetasql::ResolvedExpr> resolved_expr);

  std::vector<std::unique_ptr<SelectColumnTransformState>>&
  select_column_state_list() {
    return select_column_state_list_;
  };

  // Returns a list of output ResolvedColumns, one ResolvedColumn per
  // <select_column_state_list_> entry.  Currently only used when creating a
  // ProjectScan, ensuring that all SELECT list columns are in the scan.
  const zetasql::ResolvedColumnList resolved_column_list() const;

 private:
  std::vector<std::unique_ptr<SelectColumnTransformState>>
      select_column_state_list_;
};

struct OrderByItemTransformInfo {
  // Constructor for OrderByItems that are not in the SELECT list and must
  // transform their Exprs.
  OrderByItemTransformInfo(
      std::unique_ptr<const zetasql::ResolvedExpr> expr, bool descending,
      zetasql::ResolvedOrderByItemEnums::NullOrderMode null_order)
      : order_expression(std::move(expr)),
        select_list_index(0),
        is_descending(descending),
        null_order(null_order) {}

  // Constructor for OrderByItems that are in the SELECT list and can
  // use the ResolvedExpr from transforming the SELECT list.
  OrderByItemTransformInfo(
      int select_list_index, bool descending,
      zetasql::ResolvedOrderByItemEnums::NullOrderMode null_order)
      : order_expression(nullptr),
        select_list_index(select_list_index),
        is_descending(descending),
        null_order(null_order) {}

  std::unique_ptr<const zetasql::ResolvedExpr> order_expression;
  int select_list_index;
  zetasql::ResolvedColumn order_column;

  bool is_descending;  // Indicates DESC or ASC.
  // Indicates NULLS LAST or NULLS FIRST.
  zetasql::ResolvedOrderByItemEnums::NullOrderMode null_order;
};

// TransformerInfo is used (and mutated) to store info related to
// the analysis of a single SELECT query block. It stores information
// related to SELECT list entries, grouping and aggregation, and analytic
// functions. Detailed descriptions for each field are included below.
// See comments on Transformer::BuildGsqlResolvedScanForSelect for discussion of
// the various phases of analysis and how TransformerInfo is updated
// and referenced during that process.
// Originally modeled after ZetaSQL's QueryResolutionInfo.
class TransformerInfo {
 public:
  TransformerInfo() {
    select_list_transform_state_ = std::make_unique<SelectListTransformState>();
  }
  TransformerInfo(const TransformerInfo&) = delete;
  TransformerInfo& operator=(const TransformerInfo&) = delete;

  // Adds group by column <column>, which is computed from <expr>.
  void AddGroupByComputedColumnIfNeeded(
      const zetasql::ResolvedColumn& column,
      std::unique_ptr<const zetasql::ResolvedExpr> expr);

  void AddAggregateComputedColumn(
      const Expr* aggregate_expr,
      std::unique_ptr<const zetasql::ResolvedComputedColumn> column) {
    group_by_info_.has_aggregation = true;
    zetasql_base::InsertIfNotPresent(&group_by_info_.aggregate_expr_map, aggregate_expr,
                            column.get());
    group_by_info_.aggregate_columns_to_compute.push_back(std::move(column));
  }

  void AddGroupByVarIndexColumn(const Var* var,
                                const zetasql::ResolvedColumn& column) {
    group_by_info_.group_by_map[var] = column;
  }

  // Returns whether or not the query includes a GROUP BY clause or
  // aggregation functions.
  bool HasGroupByOrAggregation() const {
    return group_by_info_.has_group_by || group_by_info_.has_aggregation;
  }

  bool HasHavingOrOrderBy() const { return has_having_ || has_order_by_; }

  const std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>&
  group_by_columns_to_compute() const {
    return group_by_info_.group_by_columns_to_compute;
  }

  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
  release_group_by_columns_to_compute() {
    std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>> tmp;
    group_by_info_.group_by_columns_to_compute.swap(tmp);
    return tmp;
  }

  const std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>&
  aggregate_columns_to_compute() const {
    return group_by_info_.aggregate_columns_to_compute;
  }

  // Transfer ownership of aggregate_columns_to_compute, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
  release_aggregate_columns_to_compute() {
    std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>> tmp;
    group_by_info_.aggregate_columns_to_compute.swap(tmp);
    return tmp;
  }

  const std::unordered_map<const Var*, zetasql::ResolvedColumn>&
  group_by_map() const {
    return group_by_info_.group_by_map;
  }

  // Resets all state related to group by and aggregation context.  This
  // is invoked before processing DISTINCT, since DISTINCT processing
  // re-uses the same 'group_by_info_' member as the GROUP BY.
  void ClearGroupByInfo() { group_by_info_.Reset(); }

  SelectListTransformState* select_list_transform_state() {
    return select_list_transform_state_.get();
  }

  SelectColumnTransformState* GetSelectColumnState(int select_index) {
    return select_list_transform_state_
        ->select_column_state_list()[select_index - 1]
        .get();
  }

  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>*
  select_list_columns_to_compute() {
    return &select_list_columns_to_compute_;
  }

  // Transfer ownership of select_list_columns_to_compute, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
  release_select_list_columns_to_compute() {
    std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>> tmp;
    select_list_columns_to_compute_.swap(tmp);
    return tmp;
  }

  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>*
  select_list_columns_to_compute_before_aggregation() {
    return &select_list_columns_to_compute_before_aggregation_;
  }

  // Transfer ownership of select_list_columns_to_compute_before_aggregation,
  // clearing the internal storage.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
  release_select_list_columns_to_compute_before_aggregation() {
    std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>> tmp;
    select_list_columns_to_compute_before_aggregation_.swap(tmp);
    return tmp;
  }

  const std::map<const Expr*, const zetasql::ResolvedComputedColumn*>&
  aggregate_expr_map() {
    return group_by_info_.aggregate_expr_map;
  }

  const std::vector<OrderByItemTransformInfo>& order_by_item_transform_info()
      const {
    return order_by_item_transform_info_;
  }

  std::vector<OrderByItemTransformInfo>*
  mutable_order_by_item_transform_info() {
    return &order_by_item_transform_info_;
  }

  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>*
  order_by_columns_to_compute() {
    return &order_by_columns_to_compute_;
  }

  // Transfer ownership of order_by_columns_to_compute, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
  release_order_by_columns_to_compute() {
    std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>> tmp;
    order_by_columns_to_compute_.swap(tmp);
    return tmp;
  }

  void set_has_group_by(bool has_group_by) {
    group_by_info_.has_group_by = has_group_by;
  }
  bool has_group_by() const { return group_by_info_.has_group_by; }

  void set_has_having(bool has_having) { has_having_ = has_having; }
  bool has_having() const { return has_having_; }

  void set_has_order_by(bool has_order_by) { has_order_by_ = has_order_by; }
  bool has_order_by() const { return has_order_by_; }

  void set_has_select_distinct(bool has_select_distinct) {
    has_select_distinct_ = has_select_distinct;
  }
  bool has_select_distinct() const { return has_select_distinct_; }

  void set_is_post_distinct(bool is_post_distinct) {
    group_by_info_.is_post_distinct = is_post_distinct;
  }
  bool is_post_distinct() const { return group_by_info_.is_post_distinct; }

 private:
  // SELECT list information.

  // SELECT list column information.  This is used for resolving
  // ordinal references to a SELECT-column, and providing the final scan
  // for this (sub)query.
  // Always non-NULL.
  std::unique_ptr<SelectListTransformState> select_list_transform_state_;

  // SELECT list computed columns.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      select_list_columns_to_compute_;

  // Columns that must be computed before the AggregateScan. It is populated
  // with SELECT list columns if the query has GROUP BY or aggregation, and
  // either HAVING or ORDER BY is present in the query.
  // This list only contains SELECT columns that do not themselves include
  // aggregation.
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      select_list_columns_to_compute_before_aggregation_;

  // SELECT DISTINCT information
  bool has_select_distinct_ = false;

  // GROUP BY and aggregation information. Also (re)used for
  // SELECT DISTINCT.
  TransformerGroupByAndAggregateInfo group_by_info_;

  // HAVING information.
  bool has_having_ = false;

  // ORDER BY information.
  bool has_order_by_ = false;

  // List of ORDER BY information.
  std::vector<OrderByItemTransformInfo> order_by_item_transform_info_;

  // Columns that need to be computed for ORDER BY (before OrderByScan).
  std::vector<std::unique_ptr<const zetasql::ResolvedComputedColumn>>
      order_by_columns_to_compute_;
};

}  // namespace postgres_translator

#endif  // TRANSFORMER_TRANSFORMER_HELPER_H_
