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

#ifndef TRANSFORMER_EXPR_TRANSFORMER_HELPER_H_
#define TRANSFORMER_EXPR_TRANSFORMER_HELPER_H_

#include <memory>

#include "zetasql/public/id_string.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/strings/str_format.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/transformer/transformer_helper.h"

namespace postgres_translator {

// The set of ResolvedColumns referenced in a particular subquery that resolved
// as correlated references to values from a parent VarIndexScope.
// The bool value is true if the reference is to a column from more than one
// enclosing VarIndexScope away; i.e. a column that was already a correlated
// reference in the enclosing query.
typedef std::map<zetasql::ResolvedColumn, bool> CorrelatedColumnsSet;

// A list of the CorrelatedColumnsSets attached to all VarIndexScopes traversed
// while looking up a name.  The sets are ordered from child scopes to
// parent scopes; i.e. the outermost query's VarIndexScope is last.
typedef std::vector<CorrelatedColumnsSet*> CorrelatedColumnsSetList;

// The index of a Postgres `Var` (i.e. a column).
struct VarIndex {
  // The index of the scan in Query.rtable. Starting from 1.
  int varno;
  // The index of the var in Query.targetList. Starting from 1.
  int varattno;
  // The number of subqueries up for this Var. Only used by the reverse
  // transformer since the forward transformer uses VarIndexScope levels to
  // represent the subquery level.
  Index varlevelsup = 0;

  std::string DebugString() const {
    return absl::StrFormat("(%d, %d, %d)", varno, varattno, varlevelsup);
  }

  bool operator==(const VarIndex& other) const {
    return varno == other.varno && varattno == other.varattno &&
           varlevelsup == other.varlevelsup;
  }

  template <typename H>
  friend H AbslHashValue(H state, const VarIndex& v) {
    return H::combine(std::move(state), v.varno, v.varattno, v.varlevelsup);
  }
};

class VarIndexTarget {
 public:
  enum Kind { COLUMN, ACCESS_ERROR };

  // The default is an ACCESS_ERROR since there is no corresponding column.
  VarIndexTarget() : kind_(ACCESS_ERROR) {}

  // Construct a VarIndexTarget pointing at a column.
  VarIndexTarget(const zetasql::ResolvedColumn& column)
      : kind_(COLUMN), column_(column) {}

  Kind kind() const { return kind_; }
  const zetasql::ResolvedColumn& column() const { return column_; }

 private:
  Kind kind_;

  // Populated if kind_ is COLUMN.
  zetasql::ResolvedColumn column_;
};

typedef absl::flat_hash_map<VarIndex, VarIndexTarget, absl::Hash<VarIndex>>
    VarIndexMap;

// A scope for column references in a query. In PostgreSQL, column references
// are represented as Vars.
//
// VarIndexScopes can be chained together and child scopes can hide references
// from parent scopes.
//
// How correlated lookups are tracked:
//   When resolving a correlated subquery, we'll build a local VarIndexScope
//   that inherits the outer query's VarIndexScope as its previous_scope.
//
//   In the child VarIndexScope (the subquery's VarIndexScope), we'll store a
//   CorrelatedColumnsSet, and that set will store all ResolvedColumns looked
//   up via this VarIndexScope (i.e. inside the subquery) that resolve to a
//   column from a parent VarIndexScope.  This set will provide the list of
//   parameters that must be passed to the ResolvedSubqueryExpr.
//
//   When LookupVarIndex finds a Var from a parent scope, it returns the
//   CorrelatedColumnsSet pointer for the child VarIndexScope, and the
//   caller must insert into it any columns resolved from a parent scope.
//
//   With multiply nested VarIndexScopes, a ResolvedColumn resolved from the
//   outermost ancestor scope must be added to CorrelatedColumnsSets for each
//   child scope that that lookup passes through, so the columns can be added
//   to the ResolvedSubqueryExpr parameters list at each subquery nesting
//   level.  To allow this, LookupVarIndex returns a list of
//   CorrelatedColumnsSets.
//
// Modeled off of ZetaSQL's NameScope.
class VarIndexScope {
 public:
  // Make a scope with no underlying fallback scope.
  VarIndexScope() : previous_scope_(nullptr) {}

  // Make a scope with a fallback to look for names in <previous_scope>, if
  // non-NULL.
  // If <correlated_columns_set> is non-NULL, store that pointer with this scope
  // so it can be updated to compute the set of correlated column references.
  // The <correlated_columns_set> and <previous_scope> pointers must outlive
  // this NameScope.
  explicit VarIndexScope(const VarIndexScope* previous_scope,
                         CorrelatedColumnsSet* correlated_columns_set = nullptr)
      : previous_scope_(previous_scope),
        correlated_columns_set_(correlated_columns_set) {}

  VarIndexScope(const VarIndexScope* previous_scope,
                const VarIndexMap& var_index_map,
                CorrelatedColumnsSet* correlated_columns_set = nullptr)
      : previous_scope_(previous_scope),
        var_index_map_(var_index_map),
        correlated_columns_set_(correlated_columns_set) {}

  // Make a VarIndexScope that inherits mappings from <previous_scope>, if
  // non-NULL, and making the mappings from <current_scope> visible over top of
  // it.
  //
  // ZetaSQL's constructor also accepts a <correlated_columns_set>, but there
  // do not seem to be any callsites that pass it in, so we do not include it
  // here.
  VarIndexScope(const VarIndexScope* previous_scope,
                const VarIndexScope& current_scope)
      : VarIndexScope(previous_scope, current_scope.var_index_map()) {}

  // Create a copy of this pre group-by scope with var_index_map entries
  // invalidated if they do not appear in group_by_map and overridden if they do
  // appear in group_by_map, to create a post group-by scope.
  std::unique_ptr<const VarIndexScope> CreatePostGroupByScope(
      const std::unordered_map<const Var*, zetasql::ResolvedColumn>&
          group_by_map) const {
    auto new_scope = std::make_unique<VarIndexScope>(
        previous_scope_, var_index_map_, correlated_columns_set_);
    for (auto& kv : var_index_map_) {
      new_scope->MapVarIndexToTarget(kv.first, VarIndexTarget(),
                                     /*allow_override=*/true);
    }
    for (auto& kv : group_by_map) {
      // We only override vars at this subquery level.
      if (kv.first->varlevelsup != 0) {
        continue;
      }
      new_scope->MapVarIndexToTarget(
          {.varno = kv.first->varno, .varattno = kv.first->varattno}, kv.second,
          /*allow_override=*/true);
    }
    return new_scope;
  }

  // Create a copy of this scope with var_index_map entries overridden if they
  // also appear in overriding_map. Does not copy correlated_columns_set.
  std::unique_ptr<VarIndexScope> CopyVarIndexScopeWithOverridingMap(
      VarIndexMap& overriding_map) const {
    auto new_scope = std::make_unique<VarIndexScope>(
        previous_scope_, var_index_map_, /*correlated_columns_set=*/nullptr);
    for (auto& kv : overriding_map) {
      new_scope->MapVarIndexToTarget(kv.first, kv.second,
                                     /*allow_override=*/true);
    }
    return new_scope;
  }

  // Look up a name in this scope, and underlying scopes if necessary.
  // Return true and copy result into <*column> if found.
  //
  // If <correlated_columns_sets> is non-NULL, on return, it will contain
  // any non-NULL correlated_columns_set_ from any VarIndexScopes traversed
  // before reaching the VarIndexScope containing <name>.  Any column used via
  // this resolved name should be added to all of these CorrelatedColumnsSets,
  // and then passed as a parameter to the corresponding ResolvedSubqueryExpr.
  //
  // The returned sets are ordered so the ones attached to child scopes come
  // before the ones attached to their parent scopes.
  absl::StatusOr<bool> LookupVarIndex(
      const VarIndex& var_index, Index var_levels_up,
      zetasql::ResolvedColumn* column,
      CorrelatedColumnsSetList* correlated_columns_sets) const {
    if (correlated_columns_sets != nullptr) {
      correlated_columns_sets->clear();
    }

    int level = 0;
    const VarIndexScope* current = this;
    while (current != nullptr) {
      // Traverse to the correct query/subquery level.
      // Rather than having a single scope per subquery level, ZetaSQL and
      // Spangres may have one or two scopes per subquery level, depending on
      // if the subquery is in the FROM clause or another clause.
      //
      // The general rule is that each subquery level starts with an external
      // scope from outer queries if they exist (nullptr otherwise). It
      // references the external scope as it processes the FROM clause and
      // builds an independent from_scan_output_scope. After the FROM clause has
      // been transformed, the from_scan_output_scope is stacked on top of the
      // external_scope. All other clauses will be processed using the
      // from_scan_output_scope as the scope for var_levels_up = 0, and the
      // external_scope(s) as var_levels_up >= 1;
      //
      // In addition, anytime a new subquery is discovered, whether in the FROM
      // clause or another clause, a wrapper scope will be created to signal
      // the presence of a subquery. The wrapper scope can be identified by an
      // empty var_index_map and a populated correlated_columns_set_.
      //
      // If a new subquery is discovered in the FROM clause, it will have the
      // wrapper scope directly on top of the external scope. If the new
      // subquery is discovered in any other clause, it will have the
      // from_scan_output_scope stacked on top of the external_scope and the
      // subquery wrapper scope stacked on the from_scan_output_scope. As a
      // result, we cannot determine the subquery level simply by counting
      // the total number of scopes. Instead, we must count the wrapper scopes
      // which have an empty var_index_map and a populated
      // correlated_columns_set_
      if (level != var_levels_up) {
        if (current->correlated_columns_set_ != nullptr) {
          // This is an subquery wrapper scope. Increment the level.
          ZETASQL_RET_CHECK(current->var_index_map().empty());
          ++level;
        }
        current = current->previous_scope_;
        continue;
      }

      // This is the scope that matches var_levels_up.
      // Look for any matching VarIndex stored directly in this VarIndexScope.
      auto it = current->var_index_map().find(var_index);
      if (it != current->var_index_map().end()) {
        switch (it->second.kind()) {
          case VarIndexTarget::COLUMN: {
            *column = it->second.column();

            // If we are collecting correlated_columns_sets and this was a
            // correlated lookup, traverse from <this> to <current> again,
            // collecting all non-NULL correlated_columns_set_ pointers we pass.
            if (correlated_columns_sets != nullptr && current != this) {
              const VarIndexScope* scope = this;
              while (scope != current) {
                if (scope->correlated_columns_set_ != nullptr) {
                  correlated_columns_sets->push_back(
                      scope->correlated_columns_set_);
                }
                scope = scope->previous_scope_;
              }
            }
            return true;
          }
          case VarIndexTarget::ACCESS_ERROR: {
            return absl::InvalidArgumentError(
                "Expression references a column which is neither grouped nor "
                "aggregated.");
          }
        }
      }
      // The VarIndex was not found at the specified level.
      return false;
    }

    // A higher var_levels_up was specified than the number of scopes available.
    // This should never happen.
    return absl::InternalError(
        absl::StrFormat("Attempted VarIndex lookup at varlevelsup: %d but the "
                        "highest level of scopes available is %d",
                        var_levels_up, level));
  }

  bool MapVarIndexToTarget(const VarIndex& var_index,
                           const VarIndexTarget& target,
                           bool allow_override = false) {
    if (allow_override) {
      var_index_map_.insert_or_assign(var_index, target);
      return true;
    }
    return var_index_map_.insert({var_index, target}).second;
  }

  bool MapVarIndexToColumn(const VarIndex& var_index,
                           const zetasql::ResolvedColumn& column,
                           bool allow_override = false) {
    return MapVarIndexToTarget(var_index, {column}, allow_override);
  }

  void MergeFrom(const VarIndexScope& other) {
    for (const auto& kv : other.var_index_map()) {
      MapVarIndexToTarget(kv.first, kv.second, /*allow_override=*/true);
    }
  }

  std::string DebugString(const std::string& indent = "") const {
    std::string out;
    if (correlated_columns_set_ != nullptr) {
      absl::StrAppend(&out, "is an empty subquery wrapper");
    } else {
      absl::StrAppend(&out, "is an actual scope");
    }
    for (const auto& kv : var_index_map_) {
      if (!out.empty()) out += "\n";
      switch (kv.second.kind()) {
        case VarIndexTarget::COLUMN: {
          absl::StrAppend(&out, indent, kv.first.DebugString(), " -> ",
                          kv.second.column().DebugString());
          break;
        }
        case VarIndexTarget::ACCESS_ERROR: {
          absl::StrAppend(&out, indent, kv.first.DebugString(),
                          " -> ACCESS_ERROR");
          break;
        }
      }
    }
    if (previous_scope_ != nullptr) {
      if (!out.empty()) out += "\n";
      absl::StrAppend(&out, indent, " previous_scope: ",
                      previous_scope_->DebugString(absl::StrCat(indent, "  ")));
    }
    return out;
  }

  const VarIndexMap& var_index_map() const { return var_index_map_; }

 private:
  const VarIndexScope* const previous_scope_ = nullptr;  // may be NULL
  VarIndexMap var_index_map_;

  // Set used to collect correlated columns referenced from previous scopes.
  // This is not used or updated by the VarIndexScope class itself.
  // If this is non-NULL, this set pointer is returned from LookupVarIndex, and
  // any referenced columns should be added to it by the caller.
  CorrelatedColumnsSet* correlated_columns_set_ = nullptr;  // Not owned.
};

// ExprTransformerInfo is used to transform and validate an expression.
// It is passed recursively down through all expression transformation.
// Originally modeled after ZetaSQL's ExprResolutionInfo.
class ExprTransformerInfo {
 public:
  // Construct an ExprTransformerInfo that may allow aggregation.
  // Takes in a <var_index_scope_in> that is used to transform the expression
  // against, and an <aggregate_var_index_scope_in> that is used to resolve any
  // expression that is an aggregate function  argument.
  // Does not take ownership of <transformer_info_in>.
  ExprTransformerInfo(
      const VarIndexScope* var_index_scope_in,
      const VarIndexScope* aggregate_var_index_scope_in,
      bool allows_aggregation_in, bool use_post_grouping_columns_in,
      const char* clause_name_in, TransformerInfo* transformer_info_in,
      const Expr* top_level_ast_expr_in = nullptr,
      zetasql::IdString column_alias_in = zetasql::IdString())
      : var_index_scope(var_index_scope_in),
        aggregate_var_index_scope(aggregate_var_index_scope_in),
        allows_aggregation(allows_aggregation_in),
        clause_name(clause_name_in),
        transformer_info(transformer_info_in),
        use_post_grouping_columns(use_post_grouping_columns_in),
        top_level_ast_expr(top_level_ast_expr_in),
        column_alias(column_alias_in) {}

  // Construct an ExprTransformerInfo that allows aggregation.
  // Does not take ownership of <transformer_info_in>.
  // Currently used for initially resolving select list columns,
  // so never resolves against post-grouping columns.
  static ExprTransformerInfo ForAggregation(
      const VarIndexScope* var_index_scope_in,
      TransformerInfo* transformer_info_in,
      const Expr* top_level_ast_expr_in = nullptr,
      zetasql::IdString column_alias_in = zetasql::IdString()) {
    return ExprTransformerInfo(var_index_scope_in, var_index_scope_in,
                               /*allows_aggregation_in=*/true,
                               /*use_post_grouping_columns_in=*/false,
                               /*clause_name_in=*/"", transformer_info_in,
                               top_level_ast_expr_in, column_alias_in);
  }

  // Construct an ExprTransformerInfo that disallows aggregation.
  static ExprTransformerInfo ForScalarFunctions(
      const VarIndexScope* var_index_scope_in, const char* clause_name_in) {
    return ExprTransformerInfo(var_index_scope_in, var_index_scope_in,
                               /*allows_aggregation_in=*/false,
                               /*use_post_grouping_columns_in=*/false,
                               clause_name_in,
                               /*transformer_info_in=*/nullptr);
  }

  // Construct an ExprTransformerInfo that initializes itself from another
  // ExprTransformerInfo, overriding <var_index_scope>
  // has_aggregation will be updated in parent on destruction.
  // Does not take ownership of <parent>
  ExprTransformerInfo(ExprTransformerInfo* parent,
                      const VarIndexScope* var_index_scope_in,
                      const char* clause_name_in)
      : ExprTransformerInfo(
            var_index_scope_in, parent->aggregate_var_index_scope,
            parent->allows_aggregation, parent->use_post_grouping_columns,
            clause_name_in, parent->transformer_info,
            parent->top_level_ast_expr, parent->column_alias) {
    // Hack because I can't use initializer syntax and a delegated constructor.
    const_cast<ExprTransformerInfo*&>(this->parent) = parent;
  }

  ~ExprTransformerInfo() {
    // Propagate has_aggregation up the tree to the caller.
    // We assume all child ExprTransformerInfo objects will go out of scope
    // before the caller's has_ fields are examined.
    if (parent != nullptr) {
      if (has_aggregation) {
        parent->has_aggregation = true;
      }
    }
  }

  ExprTransformerInfo* const parent = nullptr;

  // VarIndexScope to use while resolving this expression.
  const VarIndexScope* const var_index_scope = nullptr;

  // VarIndexScope to use while resolving any aggregate function
  // arguments that are in this expression.
  const VarIndexScope* const aggregate_var_index_scope = nullptr;

  // Indicates whether this expression allows aggregations.
  const bool allows_aggregation;

  // <clause_name> is used to generate an error saying aggregate
  // functions are not allowed in this clause, e.g. "WHERE clause".
  // This can be empty if aggregate functions are
  // allowed, or if there is no clear clause name to use in error messages.
  const char* const clause_name;

  TransformerInfo* const transformer_info;

  // True if this expression contains an aggregation function.
  bool has_aggregation = false;

  // True if this expression should be resolved against post-grouping
  // columns.  Gets set to false when resolving arguments of aggregation
  // functions.
  bool use_post_grouping_columns = false;

  // The top-level AST expression being resolved in the current context. This
  // field is set only when resolving SELECT columns. Not owned.
  const Expr* const top_level_ast_expr = nullptr;

  // The column alias of the top-level AST expression in SELECT list, which will
  // be used as the name of the resolved column when the top-level AST
  // expression being resolved is an aggregate or an analytic function. This
  // field is set only when resolving SELECT columns.
  const zetasql::IdString column_alias = zetasql::IdString();
};
}  // namespace postgres_translator

#endif  // TRANSFORMER_EXPR_TRANSFORMER_HELPER_H_
