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

#include "third_party/spanner_pg/transformer/transformer_helper.h"

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/public/id_string.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/memory/memory.h"
#include "zetasql/base/map_util.h"

namespace postgres_translator {

SelectColumnTransformState* SelectListTransformState::AddSelectColumn(
    const TargetEntry* entry, zetasql::IdString alias, bool has_aggregation,
    std::unique_ptr<const zetasql::ResolvedExpr> resolved_expr) {
  auto select_column_state = std::make_unique<SelectColumnTransformState>(
      entry, alias, has_aggregation, std::move(resolved_expr));

  // Locally store the pointer so it can be returned.
  SelectColumnTransformState* raw_ptr = select_column_state.get();

  // Save the SelectColumnTransformState.
  select_column_state_list_.push_back(std::move(select_column_state));

  return raw_ptr;
}

const zetasql::ResolvedColumnList
SelectListTransformState::resolved_column_list() const {
  zetasql::ResolvedColumnList resolved_column_list;
  resolved_column_list.reserve(select_column_state_list_.size());
  for (const std::unique_ptr<SelectColumnTransformState>& select_column_state :
       select_column_state_list_) {
    resolved_column_list.push_back(select_column_state->resolved_select_column);
  }
  return resolved_column_list;
}

void TransformerInfo::AddGroupByComputedColumnIfNeeded(
    const zetasql::ResolvedColumn& column,
    std::unique_ptr<const zetasql::ResolvedExpr> expr) {
  group_by_info_.has_group_by = true;
  auto new_column =
      zetasql::MakeResolvedComputedColumn(column, std::move(expr));
  group_by_info_.group_by_columns_to_compute.push_back(std::move(new_column));
}

}  // namespace postgres_translator
