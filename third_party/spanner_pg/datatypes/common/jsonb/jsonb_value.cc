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

#include "third_party/spanner_pg/datatypes/common/jsonb/jsonb_value.h"

#include <string.h>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "zetasql/base/logging.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "nlohmann/json.hpp"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/datatypes/common/pg_numeric_parse.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres::datatypes::common::jsonb {

using absl::StrAppend;

class PGJSONBParser {
 public:
  PGJSONBParser(std::vector<std::unique_ptr<TreeNode>>* tree_nodes)
      : tree_nodes_(tree_nodes),
        serializer_(nlohmann::detail::output_adapter<char>(
                        normalization_output_buffer_),
                    '\0'),
        max_depth_estimate_(0),
        total_bytes_estimate_(0) {
    normalization_output_buffer_.reserve(64);
  }

  // Called when null is parsed.
  bool null() {
    AddToWorkingStack(NodeType::kNull);
    return true;
  }

  // Called when a boolean is parsed; value is passed
  bool boolean(bool value) {
    AddToWorkingStack(value ? NodeType::kTrue : NodeType::kFalse);
    return true;
  }

  // Called when a signed integer number is parsed; value is passed.
  bool number_integer(std::int64_t value) {
    AddToWorkingStack(absl::StrCat(value), /*is_numeric=*/true);
    return true;
  }

  // Called when an unsigned integer number is parsed; value is passed.
  bool number_unsigned(std::uint64_t value) {
    AddToWorkingStack(absl::StrCat(value), /*is_numeric=*/true);
    return true;
  }

  // Currently, numbers are supported with up to 4,932 digits before the decimal
  // point. This is in contrast to Postgres' support for 131,072 digits before
  // the decimal point. This is a parser limitation since the nlohmann parser
  // must be able to pase the value into a real type, and 'long double' can only
  // accurately support numbers with that high of a value. The full 16,383
  // digits after the decimal point is supported, as well as trailing zeros.
  bool number_float(long double, const std::string& str_value) {
    absl::StatusOr<std::string> normalized =
        NormalizePgNumericForJsonB(str_value);
    if (!normalized.ok()) {
      status_ = std::move(normalized).status();
      return false;
    }
    AddToWorkingStack(*normalized, true);
    return true;
  }

  // Called when a string is parsed; value is passed and can be safely moved
  // away.
  bool string(std::string& val) {
    if (!IsValidString(val)) {
      return false;
    }
    serializer_.dump(val, /*pretty_print=*/false, /*=ensure_ascii=*/false,
                     /*indent_step=*/0);
    AddToWorkingStack(normalization_output_buffer_);
    normalization_output_buffer_.clear();
    return true;
  }

  // Binary is unsupported by JSONB.
  bool binary(std::vector<std::uint8_t>&) { return false; }

  // Called when an object begins. The number of elements
  // is passed in (or -1 if unknown). Push the node onto the top of the
  // working stack.
  bool start_object(std::size_t) {
    tree_nodes_->push_back(std::make_unique<TreeNode>(NodeType::kObject));
    AddToWorkingStack(tree_nodes_->back().get());
    return true;
  }

  // Called when an object ends, remove it from the working stack.
  bool end_object() {
    TreeNode* current_obj = working_stack_.back();

    // Increase size for object's '{' and '}' characters, as well as the ": "
    // and ", " delimiters for each member.
    total_bytes_estimate_ +=
        2 + 6 * std::get<JsonbObject>(current_obj->value).size();

    max_depth_estimate_ = std::max(
        max_depth_estimate_, static_cast<uint32_t>(working_stack_.size()));

    if (working_stack_.size() > 1) {
      working_stack_.pop_back();
    }
    return true;
  }

  // Called when an array begins. The number of elements is passed in (or -1 if
  // unknown). Push the node onto the top of the stack.
  bool start_array(std::size_t elements) {
    tree_nodes_->push_back(std::make_unique<TreeNode>(NodeType::kArray));
    AddToWorkingStack(tree_nodes_->back().get());
    return true;
  }

  // Called when an array ends, remove it from the working stack.
  bool end_array() {
    TreeNode* arr = working_stack_.back();
    max_depth_estimate_ = std::max(
        max_depth_estimate_, static_cast<uint32_t>(working_stack_.size()));

    // Don't pop if this is the top most node in the stack.
    if (working_stack_.size() > 1) {
      working_stack_.pop_back();
    }
    JsonbArray& vec = std::get<JsonbArray>(arr->value);
    total_bytes_estimate_ += vec.empty() ? 2 : 2 * vec.size();
    return true;
  }

  // Called when an object key is parsed; value is passed and can be safely
  // moved away.
  bool key(std::string& key) {
    if (!IsValidString(key)) {
      return false;
    }
    raw_object_key_ = key;

    serializer_.dump(key, /*pretty_print=*/false, /*=ensure_ascii=*/false,
                     /*indent_step=*/0);
    object_key_ = normalization_output_buffer_;
    total_bytes_estimate_ += normalization_output_buffer_.size();
    normalization_output_buffer_.clear();
    return true;
  }

  // Called when any parsing error.
  bool parse_error(std::size_t, const std::string&,
                   const nlohmann::detail::exception& exc) {
    status_ = absl::InvalidArgumentError(
        absl::StrCat("Error parsing user input JSON : ", exc.what()));
    return false;
  }

  // Returns a PgJsonbValue which is a wrapper on the internal representation.
  absl::StatusOr<PgJsonbValue> GetRepresentation() && {
    ZETASQL_RETURN_IF_ERROR(status_);
    ZETASQL_RET_CHECK_EQ(working_stack_.size(), 1);
    return PgJsonbValue(working_stack_.back(), std::move(tree_nodes_),
                        max_depth_estimate_, total_bytes_estimate_);
  }

  absl::StatusOr<std::string> NormalizePgNumericForJsonB(
      absl::string_view readable_value) {
    ZETASQL_ASSIGN_OR_RETURN(std::string pg_normalized,
                     NormalizePgNumeric(readable_value));
    ZETASQL_RETURN_IF_ERROR(
        ValidateIntegralPartForJsonB(readable_value, pg_normalized));
    return pg_normalized;
  }

 private:
  bool IsValidString(absl::string_view str) {
    if (absl::StrContains(str, '\0')) {
      status_ = absl::InvalidArgumentError(
          "unsupported Unicode escape sequence \n DETAIL: \\u0000 cannot be "
          "converted to text.");
      return false;
    }
    return true;
  }

  // Adds a string value to the working stack.
  void AddToWorkingStack(const std::string str, bool is_numeric = false) {
    total_bytes_estimate_ += str.size();
    tree_nodes_->push_back(std::make_unique<TreeNode>(str, is_numeric));
    AddToWorkingStack(tree_nodes_->back().get());
  }

  void AddToWorkingStack(NodeType val) {
    tree_nodes_->push_back(std::make_unique<TreeNode>(val));
    total_bytes_estimate_ +=
        5;  // Longest length of string this can represent, "false"
    AddToWorkingStack(tree_nodes_->back().get());
  }

  // Adds a TreeNode* to the working stack.
  void AddToWorkingStack(TreeNode* node) {
    if (working_stack_.empty()) {
      working_stack_.push_back(node);
      return;
    }
    if (working_stack_.back()->tag == NodeType::kArray) {
      std::get<JsonbArray>(working_stack_.back()->value).push_back(node);
    } else {
      ABSL_DCHECK_EQ(working_stack_.back()->tag, NodeType::kObject);
      std::get<JsonbObject>(working_stack_.back()->value)[raw_object_key_] =
          std::make_pair(node, object_key_);
    }
    if (node->tag == NodeType::kArray || node->tag == NodeType::kObject) {
      working_stack_.push_back(node);
    }
  }

  inline bool IsDigit(char ch) {
    return std::isdigit(static_cast<unsigned char>(ch)) != 0;
  }

  // Verifies whether the integral part has a max length of
  // `kMaxPGJSONBNumericWholeDigits` digits to mimic a limit set by Spanner
  absl::Status ValidateIntegralPartForJsonB(absl::string_view readable_value,
                                            std::string numeric_string) {
    int start = 0;

    // Skip over the sign if it exists.
    if (start < numeric_string.size() &&
        (numeric_string[start] == '+' || numeric_string[start] == '-')) {
      ++start;
    }

    // Find the end of the integral part of the number.
    int end = start;
    while (end < numeric_string.size() && IsDigit(numeric_string[end])) {
      ++end;
    }
    if ((end - start) > kMaxPGJSONBNumericWholeDigits) {
      return Error(NumericErrorCode::WHOLE_COMPONENT_TOO_LARGE, readable_value);
    }
    return absl::OkStatus();
  }

  // This vector contains all TreeNodes created while parsing a JSONB string.
  // These nodes exist throughout the lifetime of this class.
  // unowned.
  std::vector<std::unique_ptr<TreeNode>>* tree_nodes_;

  // The working stack while constructing the internal JSONB tree
  // representation. It contains the index of the current node in the
  // tree_nodes_ vector.
  std::vector<TreeNode*> working_stack_;

  // The field keeps track of currently active object key in its raw form, and
  // is used to sort the keys inside a JSONB object. Normalized form of object
  // key can't be used to sort as it violates the Postgres' JSONB Key ordering
  // compilance due to extra "\" characters in the represenation.
  std::string raw_object_key_;

  // The field keeps track of currently active object key in its normalized form
  std::string object_key_;

  // status_ keeps track of any error that may occur during parsing.
  absl::Status status_;

  // normalization_output_buffer_ buffer is for holding output from nlohmann's
  // dump() function which is called on keys and strings.
  std::string normalization_output_buffer_;

  // serializer_ is needed in order to perform nlohmann string normalization.
  nlohmann::detail::serializer<nlohmann::basic_json<>> serializer_;

  // Keep track of the maximum depth reached.
  uint32_t max_depth_estimate_;

  // total_bytes_estimate_ keeps track of the total number of bytes the result
  // will be during computation, so that a single allocation can be done on
  // output_.
  uint32_t total_bytes_estimate_;
};

absl::Cord PgJsonbValue::PrintTreeNode(TreeNode* node) const {
  std::string output;
  output.reserve(total_bytes_estimate_);

  // Stack used for maintaining state while printing.
  std::vector<PrintingStackNode> printing_stack;
  printing_stack.reserve(max_depth_estimate_);
  AddToPrintingStack(printing_stack, &output, node);

  while (!printing_stack.empty()) {
    PrintingStackNode& printing_stack_node = printing_stack.back();
    TreeNode* tree_node = printing_stack_node.first;

    if (tree_node->tag == NodeType::kArray) {
      JsonbArray& vec = std::get<JsonbArray>(tree_node->value);
      size_t& array_iterator = std::get<TreeNodeIteratorType::kArrayIterator>(
          printing_stack_node.second.iterator);
      const bool is_beginning_array = array_iterator == 0;
      const bool is_end_array = array_iterator == vec.size();
      if (is_beginning_array && is_end_array) {
        StrAppend(&output, "[]");
        printing_stack.pop_back();
        continue;
      }
      if (is_end_array) {
        StrAppend(&output, "]");
        printing_stack.pop_back();
        continue;
      }
      if (is_beginning_array) {
        StrAppend(&output, "[");
      } else {
        StrAppend(&output, ", ");
      }
      array_iterator++;
      printing_stack.back() = printing_stack_node;
      TreeNode* ptr = vec[array_iterator - 1];
      AddToPrintingStack(printing_stack, &output, ptr);
      continue;
    }
    ABSL_DCHECK_EQ(tree_node->tag, NodeType::kObject);
    JsonbObject& obj = std::get<JsonbObject>(tree_node->value);
    auto& obj_itr = std::get<TreeNodeIteratorType::kObjectIterator>(
        printing_stack_node.second.iterator);
    if (obj.empty()) {
      StrAppend(&output, "{}");
      printing_stack.pop_back();
      continue;
    }

    const bool is_beginning_obj = obj_itr == obj.cbegin();
    const bool is_end_obj = obj_itr == obj.cend();
    if (is_beginning_obj) {
      StrAppend(&output, "{");
    } else if (!is_beginning_obj && !is_end_obj) {
      StrAppend(&output, ", ");
    } else if (is_end_obj) {
      StrAppend(&output, "}");
      printing_stack.pop_back();
      continue;
    }
    StrAppend(&output, obj_itr->second.second);
    StrAppend(&output, ": ");
    TreeNode* ptr = obj_itr->second.first;
    obj_itr++;
    printing_stack.back() = printing_stack_node;
    AddToPrintingStack(printing_stack, &output, ptr);
  }

  auto* external = new std::string(output);
  return absl::MakeCordFromExternal(
      *external, [external](absl::string_view) { delete external; });
}

// Adds a TreeNode* to the printing stack. If `node` represents a simple node
// which contains no dependencies (everything except Arrays and Objects), does
// not add to the stack and instead appends the value directly to `output`.
void PgJsonbValue::AddToPrintingStack(
    std::vector<PrintingStackNode>& printing_stack, std::string* output,
    TreeNode* node) const {
  // Don't bother adding scalars to 'printing_stack', only objects and arrays.
  switch (node->tag) {
    case NodeType::kFalse: {
      StrAppend(output, "false");
      return;
    }
    case NodeType::kTrue: {
      StrAppend(output, "true");
      return;
    }
    case NodeType::kNull: {
      StrAppend(output, "null");
      return;
    }
    case NodeType::kString:
    case NodeType::kNumeric: {
      StrAppend(output, std::get<std::string>(node->value));
      return;
    }
    case NodeType::kArray: {
      TreeNodeIterator tree_node_iterator;
      tree_node_iterator.iterator = size_t(0);
      printing_stack.emplace_back(node, tree_node_iterator);
      return;
    }
    case NodeType::kObject: {
      TreeNodeIterator tree_node_iterator;
      tree_node_iterator.iterator = std::get<JsonbObject>(node->value).cbegin();
      printing_stack.emplace_back(node, tree_node_iterator);
      return;
    }
  }
}

void PgJsonbValue::CheckRepresentation(std::vector<NodeType> types) const {
  auto print_types = [this](const std::vector<NodeType>& types) {
    std::vector<absl::string_view> type_strings;
    std::for_each(types.begin(), types.end(),
                  [this, &type_strings](NodeType type) {
                    type_strings.push_back(NodeTypeToString(type));
                  });
    return absl::StrJoin(type_strings, ", ");
  };
  ABSL_CHECK(std::find(types.begin(), types.end(),  // Crash ok.
                  static_cast<NodeType>(rep_->tag)) != types.end())
      << "Expected " << print_types(types) << " but got "
      << NodeTypeToString(static_cast<NodeType>(rep_->tag));
}

std::string PgJsonbValue::GetString() const {
  CheckRepresentation({NodeType::kString});
  return std::get<std::string>(rep_->value)
      .substr(1, std::get<std::string>(rep_->value).size() - 2);
}

absl::string_view PgJsonbValue::GetSerializedString() const {
  CheckRepresentation({NodeType::kString});
  return std::get<std::string>(rep_->value);
}

absl::string_view PgJsonbValue::GetNumeric() const {
  CheckRepresentation({NodeType::kNumeric});
  return std::get<std::string>(rep_->value);
}

absl::Cord PgJsonbValue::Serialize() const { return PrintTreeNode(rep_); }

bool PgJsonbValue::HasMember(absl::string_view key) const {
  if (!IsObject()) {
    return false;
  }
  auto it = std::get<JsonbObject>(rep_->value).find(std::string(key));
  return it != std::get<JsonbObject>(rep_->value).end();
}

absl::Status PgJsonbValue::CreateMemberIfNotExists(absl::string_view key) {
  CheckRepresentation({NodeType::kObject});
  if (HasMember(key)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Member already exists: ", key));
  }

  if (absl::StrContains(key, '\0')) {
    return absl::InvalidArgumentError(
        "unsupported Unicode escape sequence \n DETAIL: \\u0000 cannot be "
        "converted to text.");
  }

  std::string key_str(key);
  std::string serialization_output_buffer;
  nlohmann::detail::serializer<nlohmann::basic_json<>> serializer(
      nlohmann::detail::output_adapter<char>(serialization_output_buffer),
      '\0');

  serializer.dump(key, /*pretty_print=*/false, /*ensure_ascii=*/false,
                  /*indent_step=*/0);
  std::get<JsonbObject>(rep_->value)[std::string(key)] = std::make_pair(
      CreateTreeNode(NodeType::kNull), serialization_output_buffer);
  total_bytes_estimate_ += serialization_output_buffer.size() + 5;
  return absl::OkStatus();
}

std::optional<PgJsonbValue> PgJsonbValue::GetMemberIfExists(
    absl::string_view key) const {
  if (!IsObject()) {
    return std::nullopt;
  }
  auto it = std::get<JsonbObject>(rep_->value).find(std::string(key));
  if (it == std::get<JsonbObject>(rep_->value).end()) {
    return std::nullopt;
  }
  return PgJsonbValue(it->second.first, tree_nodes_, max_depth_estimate_,
                      total_bytes_estimate_);
}

std::vector<std::pair<absl::string_view, PgJsonbValue>>
PgJsonbValue::GetMembers() const {
  CheckRepresentation({NodeType::kObject, NodeType::kNull});
  if (IsNull()) {
    return {};
  }
  std::vector<std::pair<absl::string_view, PgJsonbValue>> members;
  members.reserve(std::get<JsonbObject>(rep_->value).size());
  TreeNode* ptr;
  for (const auto& member : std::get<JsonbObject>(rep_->value)) {
    ptr = member.second.first;
    members.push_back(std::make_pair(
        absl::string_view(member.first.data(), member.first.size()),
        PgJsonbValue(ptr, tree_nodes_, max_depth_estimate_,
                     total_bytes_estimate_)));
  }
  return members;
}

std::vector<std::string> PgJsonbValue::GetKeys() const {
  CheckRepresentation({NodeType::kObject, NodeType::kNull});
  if (IsNull()) {
    return {};
  }
  std::vector<std::string> keys;
  for (const auto& [key, value] : std::get<JsonbObject>(rep_->value)) {
    keys.push_back(key);
  }
  return keys;
}

bool PgJsonbValue::Exists(absl::string_view text) const {
  if (!IsValidJsonbString(text)) {
    return false;
  }
  if (IsObject()) {
    return HasMember(text);
  }

  if (IsNumeric() && std::get<std::string>(rep_->value) == text) {
    return true;
  }

  // JSONB string is serialized with additional quotation marks.
  std::string serialized_text =
      datatypes::common::jsonb::NormalizeJsonbString(text);

  if (IsArray()) {
    for (const auto& elem : std::get<JsonbArray>(rep_->value)) {
      if (elem->tag == NodeType::kString &&
          std::get<std::string>(elem->value) == serialized_text) {
        return true;
      }
    }
    return false;
  }

  if (IsString() && std::get<std::string>(rep_->value) == serialized_text) {
    return true;
  }

  return false;
}

bool PgJsonbValue::RemoveMember(absl::string_view key) {
  CheckRepresentation({NodeType::kObject});
  return std::get<JsonbObject>(rep_->value).erase(std::string(key)) > 0;
}

size_t PgJsonbValue::GetObjectSize() const {
  CheckRepresentation({NodeType::kObject});
  return std::get<JsonbObject>(rep_->value).size();
}

int64_t PgJsonbValue::GetArraySize() const {
  CheckRepresentation({NodeType::kArray});
  return std::get<JsonbArray>(rep_->value).size();
}

std::optional<PgJsonbValue> PgJsonbValue::GetArrayElementIfExists(
    int64_t index) const {
  if (!IsArray()) {
    return std::nullopt;
  }
  // Transform a negative index to a positive one.
  if (index < 0) {
    index = GetArraySize() + index;
  }
  // If it's still less than 0 or greater than or equal to the array size,
  // return nullopt as it is out of range.
  if (index < 0 || index >= GetArraySize()) {
    return std::nullopt;
  }
  return PgJsonbValue(std::get<JsonbArray>(rep_->value)[index], tree_nodes_,
                      max_depth_estimate_, total_bytes_estimate_);
}

std::vector<PgJsonbValue> PgJsonbValue::GetArrayElements() const {
  CheckRepresentation({NodeType::kArray});
  std::vector<PgJsonbValue> elements;
  elements.reserve(std::get<JsonbArray>(rep_->value).size());
  for (const auto& elem : std::get<JsonbArray>(rep_->value)) {
    elements.push_back(PgJsonbValue(elem, tree_nodes_, max_depth_estimate_,
                                    total_bytes_estimate_));
  }
  return elements;
}

absl::StatusOr<std::vector<absl::Cord>>
PgJsonbValue::GetSerializedArrayElements() const {
  if (!IsArray()) {
    return absl::InvalidArgumentError(
        "GetSerializedArrayElements can only be called on a JSONB array");
  }
  std::vector<absl::Cord> result_array;
  JsonbArray& value_array = std::get<JsonbArray>(rep_->value);
  result_array.reserve(value_array.size());
  for (const auto& elem : value_array) {
    result_array.push_back(PrintTreeNode(elem));
  }
  return result_array;
}

absl::Status PgJsonbValue::InsertArrayElement(const PgJsonbValue& jsonb_value,
                                              int64_t index) {
  CheckRepresentation({NodeType::kArray});
  if (GetArraySize() >= kJSONBMaxArraySize) {
    return absl::OutOfRangeError(absl::StrCat(
        "JSONB array size exceeds the limit of ", kJSONBMaxArraySize));
  }
  // Transform a negative index to a positive one.
  if (index < 0) {
    index = GetArraySize() + index;
  }
  // If index is still negative insert at the beginning of the array
  if (index < 0) {
    index = 0;
  }
  // If the index is greater than the array size, insert at the end of the
  // array.
  if (index >= GetArraySize()) {
    index = GetArraySize();
  }
  std::get<JsonbArray>(rep_->value)
      .insert(std::get<JsonbArray>(rep_->value).begin() + index,
              jsonb_value.rep_);
  return absl::OkStatus();
}

bool PgJsonbValue::RemoveArrayElement(int64_t index) {
  CheckRepresentation({NodeType::kArray});
  // Transform a negative index to a positive one.
  if (index < 0) {
    index = GetArraySize() + index;
  }
  // If it's still less than 0 or greater than or equal to the array size,
  // return false as it is out of range.
  if (index < 0 || index >= GetArraySize()) {
    return false;
  }
  std::get<JsonbArray>(rep_->value)
      .erase(std::get<JsonbArray>(rep_->value).begin() + index);
  return true;
}

void PgJsonbValue::CleanUpJsonbObject() {
  CheckRepresentation({NodeType::kObject});
  JsonbObject& obj = std::get<JsonbObject>(rep_->value);
  for (auto it = obj.begin(); it != obj.end();) {
    if (it->second.first->tag == NodeType::kNull) {
      it = obj.erase(it);
    } else {
      ++it;
    }
  }
}

absl::StatusOr<int32_t> PgJsonbValue::PathElementToIndex(
    absl::string_view path_element, int pos) const {
  int32_t index;
  if (!absl::SimpleAtoi(path_element, &index)) {
    return absl::InvalidArgumentError(absl::Substitute(
        "path element at position $0 is not an integer: \"$1\"", pos,
        path_element));
  }
  return index;
}

absl::StatusOr<std::optional<PgJsonbValue>> PgJsonbValue::FindAtPath(
    absl::Span<const std::string> path, FindAtPathMode mode) const {
  const PgJsonbValue* parent = this;
  // Keep the PgJsonbValue representation of the children in the path alive
  // until the end of computation.
  std::vector<PgJsonbValue> children;
  children.reserve(path.size());
  for (int i = 0; i < path.size(); ++i) {
    auto path_element = path[i];
    std::optional<PgJsonbValue> child = std::nullopt;
    if (parent->IsArray()) {
      absl::StatusOr<int32_t> index = PathElementToIndex(path_element, i + 1);
      if (mode == kIgnoreStringPathOnArrayError && !index.ok()) {
        return std::nullopt;
      }
      ZETASQL_RETURN_IF_ERROR(index.status());
      child = parent->GetArrayElementIfExists(*index);
    } else if (parent->IsObject()) {
      child = parent->GetMemberIfExists(path_element);
    }
    if (child.has_value()) {
      children.push_back(std::move(child.value()));
      parent = &children.back();
    } else {
      return std::nullopt;
    }
  }
  return *parent;
}

void PgJsonbValue::SetValue(const PgJsonbValue& value) {
  (*rep_).value = value.rep_->value;
  (*rep_).tag = value.rep_->tag;
  max_depth_estimate_ = value.max_depth_estimate_;
  total_bytes_estimate_ = value.total_bytes_estimate_;
}

absl::StatusOr<PgJsonbValue> PgJsonbValue::Parse(
    absl::string_view jsonb,
    std::vector<std::unique_ptr<TreeNode>>* tree_nodes) {
  PGJSONBParser parser(tree_nodes);
  using jsonb_parser =
      nlohmann::basic_json<std::map, std::vector, std::string, bool,
                           std::int64_t, std::uint64_t, long double>;
  jsonb_parser::sax_parse(jsonb, &parser);
  return std::move(parser).GetRepresentation();
}

PgJsonbValue PgJsonbValue::CreateEmptyArray(
    std::vector<std::unique_ptr<TreeNode>>* tree_nodes) {
  tree_nodes->push_back(std::make_unique<TreeNode>(NodeType::kArray));
  return PgJsonbValue(tree_nodes->back().get(), tree_nodes,
                      /*max_depth_estimate=*/0, /*total_bytes_estimate=*/0);
}

PgJsonbValue PgJsonbValue::CreateEmptyObject(
    std::vector<std::unique_ptr<TreeNode>>* tree_nodes) {
  tree_nodes->push_back(std::make_unique<TreeNode>(NodeType::kObject));
  return PgJsonbValue(tree_nodes->back().get(), tree_nodes,
                      /*max_depth_estimate=*/0, /*total_bytes_estimate=*/0);
}

TreeNode* PgJsonbValue::CreateTreeNode(NodeType tag) {
  std::unique_ptr<TreeNode> tree_node = std::make_unique<TreeNode>(tag);
  tree_nodes_->push_back(std::move(tree_node));
  return tree_nodes_->back().get();
}

absl::StatusOr<absl::Cord> ParseJsonb(absl::string_view json) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSIGN_OR_RETURN(PgJsonbValue result, PgJsonbValue::Parse(json, &tree_nodes));
  absl::Cord result_cord = result.Serialize();
  return result_cord;
}

std::string NormalizeJsonbString(absl::string_view value) {
  return nlohmann::json(value).dump();
}

bool IsValidJsonbString(absl::string_view str) {
  return !absl::StrContains(str, '\0');
}

}  // namespace postgres_translator::spangres::datatypes::common::jsonb
