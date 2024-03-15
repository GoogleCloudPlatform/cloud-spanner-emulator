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

#include "third_party/spanner_pg/datatypes/common/jsonb/jsonb_parse.h"

#include <string.h>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
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
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "nlohmann/detail/exceptions.hpp"
#include "nlohmann/detail/output/output_adapters.hpp"
#include "nlohmann/detail/output/serializer.hpp"
#include "nlohmann/json_fwd.hpp"
#include "nlohmann/json.hpp"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/datatypes/common/pg_numeric_parse.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres::datatypes::common::jsonb {

using absl::StrAppend;

struct ObjectKeyLess {
  constexpr bool operator()(absl::string_view lhs,
                            absl::string_view rhs) const {
    if (lhs.size() == rhs.size()) {
      return lhs < rhs;
    }
    return lhs.size() < rhs.size();
  }
};

class PGJSONBParser {
  enum NodeType : uint8_t {
    kFalse = 0,
    kTrue = 1,
    kNull = 2,
    kString = 3,
    kArray = 4,
    kObject = 5,
  };

  struct TreeNode;
  using JsonbArray = std::vector<TreeNode*>;

  // JSONB Object Value field consists of a TreeNode* along with normalized
  // representation of its corresponding JSONB Object Key field.
  using JsonObjectValue = std::pair<TreeNode*, std::string>;
  using JsonbObject = std::map<std::string, JsonObjectValue, ObjectKeyLess>;

  // TreeNode only stores one of JsonbObject, JsonbArray, or string at a
  // time. The tag field stores information about what type of object TreeNode
  // stores. Simple values like true/false/null are piggybacked in the tag
  // field.
  struct TreeNode {
    TreeNode() = delete;
    TreeNode(const TreeNode&) = delete;
    TreeNode& operator=(const TreeNode&) = delete;
    explicit TreeNode(std::string string)
        : tag(NodeType::kString), value(string) {}
    explicit TreeNode(NodeType tag) : tag(tag) {
      switch (tag) {
        case kFalse:
        case kTrue:
        case kNull:
          break;
        case kString:
          value = std::string();
          break;
        case kArray:
          value = JsonbArray();
          break;
        case kObject:
          value = JsonbObject();
          break;
      }
    }
    NodeType tag;
    std::variant<JsonbArray, JsonbObject, std::string> value;
  };

  enum TreeNodeIteratorType : uint8_t {
    kArrayIterator = 1,
    kObjectIterator = 2,
  };

  struct TreeNodeIterator {
    std::variant<std::monostate, size_t, JsonbObject::const_iterator> iterator;
  };

  using PrintingStackNode = std::pair<TreeNode*, TreeNodeIterator>;

 public:
  PGJSONBParser()
      : serializer_(nlohmann::detail::output_adapter<char>(
                        normalization_output_buffer_),
                    '\0'),
        max_depth_(0),
        total_bytes_(0) {
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
    AddToWorkingStack(absl::StrCat(value));
    return true;
  }

  // Called when an unsigned integer number is parsed; value is passed.
  bool number_unsigned(std::uint64_t value) {
    AddToWorkingStack(absl::StrCat(value));
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
    AddToWorkingStack(*normalized);
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
    tree_nodes_.push_back(std::make_unique<TreeNode>(NodeType::kObject));
    AddToWorkingStack(tree_nodes_.back().get());
    return true;
  }

  // Called when an object ends, remove it from the working stack.
  bool end_object() {
    TreeNode* current_obj = working_stack_.back();

    // Increase size for object's '{' and '}' characters, as well as the ": "
    // and ", " delimiters for each member.
    total_bytes_ += 2 + 6 * std::get<JsonbObject>(current_obj->value).size();
    max_depth_ =
        std::max(max_depth_, static_cast<uint32_t>(working_stack_.size()));
    if (working_stack_.size() > 1) {
      working_stack_.pop_back();
    }
    return true;
  }

  // Called when an array begins. The number of elements is passed in (or -1 if
  // unknown). Push the node onto the top of the stack.
  bool start_array(std::size_t elements) {
    tree_nodes_.push_back(std::make_unique<TreeNode>(NodeType::kArray));
    AddToWorkingStack(tree_nodes_.back().get());
    return true;
  }

  // Called when an array ends, remove it from the working stack.
  bool end_array() {
    TreeNode* arr = working_stack_.back();
    max_depth_ =
        std::max(max_depth_, static_cast<uint32_t>(working_stack_.size()));

    // Don't pop if this is the top most node in the stack.
    if (working_stack_.size() > 1) {
      working_stack_.pop_back();
    }
    JsonbArray& vec = std::get<JsonbArray>(arr->value);
    total_bytes_ += vec.empty() ? 2 : 2 * vec.size();
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
    total_bytes_ += normalization_output_buffer_.size();
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

  absl::StatusOr<absl::Cord> PrintTreeNode(TreeNode* node) {
    std::string output;
    output.reserve(total_bytes_);

    // Stack used for maintaining state while printing.
    std::vector<PrintingStackNode> printing_stack;
    printing_stack.reserve(max_depth_);
    ZETASQL_RET_CHECK_EQ(working_stack_.size(), 1);
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

    ABSL_DCHECK_GE(total_bytes_, output.size())
        << "Did not pre-allocate enough for output buffer";
    auto* external = new std::string(output);
    return absl::MakeCordFromExternal(
        *external, [external](absl::string_view) { delete external; });
  }

  // Converts the constructed in-memory JSONB tree representation into a
  // normalized textual output matching Postgres' semantics. Creates the output
  // using a non-recursive iterative algorithm to prevent any stack-overflow
  // from occurring. Returns an error if any was encountered during parsing.
  // Note that this function must be called exactly once, as it frees resources
  // which were created during the sax_parse phase.
  absl::StatusOr<absl::Cord> GetResultDestructive() && {
    ZETASQL_RETURN_IF_ERROR(status_);

    ZETASQL_RET_CHECK_EQ(working_stack_.size(), 1);
    ZETASQL_ASSIGN_OR_RETURN(absl::Cord result, PrintTreeNode(working_stack_[0]));

    status_.Update(absl::Status(absl::StatusCode::kFailedPrecondition,
                                "Cannot call GetResultDestructive twice"));
    return result;
  }

  absl::StatusOr<std::vector<absl::Cord>> GetResultArrayDestructive() && {
    ZETASQL_RETURN_IF_ERROR(status_);

    std::vector<absl::Cord> result_array;
    ZETASQL_RET_CHECK_EQ(working_stack_.size(), 1);
    TreeNode* array_ptr = working_stack_[0];
    if (array_ptr->tag != NodeType::kArray) {
      return absl::InvalidArgumentError(
          "ParseJsonbArray can only be called on a JSONB array");
    }

    JsonbArray& vec = std::get<JsonbArray>(array_ptr->value);
    result_array.reserve(vec.size());
    for (const auto& element : vec) {
      ZETASQL_ASSIGN_OR_RETURN(absl::Cord result, PrintTreeNode(element));
      result_array.push_back(result);
    }
    status_.Update(absl::Status(absl::StatusCode::kFailedPrecondition,
                                "Cannot call GetResultArrayDestructive twice"));
    return result_array;
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

  // Adds a TreeNode* to the printing stack. If `node` represents a simple node
  // which contains no dependencies (everything except Arrays and Objects), does
  // not add to the stack and instead appends the value directly to `output`.
  void AddToPrintingStack(std::vector<PrintingStackNode>& printing_stack,
                          std::string* output, TreeNode* node) {
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
      case NodeType::kString: {
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
        tree_node_iterator.iterator =
            std::get<JsonbObject>(node->value).cbegin();
        printing_stack.emplace_back(node, tree_node_iterator);
        return;
      }
    }
  }

  // Adds a string value to the working stack.
  void AddToWorkingStack(const std::string str) {
    tree_nodes_.push_back(std::make_unique<TreeNode>(str));
    total_bytes_ += str.size();
    AddToWorkingStack(tree_nodes_.back().get());
  }

  void AddToWorkingStack(NodeType val) {
    tree_nodes_.push_back(std::make_unique<TreeNode>(val));
    total_bytes_ += 5;  // Longest length of string this can represent, "false"
    AddToWorkingStack(tree_nodes_.back().get());
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
  std::vector<std::unique_ptr<TreeNode>> tree_nodes_;

  // The working stack while constructing the internal JSONB tree
  // representation. It contains pointers to TreeNode in tree_nodes_ vector and
  // does not modify them. Its lifetime is not longer than tree_nodes_.
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
  uint32_t max_depth_;

  // total_bytes_ keeps track of the total number of bytes the result
  // will be during computation, so that a single allocation can be done on
  // output_.
  uint32_t total_bytes_;
};

absl::StatusOr<absl::Cord> ParseJsonb(absl::string_view json) {
  PGJSONBParser parser;
  using jsonb_parser =
      nlohmann::basic_json<std::map, std::vector, std::string, bool,
                           std::int64_t, std::uint64_t, long double>;
  jsonb_parser::sax_parse(json, &parser);
  return std::move(parser).GetResultDestructive();
}

absl::StatusOr<std::vector<absl::Cord>> ParseJsonbArray(
    absl::string_view jsonb) {
  PGJSONBParser parser;
  using jsonb_parser =
      nlohmann::basic_json<std::map, std::vector, std::string, bool,
                           std::int64_t, std::uint64_t, long double>;
  jsonb_parser::sax_parse(jsonb, &parser);
  return std::move(parser).GetResultArrayDestructive();
}

std::string NormalizeJsonbString(absl::string_view value) {
  return nlohmann::json(value).dump();
}

bool IsValidJsonbString(absl::string_view str) {
  return !absl::StrContains(str, '\0');
}

}  // namespace postgres_translator::spangres::datatypes::common::jsonb
