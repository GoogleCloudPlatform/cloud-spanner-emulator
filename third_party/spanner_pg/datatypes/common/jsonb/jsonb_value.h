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

// This file declares the PG.JSONB parsing interface method.
#ifndef DATATYPES_COMMON_JSONB_JSONB_VALUE_H_
#define DATATYPES_COMMON_JSONB_JSONB_VALUE_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace postgres_translator::spangres::datatypes::common::jsonb {

// Max array size. This is a soft limit only enforced on InsertArrayElement(s),
// as a user can accidentally enter a large value. This limit is not enforced on
// parsing as an accident is much less likely. Taken from json_value.h
// (http://google3/storage/googlesql/public/json_value.h;l=27;rcl=674377206).
inline constexpr size_t kJSONBMaxArraySize = 1000000;

struct ObjectKeyLess {
  constexpr bool operator()(absl::string_view lhs,
                            absl::string_view rhs) const {
    if (lhs.size() == rhs.size()) {
      return lhs < rhs;
    }
    return lhs.size() < rhs.size();
  }
};

enum NodeType : uint8_t {
  kFalse = 0,
  kTrue = 1,
  kNull = 2,
  kString = 3,
  kArray = 4,
  kObject = 5,
  kNumeric = 6,
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
  explicit TreeNode(std::string string, bool numeric = false) : value(string) {
    if (numeric) {
      tag = kNumeric;
    } else {
      tag = kString;
    }
  }
  explicit TreeNode(NodeType tag) : tag(tag) {
    switch (tag) {
      case kFalse:
      case kTrue:
      case kNull:
        break;
      case kString:
      case kNumeric:
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

class PgJsonbValue {
 public:
  explicit PgJsonbValue(TreeNode* rep,
                        std::vector<std::unique_ptr<TreeNode>>* tree_nodes,
                        uint32_t max_depth_estimate = 0,
                        uint32_t total_bytes_estimate = 0)
      : rep_(rep),
        tree_nodes_(tree_nodes),
        max_depth_estimate_(max_depth_estimate),
        total_bytes_estimate_(total_bytes_estimate) {};

  bool IsObject() const { return rep_->tag == NodeType::kObject; };
  bool IsArray() const { return rep_->tag == NodeType::kArray; };
  bool IsString() const { return rep_->tag == NodeType::kString; };
  bool IsBoolean() const {
    return rep_->tag == NodeType::kTrue || rep_->tag == NodeType::kFalse;
  };
  bool IsNull() const { return rep_->tag == NodeType::kNull; }
  bool IsNumeric() const { return rep_->tag == NodeType::kNumeric; }
  bool IsEmpty() const {
    if (IsObject()) return GetObjectSize() == 0;
    if (IsArray()) return GetArraySize() == 0;
    return false;
  }

  static absl::StatusOr<PgJsonbValue> Parse(
      absl::string_view jsonb,
      std::vector<std::unique_ptr<TreeNode>>* tree_nodes);

  // Returns a new PgJsonbValue representing an empty array.
  static PgJsonbValue CreateEmptyArray(
      std::vector<std::unique_ptr<TreeNode>>* tree_nodes);

  // Returns a new PgJsonbValue representing an empty object.
  static PgJsonbValue CreateEmptyObject(
      std::vector<std::unique_ptr<TreeNode>>* tree_nodes);

  // Returns a string view of a JSONB string value.
  // Requires IsString() to be true.
  std::string GetString() const;

  // Returns a serialized string view of a JSONB string value with two
  // additional double quotation marks.
  // Requires IsString() to be true.
  absl::string_view GetSerializedString() const;

  // Returns a string view of a JSONB numeric value.
  // Requires IsNumeric() to be true.
  absl::string_view GetNumeric() const;

  // Returns the serialized jsonb represented by the PgJsonbValue.
  absl::Cord Serialize() const;

  // Returns true if the PgJsonbValue is an object and has a member with the
  // given 'key', otherwise returns false.
  bool HasMember(absl::string_view key) const;

  // Creates a new member with the given 'key' with a null value. If the key
  // already exists or is an invalid jsonb string, return an error.
  //
  // Preconditions: IsObject() must be true.
  absl::Status CreateMemberIfNotExists(absl::string_view key);

  // Returns the member corresponding to the given 'key' if it exists.
  // If such 'key' does not exist or if the PgJsonbValue is not an object,
  // returns std::nullopt.
  std::optional<PgJsonbValue> GetMemberIfExists(absl::string_view key) const;

  // Returns all the key/value pairs if the PgJsonbValue is an object.
  // Preconditions: Either IsObject() or IsNull() must be true.
  std::vector<std::pair<absl::string_view, PgJsonbValue>> GetMembers() const;

  // Returns all the keys if the PgJsonbValue is an object.
  // Preconditions: Either IsObject() or IsNull() must be true.
  std::vector<std::string> GetKeys() const;

  // Returns true if the `text` string exist as a top-level key, array element
  // within the JSON value, or simply equals to the JSONB scalar string.
  // This semantics is consistent with PG.JSONB's ? operator.
  bool Exists(absl::string_view text) const;

  // Removes the member corresponding to the given 'key' if it exists and the
  // PgJsonbValue is an object. Returns true if the member exists, false
  // otherwise.
  //
  // Preconditions: IsObject() must be true.
  bool RemoveMember(absl::string_view key);

  // Returns the number of members in this JSONB object.
  //
  // Preconditions: IsObject() must be true.
  size_t GetObjectSize() const;

  // Returns the number of elements in the PgJsonbValue if it is an array.
  //
  // Preconditions: IsArray() must be true.
  int64_t GetArraySize() const;

  // Returns the element at 'index' if the PgJsonbValue is an array. If the
  // index is out of range return a nullopt. Negative indices are also accepted.
  // Negative index n will map to the index n + GetArraySize()
  std::optional<PgJsonbValue> GetArrayElementIfExists(int64_t index) const;

  // Returns all the elements of the PgJsonbValue if it is an array.
  //
  // Preconditions: IsArray() must be true.
  std::vector<PgJsonbValue> GetArrayElements() const;

  // Returns all the elements of a PgJsonbValue in serialized format if it is
  // an array. Returns an error if the PgJsonbValue is not an array.
  absl::StatusOr<std::vector<absl::Cord>> GetSerializedArrayElements() const;

  // Inserts the given `jsonb_value` into the array at `index`. For out of range
  // negative value inserts a null at the beginning and for an out of range
  // positive value inserts a null at the end. Returns an error if the insertion
  // would result in an oversized jsonb array.
  //
  // Preconditions: IsArray() must be true.
  absl::Status InsertArrayElement(const PgJsonbValue& jsonb_value,
                                  int64_t index);

  // If the JSONB value being referenced is an array, removes the element at
  // 'index' and returns true. If 'index' is not in range of the array, does
  // nothing and returns false.
  // returns false.
  //
  // Precondition: IsArray() must be true
  bool RemoveArrayElement(int64_t index);

  // If the JSONB value being referenced is an object, removes all the null
  // members on the top level. Otherwise, does nothing.
  void CleanUpJsonbObject();

  // Converts a path element to a jsonb array index. If the path element is not
  // an integer or is out of range of an int32_t returns an error.
  absl::StatusOr<int32_t> PathElementToIndex(absl::string_view path_element,
                                             int pos) const;

  // Modes to control the behavior of FindAtPath.
  enum FindAtPathMode : uint8_t {
    kDefault = 0,
    // Return null rather than throwing an error if the path element is a string
    // but the parent is an array. This is used by JSONB path extraction.
    kIgnoreStringPathOnArrayError = 1,
  };

  // Returns the PgJsonbValue corresponding to the given path. The path is
  // represented as a vector of strings. Each string represents either an array
  // index or an object key.
  //
  // If we are at an array, we first try to convert the string to an integer
  // index. If it is not an integer, return an error (or null if the mode is set
  // to kIgnoreStringPathOnArrayError). If the resulting index is out of range,
  // we return std::nullopt. Otherwise continue down the path at the value of
  // the index.
  //
  // If we are at an object, we directly use the string as a key and continue
  // down the path at the member if it exists, otherwise return std::nullopt.
  //
  // If we are at a scalar, we return std::nullopt.
  //
  // jsonb = {‘a’: {‘b’: [‘hello’, [1,2,3]]}, ‘c’: [‘apple’, {‘d’: 5}]}
  // jsonb.FindRootPath({‘a’, ‘b’})
  // // [‘hello’, 1,2,3]
  //
  // jsonb.FindRootPath({‘a’, ‘b’, ‘1’, '0'})
  // // '1'
  //
  // jsonb.FindRootPath({‘a’, ‘b’, '0', ‘a’})
  // // null_opt
  //
  // jsonb.FindRootPath(jsonb, {‘a’, ‘b’, 'c'})
  // // path element at position 2 is not an integer: "c"
  absl::StatusOr<std::optional<PgJsonbValue>> FindAtPath(
      absl::Span<const std::string> path, FindAtPathMode mode = kDefault) const;

  // Set current PgJsonbValue to a new value. It is necessary that the same
  // tree_nodes_ vector is maintained for the lifetime of all PgJsonbValues.
  void SetValue(const PgJsonbValue& value);

 private:
  absl::string_view NodeTypeToString(NodeType type) const {
    switch (type) {
      case NodeType::kFalse:
      case NodeType::kTrue:
        return "scalar";
      case NodeType::kNull:
        return "null";
      case NodeType::kArray:
        return "array";
      case NodeType::kObject:
        return "object";
      case NodeType::kNumeric:
        return "numeric";
      case NodeType::kString:
        return "string";
    }
  }

  // Checks that the internal representation of the PgJsonbValue is one of the
  // expected node types. This check is used when the caller is expected to
  // verify the type of PgJsonbValue before calling a method.
  void CheckRepresentation(std::vector<NodeType> types) const;

  absl::Cord PrintTreeNode(TreeNode* node) const;

  void AddToPrintingStack(std::vector<PrintingStackNode>& printing_stack,
                          std::string* output, TreeNode* node) const;

  TreeNode* CreateTreeNode(NodeType tag);

  TreeNode* rep_;
  // unowned.
  std::vector<std::unique_ptr<TreeNode>>* tree_nodes_;
  uint32_t max_depth_estimate_;
  uint32_t total_bytes_estimate_;
};

// Checks that `json` is a valid JSON document and converts it into Spanner
// normalized representation.
absl::StatusOr<absl::Cord> ParseJsonb(absl::string_view json);

// Normalizes a string to conform to PG.JSONB's normalization rules, for
// dealing with escaping characters and rejecting \u0000.
std::string NormalizeJsonbString(absl::string_view value);

// Checks if an input Jsonb string contains unicode 0 (\0).
bool IsValidJsonbString(absl::string_view str);

}  // namespace postgres_translator::spangres::datatypes::common::jsonb

#endif  // DATATYPES_COMMON_JSONB_JSONB_VALUE_H_
