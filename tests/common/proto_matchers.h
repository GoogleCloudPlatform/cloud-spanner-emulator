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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_PROTO_MATCHERS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_PROTO_MATCHERS_H_

#include <memory>
#include <ostream>
#include <string>

#include "zetasql/base/logging.h"
#include "google/protobuf/message.h"
#include "google/protobuf/util/field_comparator.h"
#include "google/protobuf/util/message_differencer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/string_view.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

// Parses asciipb into a proto object using return type deduction.
#define PARSE_TEXT_PROTO(asciipb) \
  ::google::spanner::emulator::test::internal::ParseProtoHelper(asciipb)

namespace internal {

// Parses asciipb into a proto object.
bool ParsePartialFromAscii(const std::string& asciipb, google::protobuf::Message* proto,
                           std::string* error_text);

// Helper class which uses return type deduction to parse a text proto.
class ParseProtoHelper {
 public:
  ParseProtoHelper(absl::string_view asciipb) : asciipb_(asciipb) {}

  template <class ProtoT>
  operator ProtoT() {
    ProtoT proto;
    std::string error_text;
    ZETASQL_CHECK(ParsePartialFromAscii(asciipb_, &proto, &error_text))  // Crash ok.
        << "Failed to parse \"" << asciipb_ << "\" as a "
        << proto.GetDescriptor()->full_name() << ":\n"
        << error_text;
    return proto;
  }

 private:
  const std::string asciipb_;
};

// Options for comparing two protobufs.
struct ProtoComparison {
  ProtoComparison()
      : field_comp(google::protobuf::util::MessageDifferencer::EQUAL),
        repeated_field_comp(google::protobuf::util::MessageDifferencer::AS_LIST),
        scope(google::protobuf::util::MessageDifferencer::FULL) {}

  google::protobuf::util::MessageDifferencer::MessageFieldComparison field_comp;
  google::protobuf::util::MessageDifferencer::RepeatedFieldComparison repeated_field_comp;
  google::protobuf::util::MessageDifferencer::Scope scope;
};

// Base class for text and proto matchers.
class ProtoMatcherBase {
 public:
  ProtoMatcherBase(const ProtoComparison& comp) : comp_(comp) {}
  virtual ~ProtoMatcherBase() {}

  // Interface for subclasses.
  virtual const google::protobuf::Message* CreateExpectedProto(
      const google::protobuf::Message& arg,
      ::testing::MatchResultListener* listener) const = 0;
  virtual void DeleteExpectedProto(const google::protobuf::Message* expected) const = 0;
  virtual void PrintExpectedTo(::std::ostream* os) const = 0;

  // Accessors.
  const ProtoComparison& comp() const { return comp_; }

  // Configuration methods.
  void SetCompareRepeatedFieldsIgnoringOrdering() {
    comp_.repeated_field_comp = google::protobuf::util::MessageDifferencer::AS_SET;
  }
  void SetComparePartially() {
    comp_.scope = google::protobuf::util::MessageDifferencer::PARTIAL;
  }

  // Explanation methods.
  bool MatchAndExplain(const google::protobuf::Message& arg,
                       ::testing::MatchResultListener* listener) const;

  void DescribeRelationToExpectedProto(::std::ostream* os) const;
  void DescribeTo(::std::ostream* os) const;
  void DescribeNegationTo(::std::ostream* os) const;

 private:
  ProtoComparison comp_;
};

// Returns a copy of the given proto message.
inline google::protobuf::Message* CloneProto(const google::protobuf::Message& src) {
  google::protobuf::Message* clone = src.New();
  clone->CopyFrom(src);
  return clone;
}

// Matcher for EqualsProto when the actual argument is a proto object.
class ProtoMatcher : public ProtoMatcherBase {
 public:
  ProtoMatcher(const google::protobuf::Message& expected, const ProtoComparison& comp)
      : ProtoMatcherBase(comp), expected_(CloneProto(expected)) {}

  // Overrides for ProtoMatcherBase.
  void PrintExpectedTo(::std::ostream* os) const override {
    *os << expected_->GetDescriptor()->full_name() << " ";
    ::testing::internal::UniversalPrint(*expected_, os);
  }
  const google::protobuf::Message* CreateExpectedProto(
      const google::protobuf::Message& arg,
      ::testing::MatchResultListener* listener) const override {
    return expected_.get();
  }
  void DeleteExpectedProto(const google::protobuf::Message* expected) const override {}

 private:
  const std::shared_ptr<const google::protobuf::Message> expected_;
};

// Matcher for EqualsProto when the actual argument is a string object.
class ProtoStringMatcher : public ProtoMatcherBase {
 public:
  ProtoStringMatcher(const std::string& expected, const ProtoComparison comp)
      : ProtoMatcherBase(comp), expected_(expected) {}

  // Overrides for ProtoMatcherBase.
  const google::protobuf::Message* CreateExpectedProto(
      const google::protobuf::Message& arg,
      ::testing::MatchResultListener* listener) const override {
    google::protobuf::Message* expected_proto = arg.New();
    std::string error_text;
    if (ParsePartialFromAscii(expected_, expected_proto, &error_text)) {
      return expected_proto;
    } else {
      delete expected_proto;
      if (listener->IsInterested()) {
        *listener << "where ";
        PrintExpectedTo(listener->stream());
        *listener << " doesn't parse as a " << arg.GetDescriptor()->full_name()
                  << ":\n"
                  << error_text;
      }
      return nullptr;
    }
  }
  void DeleteExpectedProto(const google::protobuf::Message* expected) const override {
    delete expected;
  }
  void PrintExpectedTo(::std::ostream* os) const override {
    *os << "<" << expected_ << ">";
  }

 private:
  const std::string expected_;
};

}  // namespace internal

// Matcher for protos when the actual proto is a proto object.
inline ::testing::PolymorphicMatcher<internal::ProtoMatcher> EqualsProto(
    const google::protobuf::Message& x) {
  internal::ProtoComparison comp;
  comp.field_comp = google::protobuf::util::MessageDifferencer::EQUAL;
  return ::testing::MakePolymorphicMatcher(internal::ProtoMatcher(x, comp));
}

// Matcher for protos when the actual proto is in text format.
inline ::testing::PolymorphicMatcher<internal::ProtoStringMatcher> EqualsProto(
    const std::string& x) {
  internal::ProtoComparison comp;
  comp.field_comp = google::protobuf::util::MessageDifferencer::EQUAL;
  return ::testing::MakePolymorphicMatcher(
      internal::ProtoStringMatcher(x, comp));
}

namespace proto {

// Modifies the proto matcher to ignore repeated field ordering.
template <class InnerProtoMatcher>
inline InnerProtoMatcher IgnoringRepeatedFieldOrdering(
    InnerProtoMatcher inner_proto_matcher) {
  inner_proto_matcher.mutable_impl().SetCompareRepeatedFieldsIgnoringOrdering();
  return inner_proto_matcher;
}

// Modifies the proto matcher to ignore fields not present in the actual proto.
template <class InnerProtoMatcher>
inline InnerProtoMatcher Partially(InnerProtoMatcher inner_proto_matcher) {
  inner_proto_matcher.mutable_impl().SetComparePartially();
  return inner_proto_matcher;
}

}  // namespace proto

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_PROTO_MATCHERS_H_
