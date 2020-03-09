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

#include "tests/common/proto_matchers.h"

#include "google/protobuf/io/tokenizer.h"
#include "google/protobuf/text_format.h"
#include "absl/strings/substitute.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace internal {

class StringErrorCollector : public google::protobuf::io::ErrorCollector {
 public:
  explicit StringErrorCollector(std::string* error_text)
      : error_text_(error_text) {}

  void AddError(int line, int column, const std::string& message) override {
    absl::SubstituteAndAppend(error_text_, "$0($1): $2\n", line, column,
                              message);
  }

  void AddWarning(int line, int column, const std::string& message) override {
    absl::SubstituteAndAppend(error_text_, "$0($1): $2\n", line, column,
                              message);
  }

 private:
  std::string* error_text_;
  StringErrorCollector(const StringErrorCollector&) = delete;
  StringErrorCollector& operator=(const StringErrorCollector&) = delete;
};

bool ParsePartialFromAscii(const std::string& asciipb, google::protobuf::Message* proto,
                           std::string* error_text) {
  google::protobuf::TextFormat::Parser parser;
  StringErrorCollector collector(error_text);
  parser.RecordErrorsTo(&collector);
  parser.AllowPartialMessage(true);
  return parser.ParseFromString(asciipb, proto);
}

bool ProtoComparable(const google::protobuf::Message& p, const google::protobuf::Message& q) {
  return p.GetDescriptor() == q.GetDescriptor();
}

void ConfigureDifferencer(const internal::ProtoComparison& comp,
                          google::protobuf::util::DefaultFieldComparator* comparator,
                          google::protobuf::util::MessageDifferencer* differencer,
                          const google::protobuf::Descriptor* descriptor) {
  differencer->set_message_field_comparison(comp.field_comp);
  differencer->set_scope(comp.scope);
  differencer->set_repeated_field_comparison(comp.repeated_field_comp);
  differencer->set_field_comparator(comparator);
}

bool ProtoCompare(const internal::ProtoComparison& comp,
                  const google::protobuf::Message& actual,
                  const google::protobuf::Message& expected) {
  if (!ProtoComparable(actual, expected)) {
    return false;
  }

  google::protobuf::util::MessageDifferencer differencer;
  google::protobuf::util::DefaultFieldComparator field_comparator;
  ConfigureDifferencer(comp, &field_comparator, &differencer,
                       actual.GetDescriptor());

  return differencer.Compare(expected, actual);
}

std::string DescribeTypes(const google::protobuf::Message& expected,
                          const google::protobuf::Message& actual) {
  return "whose type should be " + expected.GetDescriptor()->full_name() +
         " but actually is " + actual.GetDescriptor()->full_name();
}

std::string DescribeDiff(const internal::ProtoComparison& comp,
                         const google::protobuf::Message& actual,
                         const google::protobuf::Message& expected) {
  google::protobuf::util::MessageDifferencer differencer;
  google::protobuf::util::DefaultFieldComparator field_comparator;
  ConfigureDifferencer(comp, &field_comparator, &differencer,
                       actual.GetDescriptor());

  std::string diff;
  differencer.ReportDifferencesToString(&diff);

  differencer.Compare(expected, actual);
  if (diff.length() > 0 && *(diff.end() - 1) == '\n') {
    diff.erase(diff.end() - 1);
  }

  return "with the difference:\n" + diff;
}

bool ProtoMatcherBase::MatchAndExplain(
    const google::protobuf::Message& arg,
    ::testing::MatchResultListener* listener) const {
  const google::protobuf::Message* const expected = CreateExpectedProto(arg, listener);
  if (expected == nullptr) return false;

  const bool comparable = ProtoComparable(arg, *expected);
  const bool match = comparable && ProtoCompare(comp(), arg, *expected);

  if (listener->IsInterested()) {
    if (!comparable) {
      *listener << DescribeTypes(*expected, arg);
    } else if (!match) {
      *listener << DescribeDiff(comp(), arg, *expected);
    }
  }

  DeleteExpectedProto(expected);
  return match;
}

void ProtoMatcherBase::DescribeRelationToExpectedProto(
    ::std::ostream* os) const {
  if (comp_.repeated_field_comp == google::protobuf::util::MessageDifferencer::AS_LIST) {
    *os << "(ignoring repeated field ordering) ";
  }

  *os << (comp_.scope == google::protobuf::util::MessageDifferencer::PARTIAL
              ? "partially "
              : "")
      << (comp_.field_comp == google::protobuf::util::MessageDifferencer::EQUAL
              ? "equal"
              : "equivalent")
      << " to ";
  PrintExpectedTo(os);
}

void ProtoMatcherBase::DescribeTo(::std::ostream* os) const {
  *os << "is ";
  DescribeRelationToExpectedProto(os);
}

void ProtoMatcherBase::DescribeNegationTo(::std::ostream* os) const {
  *os << "is not ";
  DescribeRelationToExpectedProto(os);
}

}  // namespace internal
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
