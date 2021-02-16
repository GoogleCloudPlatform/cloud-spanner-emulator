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

#include "backend/common/graph_dependency_helper.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {
using ::zetasql_base::testing::StatusIs;

absl::string_view IdentityFunction(const absl::string_view& str) { return str; }

using GraphDependencyHelperString =
    GraphDependencyHelper<absl::string_view, IdentityFunction>;

TEST(GraphDependencyHelperTest, SimpleNoCycle) {
  absl::string_view a = "A";
  absl::string_view b = "B";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(b));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, b));
  ZETASQL_EXPECT_OK(g.DetectCycle());
  std::vector<absl::string_view> topological_order;
  ZETASQL_EXPECT_OK(g.TopologicalOrder(&topological_order));
  EXPECT_THAT(topological_order, testing::ElementsAre(b, a));
}

// A -> B -> C<-|
//       \-> D--|
TEST(GraphDependencyHelperTest, ComplexNoCycle) {
  absl::string_view a = "A";
  absl::string_view b = "B";
  absl::string_view c = "C";
  absl::string_view d = "D";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(d));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(c));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(b));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));

  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, b));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(b, c));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(b, d));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(d, c));

  ZETASQL_EXPECT_OK(g.DetectCycle());
  std::vector<absl::string_view> topological_order;
  ZETASQL_EXPECT_OK(g.TopologicalOrder(&topological_order));
  EXPECT_THAT(topological_order, testing::ElementsAre(c, d, b, a));
}

// A --> B    C
// ^----------|
TEST(GraphDependencyHelperTest, StartingFromNonRoot) {
  absl::string_view a = "A";
  absl::string_view b = "B";
  absl::string_view c = "C";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(b));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(c));

  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, b));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(c, a));

  std::vector<absl::string_view> topological_order;
  ZETASQL_EXPECT_OK(g.TopologicalOrder(&topological_order));
  EXPECT_THAT(topological_order, testing::ElementsAre(b, a, c));
}

// A -> B -> C
// D -> E
// F
TEST(GraphDependencyHelperTest, MultipleComponantsNoCycle) {
  absl::string_view a = "A";
  absl::string_view b = "B";
  absl::string_view c = "C";
  absl::string_view d = "D";
  absl::string_view e = "E";
  absl::string_view f = "F";

  GraphDependencyHelperString g("column");
  // Nodes are inserted in an odd looking order to test insert order
  // stability of the topological sort.
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(d));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(b));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(c));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(e));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(f));

  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, b));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(b, c));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(d, e));

  ZETASQL_EXPECT_OK(g.DetectCycle());
  std::vector<absl::string_view> topological_order;
  ZETASQL_EXPECT_OK(g.TopologicalOrder(&topological_order));
  // "D" is inserted first. The topological order uses the node insertion order.
  EXPECT_THAT(topological_order, testing::ElementsAre(e, d, c, b, a, f));
}

// A -> D <- C
// B ---^
TEST(GraphDependencyHelperTest, MultipleRootNodesNoCycle) {
  absl::string_view a = "A";
  absl::string_view b = "B";
  absl::string_view c = "C";
  absl::string_view d = "D";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(b));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(c));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(d));

  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(b, d));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(c, d));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, d));

  ZETASQL_EXPECT_OK(g.DetectCycle());
  std::vector<absl::string_view> topological_order;
  ZETASQL_EXPECT_OK(g.TopologicalOrder(&topological_order));
  // "A" is inserted first. The topological order uses the node insertion order.
  EXPECT_THAT(topological_order, testing::ElementsAre(d, a, b, c));
}

// A -v
// ^- B
TEST(GraphDependencyHelperTest, SimpleCycle) {
  absl::string_view a = "A";
  absl::string_view b = "B";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));  // OK to add twice
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(b));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, b));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, b));  // OK to add twice
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(b, a));
  EXPECT_THAT(g.DetectCycle(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "Cycle detected while analysing column, "
                       "which include objects (A,B)"));

  std::vector<absl::string_view> topological_order;
  EXPECT_THAT(g.TopologicalOrder(&topological_order),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "Cycle detected while analysing column, "
                       "which include objects (A,B)"));
}

// A -\
// |  |
// \--/
TEST(GraphDependencyHelperTest, SelfCycle) {
  absl::string_view a = "A";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, a));
  EXPECT_THAT(g.DetectCycle(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "Cycle detected while analysing column, "
                       "which include objects (A)"));
}

// The "10" graph.
// A  C--> D
// |  ^    |
// v  |    v
// B  \--- E
TEST(GraphDependencyHelperTest, MultiComponantWithCycle) {
  absl::string_view a = "A";
  absl::string_view b = "B";
  absl::string_view c = "C";
  absl::string_view d = "D";
  absl::string_view e = "E";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(b));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(c));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(d));
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(e));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(a, b));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(c, d));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(d, e));
  ZETASQL_EXPECT_OK(g.AddEdgeIfNotExists(e, c));
  EXPECT_THAT(g.DetectCycle(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "Cycle detected while analysing column, "
                       "which include objects (C,D,E)"));
  std::vector<absl::string_view> topological_order;
  EXPECT_THAT(g.TopologicalOrder(&topological_order),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "Cycle detected while analysing column, "
                       "which include objects (C,D,E)"));
}

// Error condition
TEST(GraphDependencyHelperTest, AddEdgeIfNotExistsWithoutNodeFrom) {
  absl::string_view a = "A";
  absl::string_view b = "B";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(b));
  EXPECT_FALSE(g.AddEdgeIfNotExists(a, b).ok());
}

// Error condition
TEST(GraphDependencyHelperTest, AddEdgeIfNotExistsWithoutNodeTo) {
  absl::string_view a = "A";
  absl::string_view b = "B";

  GraphDependencyHelperString g("column");
  ZETASQL_EXPECT_OK(g.AddNodeIfNotExists(a));
  EXPECT_FALSE(g.AddEdgeIfNotExists(a, b).ok());
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
