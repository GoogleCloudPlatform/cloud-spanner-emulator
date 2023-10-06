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

#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type_deserializer.h"

#include <vector>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_deserializer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"

namespace postgres_translator::spangres {
namespace datatypes {

namespace {

zetasql::TypeProto MakeExtendedTypeProto(absl::string_view type_name) {
  zetasql::TypeProto type;
  type.set_type_kind(zetasql::TypeKind::TYPE_EXTENDED);
  type.set_extended_type_name(type_name);
  return type;
}

zetasql::TypeFactory* GetTypeFactory() {
  static zetasql::TypeFactory* factory = new zetasql::TypeFactory(
      zetasql::TypeFactoryOptions().IgnoreValueLifeCycle());
  return factory;
}

TEST(ExtendedTypeDeserializer, DeserializeTypes) {
  SpannerExtendedTypeDeserializer extended_type_deserializer;
  zetasql::TypeDeserializer dummy_type_deserializer(GetTypeFactory());
  std::vector<const zetasql::Type*> types{
                                            GetPgJsonbType(),
                                            };

  for (const zetasql::Type* type : types) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto deserialized_type,
        extended_type_deserializer.Deserialize(
            MakeExtendedTypeProto(type->TypeName(zetasql::PRODUCT_INTERNAL)),
            dummy_type_deserializer));
    EXPECT_EQ(deserialized_type, type);
  }
}

}  // namespace

}  // namespace datatypes
}  // namespace postgres_translator::spangres

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
