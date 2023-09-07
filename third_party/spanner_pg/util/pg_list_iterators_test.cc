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

#include "third_party/spanner_pg/util/pg_list_iterators.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace {

using postgres_translator::test::ValidMemoryContext;

TEST_F(ValidMemoryContext, StructList) {
  Node *c1 = makeBoolConst(true /* value */, false /* isnull */);
  Node *c2 = makeBoolConst(false /* value */, false /* isnull */);
  Node *c3 = makeBoolConst(false /* value */, true /* isnull */);
  List *lst = list_make3(c1, c2, c3);

  std::vector<Const *> consts;
  for (Const *_const : StructList<Const *>(lst)) {
    consts.push_back(_const);
  }

  ASSERT_EQ(consts.size(), 3);

  EXPECT_EQ(DatumGetBool(consts[0]->constvalue), true);
  EXPECT_EQ(DatumGetBool(consts[1]->constvalue), false);
  EXPECT_EQ(consts[2]->constisnull, true);
}

TEST_F(ValidMemoryContext, IntList) {
  List *lst = list_make3_int(1, 4, 9);

  std::vector<int> ints;
  for (int i : IntList(lst)) {
    ints.push_back(i);
  }

  std::vector<int> expected{1, 4, 9};
  EXPECT_EQ(ints, expected);
}

TEST_F(ValidMemoryContext, OidList) {
  List *lst = list_make3_oid((Oid)1, (Oid)4, (Oid)9);

  std::vector<Oid> oids;
  for (Oid i : OidList(lst)) {
    oids.push_back(i);
  }

  std::vector<Oid> expected{(Oid)1, (Oid)4, (Oid)9};
  EXPECT_EQ(oids, expected);
}

TEST(PgListIteratorsTest, EmptyIntList) {
  for (int i : IntList(nullptr)) {
    EXPECT_TRUE(false) << "Should never get here.  Saw: " << i;
  }

  List lst;
  lst.type = T_IntList;
  lst.length = 0;

  for (int i : IntList(&lst)) {
    EXPECT_TRUE(false) << "Should never get here.  Saw: " << i;
  }
}

TEST(PgListIteratorsTest, EmptyOidList) {
  for (Oid oid : OidList(nullptr)) {
    EXPECT_TRUE(false) << "Should never get here.  Saw: " << oid;
  }

  List lst;
  lst.type = T_OidList;
  lst.length = 0;

  for (Oid oid : OidList(&lst)) {
    EXPECT_TRUE(false) << "Should never get here.  Saw: " << oid;
  }
}

TEST(PgListIteratorsTest, EmptyStructList) {
  for (Const *_const : StructList<Const *>(nullptr)) {
    EXPECT_TRUE(false) << "Should never get here.  Saw: " << _const;
  }

  List lst;
  lst.type = T_List;
  lst.length = 0;

  for (Const *_const : StructList<Const *>(&lst)) {
    EXPECT_TRUE(false) << "Should never get here.  Saw: " << _const;
  }
}

}  // namespace
