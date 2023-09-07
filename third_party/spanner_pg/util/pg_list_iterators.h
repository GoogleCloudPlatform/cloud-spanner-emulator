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

#ifndef TRANSFORMER_PG_LIST_ITERATORS_H_
#define TRANSFORMER_PG_LIST_ITERATORS_H_

#include <algorithm>
#include <cassert>

#include "third_party/spanner_pg/postgres_includes/all.h"

template <typename ListEltType>
class ListIteratorBase {
 public:
  // Define types that this iterator deals with.
  // Expected as part of the STL iterator interface.
  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = ListEltType;
  using difference_type = size_t;
  using pointer = const ListEltType *;
  using reference = ListEltType &;

  ListIteratorBase(const List *lst, const int offset)
      : base_lst_(lst), offset_(offset) {}

  ListIteratorBase<ListEltType> &operator++() {
    ++offset_;
    return *this;
  }

  ListIteratorBase<ListEltType> operator++(int) {
    ListIteratorBase<ListEltType> iter = *this;
    ++(*this);
    return iter;
  }

  bool operator==(const ListIteratorBase<ListEltType> &other) const {
    return base_lst_ == other.base_lst_ && offset_ == other.offset_;
  }

  bool operator!=(const ListIteratorBase<ListEltType> &other) const {
    return !((*this) == other);
  }

 protected:
  const List *base_lst_;
  int offset_;
};

template <typename T>
class StructList {
  /**
   * Usage (for example):
   *
   * {
   *   List *lst = getListOfExprs();
   *   for (Expr *expr : StructList<Expr*>(lst))
   *   {
   *     // (...)
   *   }
   * }
   *
   */

 public:
  StructList(const List *lst) : lst_(lst) {
    assert(!lst || lst->type == T_List);
  }

  class StructListIterator : public ListIteratorBase<T> {
   public:
    using ListIteratorBase<T>::ListIteratorBase;

    T &operator*() const {
      // Jump through some hoops to make C++'s type system happy with
      // returning a typed reference to an untyped field
      void **target = &lfirst(list_nth_cell(this->base_lst_, this->offset_));
      T *target_typed = reinterpret_cast<T *>(target);
      return *target_typed;
    }
  };

  StructListIterator begin() { return StructListIterator(lst_, 0); }
  StructListIterator end() {
    return StructListIterator(lst_, list_length(lst_));
  }

 private:
  const List *lst_;
};

class IntList {
  /**
   * Usage (for example):
   *
   * {
   *   List *lst = getListOfInts();
   *   for (int i : IntList(lst))
   *   {
   *     // (...)
   *   }
   * }
   *
   */

 public:
  IntList(const List *lst) : lst_(lst) {
    assert(!lst || lst->type == T_IntList);
  }

  class IntListIterator : public ListIteratorBase<int> {
   public:
    using ListIteratorBase<int>::ListIteratorBase;

    int &operator*() const {
      return lfirst_int(list_nth_cell(this->base_lst_, this->offset_));
    }
  };

  IntListIterator begin() { return IntListIterator(lst_, 0); }
  IntListIterator end() { return IntListIterator(lst_, list_length(lst_)); }

 private:
  const List *lst_;
};

class OidList {
  /**
   * Usage (for example):
   *
   * {
   *   List *lst = getListOfOids();
   *   for (Oid oid : OidList(lst))
   *   {
   *     // (...)
   *   }
   * }
   *
   */

 public:
  OidList(const List *lst) : lst_(lst) {
    assert(!lst || lst->type == T_OidList);
  }

  class OidListIterator : public ListIteratorBase<Oid> {
   public:
    using ListIteratorBase<Oid>::ListIteratorBase;

    Oid &operator*() const {
      return lfirst_oid(list_nth_cell(this->base_lst_, this->offset_));
    }
  };

  OidListIterator begin() { return OidListIterator(lst_, 0); }
  OidListIterator end() { return OidListIterator(lst_, list_length(lst_)); }

 private:
  const List *lst_;
};

#endif  // TRANSFORMER_PG_LIST_ITERATORS_H_
