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

#ifndef SHIMS_MEMORY_SHIM_CC_H_
#define SHIMS_MEMORY_SHIM_CC_H_

#include <memory>

#include "third_party/spanner_pg/interface/memory_reservation_manager.h"

// This file forward-declares C++-specific symbols from `memory_shim.cc`

namespace postgres_translator {

// Memory reservation used by the current run of the PostgreSQL Analyzer.
//
// PostgreSQL's analyzer could reasonably be imagined as a class that has
// a whole bunch of functions operating on a bunch of shared objects
// that you could think of as private variables.
//
// This mental model has one key problem:  PostgreSQL is written in C.
// C doesn't have classes.  As a corrollary, it has no private variables.
//
// PostgreSQL could put this state on a "context" struct and pass that
// struct around.  However, it doesn't do this.  Instead, it puts them into
// global (now thread-local as we make their code thread-safe) variables.
// These variables must always be set up before the analyzer is called,
// and cleared after it returns and before it is called again on a
// clean new set of objects.
//
// Given all of the context above, `thread_memory_reservation` can best be
// imagined as a "private member variable" of the PostgreSQL Analyzer.
// It tracks the memory reservation manager that the Analyzer uses to
// determine if/when it runs out of memory and can no longer allocate
// new objects.
extern thread_local postgres_translator::interfaces::MemoryReservationManager*
    thread_memory_reservation;

}  // namespace postgres_translator

#endif  // SHIMS_MEMORY_SHIM_CC_H_
