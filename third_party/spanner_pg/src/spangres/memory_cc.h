#ifndef SRC_SPANGRES_MEMORY_CC_H_
#define SRC_SPANGRES_MEMORY_CC_H_

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
// C doesn't have classes. As a corollary, it has no private variables.
//
// PostgreSQL could put this state on a "context" struct and pass that
// struct around. However, it doesn't do this. Instead, it puts them into
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

#endif  // SRC_SPANGRES_MEMORY_CC_H_
