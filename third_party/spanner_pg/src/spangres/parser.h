#ifndef SRC_SPANGRES_PARSER_H_
#define SRC_SPANGRES_PARSER_H_

// This file is included by both Google C++ code and PostgreSQL C code.
#ifdef __cplusplus
#include "absl/container/flat_hash_map.h"
#include "absl/types/optional.h"

// Simple scratch space for the PG Flex scanner as modified by Spangres to log
// start and end locations of tokens
struct SpangresTokenLocations {
  absl::optional<int> last_start_location;
  absl::flat_hash_map<int, int> start_end_pairs;
};
#else
struct SpangresSpangresTokenLocations;
#endif

#endif  // SRC_SPANGRES_PARSER_H_
