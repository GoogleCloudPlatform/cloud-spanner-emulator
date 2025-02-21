#ifndef SRC_SPANGRES_MEMORY_H_
#define SRC_SPANGRES_MEMORY_H_

#include <stddef.h>
// All PostgreSQL calls to malloc/realloc should be redirected here instead.
// Direct calls to free are permitted since we release the memory reservation in
// bulk at the end of AST translation.
//
// This file is to be included in PostgreSQL (c code) files that do allocations.

#ifdef __cplusplus
extern "C" {
#endif

void* reserved_alloc(size_t size);
void* reserved_realloc(void* block, size_t old_size,
                                  size_t new_size);

#ifdef __cplusplus
}
#endif

#endif  // SRC_SPANGRES_MEMORY_H_
