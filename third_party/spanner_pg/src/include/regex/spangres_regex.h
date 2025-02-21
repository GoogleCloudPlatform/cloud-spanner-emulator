#ifndef SRC_INCLUDE_REGEX_SPANGRES_REGEX_H_
#define SRC_INCLUDE_REGEX_SPANGRES_REGEX_H_

// SPANGRES BEGIN

// This file define Spangres specific functions for interacting with the
// PostgreSQL regex library. We include these functions here instead of adding
// them in the regex.h file, because structs and methods defined in the latter
// conflict with the system POSIX <regex.h> header file.

// Frees the memory for thread-local compiled regex cache `re_array`
// on this thread.
extern void CleanupCompiledRegexCache();

// SPANGRES END

#endif  // SRC_INCLUDE_REGEX_SPANGRES_REGEX_H_
