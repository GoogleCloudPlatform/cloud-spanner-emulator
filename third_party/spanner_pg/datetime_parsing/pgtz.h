/*-------------------------------------------------------------------------
 *
 * pgtz.h
 *	  Timezone Library Integration Functions
 *
 * Note: this file contains only definitions that are private to the
 * timezone library.  Public definitions are in pgtime.h.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  datetime_parsing/pgtz.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DATETIME_PARSING_PGTZ_H
#define DATETIME_PARSING_PGTZ_H

#include <dirent.h>
#include <stdio.h>

#include <memory>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/datetime_parsing/pgtime.h"
#include "third_party/spanner_pg/datetime_parsing/tzfile.h"

// SPANNER_PG: copied from src/timezone/pgtz.h

// SPANNER_PG: use unique_ptr with a destructor for handling file descriptors for
// directories and files.
using DirPtr = std::unique_ptr<DIR, int (*)(DIR*)>;
using FilePtr = std::unique_ptr<FILE, int (*)(FILE*)>;

#define SMALLEST(a, b)	(((a) < (b)) ? (a) : (b))
#define BIGGEST(a, b)	(((a) > (b)) ? (a) : (b))

struct ttinfo
{								/* time type information */
	int32_t		tt_utoff;		/* UT offset in seconds */
	bool		tt_isdst;		/* used to set tm_isdst */
	int			tt_desigidx;	/* abbreviation list index */
	bool		tt_ttisstd;		/* transition is std time */
	bool		tt_ttisut;		/* transition is UT */
};

struct lsinfo
{								/* leap second information */
	pg_time_t	ls_trans;		/* transition time */
	int64_t		ls_corr;		/* correction to apply */
};

struct state
{
	int			leapcnt;
	int			timecnt;
	int			typecnt;
	int			charcnt;
	bool		goback;
	bool		goahead;
	pg_time_t	ats[TZ_MAX_TIMES];
	unsigned char types[TZ_MAX_TIMES];
	struct ttinfo ttis[TZ_MAX_TYPES];
	char		chars[BIGGEST(BIGGEST(TZ_MAX_CHARS + 1, 4 /* sizeof gmt */ ),
							  (2 * (TZ_STRLEN_MAX + 1)))];
	struct lsinfo lsis[TZ_MAX_LEAPS];

	/*
	 * The time type to use for early times or if no transitions. It is always
	 * zero for recent tzdb releases. It might be nonzero for data from tzdb
	 * 2018e or earlier.
	 */
	int			defaulttype;
};


struct pg_tz
{
	/* TZname contains the canonically-cased name of the timezone */
	char		TZname[TZ_STRLEN_MAX + 1];
	struct state state;
};

/* in pgtz.cc */
absl::StatusOr<FilePtr>	pg_open_tzfile(const char *name, char *canonname);

/* in localtime.cc */
/* SPANNER_PG: return an absl::Status instead of an errno */
absl::Status	tzload(const char *name, char *canonname, struct state *sp,
				   bool doextend);
bool tzparse(const char *name, struct state *sp, bool lastditch);

#endif							/* DATETIME_PARSING_PGTZ_H */
