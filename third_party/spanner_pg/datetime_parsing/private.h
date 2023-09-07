/* Private header for tzdb code.  */

#ifndef DATETIME_PARSING_PRIVATE_H
#define DATETIME_PARSING_PRIVATE_H

/*
 * This file is in the public domain, so clarified as of
 * 1996-06-05 by Arthur David Olson.
 *
 * IDENTIFICATION
 *	  src/timezone/private.h
 */

/*
 * This header is for use ONLY with the time conversion code.
 * There is no guarantee that it will remain unchanged,
 * or that it will remain at all.
 * Do NOT copy it to any system include directory.
 * Thank you!
 */

#include <limits.h>

#include "third_party/spanner_pg/datetime_parsing/pgtime.h"

// SPANNER_PG: copied from src/timezone/private.h

/* Unlike <ctype.h>'s isdigit, this also works if c < 0 | c > UCHAR_MAX. */
#define is_digit(c) ((unsigned)(c) - '0' <= 9)

/*
 * Finally, some convenience items.
 */

#define TYPE_BIT(type)	(sizeof (type) * CHAR_BIT)
#define TYPE_SIGNED(type) (((type) -1) < 0)
#define TWOS_COMPLEMENT(t) ((t) ~ (t) 0 < 0)

/*
 * Max and min values of the integer type T, of which only the bottom
 * B bits are used, and where the highest-order used bit is considered
 * to be a sign bit if T is signed.
 */
#define MAXVAL(t, b)						\
  ((t) (((t) 1 << ((b) - 1 - TYPE_SIGNED(t)))			\
	- 1 + ((t) 1 << ((b) - 1 - TYPE_SIGNED(t)))))
#define MINVAL(t, b)						\
  ((t) (TYPE_SIGNED(t) ? - TWOS_COMPLEMENT(t) - MAXVAL(t, b) : 0))

/* The extreme time values, assuming no padding.  */
#define TIME_T_MIN MINVAL(pg_time_t, TYPE_BIT(pg_time_t))
#define TIME_T_MAX MAXVAL(pg_time_t, TYPE_BIT(pg_time_t))

/*
 * INITIALIZE(x)
 */
#define INITIALIZE(x)	((x) = 0)

/* Handy macros that are independent of tzfile implementation.  */

#define YEARSPERREPEAT		400 /* years before a Gregorian repeat */

#define SECSPERMIN	60
#define MINSPERHOUR 60
#define HOURSPERDAY 24
#define DAYSPERWEEK 7
#define DAYSPERNYEAR	365
#define DAYSPERLYEAR	366
#define SECSPERHOUR (SECSPERMIN * MINSPERHOUR)
#define SECSPERDAY	((int32_t) SECSPERHOUR * HOURSPERDAY)
#define MONSPERYEAR 12

#define EPOCH_YEAR	1970

#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))

/*
 * The Gregorian year averages 365.2425 days, which is 31556952 seconds.
 */

#define AVGSECSPERYEAR		31556952L
#define SECSPERREPEAT \
  ((int64_t) YEARSPERREPEAT * (int64_t) AVGSECSPERYEAR)
#define SECSPERREPEAT_BITS	34	/* ceil(log2(SECSPERREPEAT)) */

#endif							/* !defined DATETIME_PARSING_PRIVATE_H */
