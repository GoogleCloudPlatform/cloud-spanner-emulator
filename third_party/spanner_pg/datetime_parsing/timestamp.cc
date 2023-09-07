/*-------------------------------------------------------------------------
 *
 * timestamp.cc
 *	  Functions for the built-in SQL types "timestamp" and "interval".
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  datetime_parsing/timestamp.cc
 *
 *-------------------------------------------------------------------------
 */

#include "third_party/spanner_pg/datetime_parsing/timestamp.h"

#include <limits.h>
#include <stddef.h>

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datetime_parsing/datetime.h"
#include "third_party/spanner_pg/datetime_parsing/datetime_exports.h"
#include "third_party/spanner_pg/datetime_parsing/pgtime.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp_macros.h"
#include "zetasql/base/ret_check.h"

// SPANNER_PG: copied from src/backend/utils/adt/timestamp.c

static TimeOffset time2t(const int hour, const int min, const int sec, const fsec_t fsec);
static Timestamp dt2local(Timestamp dt, int tz);

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/


/* timestamptz_in()
 * Convert a string to internal form.
 */
absl::StatusOr<TimestampTz>
timestamptz_in(absl::string_view input_string,
		absl::string_view default_timezone)
{
	TimestampTz result;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + MAXDATEFIELDS];
	std::string error;

	dterr = ParseDateTime(input_string, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeDateTime(
				field, ftype, nf, &dtype, tm, &fsec, &tz, default_timezone, &error);
	if (dterr != 0) {
		if (!error.empty())
			return absl::InvalidArgumentError(
				absl::StrFormat("%s: \"%s\"", error, input_string));
		else
			return DateTimeParseError(dterr, input_string, "timestamp with time zone");
	}

	switch (dtype)
	{
		case DTK_DATE:
			if (tm2timestamp(tm, fsec, &tz, &result) != 0)
				return absl::InvalidArgumentError(
						absl::StrFormat("timestamp out of range: \"%s\"", input_string));
			break;

		case DTK_EPOCH:
			/* SPANNER_PG: remove support for epoch */
			return absl::InvalidArgumentError("Epoch is not supported");
			break;

		case DTK_LATE:
		case DTK_EARLY:
			/* SPANNER_PG: remove support for infinity and -infinity */
			return absl::InvalidArgumentError("Infinity and -infinity are not supported");

		default:
			return absl::InvalidArgumentError(
				"invalid input syntax for type timestamp with time zone");
	}

	return result;
}

absl::Status interval_in(absl::string_view input_string, Interval* result)
{
	ZETASQL_RET_CHECK(result);
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			dtype;
	int			nf;
	int			range;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[256];

	tm->tm_year = 0;
	tm->tm_mon = 0;
	tm->tm_mday = 0;
	tm->tm_hour = 0;
	tm->tm_min = 0;
	tm->tm_sec = 0;
	fsec = 0;

	// Typmod is not supported for Interval in Spanner PG, so the range is
	// the full range.
        range = INTERVAL_FULL_RANGE;

	dterr = ParseDateTime(input_string, workbuf, sizeof(workbuf), field,
						  ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		 dterr = DecodeInterval(field, ftype, nf, range,
		 					   &dtype, tm, &fsec);

	/* if those functions think it's a bad format, try ISO8601 style */
	/*
	 * SPANNER_PG: cast the const char* to char* because strtod requires a
	 * non-const pointer. The string is not modified so this is safe.
	 */
	char* non_const_input = const_cast<char*>(input_string.data());
	if (dterr == DTERR_BAD_FORMAT)
		dterr = DecodeISO8601Interval(non_const_input,
						&dtype, tm, &fsec);

	if (dterr != 0)
	{
		if (dterr == DTERR_FIELD_OVERFLOW)
			dterr = DTERR_INTERVAL_OVERFLOW;
		return DateTimeParseError(dterr, input_string, "interval");
	}

	switch (dtype)
	{
		case DTK_DELTA:
			if (tm2interval(tm, fsec, result) != 0)
                          return absl::InvalidArgumentError("interval out of range");
			break;

		default:
                  return absl::InvalidArgumentError(
                      absl::StrFormat("unexpected dtype %d while parsing interval \"%s\"",
                                      dtype, input_string));
	}
        return absl::OkStatus();
}

void
dt2time(Timestamp jd, int *hour, int *min, int *sec, fsec_t *fsec)
{
	TimeOffset	time;

	time = jd;

	*hour = time / USECS_PER_HOUR;
	time -= (*hour) * USECS_PER_HOUR;
	*min = time / USECS_PER_MINUTE;
	time -= (*min) * USECS_PER_MINUTE;
	*sec = time / USECS_PER_SEC;
	*fsec = time - (*sec * USECS_PER_SEC);
}								/* dt2time() */

/* tm2timestamp()
 * Convert a tm structure to a timestamp data type.
 * Note that year is _not_ 1900-based, but is an explicit full value.
 * Also, month is one-based, _not_ zero-based.
 *
 * Returns -1 on failure (value out of range).
 */
int
tm2timestamp(struct pg_tm *tm, fsec_t fsec, int *tzp, Timestamp *result)
{
	TimeOffset	date;
	TimeOffset	time;

	/* Prevent overflow in Julian-day routines */
	if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;
	time = time2t(tm->tm_hour, tm->tm_min, tm->tm_sec, fsec);
	/* SPANGRES BEGIN*/
	/* checks for major overflow without overflowing so sanitizers don't get
	* mad */
	if ((LLONG_MAX - time)/ USECS_PER_DAY < date) {
		*result = 0;
		return -1;
	}
	/* SPANGRES END*/
	*result = date * USECS_PER_DAY + time;
	/* check for just-barely overflow (okay except time-of-day wraps) */
	/* caution: we want to allow 1999-12-31 24:00:00 */
	if ((*result < 0 && date > 0) ||
		(*result > 0 && date < -1))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}
	if (tzp != NULL)
		*result = dt2local(*result, -(*tzp));

	/* final range check catches just-out-of-range timestamps */
	if (!IS_VALID_TIMESTAMP(*result))
	{
		*result = 0;			/* keep compiler quiet */
		return -1;
	}

	return 0;
}

static TimeOffset
time2t(const int hour, const int min, const int sec, const fsec_t fsec)
{
	return (((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec) * USECS_PER_SEC) + fsec;
}

static Timestamp
dt2local(Timestamp dt, int tz)
{
	dt -= (tz * USECS_PER_SEC);
	return dt;
}

int
tm2interval(struct pg_tm *tm, fsec_t fsec, Interval *span)
{
	double		total_months = (double) tm->tm_year * MONTHS_PER_YEAR + tm->tm_mon;

	if (total_months > INT_MAX || total_months < INT_MIN)
		return -1;
	span->month = total_months;
	span->day = tm->tm_mday;
	span->time = (((((tm->tm_hour * INT64CONST(60)) +
					 tm->tm_min) * INT64CONST(60)) +
				   tm->tm_sec) * USECS_PER_SEC) + fsec;

	return 0;
}
