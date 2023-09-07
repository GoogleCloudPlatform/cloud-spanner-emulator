/*-------------------------------------------------------------------------
 *
 * date.cc
 *	  implements DATE and TIME data types specified in SQL standard
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  datetime_parsing/date.cc
 *
 *-------------------------------------------------------------------------
 */
#include "third_party/spanner_pg/datetime_parsing/date.h"

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datetime_parsing/datetime.h"
#include "third_party/spanner_pg/datetime_parsing/pgtime.h"

// SPANNER_PG: copied from src/backend/utils/adt/date.c

/*****************************************************************************
 *	 Date ADT
 *****************************************************************************/


/* date_in()
 * Given date text string, convert to internal date format.
 */
absl::StatusOr<DateADT>
date_in(absl::string_view input_string, absl::string_view default_timezone)
{
	DateADT		date;
	fsec_t		fsec;
	struct pg_tm tt,
			   *tm = &tt;
	int			tzp;
	int			dtype;
	int			nf;
	int			dterr;
	char	   *field[MAXDATEFIELDS];
	int			ftype[MAXDATEFIELDS];
	char		workbuf[MAXDATELEN + 1];
        std::string error;

	dterr = ParseDateTime(input_string, workbuf, sizeof(workbuf),
						  field, ftype, MAXDATEFIELDS, &nf);
	if (dterr == 0)
		dterr = DecodeDateTime(
				field, ftype, nf, &dtype, tm, &fsec, &tzp,
				default_timezone, &error);
	if (dterr != 0) {
		if (!error.empty())
			return absl::InvalidArgumentError(
				absl::StrFormat("%s: \"%s\"", error, input_string));
		else
			return DateTimeParseError(dterr, input_string, "date");
	}

	switch (dtype)
	{
		case DTK_DATE:
			break;

		case DTK_EPOCH:
			/* SPANNER_PG: remove support for epoch */
			return absl::InvalidArgumentError("Epoch is not supported");

		case DTK_LATE:
		case DTK_EARLY:
			/* SPANNER_PG: remove support for infinity and -infinity */
			return absl::InvalidArgumentError("Infinity and -infinity are not supported");

		default:
			return DateTimeParseError(DTERR_BAD_FORMAT, input_string, "date");
			break;
	}

	/* Prevent overflow in Julian-day routines */
	if (!IS_VALID_JULIAN(tm->tm_year, tm->tm_mon, tm->tm_mday))
		return absl::InvalidArgumentError(
				absl::StrFormat("date out of range: \"%s\"", input_string));

	date = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) - POSTGRES_EPOCH_JDATE;

	/* Now check for just-out-of-range dates */
	if (!IS_VALID_DATE(date))
		return absl::InvalidArgumentError(
				absl::StrFormat("date out of range: \"%s\"", input_string));

	return date;
}

/* time_overflows()
 * Check to see if a broken-down time-of-day is out of range.
 */
bool
time_overflows(int hour, int min, int sec, fsec_t fsec)
{
	/* Range-check the fields individually. */
	if (hour < 0 || hour > HOURS_PER_DAY ||
		min < 0 || min >= MINS_PER_HOUR ||
		sec < 0 || sec > SECS_PER_MINUTE ||
		fsec < 0 || fsec > USECS_PER_SEC)
		return true;

	/*
	 * Because we allow, eg, hour = 24 or sec = 60, we must check separately
	 * that the total time value doesn't exceed 24:00:00.
	 */
	if ((((((hour * MINS_PER_HOUR + min) * SECS_PER_MINUTE)
		   + sec) * USECS_PER_SEC) + fsec) > USECS_PER_DAY)
		return true;

	return false;
}
