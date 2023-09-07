/* Convert timestamp from pg_time_t to struct pg_tm.  */

/*
 * This file is in the public domain, so clarified as of
 * 1996-06-05 by Arthur David Olson.
 *
 * IDENTIFICATION
 *	  datetime_parsing/localtime.cc
 */

/*
 * Leap second handling from Bradley White.
 * POSIX-style TZ environment variable handling from Guy Harris.
 */

#include <stdlib.h>
/* this file needs to build in both frontend and backend contexts */
#include <string.h>
#include <sys/types.h>

#include <cstdio>

#include "absl/status/status.h"
#include "third_party/spanner_pg/datetime_parsing/pgtime.h"
#include "third_party/spanner_pg/datetime_parsing/pgtz.h"
#include "third_party/spanner_pg/datetime_parsing/private.h"
#include "third_party/spanner_pg/datetime_parsing/tzfile.h"
#include "zetasql/base/status_macros.h"

// SPANNER_PG: copied from src/timezone/localtime.c

/*
 * The DST rules to use if a POSIX TZ string has no rules.
 * Default to US rules as of 2017-05-07.
 * POSIX does not specify the default DST rules;
 * for historical reasons, US rules are a common default.
 */
#define TZDEFRULESTRING ",M3.2.0,M11.1.0"

/* structs ttinfo, lsinfo, state have been moved to pgtz.h */

enum r_type
{
	JULIAN_DAY,					/* Jn = Julian day */
	DAY_OF_YEAR,				/* n = day of year */
	MONTH_NTH_DAY_OF_WEEK		/* Mm.n.d = month, week, day of week */
};

struct rule
{
	enum r_type r_type;			/* type of rule */
	int			r_day;			/* day number of rule */
	int			r_week;			/* week number of rule */
	int			r_mon;			/* month number of rule */
	int32_t		r_time;			/* transition time of rule */
};

/*
 * Prototypes for static functions.
 */

static bool increment_overflow_time(pg_time_t *, int32_t);
static int64_t leapcorr(struct state const *, pg_time_t);
static bool typesequiv(struct state const *, int, int);

/* Initialize *S to a value based on UTOFF, ISDST, and DESIGIDX.  */
static void
init_ttinfo(struct ttinfo *s, int32_t utoff, bool isdst, int desigidx)
{
	s->tt_utoff = utoff;
	s->tt_isdst = isdst;
	s->tt_desigidx = desigidx;
	s->tt_ttisstd = false;
	s->tt_ttisut = false;
}

static int32_t
detzcode(const char *const codep)
{
	int32_t		result;
	int			i;
	int32_t		one = 1;
	int32_t		halfmaxval = one << (32 - 2);
	int32_t		maxval = halfmaxval - 1 + halfmaxval;
	int32_t		minval = -1 - maxval;

	result = codep[0] & 0x7f;
	for (i = 1; i < 4; ++i)
		result = (result << 8) | (codep[i] & 0xff);

	if (codep[0] & 0x80)
	{
		/*
		 * Do two's-complement negation even on non-two's-complement machines.
		 * If the result would be minval - 1, return minval.
		 */
		result -= !TWOS_COMPLEMENT(int32_t) && result != 0;
		result += minval;
	}
	return result;
}

static int64_t
detzcode64(const char *const codep)
{
	uint64_t		result;
	int			i;
	int64_t		one = 1;
	int64_t		halfmaxval = one << (64 - 2);
	int64_t		maxval = halfmaxval - 1 + halfmaxval;
	int64_t		minval = -TWOS_COMPLEMENT(int64_t) - maxval;

	result = codep[0] & 0x7f;
	for (i = 1; i < 8; ++i)
		result = (result << 8) | (codep[i] & 0xff);

	if (codep[0] & 0x80)
	{
		/*
		 * Do two's-complement negation even on non-two's-complement machines.
		 * If the result would be minval - 1, return minval.
		 */
		result -= !TWOS_COMPLEMENT(int64_t) && result != 0;
		result += minval;
	}
	return result;
}

static bool
differ_by_repeat(const pg_time_t t1, const pg_time_t t0)
{
	if (TYPE_BIT(pg_time_t) - TYPE_SIGNED(pg_time_t) < SECSPERREPEAT_BITS)
		return 0;
	return t1 - t0 == SECSPERREPEAT;
}

/* Input buffer for data read from a compiled tz file.  */
union input_buffer
{
	/* The first part of the buffer, interpreted as a header.  */
	struct tzhead tzhead;

	/* The entire buffer.  */
	char		buf[2 * sizeof(struct tzhead) + 2 * sizeof(struct state)
					+ 4 * TZ_MAX_TIMES];
};

/* Local storage needed for 'tzloadbody'.  */
union local_storage
{
	/* The results of analyzing the file's contents after it is opened.  */
	struct file_analysis
	{
		/* The input buffer.  */
		union input_buffer u;

		/* A temporary state used for parsing a TZ string in the file.  */
		struct state st;
	}			u;

	/* We don't need the "fullname" member */
};

/* Load tz data from the file named NAME into *SP.  Read extended
 * format if DOEXTEND.  Use *LSP for temporary storage.  Return 0 on
 * success, an errno value on failure.
 * PG: If "canonname" is not NULL, then on success the canonical spelling of
 * given name is stored there (the buffer must be > TZ_STRLEN_MAX bytes!).
 *
 * SPANNER_PG: return an absl::Status instead of an errno value.
 */
static absl::Status
tzloadbody(char const *name, char *canonname, struct state *sp, bool doextend,
		   union local_storage *lsp)
{
	int			i;
	int			stored;
	ssize_t		nread;
	union input_buffer *up = &lsp->u.u;
	int			tzheadsize = sizeof(struct tzhead);

	sp->goback = sp->goahead = false;

	if (!name)
	{
		name = TZDEFAULT;
		if (!name)
			return absl::InvalidArgumentError("Invalid timezone name");
	}

	if (name[0] == ':')
		++name;

	{
		/*
		 * SPANNER_PG: use a FilePtr/fread instead of a fd/read
		 * Limit the scope so that the FilePtr is closed ASAP
		 */
		ZETASQL_ASSIGN_OR_RETURN(FilePtr fp, pg_open_tzfile(name, canonname));

		nread = fread(up->buf, 1, sizeof up->buf, fp.get());
	}

	if (nread < tzheadsize)
	{
		return absl::InvalidArgumentError("Failed to load timezone file");
	}
	for (stored = 4; stored <= 8; stored *= 2)
	{
		int32_t		ttisstdcnt = detzcode(up->tzhead.tzh_ttisstdcnt);
		int32_t		ttisutcnt = detzcode(up->tzhead.tzh_ttisutcnt);
		int64_t		prevtr = 0;
		int32_t		prevcorr = 0;
		int32_t		leapcnt = detzcode(up->tzhead.tzh_leapcnt);
		int32_t		timecnt = detzcode(up->tzhead.tzh_timecnt);
		int32_t		typecnt = detzcode(up->tzhead.tzh_typecnt);
		int32_t		charcnt = detzcode(up->tzhead.tzh_charcnt);
		char const *p = up->buf + tzheadsize;

		/*
		 * Although tzfile(5) currently requires typecnt to be nonzero,
		 * support future formats that may allow zero typecnt in files that
		 * have a TZ string and no transitions.
		 */
		if (!(0 <= leapcnt && leapcnt < TZ_MAX_LEAPS
			  && 0 <= typecnt && typecnt < TZ_MAX_TYPES
			  && 0 <= timecnt && timecnt < TZ_MAX_TIMES
			  && 0 <= charcnt && charcnt < TZ_MAX_CHARS
			  && (ttisstdcnt == typecnt || ttisstdcnt == 0)
			  && (ttisutcnt == typecnt || ttisutcnt == 0)))
			return absl::InvalidArgumentError("Failed to load timezone file");
		if (nread
			< (tzheadsize		/* struct tzhead */
			   + timecnt * stored	/* ats */
			   + timecnt		/* types */
			   + typecnt * 6	/* ttinfos */
			   + charcnt		/* chars */
			   + leapcnt * (stored + 4) /* lsinfos */
			   + ttisstdcnt		/* ttisstds */
			   + ttisutcnt))	/* ttisuts */
			return absl::InvalidArgumentError("Failed to load timezone file");
		sp->leapcnt = leapcnt;
		sp->timecnt = timecnt;
		sp->typecnt = typecnt;
		sp->charcnt = charcnt;

		/*
		 * Read transitions, discarding those out of pg_time_t range. But
		 * pretend the last transition before TIME_T_MIN occurred at
		 * TIME_T_MIN.
		 */
		timecnt = 0;
		for (i = 0; i < sp->timecnt; ++i)
		{
			int64_t		at
			= stored == 4 ? detzcode(p) : detzcode64(p);

			sp->types[i] = at <= TIME_T_MAX;
			if (sp->types[i])
			{
				pg_time_t	attime
				= ((TYPE_SIGNED(pg_time_t) ? at < TIME_T_MIN : at < 0)
				   ? TIME_T_MIN : at);

				if (timecnt && attime <= sp->ats[timecnt - 1])
				{
					if (attime < sp->ats[timecnt - 1])
						return absl::InvalidArgumentError("Failed to load timezone file");
					sp->types[i - 1] = 0;
					timecnt--;
				}
				sp->ats[timecnt++] = attime;
			}
			p += stored;
		}

		timecnt = 0;
		for (i = 0; i < sp->timecnt; ++i)
		{
			unsigned char typ = *p++;

			if (sp->typecnt <= typ)
				return absl::InvalidArgumentError("Failed to load timezone file");
			if (sp->types[i])
				sp->types[timecnt++] = typ;
		}
		sp->timecnt = timecnt;
		for (i = 0; i < sp->typecnt; ++i)
		{
			struct ttinfo *ttisp;
			unsigned char isdst,
						desigidx;

			ttisp = &sp->ttis[i];
			ttisp->tt_utoff = detzcode(p);
			p += 4;
			isdst = *p++;
			if (!(isdst < 2))
				return absl::InvalidArgumentError("Failed to load timezone file");
			ttisp->tt_isdst = isdst;
			desigidx = *p++;
			if (!(desigidx < sp->charcnt))
				return absl::InvalidArgumentError("Failed to load timezone file");
			ttisp->tt_desigidx = desigidx;
		}
		for (i = 0; i < sp->charcnt; ++i)
			sp->chars[i] = *p++;
		sp->chars[i] = '\0';	/* ensure '\0' at end */

		/* Read leap seconds, discarding those out of pg_time_t range.  */
		leapcnt = 0;
		for (i = 0; i < sp->leapcnt; ++i)
		{
			int64_t		tr = stored == 4 ? detzcode(p) : detzcode64(p);
			int32_t		corr = detzcode(p + stored);

			p += stored + 4;
			/* Leap seconds cannot occur before the Epoch.  */
			if (tr < 0)
				return absl::InvalidArgumentError("Failed to load timezone file");
			if (tr <= TIME_T_MAX)
			{
				/*
				 * Leap seconds cannot occur more than once per UTC month, and
				 * UTC months are at least 28 days long (minus 1 second for a
				 * negative leap second).  Each leap second's correction must
				 * differ from the previous one's by 1 second.
				 */
				if (tr - prevtr < 28 * SECSPERDAY - 1
					|| (corr != prevcorr - 1 && corr != prevcorr + 1))
					return absl::InvalidArgumentError("Failed to load timezone file");
				sp->lsis[leapcnt].ls_trans = prevtr = tr;
				sp->lsis[leapcnt].ls_corr = prevcorr = corr;
				leapcnt++;
			}
		}
		sp->leapcnt = leapcnt;

		for (i = 0; i < sp->typecnt; ++i)
		{
			struct ttinfo *ttisp;

			ttisp = &sp->ttis[i];
			if (ttisstdcnt == 0)
				ttisp->tt_ttisstd = false;
			else
			{
				if (*p != true && *p != false)
					return absl::InvalidArgumentError("Failed to load timezone file");
				ttisp->tt_ttisstd = *p++;
			}
		}
		for (i = 0; i < sp->typecnt; ++i)
		{
			struct ttinfo *ttisp;

			ttisp = &sp->ttis[i];
			if (ttisutcnt == 0)
				ttisp->tt_ttisut = false;
			else
			{
				if (*p != true && *p != false)
					return absl::InvalidArgumentError("Failed to load timezone file");
				ttisp->tt_ttisut = *p++;
			}
		}

		/*
		 * If this is an old file, we're done.
		 */
		if (up->tzhead.tzh_version[0] == '\0')
			break;
		nread -= p - up->buf;
		memmove(up->buf, p, nread);
	}
	if (doextend && nread > 2 &&
		up->buf[0] == '\n' && up->buf[nread - 1] == '\n' &&
		sp->typecnt + 2 <= TZ_MAX_TYPES)
	{
		struct state *ts = &lsp->u.st;

		up->buf[nread - 1] = '\0';
		if (tzparse(&up->buf[1], ts, false))
		{
			/*
			 * Attempt to reuse existing abbreviations. Without this,
			 * America/Anchorage would be right on the edge after 2037 when
			 * TZ_MAX_CHARS is 50, as sp->charcnt equals 40 (for LMT AST AWT
			 * APT AHST AHDT YST AKDT AKST) and ts->charcnt equals 10 (for
			 * AKST AKDT).  Reusing means sp->charcnt can stay 40 in this
			 * example.
			 */
			int			gotabbr = 0;
			int			charcnt = sp->charcnt;

			for (i = 0; i < ts->typecnt; i++)
			{
				char	   *tsabbr = ts->chars + ts->ttis[i].tt_desigidx;
				int			j;

				for (j = 0; j < charcnt; j++)
					if (strcmp(sp->chars + j, tsabbr) == 0)
					{
						ts->ttis[i].tt_desigidx = j;
						gotabbr++;
						break;
					}
				if (!(j < charcnt))
				{
					int			tsabbrlen = strlen(tsabbr);

					if (j + tsabbrlen < TZ_MAX_CHARS)
					{
						strcpy(sp->chars + j, tsabbr);
						charcnt = j + tsabbrlen + 1;
						ts->ttis[i].tt_desigidx = j;
						gotabbr++;
					}
				}
			}
			if (gotabbr == ts->typecnt)
			{
				sp->charcnt = charcnt;

				/*
				 * Ignore any trailing, no-op transitions generated by zic as
				 * they don't help here and can run afoul of bugs in zic 2016j
				 * or earlier.
				 */
				while (1 < sp->timecnt
					   && (sp->types[sp->timecnt - 1]
						   == sp->types[sp->timecnt - 2]))
					sp->timecnt--;

				for (i = 0; i < ts->timecnt; i++)
					if (sp->timecnt == 0
						|| (sp->ats[sp->timecnt - 1]
							< ts->ats[i] + leapcorr(sp, ts->ats[i])))
						break;
				while (i < ts->timecnt
					   && sp->timecnt < TZ_MAX_TIMES)
				{
					sp->ats[sp->timecnt]
						= ts->ats[i] + leapcorr(sp, ts->ats[i]);
					sp->types[sp->timecnt] = (sp->typecnt
											  + ts->types[i]);
					sp->timecnt++;
					i++;
				}
				for (i = 0; i < ts->typecnt; i++)
					sp->ttis[sp->typecnt++] = ts->ttis[i];
			}
		}
	}
	if (sp->typecnt == 0)
		return absl::InvalidArgumentError("Failed to load timezone file");
	if (sp->timecnt > 1)
	{
		for (i = 1; i < sp->timecnt; ++i)
			if (typesequiv(sp, sp->types[i], sp->types[0]) &&
				differ_by_repeat(sp->ats[i], sp->ats[0]))
			{
				sp->goback = true;
				break;
			}
		for (i = sp->timecnt - 2; i >= 0; --i)
			if (typesequiv(sp, sp->types[sp->timecnt - 1],
						   sp->types[i]) &&
				differ_by_repeat(sp->ats[sp->timecnt - 1],
								 sp->ats[i]))
			{
				sp->goahead = true;
				break;
			}
	}

	/*
	 * Infer sp->defaulttype from the data.  Although this default type is
	 * always zero for data from recent tzdb releases, things are trickier for
	 * data from tzdb 2018e or earlier.
	 *
	 * The first set of heuristics work around bugs in 32-bit data generated
	 * by tzdb 2013c or earlier.  The workaround is for zones like
	 * Australia/Macquarie where timestamps before the first transition have a
	 * time type that is not the earliest standard-time type.  See:
	 * https://mm.icann.org/pipermail/tz/2013-May/019368.html
	 */

	/*
	 * If type 0 is unused in transitions, it's the type to use for early
	 * times.
	 */
	for (i = 0; i < sp->timecnt; ++i)
		if (sp->types[i] == 0)
			break;
	i = i < sp->timecnt ? -1 : 0;

	/*
	 * Absent the above, if there are transition times and the first
	 * transition is to a daylight time find the standard type less than and
	 * closest to the type of the first transition.
	 */
	if (i < 0 && sp->timecnt > 0 && sp->ttis[sp->types[0]].tt_isdst)
	{
		i = sp->types[0];
		while (--i >= 0)
			if (!sp->ttis[i].tt_isdst)
				break;
	}

	/*
	 * The next heuristics are for data generated by tzdb 2018e or earlier,
	 * for zones like EST5EDT where the first transition is to DST.
	 */

	/*
	 * If no result yet, find the first standard type. If there is none, punt
	 * to type zero.
	 */
	if (i < 0)
	{
		i = 0;
		while (sp->ttis[i].tt_isdst)
			if (++i >= sp->typecnt)
			{
				i = 0;
				break;
			}
	}

	/*
	 * A simple 'sp->defaulttype = 0;' would suffice here if we didn't have to
	 * worry about 2018e-or-earlier data.  Even simpler would be to remove the
	 * defaulttype member and just use 0 in its place.
	 */
	sp->defaulttype = i;

	return absl::OkStatus();
}

/* Load tz data from the file named NAME into *SP.  Read extended
 * format if DOEXTEND.  Return 0 on success, an errno value on failure.
 * PG: If "canonname" is not NULL, then on success the canonical spelling of
 * given name is stored there (the buffer must be > TZ_STRLEN_MAX bytes!).
 * SPANNER_PG: return an absl::Status instead of an errno
 */
absl::Status
tzload(const char *name, char *canonname, struct state *sp, bool doextend)
{
	union local_storage *lsp = static_cast<union local_storage*>(malloc(sizeof *lsp));

	if (!lsp)
		return absl::InvalidArgumentError(
			"Unable to allocate memory to load timezone data");
	else
	{
		absl::Status status = tzloadbody(name, canonname, sp, doextend, lsp);

		free(lsp);
		return status;
	}
}

static bool
typesequiv(const struct state *sp, int a, int b)
{
	bool		result;

	if (sp == NULL ||
		a < 0 || a >= sp->typecnt ||
		b < 0 || b >= sp->typecnt)
		result = false;
	else
	{
		const struct ttinfo *ap = &sp->ttis[a];
		const struct ttinfo *bp = &sp->ttis[b];

		result = (ap->tt_utoff == bp->tt_utoff
				  && ap->tt_isdst == bp->tt_isdst
				  && ap->tt_ttisstd == bp->tt_ttisstd
				  && ap->tt_ttisut == bp->tt_ttisut
				  && (strcmp(&sp->chars[ap->tt_desigidx],
							 &sp->chars[bp->tt_desigidx])
					  == 0));
	}
	return result;
}

static const int mon_lengths[2][MONSPERYEAR] = {
	{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
	{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

static const int year_lengths[2] = {
	DAYSPERNYEAR, DAYSPERLYEAR
};

/*
 * Given a pointer into a timezone string, scan until a character that is not
 * a valid character in a time zone abbreviation is found.
 * Return a pointer to that character.
 */

static const char *
getzname(const char *strp)
{
	char		c;

	while ((c = *strp) != '\0' && !is_digit(c) && c != ',' && c != '-' &&
		   c != '+')
		++strp;
	return strp;
}

/*
 * Given a pointer into an extended timezone string, scan until the ending
 * delimiter of the time zone abbreviation is located.
 * Return a pointer to the delimiter.
 *
 * As with getzname above, the legal character set is actually quite
 * restricted, with other characters producing undefined results.
 * We don't do any checking here; checking is done later in common-case code.
 */

static const char *
getqzname(const char *strp, const int delim)
{
	int			c;

	while ((c = *strp) != '\0' && c != delim)
		++strp;
	return strp;
}

/*
 * Given a pointer into a timezone string, extract a number from that string.
 * Check that the number is within a specified range; if it is not, return
 * NULL.
 * Otherwise, return a pointer to the first character not part of the number.
 */

static const char *
getnum(const char *strp, int *const nump, const int min, const int max)
{
	char		c;
	int			num;

	if (strp == NULL || !is_digit(c = *strp))
		return NULL;
	num = 0;
	do
	{
		num = num * 10 + (c - '0');
		if (num > max)
			return NULL;		/* illegal value */
		c = *++strp;
	} while (is_digit(c));
	if (num < min)
		return NULL;			/* illegal value */
	*nump = num;
	return strp;
}

/*
 * Given a pointer into a timezone string, extract a number of seconds,
 * in hh[:mm[:ss]] form, from the string.
 * If any error occurs, return NULL.
 * Otherwise, return a pointer to the first character not part of the number
 * of seconds.
 */

static const char *
getsecs(const char *strp, int32_t *const secsp)
{
	int			num;

	/*
	 * 'HOURSPERDAY * DAYSPERWEEK - 1' allows quasi-Posix rules like
	 * "M10.4.6/26", which does not conform to Posix, but which specifies the
	 * equivalent of "02:00 on the first Sunday on or after 23 Oct".
	 */
	strp = getnum(strp, &num, 0, HOURSPERDAY * DAYSPERWEEK - 1);
	if (strp == NULL)
		return NULL;
	*secsp = num * (int32_t) SECSPERHOUR;
	if (*strp == ':')
	{
		++strp;
		strp = getnum(strp, &num, 0, MINSPERHOUR - 1);
		if (strp == NULL)
			return NULL;
		*secsp += num * SECSPERMIN;
		if (*strp == ':')
		{
			++strp;
			/* 'SECSPERMIN' allows for leap seconds.  */
			strp = getnum(strp, &num, 0, SECSPERMIN);
			if (strp == NULL)
				return NULL;
			*secsp += num;
		}
	}
	return strp;
}

/*
 * Given a pointer into a timezone string, extract an offset, in
 * [+-]hh[:mm[:ss]] form, from the string.
 * If any error occurs, return NULL.
 * Otherwise, return a pointer to the first character not part of the time.
 */

static const char *
getoffset(const char *strp, int32_t *const offsetp)
{
	bool		neg = false;

	if (*strp == '-')
	{
		neg = true;
		++strp;
	}
	else if (*strp == '+')
		++strp;
	strp = getsecs(strp, offsetp);
	if (strp == NULL)
		return NULL;			/* illegal time */
	if (neg)
		*offsetp = -*offsetp;
	return strp;
}

/*
 * Given a pointer into a timezone string, extract a rule in the form
 * date[/time]. See POSIX section 8 for the format of "date" and "time".
 * If a valid rule is not found, return NULL.
 * Otherwise, return a pointer to the first character not part of the rule.
 */

static const char *
getrule(const char *strp, struct rule *const rulep)
{
	if (*strp == 'J')
	{
		/*
		 * Julian day.
		 */
		rulep->r_type = JULIAN_DAY;
		++strp;
		strp = getnum(strp, &rulep->r_day, 1, DAYSPERNYEAR);
	}
	else if (*strp == 'M')
	{
		/*
		 * Month, week, day.
		 */
		rulep->r_type = MONTH_NTH_DAY_OF_WEEK;
		++strp;
		strp = getnum(strp, &rulep->r_mon, 1, MONSPERYEAR);
		if (strp == NULL)
			return NULL;
		if (*strp++ != '.')
			return NULL;
		strp = getnum(strp, &rulep->r_week, 1, 5);
		if (strp == NULL)
			return NULL;
		if (*strp++ != '.')
			return NULL;
		strp = getnum(strp, &rulep->r_day, 0, DAYSPERWEEK - 1);
	}
	else if (is_digit(*strp))
	{
		/*
		 * Day of year.
		 */
		rulep->r_type = DAY_OF_YEAR;
		strp = getnum(strp, &rulep->r_day, 0, DAYSPERLYEAR - 1);
	}
	else
		return NULL;			/* invalid format */
	if (strp == NULL)
		return NULL;
	if (*strp == '/')
	{
		/*
		 * Time specified.
		 */
		++strp;
		strp = getoffset(strp, &rulep->r_time);
	}
	else
		rulep->r_time = 2 * SECSPERHOUR;	/* default = 2:00:00 */
	return strp;
}

/*
 * Given a year, a rule, and the offset from UT at the time that rule takes
 * effect, calculate the year-relative time that rule takes effect.
 */

static int32_t
transtime(const int year, const struct rule *const rulep,
		  const int32_t offset)
{
	bool		leapyear;
	int32_t		value;
	int			i;
	int			d,
				m1,
				yy0,
				yy1,
				yy2,
				dow;

	INITIALIZE(value);
	leapyear = isleap(year);
	switch (rulep->r_type)
	{

		case JULIAN_DAY:

			/*
			 * Jn - Julian day, 1 == January 1, 60 == March 1 even in leap
			 * years. In non-leap years, or if the day number is 59 or less,
			 * just add SECSPERDAY times the day number-1 to the time of
			 * January 1, midnight, to get the day.
			 */
			value = (rulep->r_day - 1) * SECSPERDAY;
			if (leapyear && rulep->r_day >= 60)
				value += SECSPERDAY;
			break;

		case DAY_OF_YEAR:

			/*
			 * n - day of year. Just add SECSPERDAY times the day number to
			 * the time of January 1, midnight, to get the day.
			 */
			value = rulep->r_day * SECSPERDAY;
			break;

		case MONTH_NTH_DAY_OF_WEEK:

			/*
			 * Mm.n.d - nth "dth day" of month m.
			 */

			/*
			 * Use Zeller's Congruence to get day-of-week of first day of
			 * month.
			 */
			m1 = (rulep->r_mon + 9) % 12 + 1;
			yy0 = (rulep->r_mon <= 2) ? (year - 1) : year;
			yy1 = yy0 / 100;
			yy2 = yy0 % 100;
			dow = ((26 * m1 - 2) / 10 +
				   1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
			if (dow < 0)
				dow += DAYSPERWEEK;

			/*
			 * "dow" is the day-of-week of the first day of the month. Get the
			 * day-of-month (zero-origin) of the first "dow" day of the month.
			 */
			d = rulep->r_day - dow;
			if (d < 0)
				d += DAYSPERWEEK;
			for (i = 1; i < rulep->r_week; ++i)
			{
				if (d + DAYSPERWEEK >=
					mon_lengths[(int) leapyear][rulep->r_mon - 1])
					break;
				d += DAYSPERWEEK;
			}

			/*
			 * "d" is the day-of-month (zero-origin) of the day we want.
			 */
			value = d * SECSPERDAY;
			for (i = 0; i < rulep->r_mon - 1; ++i)
				value += mon_lengths[(int) leapyear][i] * SECSPERDAY;
			break;
	}

	/*
	 * "value" is the year-relative time of 00:00:00 UT on the day in
	 * question. To get the year-relative time of the specified local time on
	 * that day, add the transition time and the current offset from UT.
	 */
	return value + rulep->r_time + offset;
}

/*
 * Given a POSIX section 8-style TZ string, fill in the rule tables as
 * appropriate.
 * Returns true on success, false on failure.
 */
bool
tzparse(const char *name, struct state *sp, bool lastditch)
{
	const char *stdname;
	const char *dstname = NULL;
	size_t		stdlen;
	size_t		dstlen;
	size_t		charcnt;
	int32_t		stdoffset;
	int32_t		dstoffset;
	char	   *cp;
	bool		load_ok;

	stdname = name;
	if (lastditch)
	{
		/* Unlike IANA, don't assume name is exactly "GMT" */
		stdlen = strlen(name);	/* length of standard zone name */
		name += stdlen;
		stdoffset = 0;
	}
	else
	{
		if (*name == '<')
		{
			name++;
			stdname = name;
			name = getqzname(name, '>');
			if (*name != '>')
				return false;
			stdlen = name - stdname;
			name++;
		}
		else
		{
			name = getzname(name);
			stdlen = name - stdname;
		}
		if (*name == '\0')		/* we allow empty STD abbrev, unlike IANA */
			return false;
		name = getoffset(name, &stdoffset);
		if (name == NULL)
			return false;
	}
	charcnt = stdlen + 1;
	if (sizeof sp->chars < charcnt)
		return false;

	/*
	 * The IANA code always tries to tzload(TZDEFRULES) here.  We do not want
	 * to do that; it would be bad news in the lastditch case, where we can't
	 * assume pg_open_tzfile() is sane yet.  Moreover, if we did load it and
	 * it contains leap-second-dependent info, that would cause problems too.
	 * Finally, IANA has deprecated the TZDEFRULES feature, so it presumably
	 * will die at some point.  Desupporting it now seems like good
	 * future-proofing.
	 */
	load_ok = false;
	sp->goback = sp->goahead = false;	/* simulate failed tzload() */
	sp->leapcnt = 0;			/* intentionally assume no leap seconds */

	if (*name != '\0')
	{
		if (*name == '<')
		{
			dstname = ++name;
			name = getqzname(name, '>');
			if (*name != '>')
				return false;
			dstlen = name - dstname;
			name++;
		}
		else
		{
			dstname = name;
			name = getzname(name);
			dstlen = name - dstname;	/* length of DST abbr. */
		}
		if (!dstlen)
			return false;
		charcnt += dstlen + 1;
		if (sizeof sp->chars < charcnt)
			return false;
		if (*name != '\0' && *name != ',' && *name != ';')
		{
			name = getoffset(name, &dstoffset);
			if (name == NULL)
				return false;
		}
		else
			dstoffset = stdoffset - SECSPERHOUR;
		if (*name == '\0' && !load_ok)
			name = TZDEFRULESTRING;
		if (*name == ',' || *name == ';')
		{
			struct rule start;
			struct rule end;
			int			year;
			int			yearlim;
			int			timecnt;
			pg_time_t	janfirst;
			int32_t		janoffset = 0;
			int			yearbeg;

			++name;
			if ((name = getrule(name, &start)) == NULL)
				return false;
			if (*name++ != ',')
				return false;
			if ((name = getrule(name, &end)) == NULL)
				return false;
			if (*name != '\0')
				return false;
			sp->typecnt = 2;	/* standard time and DST */

			/*
			 * Two transitions per year, from EPOCH_YEAR forward.
			 */
			init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
			init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
			sp->defaulttype = 0;
			timecnt = 0;
			janfirst = 0;
			yearbeg = EPOCH_YEAR;

			do
			{
				int32_t		yearsecs
				= year_lengths[isleap(yearbeg - 1)] * SECSPERDAY;

				yearbeg--;
				if (increment_overflow_time(&janfirst, -yearsecs))
				{
					janoffset = -yearsecs;
					break;
				}
			} while (EPOCH_YEAR - YEARSPERREPEAT / 2 < yearbeg);

			yearlim = yearbeg + YEARSPERREPEAT + 1;
			for (year = yearbeg; year < yearlim; year++)
			{
				int32_t
							starttime = transtime(year, &start, stdoffset),
							endtime = transtime(year, &end, dstoffset);
				int32_t
							yearsecs = (year_lengths[isleap(year)]
										* SECSPERDAY);
				bool		reversed = endtime < starttime;

				if (reversed)
				{
					int32_t		swap = starttime;

					starttime = endtime;
					endtime = swap;
				}
				if (reversed
					|| (starttime < endtime
						&& (endtime - starttime
							< (yearsecs
							   + (stdoffset - dstoffset)))))
				{
					if (TZ_MAX_TIMES - 2 < timecnt)
						break;
					sp->ats[timecnt] = janfirst;
					if (!increment_overflow_time
						(&sp->ats[timecnt],
						 janoffset + starttime))
						sp->types[timecnt++] = !reversed;
					sp->ats[timecnt] = janfirst;
					if (!increment_overflow_time
						(&sp->ats[timecnt],
						 janoffset + endtime))
					{
						sp->types[timecnt++] = reversed;
						yearlim = year + YEARSPERREPEAT + 1;
					}
				}
				if (increment_overflow_time
					(&janfirst, janoffset + yearsecs))
					break;
				janoffset = 0;
			}
			sp->timecnt = timecnt;
			if (!timecnt)
			{
				sp->ttis[0] = sp->ttis[1];
				sp->typecnt = 1;	/* Perpetual DST.  */
			}
			else if (YEARSPERREPEAT < year - yearbeg)
				sp->goback = sp->goahead = true;
		}
		else
		{
			int32_t		theirstdoffset;
			int32_t		theirdstoffset;
			int32_t		theiroffset;
			bool		isdst;
			int			i;
			int			j;

			if (*name != '\0')
				return false;

			/*
			 * Initial values of theirstdoffset and theirdstoffset.
			 */
			theirstdoffset = 0;
			for (i = 0; i < sp->timecnt; ++i)
			{
				j = sp->types[i];
				if (!sp->ttis[j].tt_isdst)
				{
					theirstdoffset =
						-sp->ttis[j].tt_utoff;
					break;
				}
			}
			theirdstoffset = 0;
			for (i = 0; i < sp->timecnt; ++i)
			{
				j = sp->types[i];
				if (sp->ttis[j].tt_isdst)
				{
					theirdstoffset =
						-sp->ttis[j].tt_utoff;
					break;
				}
			}

			/*
			 * Initially we're assumed to be in standard time.
			 */
			isdst = false;
			theiroffset = theirstdoffset;

			/*
			 * Now juggle transition times and types tracking offsets as you
			 * do.
			 */
			for (i = 0; i < sp->timecnt; ++i)
			{
				j = sp->types[i];
				sp->types[i] = sp->ttis[j].tt_isdst;
				if (sp->ttis[j].tt_ttisut)
				{
					/* No adjustment to transition time */
				}
				else
				{
					/*
					 * If daylight saving time is in effect, and the
					 * transition time was not specified as standard time, add
					 * the daylight saving time offset to the transition time;
					 * otherwise, add the standard time offset to the
					 * transition time.
					 */
					/*
					 * Transitions from DST to DDST will effectively disappear
					 * since POSIX provides for only one DST offset.
					 */
					if (isdst && !sp->ttis[j].tt_ttisstd)
					{
						sp->ats[i] += dstoffset -
							theirdstoffset;
					}
					else
					{
						sp->ats[i] += stdoffset -
							theirstdoffset;
					}
				}
				theiroffset = -sp->ttis[j].tt_utoff;
				if (sp->ttis[j].tt_isdst)
					theirdstoffset = theiroffset;
				else
					theirstdoffset = theiroffset;
			}

			/*
			 * Finally, fill in ttis.
			 */
			init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
			init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
			sp->typecnt = 2;
			sp->defaulttype = 0;
		}
	}
	else
	{
		dstlen = 0;
		sp->typecnt = 1;		/* only standard time */
		sp->timecnt = 0;
		init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
		sp->defaulttype = 0;
	}
	sp->charcnt = charcnt;
	cp = sp->chars;
	memcpy(cp, stdname, stdlen);
	cp += stdlen;
	*cp++ = '\0';
	if (dstlen != 0)
	{
		memcpy(cp, dstname, dstlen);
		*(cp + dstlen) = '\0';
	}
	return true;
}

static bool
increment_overflow_time(pg_time_t *tp, int32_t j)
{
	/*----------
	 * This is like
	 * 'if (! (TIME_T_MIN <= *tp + j && *tp + j <= TIME_T_MAX)) ...',
	 * except that it does the right thing even if *tp + j would overflow.
	 *----------
	 */
	if (!(j < 0
		  ? (TYPE_SIGNED(pg_time_t) ? TIME_T_MIN - j <= *tp : -1 - j < *tp)
		  : *tp <= TIME_T_MAX - j))
		return true;
	*tp += j;
	return false;
}

static int64_t
leapcorr(struct state const *sp, pg_time_t t)
{
	struct lsinfo const *lp;
	int			i;

	i = sp->leapcnt;
	while (--i >= 0)
	{
		lp = &sp->lsis[i];
		if (t >= lp->ls_trans)
			return lp->ls_corr;
	}
	return 0;
}

/*
 * Find the next DST transition time in the given zone after the given time
 *
 * *timep and *tz are input arguments, the other parameters are output values.
 *
 * When the function result is 1, *boundary is set to the pg_time_t
 * representation of the next DST transition time after *timep,
 * *before_gmtoff and *before_isdst are set to the GMT offset and isdst
 * state prevailing just before that boundary (in particular, the state
 * prevailing at *timep), and *after_gmtoff and *after_isdst are set to
 * the state prevailing just after that boundary.
 *
 * When the function result is 0, there is no known DST transition
 * after *timep, but *before_gmtoff and *before_isdst indicate the GMT
 * offset and isdst state prevailing at *timep.  (This would occur in
 * DST-less time zones, or if a zone has permanently ceased using DST.)
 *
 * A function result of -1 indicates failure (this case does not actually
 * occur in our current implementation).
 */
int
pg_next_dst_boundary(const pg_time_t *timep,
					 long int *before_gmtoff,
					 int *before_isdst,
					 pg_time_t *boundary,
					 long int *after_gmtoff,
					 int *after_isdst,
					 const pg_tz *tz)
{
	const struct state *sp;
	const struct ttinfo *ttisp;
	int			i;
	int			j;
	const pg_time_t t = *timep;

	sp = &tz->state;
	if (sp->timecnt == 0)
	{
		/* non-DST zone, use lowest-numbered standard type */
		i = 0;
		while (sp->ttis[i].tt_isdst)
			if (++i >= sp->typecnt)
			{
				i = 0;
				break;
			}
		ttisp = &sp->ttis[i];
		*before_gmtoff = ttisp->tt_utoff;
		*before_isdst = ttisp->tt_isdst;
		return 0;
	}
	if ((sp->goback && t < sp->ats[0]) ||
		(sp->goahead && t > sp->ats[sp->timecnt - 1]))
	{
		/* For values outside the transition table, extrapolate */
		pg_time_t	newt = t;
		pg_time_t	seconds;
		pg_time_t	tcycles;
		int64_t		icycles;
		int			result;

		if (t < sp->ats[0])
			seconds = sp->ats[0] - t;
		else
			seconds = t - sp->ats[sp->timecnt - 1];
		--seconds;
		tcycles = seconds / YEARSPERREPEAT / AVGSECSPERYEAR;
		++tcycles;
		icycles = tcycles;
		if (tcycles - icycles >= 1 || icycles - tcycles >= 1)
			return -1;
		seconds = icycles;
		seconds *= YEARSPERREPEAT;
		seconds *= AVGSECSPERYEAR;
		if (t < sp->ats[0])
			newt += seconds;
		else
			newt -= seconds;
		if (newt < sp->ats[0] ||
			newt > sp->ats[sp->timecnt - 1])
			return -1;			/* "cannot happen" */

		result = pg_next_dst_boundary(&newt, before_gmtoff,
									  before_isdst,
									  boundary,
									  after_gmtoff,
									  after_isdst,
									  tz);
		if (t < sp->ats[0])
			*boundary -= seconds;
		else
			*boundary += seconds;
		return result;
	}

	if (t >= sp->ats[sp->timecnt - 1])
	{
		/* No known transition > t, so use last known segment's type */
		i = sp->types[sp->timecnt - 1];
		ttisp = &sp->ttis[i];
		*before_gmtoff = ttisp->tt_utoff;
		*before_isdst = ttisp->tt_isdst;
		return 0;
	}
	if (t < sp->ats[0])
	{
		/* For "before", use lowest-numbered standard type */
		i = 0;
		while (sp->ttis[i].tt_isdst)
			if (++i >= sp->typecnt)
			{
				i = 0;
				break;
			}
		ttisp = &sp->ttis[i];
		*before_gmtoff = ttisp->tt_utoff;
		*before_isdst = ttisp->tt_isdst;
		*boundary = sp->ats[0];
		/* And for "after", use the first segment's type */
		i = sp->types[0];
		ttisp = &sp->ttis[i];
		*after_gmtoff = ttisp->tt_utoff;
		*after_isdst = ttisp->tt_isdst;
		return 1;
	}
	/* Else search to find the boundary following t */
	{
		int			lo = 1;
		int			hi = sp->timecnt - 1;

		while (lo < hi)
		{
			int			mid = (lo + hi) >> 1;

			if (t < sp->ats[mid])
				hi = mid;
			else
				lo = mid + 1;
		}
		i = lo;
	}
	j = sp->types[i - 1];
	ttisp = &sp->ttis[j];
	*before_gmtoff = ttisp->tt_utoff;
	*before_isdst = ttisp->tt_isdst;
	*boundary = sp->ats[i];
	j = sp->types[i];
	ttisp = &sp->ttis[j];
	*after_gmtoff = ttisp->tt_utoff;
	*after_isdst = ttisp->tt_isdst;
	return 1;
}

/*
 * Identify a timezone abbreviation's meaning in the given zone
 *
 * Determine the GMT offset and DST flag associated with the abbreviation.
 * This is generally used only when the abbreviation has actually changed
 * meaning over time; therefore, we also take a UTC cutoff time, and return
 * the meaning in use at or most recently before that time, or the meaning
 * in first use after that time if the abbrev was never used before that.
 *
 * On success, returns true and sets *gmtoff and *isdst.  If the abbreviation
 * was never used at all in this zone, returns false.
 *
 * Note: abbrev is matched case-sensitively; it should be all-upper-case.
 */
bool
pg_interpret_timezone_abbrev(const char *abbrev,
							 const pg_time_t *timep,
							 long int *gmtoff,
							 int *isdst,
							 const pg_tz *tz)
{
	const struct state *sp;
	const char *abbrs;
	const struct ttinfo *ttisp;
	int			abbrind;
	int			cutoff;
	int			i;
	const pg_time_t t = *timep;

	sp = &tz->state;

	/*
	 * Locate the abbreviation in the zone's abbreviation list.  We assume
	 * there are not duplicates in the list.
	 */
	abbrs = sp->chars;
	abbrind = 0;
	while (abbrind < sp->charcnt)
	{
		if (strcmp(abbrev, abbrs + abbrind) == 0)
			break;
		while (abbrs[abbrind] != '\0')
			abbrind++;
		abbrind++;
	}
	if (abbrind >= sp->charcnt)
		return false;			/* not there! */

	/*
	 * Unlike pg_next_dst_boundary, we needn't sweat about extrapolation
	 * (goback/goahead zones).  Finding the newest or oldest meaning of the
	 * abbreviation should get us what we want, since extrapolation would just
	 * be repeating the newest or oldest meanings.
	 *
	 * Use binary search to locate the first transition > cutoff time.
	 */
	{
		int			lo = 0;
		int			hi = sp->timecnt;

		while (lo < hi)
		{
			int			mid = (lo + hi) >> 1;

			if (t < sp->ats[mid])
				hi = mid;
			else
				lo = mid + 1;
		}
		cutoff = lo;
	}

	/*
	 * Scan backwards to find the latest interval using the given abbrev
	 * before the cutoff time.
	 */
	for (i = cutoff - 1; i >= 0; i--)
	{
		ttisp = &sp->ttis[sp->types[i]];
		if (ttisp->tt_desigidx == abbrind)
		{
			*gmtoff = ttisp->tt_utoff;
			*isdst = ttisp->tt_isdst;
			return true;
		}
	}

	/*
	 * Not there, so scan forwards to find the first one after.
	 */
	for (i = cutoff; i < sp->timecnt; i++)
	{
		ttisp = &sp->ttis[sp->types[i]];
		if (ttisp->tt_desigidx == abbrind)
		{
			*gmtoff = ttisp->tt_utoff;
			*isdst = ttisp->tt_isdst;
			return true;
		}
	}

	return false;				/* hm, not actually used in any interval? */
}
