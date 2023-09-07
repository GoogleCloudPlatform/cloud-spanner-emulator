/*-------------------------------------------------------------------------
 *
 * pgtz.cc
 *	  Timezone Library Integration Functions
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  datetime_parsing/pgtz.cc
 *
 *-------------------------------------------------------------------------
 */
#include "third_party/spanner_pg/datetime_parsing/pgtz.h"

#include <dirent.h>
#include <time.h>

#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/const_init.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "third_party/spanner_pg/datetime_parsing/miscadmin.h"
#include "third_party/spanner_pg/datetime_parsing/pg_config_manual.h"
#include "third_party/spanner_pg/datetime_parsing/pgtime.h"
#include "third_party/spanner_pg/datetime_parsing/port.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp.h"
#include "zetasql/base/no_destructor.h"

// SPANNER_PG: copied from src/timezone/pgtz.c

static bool scan_directory_ci(const char *dirname,
							  const char *fname, int fnamelen,
							  char *canonname, int canonnamelen);


/*
 * Return full pathname of timezone data directory
 * SPANNER_PG: PostgreSQL has an alternate option if SYSTEMTZDIR is undefined,
 * but Spanner PG defines it in the BUILD file so we can just return it here.
 */
static const char *
pg_TZDIR(void)
{
	return SYSTEMTZDIR;
}


/*
 * Given a timezone name, open() the timezone data file.  Return the
 * file descriptor if successful, -1 if not.
 *
 * The input name is searched for case-insensitively (we assume that the
 * timezone database does not contain case-equivalent names).
 *
 * If "canonname" is not NULL, then on success the canonical spelling of the
 * given name is stored there (the buffer must be > TZ_STRLEN_MAX bytes!).
 *
 * SPANNER_PG: return a FilePtr that will close the fd on destruction instead of
 * a bare fd.
 */
absl::StatusOr<FilePtr>
pg_open_tzfile(const char *name, char *canonname)
{
	const char *fname;
	char		fullname[MAXPGPATH];
	int			fullnamelen;
	int			orignamelen;

	/* Initialize fullname with base name of tzdata directory */
	strlcpy(fullname, pg_TZDIR(), sizeof(fullname));
	orignamelen = fullnamelen = strlen(fullname);

	if (fullnamelen + 1 + strlen(name) >= MAXPGPATH)
		return absl::InvalidArgumentError("Unable to open timezone file");	/* not gonna fit */

	/*
	 * If the caller doesn't need the canonical spelling, first just try to
	 * open the name as-is.  This can be expected to succeed if the given name
	 * is already case-correct, or if the filesystem is case-insensitive; and
	 * we don't need to distinguish those situations if we aren't tasked with
	 * reporting the canonical spelling.
	 */
	if (canonname == NULL)
	{
		fullname[fullnamelen] = '/';
		/* test above ensured this will fit: */
		strcpy(fullname + fullnamelen + 1, name);
		/* SPANNER_PG: use fopen and a FilePtr instead of open and a bare fd */
		FILE* fp = fopen(fullname, "rb");
		if (fp != nullptr)
			return FilePtr(fp, fclose);
		/* If that didn't work, fall through to do it the hard way */
		fullname[fullnamelen] = '\0';
	}

	/*
	 * Loop to split the given name into directory levels; for each level,
	 * search using scan_directory_ci().
	 */
	fname = name;
	for (;;)
	{
		const char *slashptr;
		int			fnamelen;

		slashptr = strchr(fname, '/');
		if (slashptr)
			fnamelen = slashptr - fname;
		else
			fnamelen = strlen(fname);
		if (!scan_directory_ci(fullname, fname, fnamelen,
							   fullname + fullnamelen + 1,
							   MAXPGPATH - fullnamelen - 1))
			return absl::InvalidArgumentError("Unable to open timezone file");
		fullname[fullnamelen++] = '/';
		fullnamelen += strlen(fullname + fullnamelen);
		if (slashptr)
			fname = slashptr + 1;
		else
			break;
	}

	if (canonname)
		strlcpy(canonname, fullname + orignamelen + 1, TZ_STRLEN_MAX + 1);

	/* SPANNER_PG: use fopen and a FilePtr instead of open and a bare fd */
	return FilePtr(fopen(fullname, "rb"), fclose);
}


/*
 * Scan specified directory for a case-insensitive match to fname
 * (of length fnamelen --- fname may not be null terminated!).  If found,
 * copy the actual filename into canonname and return true.
 */
static bool
scan_directory_ci(const char *dirname, const char *fname, int fnamelen,
				  char *canonname, int canonnamelen)
{
	bool		found = false;
	DIR		   *dirdesc;
	struct dirent *direntry;

	/*
	 * SPANNER_PG: replace AllocateDir with opendir and ReadDirExtended with
	 * readdir.
	 * AllocateDir relies on PostgreSQL's fd cache that preserves an fd for
	 * the duration of the transaction. Spanner PG uses a unique_ptr that
	 * will call closedir and release the fd when this function returns.
	 *
	 * AllocateDir and ReadDirExtended also rely on PostgreSQL's elog and
	 * ereport for handling cases where the directory cannot be opened or
	 * read. Spanner PG will return false here and treat an unreadable
	 * directory the same way as a timezone file that could not be found.
	 */
	dirdesc = opendir(dirname);
	if (dirdesc == NULL) {
		return false;
	}
	DirPtr dir_ptr = DirPtr(dirdesc, closedir);

	while ((direntry = readdir(dirdesc)) != NULL)
	{
		/*
		 * Ignore . and .., plus any other "hidden" files.  This is a security
		 * measure to prevent access to files outside the timezone directory.
		 */
		if (direntry->d_name[0] == '.')
			continue;

		if (strlen(direntry->d_name) == fnamelen &&
			pg_strncasecmp(direntry->d_name, fname, fnamelen) == 0)
		{
			/* Found our match */
			strlcpy(canonname, direntry->d_name, canonnamelen);
			found = true;
			break;
		}
	}

	/* SPANNER_PG: no need to explicitly close the dir */

	return found;
}

/*
 * SPANNER_PG: make the timezone cache thread-safe, following the model of the
 * absl timezone cache instead of the PostgreSQL thread local model.
 */
ABSL_CONST_INIT absl::Mutex timezone_cache_mutex(absl::kConstInit);
typedef absl::flat_hash_map<std::string, std::unique_ptr<pg_tz>> TimezoneHashMap;
zetasql_base::NoDestructor<TimezoneHashMap> timezone_cache
	ABSL_GUARDED_BY(timezone_cache_mutex)
	ABSL_PT_GUARDED_BY(timezone_cache_mutex);

/*
 * Lookup a timezone by name. Return NULL if the timezone is not found.
 */
pg_tz* FindTimezone(const std::string& timezone_name) {
	absl::ReaderMutexLock l(&timezone_cache_mutex);

	auto it = timezone_cache->find(timezone_name);
	if (it != timezone_cache->end()){
		return it->second.get();
	} else {
		return nullptr;
	}
}

/*
 * Save a timezone to the cache.
 */
void AddTimezoneToCache(const std::string& timezone_name,
			const std::string& canonname,
			struct state* tzstate) {
        std::unique_ptr<pg_tz> timezone = std::make_unique<pg_tz>();

        /*
	 * SPANNER_PG: TZname is populated, but never read when parsing datetime
	 * values. It is only read when outputting the timezone name back to
	 * the user. We're keeping it here in case it's needed in the future.
	 */
	strcpy(timezone->TZname, canonname.c_str());
	memcpy(&timezone->state, tzstate, sizeof(*tzstate));

	absl::WriterMutexLock l(&timezone_cache_mutex);
	timezone_cache->insert_or_assign(timezone_name, std::move(timezone));
}

/*
 * Load a timezone from file or from cache.
 * Does not verify that the timezone is acceptable!
 *
 * "GMT" is always interpreted as the tzparse() definition, without attempting
 * to load a definition from the filesystem.  This has a number of benefits:
 * 1. It's guaranteed to succeed, so we don't have the failure mode wherein
 * the bootstrap default timezone setting doesn't work (as could happen if
 * the OS attempts to supply a leap-second-aware version of "GMT").
 * 2. Because we aren't accessing the filesystem, we can safely initialize
 * the "GMT" zone definition before my_exec_path is known.
 * 3. It's quick enough that we don't waste much time when the bootstrap
 * default timezone setting is later overridden from postgresql.conf.
 * SPANNER_PG: use a C++ TimezoneHashTable instead of the PostgreSQL C hashtable.
 */
pg_tz *
pg_tzset(absl::string_view name)
{
	pg_tz *tz;
	struct state tzstate;
	char		uppername[TZ_STRLEN_MAX + 1];
	char		canonname[TZ_STRLEN_MAX + 1];
	char	   *p;

	if (name.size() > TZ_STRLEN_MAX)
		return nullptr;			/* not going to fit */

	/*
	 * Upcase the given name to perform a case-insensitive hashtable search.
	 * (We could alternatively downcase it, but we prefer upcase so that we
	 * can get consistently upcased results from tzparse() in case the name is
	 * a POSIX-style timezone spec.)
	 */
	p = uppername;
	for (int i = 0; i < name.size(); ++i) {
		*p++ = pg_toupper((unsigned char) name.at(i));
	}
	*p = '\0';

	tz = FindTimezone(uppername);

	if (tz)
	{
		/* Timezone found in cache, nothing more to do */
		return tz;
	}

	/*
	 * "GMT" is always sent to tzparse(), as per discussion above.
	 */
	if (strcmp(uppername, "GMT") == 0)
	{
		if (!tzparse(uppername, &tzstate, true))
		{
			/* This really, really should not happen ... */
			return nullptr;
		}
		/* Use uppercase name as canonical */
		strcpy(canonname, uppername);
	}
	else if (!tzload(uppername, canonname, &tzstate, true).ok())
	{
		if (uppername[0] == ':' || !tzparse(uppername, &tzstate, false))
		{
			/* Unknown timezone. Fail our call instead of loading GMT! */
			return nullptr;
		}
		/* For POSIX timezone specs, use uppercase name as canonical */
		strcpy(canonname, uppername);
	}

	/* Save timezone in the cache */
	AddTimezoneToCache(uppername, canonname, &tzstate);
	return FindTimezone(uppername);
}
