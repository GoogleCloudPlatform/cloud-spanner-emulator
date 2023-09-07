/*-------------------------------------------------------------------------
 *
 * archive.c
 *	  Routines to access WAL archives from frontend
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/fe_utils/archive.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/xlog_internal.h"
#include "common/archive.h"
#include "common/logging.h"
#include "fe_utils/archive.h"


/*
 * RestoreArchivedFile
 *
 * Attempt to retrieve the specified file from off-line archival storage.
 * If successful, return a file descriptor of the restored file, else
 * return -1.
 *
 * For fixed-size files, the caller may pass the expected size as an
 * additional crosscheck on successful recovery.  If the file size is not
 * known, set expectedSize = 0.
 */
int
RestoreArchivedFile(const char *path, const char *xlogfname,
					off_t expectedSize, const char *restoreCommand)
{
	char		xlogpath[MAXPGPATH];
	char	   *xlogRestoreCmd;
	int			rc;
	struct stat stat_buf;

	snprintf(xlogpath, MAXPGPATH, "%s/" XLOGDIR "/%s", path, xlogfname);

	xlogRestoreCmd = BuildRestoreCommand(restoreCommand, xlogpath,
										 xlogfname, NULL);
	if (xlogRestoreCmd == NULL)
	{
		pg_log_fatal("cannot use restore_command with %%r placeholder");
		exit(1);
	}

	/*
	 * Execute restore_command, which should copy the missing file from
	 * archival storage.
	 */
	rc = system(xlogRestoreCmd);
	pfree(xlogRestoreCmd);

	if (rc == 0)
	{
		/*
		 * Command apparently succeeded, but let's make sure the file is
		 * really there now and has the correct size.
		 */
		if (stat(xlogpath, &stat_buf) == 0)
		{
			if (expectedSize > 0 && stat_buf.st_size != expectedSize)
			{
				pg_log_fatal("unexpected file size for \"%s\": %lu instead of %lu",
							 xlogfname, (unsigned long) stat_buf.st_size,
							 (unsigned long) expectedSize);
				exit(1);
			}
			else
			{
				int			xlogfd = open(xlogpath, O_RDONLY | PG_BINARY, 0);

				if (xlogfd < 0)
				{
					pg_log_fatal("could not open file \"%s\" restored from archive: %m",
								 xlogpath);
					exit(1);
				}
				else
					return xlogfd;
			}
		}
		else
		{
			if (errno != ENOENT)
			{
				pg_log_fatal("could not stat file \"%s\": %m",
							 xlogpath);
				exit(1);
			}
		}
	}

	/*
	 * If the failure was due to a signal, then it would be misleading to
	 * return with a failure at restoring the file.  So just bail out and
	 * exit.  Hard shell errors such as "command not found" are treated as
	 * fatal too.
	 */
	if (wait_result_is_any_signal(rc, true))
	{
		pg_log_fatal("restore_command failed: %s",
					 wait_result_to_str(rc));
		exit(1);
	}

	/*
	 * The file is not available, so just let the caller decide what to do
	 * next.
	 */
	pg_log_error("could not restore file \"%s\" from archive",
				 xlogfname);
	return -1;
}
