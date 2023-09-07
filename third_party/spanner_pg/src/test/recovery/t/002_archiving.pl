# test for archiving with hot standby
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 4;
use File::Copy;

# Initialize master node, doing archives
my $node_master = get_new_node('master');
$node_master->init(
	has_archiving    => 1,
	allows_streaming => 1);
my $backup_name = 'my_backup';

# Start it
$node_master->start;

# Take backup for standby
$node_master->backup($backup_name);

# Initialize standby node from backup, fetching WAL from archives
my $node_standby = get_new_node('standby');
# Note that this makes the standby store its contents on the archives
# of the primary.
$node_standby->init_from_backup($node_master, $backup_name,
	has_restoring => 1);
$node_standby->append_conf('postgresql.conf',
	"wal_retrieve_retry_interval = '100ms'");
$node_standby->start;

# Create some content on master
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a");
my $current_lsn =
  $node_master->safe_psql('postgres', "SELECT pg_current_wal_lsn();");

# Force archiving of WAL file to make it present on master
$node_master->safe_psql('postgres', "SELECT pg_switch_wal()");

# Add some more content, it should not be present on standby
$node_master->safe_psql('postgres',
	"INSERT INTO tab_int VALUES (generate_series(1001,2000))");

# Wait until necessary replay has been done on standby
my $caughtup_query =
  "SELECT '$current_lsn'::pg_lsn <= pg_last_wal_replay_lsn()";
$node_standby->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for standby to catch up";

my $result =
  $node_standby->safe_psql('postgres', "SELECT count(*) FROM tab_int");
is($result, qq(1000), 'check content from archives');

# Check the presence of temporary files specifically generated during
# archive recovery.  To ensure the presence of the temporary history
# file, switch to a timeline large enough to allow a standby to recover
# a history file from an archive.  As this requires at least two timeline
# switches, promote the existing standby first.  Then create a second
# standby based on the primary, using its archives.  Finally, the second
# standby is promoted.
$node_standby->promote;

# Wait until the history file has been stored on the archives of the
# primary once the promotion of the standby completes.  This ensures that
# the second standby created below will be able to restore this file,
# creating a RECOVERYHISTORY.
my $primary_archive = $node_master->archive_dir;
$caughtup_query =
  "SELECT size IS NOT NULL FROM pg_stat_file('$primary_archive/00000002.history')";
$node_master->poll_query_until('postgres', $caughtup_query)
  or die "Timed out while waiting for archiving of 00000002.history";

my $node_standby2 = get_new_node('standby2');
$node_standby2->init_from_backup($node_master, $backup_name,
	has_restoring => 1);
$node_standby2->start;

my $log_location = -s $node_standby2->logfile;

# Now promote standby2, and check that temporary files specifically
# generated during archive recovery are removed by the end of recovery.
$node_standby2->promote;

# Check the logs of the standby to see that the commands have failed.
my $log_contents       = slurp_file($node_standby2->logfile, $log_location);
my $node_standby2_data = $node_standby2->data_dir;

like(
	$log_contents,
	qr/restored log file "00000002.history" from archive/s,
	"00000002.history retrieved from the archives");
ok( !-f "$node_standby2_data/pg_wal/RECOVERYHISTORY",
	"RECOVERYHISTORY removed after promotion");
ok( !-f "$node_standby2_data/pg_wal/RECOVERYXLOG",
	"RECOVERYXLOG removed after promotion");
