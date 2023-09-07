# Test for timeline switch
use strict;
use warnings;
use File::Path qw(rmtree);
use PostgresNode;
use TestLib;
use Test::More tests => 3;

$ENV{PGDATABASE} = 'postgres';

# Ensure that a cascading standby is able to follow a newly-promoted standby
# on a new timeline.

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->start;

# Take backup
my $backup_name = 'my_backup';
$node_master->backup($backup_name);

# Create two standbys linking to it
my $node_standby_1 = get_new_node('standby_1');
$node_standby_1->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_1->start;
my $node_standby_2 = get_new_node('standby_2');
$node_standby_2->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_2->start;

# Create some content on master
$node_master->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT generate_series(1,1000) AS a");

# Wait until standby has replayed enough data on standby 1
$node_master->wait_for_catchup($node_standby_1, 'replay',
	$node_master->lsn('write'));

# Stop and remove master
$node_master->teardown_node;

# promote standby 1 using "pg_promote", switching it to a new timeline
my $psql_out = '';
$node_standby_1->psql(
	'postgres',
	"SELECT pg_promote(wait_seconds => 300)",
	stdout => \$psql_out);
is($psql_out, 't', "promotion of standby with pg_promote");

# Switch standby 2 to replay from standby 1
my $connstr_1 = $node_standby_1->connstr;
$node_standby_2->append_conf(
	'postgresql.conf', qq(
primary_conninfo='$connstr_1'
));
$node_standby_2->restart;

# Insert some data in standby 1 and check its presence in standby 2
# to ensure that the timeline switch has been done.
$node_standby_1->safe_psql('postgres',
	"INSERT INTO tab_int VALUES (generate_series(1001,2000))");
$node_standby_1->wait_for_catchup($node_standby_2, 'replay',
	$node_standby_1->lsn('write'));

my $result =
  $node_standby_2->safe_psql('postgres', "SELECT count(*) FROM tab_int");
is($result, qq(2000), 'check content of standby 2');


# Ensure that a standby is able to follow a master on a newer timeline
# when WAL archiving is enabled.

# Initialize master node
my $node_master_2 = get_new_node('master_2');
$node_master_2->init(allows_streaming => 1, has_archiving => 1);
$node_master_2->append_conf(
	'postgresql.conf', qq(
wal_keep_size = 512MB
));
$node_master_2->start;

# Take backup
$node_master_2->backup($backup_name);

# Create standby node
my $node_standby_3 = get_new_node('standby_3');
$node_standby_3->init_from_backup($node_master_2, $backup_name,
	has_streaming => 1);

# Restart master node in standby mode and promote it, switching it
# to a new timeline.
$node_master_2->set_standby_mode;
$node_master_2->restart;
$node_master_2->promote;

# Start standby node, create some content on master and check its presence
# in standby, to ensure that the timeline switch has been done.
$node_standby_3->start;
$node_master_2->safe_psql('postgres',
	"CREATE TABLE tab_int AS SELECT 1 AS a");
$node_master_2->wait_for_catchup($node_standby_3, 'replay',
	$node_master_2->lsn('write'));

my $result_2 =
  $node_standby_3->safe_psql('postgres', "SELECT count(*) FROM tab_int");
is($result_2, qq(1), 'check content of standby 3');
