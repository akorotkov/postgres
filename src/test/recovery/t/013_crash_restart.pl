#
# Tests restarts of postgres due to crashes of a subprocess.
#
# Two longer-running psql subprocesses are used: One to kill a
# backend, triggering a crash-restart cycle, one to detect when
# postmaster noticed the backend died.  The second backend is
# necessary because it's otherwise hard to determine if postmaster is
# still accepting new sessions (because it hasn't noticed that the
# backend died), or because it's already restarted.
#
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use Config;
use Time::HiRes qw(usleep);

if ($Config{osname} eq 'MSWin32')
{
	# some Windows Perls at least don't like IPC::Run's
	# start/kill_kill regime.
	plan skip_all => "Test fails on Windows perl";
}
else
{
	plan tests => 12;
}

my $node = get_new_node('master');
$node->init(allows_streaming => 1);
$node->start();

# by default PostgresNode doesn't doesn't restart after a crash
$node->safe_psql('postgres',
				 q[ALTER SYSTEM SET restart_after_crash = 1;
				   ALTER SYSTEM SET log_connections = 1;
				   SELECT pg_reload_conf();]);

# Run psql, keeping session alive, so we have an alive backend to kill.
my ($killme_stdin, $killme_stdout, $killme_stderr) = ('', '', '');
my $killme = IPC::Run::start(
	[   'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres') ],
	'<',
	\$killme_stdin,
	'>',
	\$killme_stdout,
	'2>',
	\$killme_stderr);

# Need a second psql to check if crash-restart happened.
my ($monitor_stdin, $monitor_stdout, $monitor_stderr) = ('', '', '');
my $monitor = IPC::Run::start(
	[   'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-f', '-', '-d',
		$node->connstr('postgres') ],
	'<',
	\$monitor_stdin,
	'>',
	\$monitor_stdout,
	'2>',
	\$monitor_stderr);

#create table, insert row that should survive
$killme_stdin .= q[
CREATE TABLE alive(status text);
INSERT INTO alive VALUES($$committed-before-sigquit$$);
SELECT pg_backend_pid();
];
$killme->pump until $killme_stdout =~ /[[:digit:]]+[\r\n]$/;
my $pid = $killme_stdout;
chomp($pid);
$killme_stdout = '';

#insert a row that should *not* survive, due to in-progress xact
$killme_stdin .= q[
BEGIN;
INSERT INTO alive VALUES($$in-progress-before-sigquit$$) RETURNING status;
];
$killme->pump until $killme_stdout =~ /in-progress-before-sigquit/;
$killme_stdout = '';


# Start longrunning query in second session, it's failure will signal
# that crash-restart has occurred.
$monitor_stdin .= q[
SELECT pg_sleep(3600);
];
$monitor->pump;


# kill once with QUIT - we expect psql to exit, while emitting error message first
my $cnt = kill 'QUIT', $pid;

# Exactly process should have been alive to be killed
is($cnt, 1, "exactly one process killed with SIGQUIT");

# Check that psql sees the killed backend  as having been terminated
$killme_stdin .= q[
SELECT 1;
];
$killme->pump until $killme_stderr =~ /WARNING:  terminating connection because of crash of another server process/;

ok(1, "psql query died successfully after SIGQUIT");
$killme->kill_kill;

# Check if the crash-restart cycle has occurred
$monitor->pump until $monitor_stderr =~ /WARNING:  terminating connection because of crash of another server process/;
$monitor->kill_kill;
ok(1, "psql monitor died successfully after SIGQUIT");

# Wait till server restarts
is($node->poll_query_until('postgres', 'SELECT $$restarted$$;', 'restarted'), "1", "reconnected after SIGQUIT");

# restart psql processes, now that the crash cycle finished
($killme_stdin, $killme_stdout, $killme_stderr) = ('', '', '');
$killme->run();
($monitor_stdin, $monitor_stdout, $monitor_stderr) = ('', '', '');
$monitor->run();


# Acquire pid of new backend
$killme_stdin .= q[
SELECT pg_backend_pid();
];
$killme->pump until $killme_stdout =~ /[[:digit:]]+[\r\n]$/;
$pid = $killme_stdout;
chomp($pid);
$pid = $killme_stdout;

# Insert test rows
$killme_stdin .= q[
INSERT INTO alive VALUES($$committed-before-sigkill$$) RETURNING status;
BEGIN;
INSERT INTO alive VALUES($$in-progress-before-sigkill$$) RETURNING status;
];
$killme->pump until $killme_stdout =~ /in-progress-before-sigkill/;
$killme_stdout = '';

$monitor_stdin .= q[
SELECT $$restart$$;
];
$monitor->pump until $monitor_stdout =~ /restart/;
$monitor_stdout = '';

# Re-start longrunning query in second session, it's failure will signal
# that crash-restart has occurred.
$monitor_stdin = q[
SELECT pg_sleep(3600);
];
$monitor->pump_nb; # don't wait for query results to come back


# kill with SIGKILL this time - we expect the backend to exit, without
# being able to emit an error error message
$cnt = kill 'KILL', $pid;
is($cnt, 1, "exactly one process killed with KILL");

# Check that psql sees the server as being terminated. No WARNING,
# because signal handlers aren't being run on SIGKILL.
$killme_stdin .= q[
SELECT 1;
];
$killme->pump until $killme_stderr =~ /server closed the connection unexpectedly/;
$killme->kill_kill;
ok(1, "psql query died successfully after SIGKILL");

# Wait till server restarts (note that we should get the WARNING here)
$monitor->pump until $monitor_stderr =~ /WARNING:  terminating connection because of crash of another server process/;
ok(1, "psql monitor died successfully after SIGKILL");
$monitor->kill_kill;

# Wait till server restarts
is($node->poll_query_until('postgres', 'SELECT 1', '1'), "1", "reconnected after SIGKILL");

# Make sure the committed rows survived, in-progress ones not
is($node->safe_psql('postgres', 'SELECT * FROM alive'),
   "committed-before-sigquit\ncommitted-before-sigkill", 'data survived');

is($node->safe_psql('postgres', 'INSERT INTO alive VALUES($$before-orderly-restart$$) RETURNING status'),
   'before-orderly-restart', 'can still write after crash restart');

# Just to be sure, check that an orderly restart now still works
$node->restart();

is($node->safe_psql('postgres', 'SELECT * FROM alive'),
   "committed-before-sigquit\ncommitted-before-sigkill\nbefore-orderly-restart", 'data survived');

is($node->safe_psql('postgres', 'INSERT INTO alive VALUES($$after-orderly-restart$$) RETURNING status'),
   'after-orderly-restart', 'can still write after orderly restart');

$node->stop();
