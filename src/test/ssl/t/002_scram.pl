# Test SCRAM authentication and TLS channel binding types

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 1;
use ServerSetup;
use File::Copy;

# This is the hostname used to connect to the server.
my $SERVERHOSTADDR = '127.0.0.1';

# Allocation of base connection string shared among multiple tests.
my $common_connstr;

# Set up the server.

note "setting up data directory";
my $node = get_new_node('master');
$node->init;

# PGHOST is enforced here to set up the node, subsequent connections
# will use a dedicated connection string.
$ENV{PGHOST} = $node->host;
$ENV{PGPORT} = $node->port;
$node->start;

# Configure server for SSL connections, with password handling.
configure_test_server_for_ssl($node, $SERVERHOSTADDR, "scram-sha-256",
							  "pass", "scram-sha-256");
switch_server_cert($node, 'server-cn-only');
$ENV{PGPASSWORD} = "pass";
$common_connstr =
"user=ssltestuser dbname=trustdb sslmode=require hostaddr=$SERVERHOSTADDR";

test_connect_ok($common_connstr, '',
				"SCRAM authentication with default channel binding");
