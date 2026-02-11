use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg qw(:conn);

my $conninfo = $ENV{TEST_PG_CONNINFO} || 'dbname=postgres';

plan tests => 7;

# Test 1: basic object creation
{
    my $pg = EV::Pg->new(on_error => sub {});
    ok(defined $pg, 'new without conninfo');
    is($pg->is_connected, 0, 'not connected yet');
}

# Test 2: connect
{
    my $connected = 0;
    my $error_msg;

    my $pg = EV::Pg->new(
        conninfo   => $conninfo,
        on_connect => sub {
            $connected = 1;
            EV::break;
        },
        on_error   => sub {
            $error_msg = $_[0];
            EV::break;
        },
    );

    my $timeout = EV::timer(5, 0, sub { EV::break });
    EV::run;

    if ($error_msg) {
        diag("Connection error: $error_msg");
        diag("Set TEST_PG_CONNINFO to a valid connection string");
        ok(1, "connect - skipped (no PostgreSQL)") for 1..5;
        done_testing;
        exit;
    }

    ok($connected, 'connected successfully');
    is($pg->is_connected, 1, 'is_connected returns 1');
    ok($pg->backend_pid > 0, 'backend_pid is positive');

    is($pg->status, CONNECTION_OK, 'status is CONNECTION_OK');
    ok(defined $pg->db, 'db returns a value');

    $pg->finish;
}
