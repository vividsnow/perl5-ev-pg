use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg qw(:verbosity);

my $conninfo = $ENV{TEST_PG_CONNINFO} || 'dbname=postgres';

plan tests => 23;

my $pg;

sub with_pg {
    my (%opts) = @_;
    my $cb = delete $opts{cb};
    $pg = EV::Pg->new(
        conninfo   => $conninfo,
        on_connect => sub { $cb->() },
        on_error   => sub {
            diag("Error: $_[0]");
            EV::break;
        },
        %opts,
    );
    my $timeout = EV::timer(5, 0, sub { EV::break });
    EV::run;
    $pg->finish if $pg->is_connected;
}

# --- handler getter round-trip ---
with_pg(cb => sub {
    my $orig = sub { 1 };
    $pg->on_notify($orig);
    my $got = $pg->on_notify;
    is(ref $got, 'CODE', 'handler getter returns CODE ref');
    # getter must NOT destroy the handler
    my $got2 = $pg->on_notify;
    is(ref $got2, 'CODE', 'handler getter preserves handler on second call');
    # clear with undef
    $pg->on_notify(undef);
    my $got3 = $pg->on_notify;
    ok(!defined $got3 || $got3 eq '', 'handler cleared with undef');
    EV::break;
});

# --- connection info accessors ---
with_pg(cb => sub {
    ok($pg->server_version > 0, 'server_version is positive');
    ok(defined $pg->user && length($pg->user) > 0, 'user returns value');
    ok(defined $pg->host, 'host returns value');
    ok(defined $pg->port && $pg->port =~ /^\d+$/, 'port returns numeric value');
    is($pg->ssl_in_use, 0, 'ssl_in_use returns 0 for local conn');
    EV::break;
});

# --- set_client_encoding ---
with_pg(cb => sub {
    my $orig = $pg->client_encoding;
    ok(defined $orig, "client_encoding: $orig");
    $pg->set_client_encoding('SQL_ASCII');
    is($pg->client_encoding, 'SQL_ASCII', 'set_client_encoding to SQL_ASCII');
    $pg->set_client_encoding($orig);
    is($pg->client_encoding, $orig, 'restored original encoding');
    EV::break;
});

# --- NULL values in results ---
with_pg(cb => sub {
    $pg->query("select 42::int as num, null::text as empty", sub {
        my ($rows, $err) = @_;
        ok(!$err, 'null test: no error');
        is($rows->[0][0], '42', 'null test: non-null value');
        ok(!defined $rows->[0][1], 'null test: NULL is undef');
        EV::break;
    });
});

# --- describe_portal ---
with_pg(cb => sub {
    $pg->query("begin", sub {
        my ($data, $err) = @_;
        ok(!$err, 'describe_portal: BEGIN');

        $pg->query("declare test_cursor cursor for select 1 as val", sub {
            my ($data2, $err2) = @_;
            ok(!$err2, 'describe_portal: DECLARE CURSOR');

            $pg->describe_portal("test_cursor", sub {
                my ($meta, $err3) = @_;
                ok(!$err3, 'describe_portal: no error');
                is($meta->{nfields}, 1, 'describe_portal: 1 field');
                is($meta->{fields}[0]{name}, 'val', 'describe_portal: field name is val');

                $pg->query("rollback", sub { EV::break });
            });
        });
    });
});

# --- COPY OUT ---
with_pg(cb => sub {
    $pg->query("create temp table copyout_test (id int, name text)", sub {
        my ($data, $err) = @_;
        ok(!$err, 'copy_out: created table');

        $pg->query("insert into copyout_test values (1, 'Alice'), (2, 'Bob')", sub {
            my ($data2, $err2) = @_;
            ok(!$err2, 'copy_out: inserted rows');

            my @rows;
            $pg->query("copy copyout_test to stdout", sub {
                my ($data3, $err3) = @_;
                if (defined $data3 && !ref($data3) && $data3 eq 'COPY_OUT') {
                    # drain copy data
                    while (1) {
                        my $line = $pg->get_copy_data;
                        if (!defined $line) {
                            next;
                        }
                        elsif ($line eq '-1') {
                            last;
                        }
                        else {
                            push @rows, $line;
                        }
                    }
                    return;
                }
                is(scalar @rows, 2, 'copy_out: got 2 rows');
                EV::break;
            });
        });
    });
});

# --- connection failure ---
{
    my $err_received;
    my $pg_bad = EV::Pg->new(
        conninfo => 'host=127.0.0.1 port=1 dbname=nonexistent connect_timeout=1',
        on_connect => sub {
            fail('should not connect');
            EV::break;
        },
        on_error => sub {
            $err_received = $_[0];
            EV::break;
        },
    );
    my $timeout = EV::timer(3, 0, sub { EV::break });
    EV::run;
    ok(defined $err_received, 'connection failure: on_error called');
}
