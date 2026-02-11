use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg qw(:status :verbosity);

my $conninfo = $ENV{TEST_PG_CONNINFO} || 'dbname=postgres';

plan tests => 18;

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

# notice handler
with_pg(
    on_notice => sub {
        my ($msg) = @_;
        like($msg, qr/hello from notice/, 'on_notice received RAISE NOTICE');
    },
    cb => sub {
        $pg->query("do \$\$ begin raise notice 'hello from notice'; end \$\$", sub {
            my ($data, $err) = @_;
            ok(!$err, 'notice: query completed');
            EV::break;
        });
    },
);

# cancel
{
    my $cancel_timer;
    with_pg(cb => sub {
        $pg->query("select pg_sleep(30)", sub {
            my ($data, $err) = @_;
            ok($err, 'cancel: query received error');
            undef $cancel_timer;
            EV::break;
        });
        $cancel_timer = EV::timer(0.5, 0, sub {
            my $cancel_err = $pg->cancel;
            ok(!defined $cancel_err, 'cancel: PQcancel succeeded');
        });
    });
}

# COPY IN
with_pg(cb => sub {
    $pg->query("create temp table copy_test (id int, name text)", sub {
        my ($data, $err) = @_;
        ok(!$err, 'copy_in: created table');

        $pg->query("copy copy_test from stdin", sub {
            my ($data, $err) = @_;
            if (defined $data && !ref($data) && $data eq 'COPY_IN') {
                is($data, 'COPY_IN', 'copy_in: got COPY_IN');
                $pg->put_copy_data("1\tAlice\n");
                $pg->put_copy_data("2\tBob\n");
                $pg->put_copy_end;
                return;
            }
            ok(!$err, 'copy_in: completed without error');
            ok(defined $data, 'copy_in: got cmd_tuples');
            EV::break;
        });
    });
});

# escape_bytea / unescape_bytea round-trip
with_pg(cb => sub {
    my $binary = "\x00\x01\xff\xfe binary data";
    my $escaped = $pg->escape_bytea($binary);
    ok(defined $escaped, 'escape_bytea returns value');
    my $unescaped = EV::Pg->unescape_bytea($escaped);
    is($unescaped, $binary, 'bytea round-trip preserves data');
    EV::break;
});

# client_encoding
with_pg(cb => sub {
    my $enc = $pg->client_encoding;
    ok(defined $enc && length($enc) > 0, "client_encoding: $enc");
    EV::break;
});

# single row mode
with_pg(cb => sub {
    my @rows;
    $pg->query("select generate_series(1,3) as n", sub {
        my ($data, $err) = @_;
        if (ref $data eq 'ARRAY' && @$data > 0) {
            push @rows, $data->[0][0];
            return;
        }
        # Final TUPLES_OK with 0 rows
        is(scalar @rows, 3, 'single_row: got 3 rows');
        is_deeply(\@rows, ['1', '2', '3'], 'single_row: correct values');
        is(ref $data, 'ARRAY', 'single_row: final is arrayref');
        is(scalar @$data, 0, 'single_row: final has 0 tuples');
        EV::break;
    });
    $pg->set_single_row_mode;
});

# ssl_attribute (may return undef if not using SSL)
with_pg(cb => sub {
    my $lib = $pg->ssl_attribute("library");
    ok(1, 'ssl_attribute: did not crash');
    EV::break;
});

# set_error_verbosity
with_pg(cb => sub {
    my $prev = $pg->set_error_verbosity(PQERRORS_VERBOSE);
    is($prev, PQERRORS_DEFAULT, 'set_error_verbosity: was DEFAULT');
    my $prev2 = $pg->set_error_verbosity(PQERRORS_DEFAULT);
    is($prev2, PQERRORS_VERBOSE, 'set_error_verbosity: was VERBOSE');
    EV::break;
});
