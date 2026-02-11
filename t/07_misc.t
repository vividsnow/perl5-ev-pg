use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg qw(:transaction);

my $conninfo = $ENV{TEST_PG_CONNINFO} || 'dbname=postgres';

plan tests => 14;

my $pg;

sub with_pg {
    my ($cb) = @_;
    $pg = EV::Pg->new(
        conninfo   => $conninfo,
        on_connect => sub { $cb->() },
        on_error   => sub {
            diag("Error: $_[0]");
            EV::break;
        },
    );
    my $timeout = EV::timer(5, 0, sub { EV::break });
    EV::run;
    $pg->finish if $pg->is_connected;
}

# escape_literal
with_pg(sub {
    my $escaped = $pg->escape_literal("hello'world");
    like($escaped, qr/'hello''world'/, 'escape_literal quotes correctly');
    EV::break;
});

# escape_identifier
with_pg(sub {
    my $escaped = $pg->escape_identifier("my table");
    like($escaped, qr/"my table"/, 'escape_identifier quotes correctly');
    EV::break;
});

# parameter_status
with_pg(sub {
    my $encoding = $pg->parameter_status('server_encoding');
    ok(defined $encoding, 'parameter_status returns a value');
    EV::break;
});

# transaction_status
with_pg(sub {
    is($pg->transaction_status, PQTRANS_IDLE, 'transaction_status idle when connected');
    EV::break;
});

# error callback on syntax error
with_pg(sub {
    $pg->query("invalid sql", sub {
        my ($data, $err) = @_;
        ok($err, 'syntax error: got error string');
        ok(!defined $data, 'syntax error: data is undef');
        EV::break;
    });
});

# reset
with_pg(sub {
    ok($pg->is_connected, 'connected before reset');
    $pg->on_connect(sub {
        ok($pg->is_connected, 'reconnected after reset');
        EV::break;
    });
    $pg->reset;
});

# double finish
with_pg(sub {
    $pg->finish;
    ok(!$pg->is_connected, 'not connected after finish');
    $pg->finish;
    ok(1, 'double finish did not crash');
    EV::break;
});

# skip_pending
with_pg(sub {
    my $skipped = 0;
    $pg->query("select pg_sleep(10)", sub {
        my ($data, $err) = @_;
        $skipped = 1;
        ok($err, 'skipped query received error');
    });
    ok($pg->pending_count > 0, 'pending_count > 0 after send');
    $pg->skip_pending;
    is($pg->pending_count, 0, 'pending_count 0 after skip');
    EV::break;
});

# DESTROY during callback
{
    my $survived = 0;
    my $pg2;
    $pg2 = EV::Pg->new(
        conninfo   => $conninfo,
        on_connect => sub {
            $pg2->query("select 1", sub {
                if (!$survived) {
                    undef $pg2;
                    $survived = 1;
                }
                EV::break;
            });
        },
        on_error => sub {
            diag("Error: $_[0]");
            EV::break;
        },
    );
    my $timeout = EV::timer(5, 0, sub { EV::break });
    EV::run;
    ok($survived, 'DESTROY during callback did not crash');
}
