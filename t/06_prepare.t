use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg;

my $conninfo = $ENV{TEST_PG_CONNINFO} || 'dbname=postgres';

plan tests => 7;

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

# prepare + query_prepared
with_pg(sub {
    $pg->prepare("test_add", "select \$1::int + \$2::int as sum", sub {
        my ($data, $err) = @_;
        ok(!$err, 'prepare: no error');
        ok(defined $data, 'prepare: got cmd_tuples');

        $pg->query_prepared("test_add", [10, 20], sub {
            my ($rows, $err2) = @_;
            ok(!$err2, 'query_prepared: no error');
            is($rows->[0][0], '30', 'query_prepared: 10+20=30');
            EV::break;
        });
    });
});

# describe_prepared
with_pg(sub {
    $pg->prepare("desc_stmt", "select \$1::int as val", sub {
        my ($data, $err) = @_;
        ok(!$err, 'prepare for describe');

        $pg->describe_prepared("desc_stmt", sub {
            my ($meta, $err2) = @_;
            ok(!$err2, 'describe_prepared: no error');
            is($meta->{nparams}, 1, 'describe_prepared: 1 param');
            EV::break;
        });
    });
});
