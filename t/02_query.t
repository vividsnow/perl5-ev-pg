use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg;

my $conninfo = $ENV{TEST_PG_CONNINFO} || 'dbname=postgres';

plan tests => 12;

my $pg;

# Helper: connect, run a sub, then finish
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

# Test simple query
with_pg(sub {
    $pg->query("select 1 as num, 'hello' as greeting", sub {
        my ($rows, $err) = @_;
        ok(!$err, 'query: no error');
        is(ref $rows, 'ARRAY', 'query: got arrayref');
        is(scalar @$rows, 1, 'query: 1 row');
        is(scalar @{$rows->[0]}, 2, 'query: 2 columns');
        is($rows->[0][0], '1', 'query: value(0,0)');
        is($rows->[0][1], 'hello', 'query: value(0,1)');
        EV::break;
    });
});

# Test query_params
with_pg(sub {
    $pg->query_params("select \$1::int + \$2::int as sum", [10, 20], sub {
        my ($rows, $err) = @_;
        ok(!$err, 'query_params: no error');
        is($rows->[0][0], '30', 'query_params: 10+20=30');
        EV::break;
    });
});

# Test NULL params
with_pg(sub {
    $pg->query_params("select \$1::text is null as isnull", [undef], sub {
        my ($rows, $err) = @_;
        ok(!$err, 'null param: no error');
        is($rows->[0][0], 't', 'null param: IS NULL is true');
        EV::break;
    });
});

# Test multi-row result
with_pg(sub {
    $pg->query("select 1 as a, 2 as b union all select 3, 4", sub {
        my ($rows, $err) = @_;
        ok(!$err, 'multi-row: no error');
        is_deeply($rows, [['1','2'],['3','4']], 'multi-row: correct values');
        EV::break;
    });
});
