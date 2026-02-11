use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg qw(:status :pipeline);

my $conninfo = $ENV{TEST_PG_CONNINFO} || 'dbname=postgres';

plan tests => 10;

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

# Basic pipeline test
with_pg(sub {
    is($pg->pipeline_status, PQ_PIPELINE_OFF, 'pipeline initially off');

    $pg->enter_pipeline;
    is($pg->pipeline_status, PQ_PIPELINE_ON, 'pipeline on after enter');

    my @results;

    $pg->query_params("select 1", [], sub {
        my ($rows, $err) = @_;
        push @results, $err ? "err:$err" : $rows->[0][0];
    });

    $pg->query_params("select 2", [], sub {
        my ($rows, $err) = @_;
        push @results, $err ? "err:$err" : $rows->[0][0];
    });

    $pg->query_params("select 3", [], sub {
        my ($rows, $err) = @_;
        push @results, $err ? "err:$err" : $rows->[0][0];
    });

    $pg->pipeline_sync(sub {
        my ($ok, $err) = @_;
        ok($ok, 'pipeline_sync callback called with success');
        is_deeply(\@results, ['1', '2', '3'], 'pipeline: all 3 results received in order');

        $pg->exit_pipeline;
        is($pg->pipeline_status, PQ_PIPELINE_OFF, 'pipeline off after exit');
        EV::break;
    });

    $pg->send_flush_request;
});

# Pipeline with error
with_pg(sub {
    $pg->enter_pipeline;

    my @results;

    $pg->query_params("select 1", [], sub {
        my ($rows, $err) = @_;
        push @results, $err ? "err" : $rows->[0][0];
    });

    # This should fail (syntax error)
    $pg->query_params("invalid sql here", [], sub {
        my ($rows, $err) = @_;
        push @results, $err ? "err" : "ok";
    });

    $pg->query_params("select 3", [], sub {
        my ($rows, $err) = @_;
        push @results, $err ? "aborted" : $rows->[0][0];
    });

    $pg->pipeline_sync(sub {
        my ($ok, $err) = @_;
        ok(1, 'sync after error pipeline');
        is($results[0], '1', 'first query succeeded before error');
        is($results[1], 'err', 'second query errored');
        is($results[2], 'aborted', 'third query aborted');

        $pg->exit_pipeline;
        EV::break;
    });

    $pg->send_flush_request;
});

# pending_count test
with_pg(sub {
    is($pg->pending_count, 0, 'pending_count starts at 0');
    $pg->query("select 1", sub { EV::break });
});
