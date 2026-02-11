use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg;

my $conninfo = $ENV{TEST_PG_CONNINFO} || 'dbname=postgres';

plan tests => 4;

my @notifications;

my $pg;
$pg = EV::Pg->new(
    conninfo   => $conninfo,
    on_connect => sub {
        $pg->on_notify(sub {
            my ($channel, $payload, $pid) = @_;
            push @notifications, { channel => $channel, payload => $payload, pid => $pid };
        });

        $pg->query("listen test_channel", sub {
            my ($res, $err) = @_;
            ok(!$err, 'LISTEN succeeded');

            # Send notification via same connection.
            # The notification arrives in the same read as the NOTIFY response,
            # and drain_notifies runs before process_results, so it's already
            # in @notifications when this callback fires.
            $pg->query("notify test_channel, 'hello_payload'", sub {
                my ($res2, $err2) = @_;
                ok(!$err2, 'NOTIFY succeeded');

                ok(scalar @notifications >= 1, 'received notification');
                if (@notifications) {
                    is($notifications[0]{channel}, 'test_channel', 'correct channel');
                }
                else {
                    ok(0, 'no notification received');
                }
                EV::break;
            });
        });
    },
    on_error => sub {
        diag("Error: $_[0]");
        EV::break;
    },
);

my $timeout = EV::timer(5, 0, sub { EV::break });
EV::run;
$pg->finish if $pg->is_connected;
