use strict;
use warnings;
use Test::More;
use EV;
use EV::Pg;

plan tests => 2;

# Test that objects are properly cleaned up
{
    my $destroyed = 0;
    {
        my $pg = EV::Pg->new(on_error => sub {});
        ok(defined $pg, 'object created');
    }
    # $pg goes out of scope - DESTROY should be called
    ok(1, 'object destroyed without crash');
}
