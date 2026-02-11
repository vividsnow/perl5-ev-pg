use strict;
use warnings;
use ExtUtils::MakeMaker;
use EV::MakeMaker;

my $pg_config = $ENV{PG_CONFIG} || 'pg_config';

my $pg_incdir = `$pg_config --includedir 2>/dev/null`;
chomp $pg_incdir;
die "Cannot find pg_config. Install libpq-dev or set PG_CONFIG env var.\n"
    unless $pg_incdir && -d $pg_incdir;

my $pg_libdir = `$pg_config --libdir 2>/dev/null`;
chomp $pg_libdir;

my %ev = EV::MakeMaker::ev_args();

WriteMakefile(
    NAME          => 'EV::Pg',
    VERSION_FROM  => 'lib/EV/Pg.pm',
    ABSTRACT      => 'Async PostgreSQL client using libpq and EV',
    AUTHOR        => 'Yury Kotlyarov',
    LICENSE       => 'perl_5',
    MIN_PERL_VERSION => '5.012',
    PREREQ_PM     => { 'EV' => '4.11' },
    XS            => { 'Pg.xs' => 'Pg.c' },
    INC           => "-I$pg_incdir $ev{INC}",
    LIBS          => ["-L$pg_libdir -lpq"],
    OBJECT        => 'Pg$(OBJ_EXT)',
    CONFIGURE_REQUIRES => {
        'EV::MakeMaker'       => 0,
        'ExtUtils::MakeMaker' => '6.64',
    },
    TEST_REQUIRES => {
        'Test::More' => '0.98',
    },
    META_MERGE => {
        'meta-spec' => { version => 2 },
    },
);
