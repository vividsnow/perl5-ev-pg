# EV::Pg

Asynchronous PostgreSQL client for Perl using libpq and the EV event loop.

## Features

- Non-blocking queries via libpq async API
- Pipeline mode for batched queries
- Prepared statements
- Single-row mode
- COPY IN/OUT
- LISTEN/NOTIFY
- Connection reset
- Notice receiver

## Synopsis

```perl
use EV;
use EV::Pg;

my $pg = EV::Pg->new(
    conninfo   => 'dbname=mydb',
    on_connect => sub {
        $pg->query("select 1, 'hello'", sub {
            my ($rows, $err) = @_;
            die $err if $err;
            say $rows->[0][1];  # hello
            EV::break;
        });
    },
    on_error => sub { die "PG error: $_[0]\n" },
);
EV::run;
```

## Parameterized queries

```perl
$pg->query_params(
    "select $1::int + $2::int",
    [10, 20],
    sub {
        my ($rows, $err) = @_;
        say $rows->[0][0];  # 30
    },
);
```

## Prepared statements

```perl
$pg->prepare(stmt => "select $1::int", sub {
    $pg->query_prepared(stmt => [42], sub {
        my ($rows, $err) = @_;
        say $rows->[0][0];  # 42
    });
});
```

## Pipeline mode

```perl
use EV::Pg qw(:pipeline);

$pg->enter_pipeline;
for my $i (0 .. 999) {
    $pg->query_params("select $1::int", [$i], sub { ... });
}
$pg->pipeline_sync(sub {
    $pg->exit_pipeline;
});
$pg->send_flush_request;
```

## Callback convention

All query callbacks receive `($result, $error)`:

- **SELECT**: `(\@rows, undef)` where each row is an arrayref
- **INSERT/UPDATE/DELETE**: `($cmd_tuples, undef)`
- **Error**: `(undef, $error_message)`
- **COPY**: `("COPY_IN", undef)` or `("COPY_OUT", undef)`
- **Pipeline sync**: `(1)`

## Installation

Requires libpq (PostgreSQL client library) and EV.

```sh
# Debian/Ubuntu
sudo apt-get install libpq-dev

# macOS
brew install libpq

# FreeBSD
pkg install postgresql17-client

# Then:
perl Makefile.PL
make
make test
make install
```

## Benchmark

500k queries over Unix socket, PostgreSQL 18, libpq 18:

| Workload | EV::Pg sequential | EV::Pg pipeline | DBD::Pg sync | DBD::Pg async+EV |
|----------|------------------:|----------------:|-------------:|-----------------:|
| SELECT   | 81,550 q/s | 142,510 q/s | 74,129 q/s | 67,397 q/s |
| INSERT   | 64,500 q/s | 90,185 q/s | 58,995 q/s | 54,914 q/s |
| UPSERT   | 40,006 q/s | 43,603 q/s | 37,818 q/s | 36,807 q/s |

EV::Pg sequential uses prepared statements (parse once, bind+execute per call).
Pipeline mode batches queries with `pipeline_sync` every 5000 queries.

## License

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.
