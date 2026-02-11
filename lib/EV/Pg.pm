package EV::Pg;
use strict;
use warnings;

use EV;

BEGIN {
    our $VERSION = '0.01';
    use XSLoader;
    XSLoader::load __PACKAGE__, $VERSION;
}

use Exporter 'import';

our @EXPORT_OK;
our %EXPORT_TAGS;

# Result status constants
use constant {
    PGRES_EMPTY_QUERY    => 0,
    PGRES_COMMAND_OK     => 1,
    PGRES_TUPLES_OK      => 2,
    PGRES_COPY_OUT       => 3,
    PGRES_COPY_IN        => 4,
    PGRES_BAD_RESPONSE   => 5,
    PGRES_NONFATAL_ERROR => 6,
    PGRES_FATAL_ERROR    => 7,
    PGRES_COPY_BOTH      => 8,
    PGRES_SINGLE_TUPLE   => 9,
    PGRES_PIPELINE_SYNC  => 10,
    PGRES_PIPELINE_ABORTED => 11,
};

# Connection status
use constant {
    CONNECTION_OK  => 0,
    CONNECTION_BAD => 1,
};

# Transaction status
use constant {
    PQTRANS_IDLE    => 0,
    PQTRANS_ACTIVE  => 1,
    PQTRANS_INTRANS => 2,
    PQTRANS_INERROR => 3,
    PQTRANS_UNKNOWN => 4,
};

# Pipeline status
use constant {
    PQ_PIPELINE_OFF     => 0,
    PQ_PIPELINE_ON      => 1,
    PQ_PIPELINE_ABORTED => 2,
};

use constant {
    PQERRORS_TERSE    => 0,
    PQERRORS_DEFAULT  => 1,
    PQERRORS_VERBOSE  => 2,
    PQERRORS_SQLSTATE => 3,
};

# Diagnostic field codes
use constant {
    PG_DIAG_SEVERITY           => ord('S'),
    PG_DIAG_SEVERITY_NONLOCALIZED => ord('V'),
    PG_DIAG_SQLSTATE           => ord('C'),
    PG_DIAG_MESSAGE_PRIMARY    => ord('M'),
    PG_DIAG_MESSAGE_DETAIL     => ord('D'),
    PG_DIAG_MESSAGE_HINT       => ord('H'),
    PG_DIAG_STATEMENT_POSITION => ord('P'),
    PG_DIAG_INTERNAL_POSITION  => ord('p'),
    PG_DIAG_INTERNAL_QUERY     => ord('q'),
    PG_DIAG_CONTEXT            => ord('W'),
    PG_DIAG_SCHEMA_NAME        => ord('s'),
    PG_DIAG_TABLE_NAME         => ord('t'),
    PG_DIAG_COLUMN_NAME        => ord('c'),
    PG_DIAG_DATATYPE_NAME      => ord('d'),
    PG_DIAG_CONSTRAINT_NAME    => ord('n'),
    PG_DIAG_SOURCE_FILE        => ord('F'),
    PG_DIAG_SOURCE_LINE        => ord('L'),
    PG_DIAG_SOURCE_FUNCTION    => ord('R'),
};

$EXPORT_TAGS{status} = [qw(
    PGRES_EMPTY_QUERY PGRES_COMMAND_OK PGRES_TUPLES_OK
    PGRES_COPY_OUT PGRES_COPY_IN PGRES_BAD_RESPONSE
    PGRES_NONFATAL_ERROR PGRES_FATAL_ERROR PGRES_COPY_BOTH
    PGRES_SINGLE_TUPLE PGRES_PIPELINE_SYNC PGRES_PIPELINE_ABORTED
)];

$EXPORT_TAGS{conn} = [qw(CONNECTION_OK CONNECTION_BAD)];

$EXPORT_TAGS{transaction} = [qw(
    PQTRANS_IDLE PQTRANS_ACTIVE PQTRANS_INTRANS
    PQTRANS_INERROR PQTRANS_UNKNOWN
)];

$EXPORT_TAGS{pipeline} = [qw(
    PQ_PIPELINE_OFF PQ_PIPELINE_ON PQ_PIPELINE_ABORTED
)];

$EXPORT_TAGS{verbosity} = [qw(
    PQERRORS_TERSE PQERRORS_DEFAULT PQERRORS_VERBOSE PQERRORS_SQLSTATE
)];

$EXPORT_TAGS{diag} = [qw(
    PG_DIAG_SEVERITY PG_DIAG_SEVERITY_NONLOCALIZED PG_DIAG_SQLSTATE
    PG_DIAG_MESSAGE_PRIMARY PG_DIAG_MESSAGE_DETAIL PG_DIAG_MESSAGE_HINT
    PG_DIAG_STATEMENT_POSITION PG_DIAG_INTERNAL_POSITION PG_DIAG_INTERNAL_QUERY
    PG_DIAG_CONTEXT PG_DIAG_SCHEMA_NAME PG_DIAG_TABLE_NAME
    PG_DIAG_COLUMN_NAME PG_DIAG_DATATYPE_NAME PG_DIAG_CONSTRAINT_NAME
    PG_DIAG_SOURCE_FILE PG_DIAG_SOURCE_LINE PG_DIAG_SOURCE_FUNCTION
)];

{
    my %seen;
    @EXPORT_OK = grep { !$seen{$_}++ } map { @$_ } values %EXPORT_TAGS;
    $EXPORT_TAGS{all} = \@EXPORT_OK;
}

*q          = \&query;
*qp         = \&query_params;
*qx         = \&query_prepared;
*prep       = \&prepare;
*reconnect  = \&reset;
*disconnect = \&finish;
*flush      = \&send_flush_request;
*sync       = \&pipeline_sync;
*quote      = \&escape_literal;
*quote_id   = \&escape_identifier;
*errstr     = \&error_message;
*txn_status = \&transaction_status;
*pid        = \&backend_pid;

sub new {
    my ($class, %args) = @_;

    my $loop = delete $args{loop} || EV::default_loop;
    my $self = $class->_new($loop);

    $self->on_error(delete $args{on_error} || sub { die @_ });
    $self->on_connect(delete $args{on_connect})   if $args{on_connect};
    $self->on_notify(delete $args{on_notify})     if $args{on_notify};
    $self->on_notice(delete $args{on_notice})     if $args{on_notice};

    my $conninfo = delete $args{conninfo};
    if (defined $conninfo) {
        $self->connect($conninfo);
    }

    $self;
}

1;
