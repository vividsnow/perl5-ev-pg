#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "EVAPI.h"
#include <libpq-fe.h>

#include "ngx_queue.h"

typedef struct ev_pg_s ev_pg_t;
typedef struct ev_pg_cb_s ev_pg_cb_t;

typedef ev_pg_t* EV__Pg;
typedef struct ev_loop* EV__Loop;

#define EV_PG_MAGIC 0xDEADBEEF
#define EV_PG_FREED 0xFEEDFACE

struct ev_pg_s {
    unsigned int magic;
    struct ev_loop *loop;
    PGconn *conn;

    ev_io    rio, wio;
    int      reading, writing;
    int      fd;

    int      connecting;
    char    *conninfo;

    ngx_queue_t cb_queue;
    int         pending_count;
    int         copy_mode;
    PGresult   *pending_result;

    SV *on_connect;
    SV *on_error;
    SV *on_notify;
    SV *on_notice;

    int callback_depth;
};

struct ev_pg_cb_s {
    SV          *cb;
    ngx_queue_t  queue;
    int          is_pipeline_sync;
    int          is_describe;
};

static void connect_poll_cb(EV_P_ ev_io *w, int revents);
static void io_read_cb(EV_P_ ev_io *w, int revents);
static void io_write_cb(EV_P_ ev_io *w, int revents);
static void start_reading(ev_pg_t *self);
static void stop_reading(ev_pg_t *self);
static void start_writing(ev_pg_t *self);
static void stop_writing(ev_pg_t *self);
static void drain_notifies(ev_pg_t *self);
static void process_results(ev_pg_t *self);
static void check_flush(ev_pg_t *self);
static void emit_error(ev_pg_t *self, const char *msg);
static void cleanup_connection(ev_pg_t *self);
static int  check_destroyed(ev_pg_t *self);

static ev_pg_cb_t *cbt_freelist = NULL;

static ev_pg_cb_t* alloc_cbt(void) {
    ev_pg_cb_t *cbt;
    if (cbt_freelist) {
        cbt = cbt_freelist;
        cbt_freelist = *(ev_pg_cb_t **)cbt;
    } else {
        Newx(cbt, 1, ev_pg_cb_t);
    }
    return cbt;
}

static void release_cbt(ev_pg_cb_t *cbt) {
    *(ev_pg_cb_t **)cbt = cbt_freelist;
    cbt_freelist = cbt;
}

static void start_reading(ev_pg_t *self) {
    if (!self->reading && self->fd >= 0) {
        ev_io_start(self->loop, &self->rio);
        self->reading = 1;
    }
}

static void stop_reading(ev_pg_t *self) {
    if (self->reading) {
        ev_io_stop(self->loop, &self->rio);
        self->reading = 0;
    }
}

static void start_writing(ev_pg_t *self) {
    if (!self->writing && self->fd >= 0) {
        ev_io_start(self->loop, &self->wio);
        self->writing = 1;
    }
}

static void stop_writing(ev_pg_t *self) {
    if (self->writing) {
        ev_io_stop(self->loop, &self->wio);
        self->writing = 0;
    }
}

static int check_destroyed(ev_pg_t *self) {
    if (self->magic == EV_PG_FREED &&
        self->callback_depth == 0) {
        Safefree(self);
        return 1;
    }
    return 0;
}

static void emit_error(ev_pg_t *self, const char *msg) {
    if (NULL == self->on_error) return;

    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    XPUSHs(sv_2mortal(newSVpv(msg, 0)));
    PUTBACK;

    call_sv(self->on_error, G_DISCARD | G_EVAL);
    if (SvTRUE(ERRSV)) {
        warn("EV::Pg: exception in error handler: %s", SvPV_nolen(ERRSV));
    }

    FREETMPS;
    LEAVE;
}

/* Takes ownership of res */
static void deliver_result(ev_pg_t *self, PGresult *res) {
    ngx_queue_t *q;
    ev_pg_cb_t *cbt;
    ExecStatusType st;

    if (ngx_queue_empty(&self->cb_queue)) {
        PQclear(res);
        return;
    }

    q = ngx_queue_head(&self->cb_queue);
    cbt = ngx_queue_data(q, ev_pg_cb_t, queue);

    st = PQresultStatus(res);

    self->callback_depth++;

    {
        dSP;
        ENTER;
        SAVETMPS;
        PUSHMARK(SP);

        if (st == PGRES_FATAL_ERROR || st == PGRES_PIPELINE_ABORTED) {
            const char *errmsg = PQresultErrorMessage(res);
            const char *msg = (errmsg && errmsg[0])
                ? errmsg
                : (st == PGRES_PIPELINE_ABORTED ? "pipeline aborted" : "unknown error");
            PUSHs(&PL_sv_undef);
            PUSHs(sv_2mortal(newSVpv(msg, 0)));
        }
        else if (st == PGRES_PIPELINE_SYNC) {
            PUSHs(sv_2mortal(newSViv(1)));
        }
        else if (st == PGRES_COPY_IN || st == PGRES_COPY_OUT || st == PGRES_COPY_BOTH) {
            const char *tag = (st == PGRES_COPY_IN) ? "COPY_IN"
                            : (st == PGRES_COPY_OUT) ? "COPY_OUT"
                            : "COPY_BOTH";
            PUSHs(sv_2mortal(newSVpv(tag, 0)));
        }
        else if (cbt->is_describe) {
            HV *meta = newHV();
            int nf = PQnfields(res);
            int np = PQnparams(res);
            int i;

            (void)hv_store(meta, "nfields", 7, newSViv(nf), 0);
            (void)hv_store(meta, "nparams", 7, newSViv(np), 0);

            if (nf > 0) {
                AV *fields = newAV();
                av_extend(fields, nf - 1);
                for (i = 0; i < nf; i++) {
                    HV *fld = newHV();
                    (void)hv_store(fld, "name", 4, newSVpv(PQfname(res, i), 0), 0);
                    (void)hv_store(fld, "type", 4, newSVuv(PQftype(res, i)), 0);
                    av_push(fields, newRV_noinc((SV*)fld));
                }
                (void)hv_store(meta, "fields", 6, newRV_noinc((SV*)fields), 0);
            }

            if (np > 0) {
                AV *ptypes = newAV();
                av_extend(ptypes, np - 1);
                for (i = 0; i < np; i++) {
                    av_push(ptypes, newSVuv(PQparamtype(res, i)));
                }
                (void)hv_store(meta, "paramtypes", 10, newRV_noinc((SV*)ptypes), 0);
            }

            PUSHs(sv_2mortal(newRV_noinc((SV*)meta)));
        }
        else if (st == PGRES_TUPLES_OK || st == PGRES_SINGLE_TUPLE) {
            int nrows = PQntuples(res);
            int ncols = PQnfields(res);
            AV *rows = newAV();
            int r, c;
            if (nrows > 0) av_extend(rows, nrows - 1);
            for (r = 0; r < nrows; r++) {
                AV *row = newAV();
                if (ncols > 0) av_extend(row, ncols - 1);
                for (c = 0; c < ncols; c++) {
                    if (PQgetisnull(res, r, c)) {
                        av_push(row, newSV(0));
                    } else {
                        av_push(row, newSVpvn(PQgetvalue(res, r, c),
                                              PQgetlength(res, r, c)));
                    }
                }
                av_push(rows, newRV_noinc((SV*)row));
            }
            PUSHs(sv_2mortal(newRV_noinc((SV*)rows)));
        }
        else {
            /* COMMAND_OK — pass cmd_tuples string */
            const char *ct = PQcmdTuples(res);
            PUSHs(sv_2mortal(newSVpv(ct ? ct : "", 0)));
        }

        PUTBACK;
        call_sv(cbt->cb, G_DISCARD | G_EVAL);
        if (SvTRUE(ERRSV)) {
            warn("EV::Pg: exception in callback: %s", SvPV_nolen(ERRSV));
        }
        FREETMPS;
        LEAVE;
    }

    PQclear(res);
    self->callback_depth--;
}

static void advance_cb_queue(ev_pg_t *self) {
    ngx_queue_t *q;
    ev_pg_cb_t *cbt;

    if (ngx_queue_empty(&self->cb_queue)) return;

    q = ngx_queue_head(&self->cb_queue);
    cbt = ngx_queue_data(q, ev_pg_cb_t, queue);

    ngx_queue_remove(q);
    self->pending_count--;
    SvREFCNT_dec(cbt->cb);
    release_cbt(cbt);
}

static void drain_notifies(ev_pg_t *self) {
    PGnotify *notify;

    if (NULL == self->on_notify) return;

    while (self->conn && (notify = PQnotifies(self->conn)) != NULL) {
        self->callback_depth++;

        {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            XPUSHs(sv_2mortal(newSVpv(notify->relname, 0)));
            XPUSHs(sv_2mortal(newSVpv(notify->extra, 0)));
            XPUSHs(sv_2mortal(newSViv(notify->be_pid)));
            PUTBACK;

            call_sv(self->on_notify, G_DISCARD | G_EVAL);
            if (SvTRUE(ERRSV)) {
                warn("EV::Pg: exception in notify handler: %s", SvPV_nolen(ERRSV));
            }

            FREETMPS;
            LEAVE;
        }

        PQfreemem(notify);

        self->callback_depth--;
        if (self->magic != EV_PG_MAGIC) return;
    }
}

static void process_results(ev_pg_t *self) {
    PGresult *res;
    PGresult *last_res = self->pending_result;
    self->pending_result = NULL;

    while (self->conn && !PQisBusy(self->conn)) {
        res = PQgetResult(self->conn);

        if (NULL == res) {
            /* Deliver AFTER consuming NULL so conn is ready for new queries in callback */
            if (last_res != NULL) {
                self->copy_mode = 0;
                deliver_result(self, last_res);
                last_res = NULL;
                if (self->magic != EV_PG_MAGIC) return;
            }

            if (self->copy_mode) break;

            if (!ngx_queue_empty(&self->cb_queue)) {
                ngx_queue_t *q = ngx_queue_head(&self->cb_queue);
                ev_pg_cb_t *cbt = ngx_queue_data(q, ev_pg_cb_t, queue);
                if (!cbt->is_pipeline_sync) {
                    advance_cb_queue(self);
                }
            }
            if (ngx_queue_empty(&self->cb_queue)) break;
            continue;
        }

        {
            ExecStatusType st = PQresultStatus(res);
            if (st == PGRES_PIPELINE_SYNC) {
                deliver_result(self, res);
                if (self->magic != EV_PG_MAGIC) return;
                advance_cb_queue(self);
                continue;
            }
            if (st == PGRES_COPY_IN || st == PGRES_COPY_OUT || st == PGRES_COPY_BOTH) {
                self->copy_mode = 1;
                deliver_result(self, res);
                if (self->magic != EV_PG_MAGIC) return;
                break;
            }
            if (st == PGRES_SINGLE_TUPLE) {
                deliver_result(self, res);
                if (self->magic != EV_PG_MAGIC) return;
                continue;
            }
        }

        if (last_res != NULL) {
            PQclear(last_res);
        }
        last_res = res;
    }

    self->pending_result = last_res;
}

static void check_flush(ev_pg_t *self) {
    int ret = PQflush(self->conn);
    if (ret == 1) {
        start_writing(self);
    }
    else if (ret == -1) {
        emit_error(self, PQerrorMessage(self->conn));
    }
    else {
        stop_writing(self);
    }
}

static void io_read_cb(EV_P_ ev_io *w, int revents) {
    ev_pg_t *self = (ev_pg_t *)w->data;
    (void)loop;
    (void)revents;

    if (self == NULL || self->magic != EV_PG_MAGIC) return;
    if (self->conn == NULL) return;

    if (!PQconsumeInput(self->conn)) {
        self->callback_depth++;
        emit_error(self, PQerrorMessage(self->conn));
        self->callback_depth--;
        if (check_destroyed(self)) return;
        cleanup_connection(self);
        return;
    }

    self->callback_depth++;

    drain_notifies(self);
    if (self->magic != EV_PG_MAGIC) {
        self->callback_depth--;
        check_destroyed(self);
        return;
    }

    process_results(self);
    if (self->magic != EV_PG_MAGIC) {
        self->callback_depth--;
        check_destroyed(self);
        return;
    }

    if (self->conn) check_flush(self);

    self->callback_depth--;
    check_destroyed(self);
}

static void io_write_cb(EV_P_ ev_io *w, int revents) {
    ev_pg_t *self = (ev_pg_t *)w->data;
    int ret;
    (void)loop;
    (void)revents;

    if (self == NULL || self->magic != EV_PG_MAGIC) return;
    if (self->conn == NULL) return;

    ret = PQflush(self->conn);
    if (ret == 0) {
        stop_writing(self);
    }
    else if (ret == -1) {
        self->callback_depth++;
        emit_error(self, PQerrorMessage(self->conn));
        self->callback_depth--;
        if (check_destroyed(self)) return;
        cleanup_connection(self);
    }
}

static void reinit_io_watchers(ev_pg_t *self) {
    stop_reading(self);
    stop_writing(self);

    self->fd = PQsocket(self->conn);
    if (self->fd < 0) return;

    ev_io_init(&self->rio, io_read_cb, self->fd, EV_READ);
    self->rio.data = (void *)self;
    ev_io_init(&self->wio, io_write_cb, self->fd, EV_WRITE);
    self->wio.data = (void *)self;

    start_reading(self);
    check_flush(self);
}

static void connect_poll_cb(EV_P_ ev_io *w, int revents) {
    ev_pg_t *self = (ev_pg_t *)w->data;
    PostgresPollingStatusType poll_status;
    (void)loop;
    (void)revents;

    if (self == NULL || self->magic != EV_PG_MAGIC) return;

    poll_status = PQconnectPoll(self->conn);

    switch (poll_status) {
    case PGRES_POLLING_READING:
        start_reading(self);
        stop_writing(self);
        break;

    case PGRES_POLLING_WRITING:
        stop_reading(self);
        start_writing(self);
        break;

    case PGRES_POLLING_OK:
        self->connecting = 0;
        reinit_io_watchers(self);

        if (NULL != self->on_connect) {
            self->callback_depth++;

            {
                dSP;
                ENTER;
                SAVETMPS;
                PUSHMARK(SP);
                PUTBACK;

                call_sv(self->on_connect, G_DISCARD | G_EVAL);
                if (SvTRUE(ERRSV)) {
                    warn("EV::Pg: exception in connect handler: %s", SvPV_nolen(ERRSV));
                }

                FREETMPS;
                LEAVE;
            }

            self->callback_depth--;
            check_destroyed(self);
        }
        break;

    case PGRES_POLLING_FAILED:
        self->connecting = 0;
        self->callback_depth++;
        emit_error(self, PQerrorMessage(self->conn));
        self->callback_depth--;
        if (check_destroyed(self)) return;
        cleanup_connection(self);
        break;

    default:
        break;
    }
}

static void cleanup_connection(ev_pg_t *self) {
    PGconn *conn;

    stop_reading(self);
    stop_writing(self);
    self->fd = -1;
    self->connecting = 0;
    self->copy_mode = 0;

    if (self->pending_result) {
        PQclear(self->pending_result);
        self->pending_result = NULL;
    }

    conn = self->conn;
    self->conn = NULL;
    if (conn) {
        PQfinish(conn);
    }
}

static void cancel_pending(ev_pg_t *self, const char *errmsg) {
    ngx_queue_t *q;
    ev_pg_cb_t *cbt;

    self->callback_depth++;

    while (!ngx_queue_empty(&self->cb_queue)) {
        q = ngx_queue_head(&self->cb_queue);
        cbt = ngx_queue_data(q, ev_pg_cb_t, queue);
        ngx_queue_remove(q);
        self->pending_count--;

        if (NULL != cbt->cb) {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUSHs(&PL_sv_undef);
            PUSHs(sv_2mortal(newSVpv(errmsg, 0)));
            PUTBACK;
            call_sv(cbt->cb, G_DISCARD | G_EVAL);
            if (SvTRUE(ERRSV)) {
                warn("EV::Pg: exception in callback during cancel: %s", SvPV_nolen(ERRSV));
            }
            FREETMPS;
            LEAVE;

            SvREFCNT_dec(cbt->cb);
        }
        release_cbt(cbt);

        if (self->magic != EV_PG_MAGIC) break;
    }

    self->callback_depth--;
}

static ev_pg_cb_t* push_cb(ev_pg_t *self, SV *cb, int is_sync) {
    ev_pg_cb_t *cbt = alloc_cbt();
    cbt->cb = SvREFCNT_inc(cb);
    cbt->is_pipeline_sync = is_sync;
    cbt->is_describe = 0;
    ngx_queue_insert_tail(&self->cb_queue, &cbt->queue);
    self->pending_count++;
    return cbt;
}

static void post_send(ev_pg_t *self) {
    check_flush(self);
}

static SV* handler_accessor(SV **slot, SV *handler, int has_arg) {
    if (has_arg) {
        if (NULL != *slot) {
            SvREFCNT_dec(*slot);
            *slot = NULL;
        }
        if (NULL != handler && SvOK(handler) &&
            SvROK(handler) && SvTYPE(SvRV(handler)) == SVt_PVCV) {
            *slot = SvREFCNT_inc(handler);
        }
    }

    return (NULL != *slot)
        ? SvREFCNT_inc(*slot)
        : &PL_sv_undef;
}

static void notice_receiver(void *arg, const PGresult *res) {
    ev_pg_t *self = (ev_pg_t *)arg;
    const char *msg;

    if (self->magic != EV_PG_MAGIC) return;
    if (NULL == self->on_notice) return;

    msg = PQresultErrorMessage(res);
    if (!msg || !msg[0]) return;

    self->callback_depth++;
    {
        dSP;
        ENTER;
        SAVETMPS;
        PUSHMARK(SP);
        XPUSHs(sv_2mortal(newSVpv(msg, 0)));
        PUTBACK;

        call_sv(self->on_notice, G_DISCARD | G_EVAL);
        if (SvTRUE(ERRSV)) {
            warn("EV::Pg: exception in notice handler: %s", SvPV_nolen(ERRSV));
        }

        FREETMPS;
        LEAVE;
    }
    self->callback_depth--;
}

MODULE = EV::Pg  PACKAGE = EV::Pg

BOOT:
{
    I_EV_API("EV::Pg");
}

EV::Pg
_new(char *class, EV::Loop loop)
CODE:
{
    PERL_UNUSED_VAR(class);
    Newxz(RETVAL, 1, ev_pg_t);
    RETVAL->magic = EV_PG_MAGIC;
    RETVAL->loop = loop;
    RETVAL->fd = -1;
    ngx_queue_init(&RETVAL->cb_queue);
}
OUTPUT:
    RETVAL

void
DESTROY(EV::Pg self)
CODE:
{
    if (self->magic != EV_PG_MAGIC) {
        if (self->magic == EV_PG_FREED) return;
        return;
    }

    self->magic = EV_PG_FREED;

    stop_reading(self);
    stop_writing(self);

    if (PL_dirty) {
        if (self->pending_result) PQclear(self->pending_result);
        if (NULL != self->conninfo) Safefree(self->conninfo);
        Safefree(self);
        return;
    }

    cancel_pending(self, "object destroyed");

    if (self->pending_result) {
        PQclear(self->pending_result);
        self->pending_result = NULL;
    }

    {
        PGconn *conn = self->conn;
        self->conn = NULL;
        self->loop = NULL;
        self->fd = -1;
        if (conn) PQfinish(conn);
    }

    if (NULL != self->on_connect) {
        SvREFCNT_dec(self->on_connect);
        self->on_connect = NULL;
    }
    if (NULL != self->on_error) {
        SvREFCNT_dec(self->on_error);
        self->on_error = NULL;
    }
    if (NULL != self->on_notify) {
        SvREFCNT_dec(self->on_notify);
        self->on_notify = NULL;
    }
    if (NULL != self->on_notice) {
        SvREFCNT_dec(self->on_notice);
        self->on_notice = NULL;
    }
    if (NULL != self->conninfo) {
        Safefree(self->conninfo);
        self->conninfo = NULL;
    }

    if (self->callback_depth > 0) {
        /* deferred free: check_destroyed will Safefree when depth hits 0 */
    }
    else {
        Safefree(self);
    }
}

void
connect(EV::Pg self, const char *conninfo)
CODE:
{
    size_t len;

    if (NULL != self->conn) {
        croak("already connected");
    }

    if (NULL != self->conninfo) Safefree(self->conninfo);
    len = strlen(conninfo);
    Newx(self->conninfo, len + 1, char);
    Copy(conninfo, self->conninfo, len + 1, char);

    self->conn = PQconnectStart(conninfo);
    if (NULL == self->conn) {
        croak("cannot allocate PGconn");
    }

    if (PQstatus(self->conn) == CONNECTION_BAD) {
        SV *errsv = newSVpv(PQerrorMessage(self->conn), 0);
        SAVEFREESV(errsv);
        PQfinish(self->conn);
        self->conn = NULL;
        croak("connection failed: %s", SvPV_nolen(errsv));
    }

    PQsetnonblocking(self->conn, 1);
    PQsetNoticeReceiver(self->conn, notice_receiver, self);

    self->connecting = 1;
    self->fd = PQsocket(self->conn);

    if (self->fd < 0) {
        PQfinish(self->conn);
        self->conn = NULL;
        self->connecting = 0;
        croak("PQsocket returned invalid fd");
    }

    ev_io_init(&self->rio, connect_poll_cb, self->fd, EV_READ);
    self->rio.data = (void *)self;
    ev_io_init(&self->wio, connect_poll_cb, self->fd, EV_WRITE);
    self->wio.data = (void *)self;

    start_writing(self);
}

void
reset(EV::Pg self)
CODE:
{
    if (NULL == self->conninfo) {
        croak("no previous connection to reset");
    }

    cancel_pending(self, "connection reset");
    cleanup_connection(self);

    self->conn = PQconnectStart(self->conninfo);
    if (NULL == self->conn) {
        croak("cannot allocate PGconn");
    }

    if (PQstatus(self->conn) == CONNECTION_BAD) {
        SV *errsv = newSVpv(PQerrorMessage(self->conn), 0);
        SAVEFREESV(errsv);
        PQfinish(self->conn);
        self->conn = NULL;
        croak("reset failed: %s", SvPV_nolen(errsv));
    }

    PQsetnonblocking(self->conn, 1);
    PQsetNoticeReceiver(self->conn, notice_receiver, self);

    self->connecting = 1;
    self->fd = PQsocket(self->conn);

    if (self->fd < 0) {
        PQfinish(self->conn);
        self->conn = NULL;
        self->connecting = 0;
        croak("PQsocket returned invalid fd");
    }

    ev_io_init(&self->rio, connect_poll_cb, self->fd, EV_READ);
    self->rio.data = (void *)self;
    ev_io_init(&self->wio, connect_poll_cb, self->fd, EV_WRITE);
    self->wio.data = (void *)self;

    start_writing(self);
}

void
finish(EV::Pg self)
CODE:
{
    cancel_pending(self, "connection finished");
    cleanup_connection(self);
}

SV*
on_connect(EV::Pg self, SV *handler = NULL)
CODE:
{
    RETVAL = handler_accessor(&self->on_connect, handler, items > 1);
}
OUTPUT:
    RETVAL

SV*
on_error(EV::Pg self, SV *handler = NULL)
CODE:
{
    RETVAL = handler_accessor(&self->on_error, handler, items > 1);
}
OUTPUT:
    RETVAL

SV*
on_notify(EV::Pg self, SV *handler = NULL)
CODE:
{
    RETVAL = handler_accessor(&self->on_notify, handler, items > 1);
}
OUTPUT:
    RETVAL

SV*
on_notice(EV::Pg self, SV *handler = NULL)
CODE:
{
    RETVAL = handler_accessor(&self->on_notice, handler, items > 1);
}
OUTPUT:
    RETVAL

void
query(EV::Pg self, const char *sql, SV *cb)
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    if (!PQsendQuery(self->conn, sql)) {
        croak("PQsendQuery failed: %s", PQerrorMessage(self->conn));
    }

    push_cb(self, cb, 0);
    post_send(self);
}

void
query_params(EV::Pg self, const char *sql, SV *params_ref, SV *cb)
PREINIT:
    AV *params;
    int nparams, i;
    const char *stack_pv[16];
    const char **pv;
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }
    if (!SvROK(params_ref) || SvTYPE(SvRV(params_ref)) != SVt_PVAV) {
        croak("params must be an ARRAY reference");
    }

    params = (AV *)SvRV(params_ref);
    nparams = (int)(av_len(params) + 1);

    if (nparams == 0) {
        if (!PQsendQueryParams(self->conn, sql, 0, NULL, NULL, NULL, NULL, 0)) {
            croak("PQsendQueryParams failed: %s", PQerrorMessage(self->conn));
        }
    }
    else {
        if (nparams <= 16) {
            Zero(stack_pv, nparams, const char *);
            pv = stack_pv;
        } else {
            Newxz(pv, nparams, const char *);
        }

        for (i = 0; i < nparams; i++) {
            SV **svp = av_fetch(params, i, 0);
            if (svp && SvOK(*svp)) {
                pv[i] = SvPV_nolen(*svp);
            }
        }

        if (!PQsendQueryParams(self->conn, sql, nparams,
                               NULL, pv, NULL, NULL, 0)) {
            if (pv != stack_pv) Safefree(pv);
            croak("PQsendQueryParams failed: %s", PQerrorMessage(self->conn));
        }

        if (pv != stack_pv) Safefree(pv);
    }

    push_cb(self, cb, 0);
    post_send(self);
}

void
prepare(EV::Pg self, const char *name, const char *sql, SV *cb)
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    if (!PQsendPrepare(self->conn, name, sql, 0, NULL)) {
        croak("PQsendPrepare failed: %s", PQerrorMessage(self->conn));
    }

    push_cb(self, cb, 0);
    post_send(self);
}

void
query_prepared(EV::Pg self, const char *name, SV *params_ref, SV *cb)
PREINIT:
    AV *params;
    int nparams, i;
    const char *stack_pv[16];
    const char **pv;
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }
    if (!SvROK(params_ref) || SvTYPE(SvRV(params_ref)) != SVt_PVAV) {
        croak("params must be an ARRAY reference");
    }

    params = (AV *)SvRV(params_ref);
    nparams = (int)(av_len(params) + 1);

    if (nparams == 0) {
        if (!PQsendQueryPrepared(self->conn, name, 0, NULL, NULL, NULL, 0)) {
            croak("PQsendQueryPrepared failed: %s", PQerrorMessage(self->conn));
        }
    }
    else {
        if (nparams <= 16) {
            Zero(stack_pv, nparams, const char *);
            pv = stack_pv;
        } else {
            Newxz(pv, nparams, const char *);
        }

        for (i = 0; i < nparams; i++) {
            SV **svp = av_fetch(params, i, 0);
            if (svp && SvOK(*svp)) {
                pv[i] = SvPV_nolen(*svp);
            }
        }

        if (!PQsendQueryPrepared(self->conn, name, nparams,
                                 pv, NULL, NULL, 0)) {
            if (pv != stack_pv) Safefree(pv);
            croak("PQsendQueryPrepared failed: %s", PQerrorMessage(self->conn));
        }

        if (pv != stack_pv) Safefree(pv);
    }

    push_cb(self, cb, 0);
    post_send(self);
}

void
describe_prepared(EV::Pg self, const char *name, SV *cb)
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    if (!PQsendDescribePrepared(self->conn, name)) {
        croak("PQsendDescribePrepared failed: %s", PQerrorMessage(self->conn));
    }

    push_cb(self, cb, 0)->is_describe = 1;
    post_send(self);
}

void
describe_portal(EV::Pg self, const char *name, SV *cb)
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    if (!PQsendDescribePortal(self->conn, name)) {
        croak("PQsendDescribePortal failed: %s", PQerrorMessage(self->conn));
    }

    push_cb(self, cb, 0)->is_describe = 1;
    post_send(self);
}

void
enter_pipeline(EV::Pg self)
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!PQenterPipelineMode(self->conn)) {
        croak("PQenterPipelineMode failed: %s", PQerrorMessage(self->conn));
    }
}

void
exit_pipeline(EV::Pg self)
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!PQexitPipelineMode(self->conn)) {
        croak("PQexitPipelineMode failed: %s", PQerrorMessage(self->conn));
    }
}

int
pipeline_status(EV::Pg self)
CODE:
{
    if (NULL == self->conn) {
        RETVAL = 0;
    }
    else {
        RETVAL = (int)PQpipelineStatus(self->conn);
    }
}
OUTPUT:
    RETVAL

void
pipeline_sync(EV::Pg self, SV *cb)
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    if (!PQpipelineSync(self->conn)) {
        croak("PQpipelineSync failed: %s", PQerrorMessage(self->conn));
    }

    push_cb(self, cb, 1);
    post_send(self);
}

void
send_flush_request(EV::Pg self)
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (!PQsendFlushRequest(self->conn)) {
        croak("PQsendFlushRequest failed: %s", PQerrorMessage(self->conn));
    }
    post_send(self);
}

int
set_single_row_mode(EV::Pg self)
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    RETVAL = PQsetSingleRowMode(self->conn);
}
OUTPUT:
    RETVAL

int
put_copy_data(EV::Pg self, SV *data)
PREINIT:
    STRLEN len;
    const char *buf;
CODE:
{
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    buf = SvPV(data, len);
    RETVAL = PQputCopyData(self->conn, buf, (int)len);
    if (RETVAL == 1) {
        check_flush(self);
    }
}
OUTPUT:
    RETVAL

int
put_copy_end(EV::Pg self, SV *errmsg = NULL)
CODE:
{
    const char *msg = NULL;
    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }
    if (errmsg && SvOK(errmsg)) {
        msg = SvPV_nolen(errmsg);
    }
    RETVAL = PQputCopyEnd(self->conn, msg);
    if (RETVAL == 1) {
        check_flush(self);
    }
}
OUTPUT:
    RETVAL

SV*
get_copy_data(EV::Pg self)
CODE:
{
    char *buf = NULL;
    int len;

    if (NULL == self->conn || self->connecting) {
        croak("not connected");
    }

    len = PQgetCopyData(self->conn, &buf, 1);
    if (len > 0) {
        RETVAL = newSVpvn(buf, len);
        PQfreemem(buf);
    }
    else if (len == -1) {
        RETVAL = newSViv(-1);
        ev_invoke(self->loop, &self->rio, EV_READ);
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

int
status(EV::Pg self)
CODE:
{
    RETVAL = (NULL != self->conn) ? (int)PQstatus(self->conn) : (int)CONNECTION_BAD;
}
OUTPUT:
    RETVAL

SV*
error_message(EV::Pg self)
CODE:
{
    if (NULL != self->conn) {
        const char *msg = PQerrorMessage(self->conn);
        RETVAL = (msg && msg[0]) ? newSVpv(msg, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

int
transaction_status(EV::Pg self)
CODE:
{
    RETVAL = (NULL != self->conn) ? (int)PQtransactionStatus(self->conn) : (int)PQTRANS_UNKNOWN;
}
OUTPUT:
    RETVAL

SV*
parameter_status(EV::Pg self, const char *name)
CODE:
{
    if (NULL != self->conn) {
        const char *val = PQparameterStatus(self->conn, name);
        RETVAL = (val) ? newSVpv(val, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

int
socket(EV::Pg self)
CODE:
{
    RETVAL = (NULL != self->conn) ? PQsocket(self->conn) : -1;
}
OUTPUT:
    RETVAL

int
backend_pid(EV::Pg self)
CODE:
{
    RETVAL = (NULL != self->conn) ? PQbackendPID(self->conn) : 0;
}
OUTPUT:
    RETVAL

int
server_version(EV::Pg self)
CODE:
{
    RETVAL = (NULL != self->conn) ? PQserverVersion(self->conn) : 0;
}
OUTPUT:
    RETVAL

SV*
db(EV::Pg self)
CODE:
{
    if (NULL != self->conn) {
        const char *val = PQdb(self->conn);
        RETVAL = (val) ? newSVpv(val, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

SV*
user(EV::Pg self)
CODE:
{
    if (NULL != self->conn) {
        const char *val = PQuser(self->conn);
        RETVAL = (val) ? newSVpv(val, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

SV*
host(EV::Pg self)
CODE:
{
    if (NULL != self->conn) {
        const char *val = PQhost(self->conn);
        RETVAL = (val) ? newSVpv(val, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

SV*
port(EV::Pg self)
CODE:
{
    if (NULL != self->conn) {
        const char *val = PQport(self->conn);
        RETVAL = (val) ? newSVpv(val, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

int
is_connected(EV::Pg self)
CODE:
{
    RETVAL = (NULL != self->conn && !self->connecting &&
              PQstatus(self->conn) == CONNECTION_OK) ? 1 : 0;
}
OUTPUT:
    RETVAL

int
ssl_in_use(EV::Pg self)
CODE:
{
    RETVAL = (NULL != self->conn) ? PQsslInUse(self->conn) : 0;
}
OUTPUT:
    RETVAL

SV*
ssl_attribute(EV::Pg self, const char *name)
CODE:
{
    if (NULL != self->conn) {
        const char *val = PQsslAttribute(self->conn, name);
        RETVAL = val ? newSVpv(val, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

SV*
escape_literal(EV::Pg self, SV *str)
PREINIT:
    STRLEN len;
    const char *s;
    char *escaped;
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    s = SvPV(str, len);
    escaped = PQescapeLiteral(self->conn, s, len);
    if (NULL == escaped) {
        croak("PQescapeLiteral failed: %s", PQerrorMessage(self->conn));
    }
    RETVAL = newSVpv(escaped, 0);
    PQfreemem(escaped);
}
OUTPUT:
    RETVAL

SV*
escape_identifier(EV::Pg self, SV *str)
PREINIT:
    STRLEN len;
    const char *s;
    char *escaped;
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    s = SvPV(str, len);
    escaped = PQescapeIdentifier(self->conn, s, len);
    if (NULL == escaped) {
        croak("PQescapeIdentifier failed: %s", PQerrorMessage(self->conn));
    }
    RETVAL = newSVpv(escaped, 0);
    PQfreemem(escaped);
}
OUTPUT:
    RETVAL

int
pending_count(EV::Pg self)
CODE:
{
    RETVAL = self->pending_count;
}
OUTPUT:
    RETVAL

void
skip_pending(EV::Pg self)
CODE:
{
    cancel_pending(self, "skipped");
}

int
lib_version(char *class)
CODE:
{
    PERL_UNUSED_VAR(class);
    RETVAL = PQlibVersion();
}
OUTPUT:
    RETVAL

SV*
cancel(EV::Pg self)
PREINIT:
    PGcancel *cn;
    char errbuf[256];
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    cn = PQgetCancel(self->conn);
    if (NULL == cn) {
        croak("PQgetCancel failed");
    }
    if (!PQcancel(cn, errbuf, sizeof(errbuf))) {
        PQfreeCancel(cn);
        RETVAL = newSVpv(errbuf, 0);
    }
    else {
        PQfreeCancel(cn);
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

SV*
escape_bytea(EV::Pg self, SV *data)
PREINIT:
    STRLEN len;
    const unsigned char *buf;
    unsigned char *escaped;
    size_t escaped_len;
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    buf = (const unsigned char *)SvPV(data, len);
    escaped = PQescapeByteaConn(self->conn, buf, len, &escaped_len);
    if (NULL == escaped) {
        croak("PQescapeByteaConn failed: %s", PQerrorMessage(self->conn));
    }
    RETVAL = newSVpvn((char *)escaped, escaped_len - 1);
    PQfreemem(escaped);
}
OUTPUT:
    RETVAL

SV*
unescape_bytea(char *class, SV *data)
PREINIT:
    STRLEN len;
    const unsigned char *buf;
    unsigned char *unescaped;
    size_t unescaped_len;
CODE:
{
    PERL_UNUSED_VAR(class);
    buf = (const unsigned char *)SvPV(data, len);
    unescaped = PQunescapeBytea(buf, &unescaped_len);
    if (NULL == unescaped) {
        croak("PQunescapeBytea failed");
    }
    RETVAL = newSVpvn((char *)unescaped, unescaped_len);
    PQfreemem(unescaped);
}
OUTPUT:
    RETVAL

SV*
client_encoding(EV::Pg self)
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    RETVAL = newSVpv(pg_encoding_to_char(PQclientEncoding(self->conn)), 0);
}
OUTPUT:
    RETVAL

void
set_client_encoding(EV::Pg self, const char *encoding)
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    if (PQsetClientEncoding(self->conn, encoding) != 0) {
        croak("PQsetClientEncoding failed: %s", PQerrorMessage(self->conn));
    }
}

int
set_error_verbosity(EV::Pg self, int verbosity)
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    RETVAL = (int)PQsetErrorVerbosity(self->conn, (PGVerbosity)verbosity);
}
OUTPUT:
    RETVAL

