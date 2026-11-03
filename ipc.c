/* ipc.c - Logging, file handling, shared memory, bounded buffer, order tracking. */
#include "common.h"

/* ======== logger ======== */
/* logger.c - One write() per line so output from different processes
 * never interleaves in the middle of a line. */
#include <stdarg.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

void log_msg(const char *tag, const char *fmt, ...)
{
    char body[512];
    char line[640];
    struct timespec ts;
    struct tm tm;
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);

    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tm);

    int n = snprintf(line, sizeof line, "[%02d:%02d:%02d.%03ld] [%-10s %5d] %s\n",
                     tm.tm_hour, tm.tm_min, tm.tm_sec, ts.tv_nsec / 1000000L,
                     tag, (int)getpid(), body);
    if (n < 0)
        return;
    if (n >= (int)sizeof line)
        n = (int)sizeof line - 1;

    ssize_t r = write(STDOUT_FILENO, line, (size_t)n);
    (void)r;
}

/* ======== file_utils ======== */
/* file_utils.c - File handling helpers. */
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

int file_create_truncate(const char *path, mode_t mode)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0)
        return -1;
    int rc = fchmod(fd, mode);          /* umask must not weaken/strip bits */
    close(fd);
    return rc;
}

int file_append_line(const char *path, const char *line)
{
    char buf[512];
    int n = (int)strnlen(line, sizeof buf - 2);
    memcpy(buf, line, (size_t)n);
    buf[n++] = '\n';

    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, LOG_MODE);
    if (fd < 0)
        return -1;
    /* A single write() on an O_APPEND fd is atomic w.r.t. other appenders. */
    ssize_t w = write(fd, buf, (size_t)n);
    close(fd);
    return (w == n) ? 0 : -1;
}

int file_get_mode(const char *path)
{
    struct stat st;
    if (stat(path, &st) != 0)
        return -1;
    return (int)(st.st_mode & 07777);
}

int file_copy_preserve(const char *src, const char *dst)
{
    struct stat st;
    char buf[4096];
    ssize_t n;

    int in = open(src, O_RDONLY);
    if (in < 0)
        return -1;
    if (fstat(in, &st) != 0) {
        close(in);
        return -1;
    }

    int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (out < 0) {
        close(in);
        return -1;
    }

    while ((n = read(in, buf, sizeof buf)) > 0) {
        ssize_t off = 0;
        while (off < n) {
            ssize_t w = write(out, buf + off, (size_t)(n - off));
            if (w < 0) {
                if (errno == EINTR)
                    continue;
                close(in);
                close(out);
                return -1;
            }
            off += w;
        }
    }

    int rc = (n < 0) ? -1 : 0;

    /* Preserve permissions, ownership (if allowed) and timestamps. */
    if (fchmod(out, st.st_mode & 07777) != 0)
        rc = -1;
    if (fchown(out, st.st_uid, st.st_gid) != 0) {
        /* Not privileged to change owner: best effort only. */
    }
    struct timespec times[2] = { st.st_atim, st.st_mtim };
    futimens(out, times);

    close(in);
    close(out);
    return rc;
}

/* ======== shm_state ======== */
/* shm_state.c - Creation and locking of the shared state segment. */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

shared_state_t *g_state = NULL;

shared_state_t *state_create(const config_t *cfg)
{
    shm_unlink(SHM_NAME);                         /* remove stale segment */

    int fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, SHM_MODE);
    if (fd < 0) {
        perror("shm_open");
        return NULL;
    }
    fchmod(fd, SHM_MODE);

    if (ftruncate(fd, (off_t)sizeof(shared_state_t)) != 0) {
        perror("ftruncate");
        close(fd);
        return NULL;
    }

    void *mem = mmap(NULL, sizeof(shared_state_t), PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    if (mem == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return NULL;
    }

    struct stat st;
    fstat(fd, &st);
    close(fd);

    g_state = (shared_state_t *)mem;
    memset(g_state, 0, sizeof *g_state);
    g_state->shm_mode = (int)(st.st_mode & 07777);

    /* Process-shared + robust mutex: survives the death of a lock holder. */
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&g_state->lock, &attr);
    pthread_mutexattr_destroy(&attr);

    /* Bounded buffer: `empty` starts at the capacity, `full` at zero. */
    sem_init(&g_state->sem_empty, 1, (unsigned)cfg->queue_cap);
    sem_init(&g_state->sem_full, 1, 0);

    g_state->queue_cap      = cfg->queue_cap;
    g_state->total_expected = cfg->producers * cfg->orders_per_producer;
    g_state->next_order_id  = 1;

    inventory_init();
    return g_state;
}

void state_destroy(void)
{
    if (!g_state)
        return;
    sem_destroy(&g_state->sem_empty);
    sem_destroy(&g_state->sem_full);
    pthread_mutex_destroy(&g_state->lock);
    munmap(g_state, sizeof(shared_state_t));
    g_state = NULL;
    shm_unlink(SHM_NAME);
}

int state_lock(void)
{
    int rc = pthread_mutex_lock(&g_state->lock);
    if (rc == EOWNERDEAD) {                       /* previous owner died */
        pthread_mutex_consistent(&g_state->lock);
        rc = 0;
    }
    return rc;
}

void state_unlock(void)
{
    pthread_mutex_unlock(&g_state->lock);
}

/* ======== order_queue ======== */
/* order_queue.c - Bounded buffer: POSIX mqueue + POSIX semaphores. */
#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

long oq_system_max_msgs(void)
{
    long v = 10;
    FILE *f = fopen("/proc/sys/fs/mqueue/msg_max", "r");
    if (f) {
        if (fscanf(f, "%ld", &v) != 1)
            v = 10;
        fclose(f);
    }
    return v;
}

mqd_t oq_create(int capacity)
{
    struct mq_attr attr = {0};
    attr.mq_maxmsg  = capacity;
    attr.mq_msgsize = sizeof(order_msg_t);

    mq_unlink(MQ_NAME);                              /* remove stale queue */
    mqd_t mq = mq_open(MQ_NAME, O_CREAT | O_EXCL | O_RDWR, MQ_MODE, &attr);
    if (mq == (mqd_t)-1) {
        perror("mq_open(create)");
        if (errno == EINVAL)
            fprintf(stderr, "  queue size %d exceeds /proc/sys/fs/mqueue/msg_max\n",
                    capacity);
        return (mqd_t)-1;
    }

    /* Level 5: make sure the queue really has the permissions we asked for. */
    struct stat st;
    fchmod((int)mq, MQ_MODE);
    if (fstat((int)mq, &st) == 0 && g_state)
        g_state->mq_mode = (int)(st.st_mode & 07777);
    return mq;
}

mqd_t oq_open(int flags)
{
    mqd_t mq = mq_open(MQ_NAME, flags);
    if (mq == (mqd_t)-1)
        perror("mq_open(open)");
    return mq;
}

void oq_destroy(void)
{
    mq_unlink(MQ_NAME);
}

long oq_depth(mqd_t mq)
{
    struct mq_attr a;
    if (mq_getattr(mq, &a) != 0)
        return -1;
    return a.mq_curmsgs;
}

/* Wait on a semaphore in short slices so a signal flag is always noticed. */
static int timed_wait(sem_t *s, volatile sig_atomic_t *stop)
{
    for (;;) {
        if (stop && *stop)
            return 1;

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 200 * 1000 * 1000;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }

        if (sem_timedwait(s, &ts) == 0)
            return 0;
        if (errno == ETIMEDOUT || errno == EINTR)
            continue;
        return -1;
    }
}

int oq_acquire_slot(volatile sig_atomic_t *stop, int *had_to_wait)
{
    *had_to_wait = 0;
    if (sem_trywait(&g_state->sem_empty) == 0)
        return 0;
    if (errno != EAGAIN && errno != EINTR)
        return -1;

    *had_to_wait = 1;                                /* buffer is full */
    return timed_wait(&g_state->sem_empty, stop);
}

int oq_send(mqd_t mq, const order_msg_t *o)
{
    if (mq_send(mq, (const char *)o, sizeof *o, (unsigned)o->priority) != 0) {
        sem_post(&g_state->sem_empty);               /* give the slot back */
        return -1;
    }
    sem_post(&g_state->sem_full);
    return 0;
}

int oq_receive(mqd_t mq, order_msg_t *o, unsigned *prio,
               volatile sig_atomic_t *stop)
{
    int rc = timed_wait(&g_state->sem_full, stop);
    if (rc != 0)
        return rc;

    ssize_t n = mq_receive(mq, (char *)o, sizeof *o, prio);
    if (n < 0) {
        sem_post(&g_state->sem_full);
        return -1;
    }
    return 0;
}

void oq_release_slot(void)
{
    sem_post(&g_state->sem_empty);
}

/* ======== tracker ======== */
/* tracker.c - Order Tracking Service. */
#include <stdlib.h>

const char *status_name(order_status_t s)
{
    switch (s) {
    case ORDER_NONE:            return "NONE";
    case ORDER_SUBMITTED:       return "SUBMITTED";
    case ORDER_FRAUD_CLEARED:   return "FRAUD_CLEARED";
    case ORDER_PAID:            return "PAID";
    case ORDER_RESERVED:        return "RESERVED";
    case ORDER_SHIPPED:         return "SHIPPED";
    case ORDER_COMPLETED:       return "COMPLETED";
    case ORDER_REJECTED_FRAUD:  return "REJECTED_FRAUD";
    case ORDER_FAILED_PAYMENT:  return "FAILED_PAYMENT";
    case ORDER_FAILED_STOCK:    return "FAILED_STOCK";
    }
    return "?";
}

order_status_t tracker_get(uint32_t order_id)
{
    state_lock();
    order_status_t s = g_state->status[order_id];
    state_unlock();
    return s;
}

void tracker_set(uint32_t order_id, order_status_t s)
{
    state_lock();
    g_state->status[order_id] = s;
    state_unlock();
}

void tracker_finish_locked(const order_msg_t *o, order_status_t terminal)
{
    stats_t *st = &g_state->stats;

    g_state->status[o->order_id] = terminal;
    switch (terminal) {
    case ORDER_COMPLETED:
        st->completed++;
        st->revenue_cents += (uint64_t)o->quantity * (uint64_t)o->unit_price_cents;
        break;
    case ORDER_REJECTED_FRAUD:
        st->rejected_fraud++;
        st->critical_events++;
        break;
    case ORDER_FAILED_PAYMENT:
        st->failed_payment++;
        break;
    case ORDER_FAILED_STOCK:
        st->failed_stock++;
        break;
    default:
        break;
    }
}

void tracker_finish(const order_msg_t *o, order_status_t terminal)
{
    state_lock();
    tracker_finish_locked(o, terminal);
    state_unlock();
}

pending_node_t *tracker_collect_exceptions(void)
{
    pending_node_t *head = NULL, *tail = NULL;

    state_lock();
    for (uint32_t id = 1; id < g_state->next_order_id; id++) {
        order_status_t s = g_state->status[id];
        if (s == ORDER_COMPLETED)
            continue;
        pending_node_t *n = malloc(sizeof *n);
        if (!n)
            break;
        n->order_id = id;
        n->status   = s;
        n->next     = NULL;
        if (tail)
            tail->next = n;
        else
            head = n;
        tail = n;
    }
    state_unlock();
    return head;
}

void tracker_free_list(pending_node_t *head)
{
    while (head) {
        pending_node_t *next = head->next;
        free(head);
        head = next;
    }
}
