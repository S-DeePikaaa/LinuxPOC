/* supervisor.c - main(), process supervision, scaling, recovery and final report. */
#include "common.h"

/* ======== supervisor ======== */
/* supervisor.c - Creates IPC objects, forks producers/consumers/dashboard,
 * restarts crashed consumers, scales the consumer pool dynamically and
 * prints the final report. */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static const config_t *g_cfg;
static pid_t prod_pid[MAX_PRODUCERS];
static pid_t cons_pid[MAX_CONSUMERS];
static pid_t dash_pid;
static mqd_t g_mq = (mqd_t)-1;

static volatile sig_atomic_t g_shutdown = 0;     /* Ctrl-C / SIGTERM */
static int g_final = 0;                          /* final teardown: no respawn */

static void on_signal(int sig)
{
    (void)sig;
    g_shutdown = 1;
}

static void reset_child_signals(void)
{
    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);
}

/* ------------------------------------------------------------------ */
/* Process creation                                                    */
/* ------------------------------------------------------------------ */
static void spawn_producer(int id)
{
    pid_t pid = fork();
    if (pid == 0) {
        reset_child_signals();
        producer_main(id, g_cfg);
        _exit(0);
    }
    if (pid > 0)
        prod_pid[id] = pid;
    else
        perror("fork(producer)");
}

static void spawn_consumer(int slot)
{
    consumer_slot_t *s = &g_state->slots[slot];
    s->active   = 1;
    s->retiring = 0;

    pid_t pid = fork();
    if (pid == 0) {
        reset_child_signals();
        consumer_main(slot, g_cfg);
        _exit(0);
    }
    if (pid > 0)
        cons_pid[slot] = pid;
    else {
        perror("fork(consumer)");
        s->active = 0;
    }
}

static void spawn_dashboard(void)
{
    pid_t pid = fork();
    if (pid == 0) {
        reset_child_signals();
        dashboard_main(g_cfg);
        _exit(0);
    }
    if (pid > 0)
        dash_pid = pid;
}

/* ------------------------------------------------------------------ */
/* Child supervision                                                   */
/* ------------------------------------------------------------------ */
static int count_alive(const pid_t *arr, int n)
{
    int c = 0;
    for (int i = 0; i < n; i++)
        if (arr[i] > 0)
            c++;
    return c;
}

static void reap_children(void)
{
    int status;
    pid_t p;

    while ((p = waitpid(-1, &status, WNOHANG)) > 0) {
        if (p == dash_pid) {
            dash_pid = 0;
            continue;
        }

        int handled = 0;
        for (int i = 0; i < MAX_PRODUCERS && !handled; i++)
            if (prod_pid[i] == p) {
                prod_pid[i] = 0;
                handled = 1;
            }
        if (handled)
            continue;

        for (int s = 0; s < MAX_CONSUMERS; s++) {
            if (cons_pid[s] != p)
                continue;
            cons_pid[s] = 0;

            int crashed = WIFSIGNALED(status) ||
                          (WIFEXITED(status) && WEXITSTATUS(status) != 0);
            if (crashed && !g_final) {
                log_msg("SUPERVISOR", "consumer slot %d (pid %d) DIED (%s %d) - restarting",
                        s, (int)p,
                        WIFSIGNALED(status) ? "signal" : "exit code",
                        WIFSIGNALED(status) ? WTERMSIG(status) : WEXITSTATUS(status));
                state_lock();
                g_state->stats.consumer_crashes++;
                state_unlock();
                spawn_consumer(s);               /* recovery happens in child */
            } else {
                g_state->slots[s].active   = 0;
                g_state->slots[s].retiring = 0;
            }
            break;
        }
    }
}

static void refresh_active_consumers(void)
{
    int n = count_alive(cons_pid, MAX_CONSUMERS);
    state_lock();
    g_state->active_consumers = n;
    state_unlock();
}

/* Dynamic process management: grow when the queue is filling up,
 * shrink when it has been drained. */
static void autoscale(void)
{
    static struct timespec last = {0, 0};
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    double since = (double)(now.tv_sec - last.tv_sec) +
                   (double)(now.tv_nsec - last.tv_nsec) / 1e9;
    if (since < 0.7)
        return;
    if (g_cfg->max_consumers <= g_cfg->consumers)
        return;

    long depth = oq_depth(g_mq);
    int  cap   = g_cfg->queue_cap;
    int  eff = 0;                       /* consumers that are not retiring */
    for (int s = 0; s < MAX_CONSUMERS; s++)
        if (cons_pid[s] > 0 && !g_state->slots[s].retiring)
            eff++;

    int high = (cap * 3) / 4 > 2 ? (cap * 3) / 4 : 2;

    if (depth >= high && eff < g_cfg->max_consumers) {
        for (int s = 0; s < MAX_CONSUMERS; s++) {
            if (cons_pid[s] == 0) {
                log_msg("SUPERVISOR", "queue depth %ld/%d -> SCALE UP (new consumer in slot %d)",
                        depth, cap, s);
                spawn_consumer(s);
                last = now;
                return;
            }
        }
    } else if (depth == 0 && eff > g_cfg->consumers) {
        for (int s = MAX_CONSUMERS - 1; s >= 0; s--) {
            if (cons_pid[s] > 0 && !g_state->slots[s].retiring) {
                log_msg("SUPERVISOR", "queue empty -> SCALE DOWN (retiring slot %d)", s);
                g_state->slots[s].retiring = 1;
                kill(cons_pid[s], SIGUSR1);
                last = now;
                return;
            }
        }
    }
}

static uint64_t finished_orders(void)
{
    state_lock();
    stats_t *s = &g_state->stats;
    uint64_t n = s->completed + s->rejected_fraud + s->failed_payment + s->failed_stock;
    state_unlock();
    return n;
}

static void terminate_all(void)
{
    g_final = 1;

    for (int i = 0; i < MAX_PRODUCERS; i++)
        if (prod_pid[i] > 0)
            kill(prod_pid[i], SIGTERM);
    for (int i = 0; i < MAX_CONSUMERS; i++)
        if (cons_pid[i] > 0)
            kill(cons_pid[i], SIGTERM);
    if (dash_pid > 0)
        kill(dash_pid, SIGTERM);

    for (int tries = 0; tries < 250; tries++) {          /* up to ~5 s */
        reap_children();
        if (count_alive(prod_pid, MAX_PRODUCERS) == 0 &&
            count_alive(cons_pid, MAX_CONSUMERS) == 0 && dash_pid == 0)
            return;
        usleep(20000);
    }

    for (int i = 0; i < MAX_PRODUCERS; i++)
        if (prod_pid[i] > 0)
            kill(prod_pid[i], SIGKILL);
    for (int i = 0; i < MAX_CONSUMERS; i++)
        if (cons_pid[i] > 0)
            kill(cons_pid[i], SIGKILL);
    if (dash_pid > 0)
        kill(dash_pid, SIGKILL);
    while (waitpid(-1, NULL, 0) > 0)
        ;
}

/* ------------------------------------------------------------------ */
/* Final report                                                        */
/* ------------------------------------------------------------------ */
static void report_permissions(void)
{
    printf("\n--- Permission report (Level 5: preserve permissions) ---\n");
    printf("  POSIX message queue %-22s mode %04o (requested %04o)\n",
           MQ_NAME, g_state->mq_mode, MQ_MODE);
    printf("  POSIX shared memory %-22s mode %04o (requested %04o)\n",
           SHM_NAME, g_state->shm_mode, SHM_MODE);

    const char *files[] = { LOG_SUBMITTED, LOG_PROCESSED, LOG_NOTIFY };
    for (size_t i = 0; i < sizeof files / sizeof files[0]; i++)
        printf("  file %-26s mode %04o (requested %04o)\n",
               files[i], file_get_mode(files[i]) & 07777, LOG_MODE);

    if (file_copy_preserve(LOG_SUBMITTED, LOG_ARCHIVE) == 0) {
        int a = file_get_mode(LOG_SUBMITTED), b = file_get_mode(LOG_ARCHIVE);
        printf("  archive copy %s -> %s : %04o -> %04o  %s\n", LOG_SUBMITTED,
               LOG_ARCHIVE, a, b, a == b ? "[permissions preserved]" : "[MISMATCH]");
    } else {
        printf("  archive copy failed: %s\n", strerror(errno));
    }
}

static int print_final_report(void)
{
    stats_t s;
    state_lock();
    s = g_state->stats;
    state_unlock();

    uint64_t finished = s.completed + s.rejected_fraud + s.failed_payment + s.failed_stock;
    int bad_inv = inventory_verify();

    printf("\n================ FINAL REPORT ================\n");
    printf("  Orders expected        : %d\n", g_state->total_expected);
    printf("  Orders submitted       : %llu  (express %llu / normal %llu / bulk %llu)\n",
           (unsigned long long)s.produced,
           (unsigned long long)s.by_priority[PRIO_EXPRESS],
           (unsigned long long)s.by_priority[PRIO_NORMAL],
           (unsigned long long)s.by_priority[PRIO_BULK]);
    printf("  Orders received        : %llu\n", (unsigned long long)s.received);
    printf("  Completed              : %llu\n", (unsigned long long)s.completed);
    printf("  Rejected (fraud)       : %llu\n", (unsigned long long)s.rejected_fraud);
    printf("  Failed (payment)       : %llu\n", (unsigned long long)s.failed_payment);
    printf("  Failed (out of stock)  : %llu\n", (unsigned long long)s.failed_stock);
    printf("  Finished in total      : %llu\n", (unsigned long long)finished);
    printf("  Producer waits (full)  : %llu\n", (unsigned long long)s.producer_blocked);
    printf("  Consumer crashes       : %llu   recovered orders: %llu\n",
           (unsigned long long)s.consumer_crashes, (unsigned long long)s.recovered);
    printf("  Critical events        : %llu\n", (unsigned long long)s.critical_events);
    printf("  Revenue                : $%llu.%02llu\n",
           (unsigned long long)(s.revenue_cents / 100),
           (unsigned long long)(s.revenue_cents % 100));
    printf("  Inventory sync check   : %s\n", bad_inv == 0 ? "OK (initial = stock + sold)" : "MISMATCH");

    printf("  Stock left per product :");
    for (int i = 0; i < MAX_PRODUCTS; i++)
        printf(" %d", g_state->stock[i]);
    printf("\n");

    /* Linked list of orders that did not complete normally. */
    pending_node_t *list = tracker_collect_exceptions();
    int shown = 0, total = 0;
    printf("  Exceptions (linked list):");
    for (pending_node_t *n = list; n; n = n->next) {
        total++;
        if (shown < 12) {
            printf(" #%u=%s", n->order_id, status_name(n->status));
            shown++;
        }
    }
    if (total == 0)
        printf(" none");
    else if (total > shown)
        printf(" ... (+%d more)", total - shown);
    printf("\n");
    tracker_free_list(list);

    if (g_cfg->level >= 5)
        report_permissions();
    else
        file_copy_preserve(LOG_SUBMITTED, LOG_ARCHIVE);

    printf("==============================================\n");

    int ok = (finished == (uint64_t)g_state->total_expected) &&
             (s.produced == (uint64_t)g_state->total_expected) && bad_inv == 0;
    printf("RESULT: %s\n", ok ? "PASS - no data loss, no race conditions detected"
                              : "INCOMPLETE / INCONSISTENT");
    return ok ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/* Entry point                                                         */
/* ------------------------------------------------------------------ */
int supervisor_run(const config_t *cfg)
{
    g_cfg = cfg;

    if (!state_create(cfg))
        return 1;
    g_mq = oq_create(cfg->queue_cap);
    if (g_mq == (mqd_t)-1) {
        state_destroy();
        return 1;
    }

    file_create_truncate(LOG_SUBMITTED, LOG_MODE);
    file_create_truncate(LOG_PROCESSED, LOG_MODE);
    file_create_truncate(LOG_NOTIFY, LOG_MODE);

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    log_msg("SUPERVISOR",
            "level %d | %d producer(s) | %d..%d consumer(s) | queue size %d | "
            "priority %s | crash injection %s",
            cfg->level, cfg->producers, cfg->consumers, cfg->max_consumers,
            cfg->queue_cap, cfg->use_priority ? "ON" : "off",
            cfg->crash_every ? "ON" : "off");

    fflush(NULL);
    if (!cfg->quiet)
        spawn_dashboard();
    for (int s = 0; s < cfg->consumers; s++)
        spawn_consumer(s);
    for (int p = 0; p < cfg->producers; p++)
        spawn_producer(p);
    refresh_active_consumers();

    uint64_t last_done = 0, last_produced = 0;
    time_t   last_change = time(NULL);

    while (!g_shutdown) {
        reap_children();
        refresh_active_consumers();
        autoscale();

        int alive_prod = count_alive(prod_pid, MAX_PRODUCERS);
        uint64_t done = finished_orders();
        state_lock();
        uint64_t produced = g_state->stats.produced;
        state_unlock();

        if (alive_prod == 0 && done >= (uint64_t)g_state->total_expected)
            break;

        /* Watchdog: nothing moves any more -> stop instead of hanging. */
        if (done != last_done || produced != last_produced) {
            last_done = done;
            last_produced = produced;
            last_change = time(NULL);
        } else if (time(NULL) - last_change > 15) {
            log_msg("SUPERVISOR", "no progress for 15 s - aborting run");
            break;
        }
        usleep(50000);
    }

    if (g_shutdown)
        log_msg("SUPERVISOR", "signal received - shutting down");
    else
        log_msg("SUPERVISOR", "all orders processed - stopping workers");

    terminate_all();
    int rc = print_final_report();

    mq_close(g_mq);
    oq_destroy();
    state_destroy();
    return rc;
}

/* ======== main ======== */
/*
 * main.c - Command line parsing and level presets.
 *
 * Producer-Consumer with POSIX Message Queues + Semaphores
 * E-Commerce Order Processing System
 */
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static void usage(const char *prog)
{
    printf(
"Usage: %s [options]\n"
"  -l LEVEL   feature level 1..5 (default 5)\n"
"               1 single producer / single consumer\n"
"               2 + bounded buffer (small finite queue)\n"
"               3 + multiple producers & consumers\n"
"               4 + priority messages\n"
"               5 + preserve permissions (+ permission report)\n"
"  -p N       number of producers (order sources)   [max %d]\n"
"  -c N       number of consumers (initial/minimum) [max %d]\n"
"  -m N       maximum consumers for dynamic scaling\n"
"  -n N       orders submitted by each producer\n"
"  -q N       bounded buffer (queue) size\n"
"  -x N       crash a consumer on ~1 in N stages (failure recovery demo)\n"
"  -i MS      dashboard refresh interval in ms (default 1000)\n"
"  -s SEED    random seed\n"
"  -Q         quiet: no dashboard\n"
"  -h         this help\n", prog, MAX_PRODUCERS, MAX_CONSUMERS);
}

static void apply_level_defaults(config_t *c, int *p, int *cn, int *q, int *m)
{
    int dp, dc, dq;

    switch (c->level) {
    case 1:  dp = 1; dc = 1; dq = 10;                break;
    case 2:  dp = 1; dc = 1; dq = 3;                 break;
    case 3:  dp = 3; dc = 3; dq = DEFAULT_QUEUE_CAP; break;
    default: dp = 3; dc = 2; dq = DEFAULT_QUEUE_CAP; break;
    }
    if (*p  < 0) *p  = dp;
    if (*cn < 0) *cn = dc;
    if (*q  < 0) *q  = dq;

    c->use_priority = (c->level >= 4);               /* Level 4+ */

    if (*m < 0)
        *m = (c->level >= 5) ? ((*cn + 2 <= MAX_CONSUMERS) ? *cn + 2 : MAX_CONSUMERS)
                             : *cn;
}

int main(int argc, char **argv)
{
    config_t cfg = {0};
    int p = -1, c = -1, q = -1, m = -1, n = 30, opt;

    cfg.level            = 5;
    cfg.dash_interval_ms = 1000;
    cfg.seed             = 42;

    while ((opt = getopt(argc, argv, "l:p:c:m:n:q:x:i:s:Qh")) != -1) {
        switch (opt) {
        case 'l': cfg.level = atoi(optarg);            break;
        case 'p': p = atoi(optarg);                    break;
        case 'c': c = atoi(optarg);                    break;
        case 'm': m = atoi(optarg);                    break;
        case 'n': n = atoi(optarg);                    break;
        case 'q': q = atoi(optarg);                    break;
        case 'x': cfg.crash_every = atoi(optarg);      break;
        case 'i': cfg.dash_interval_ms = atoi(optarg); break;
        case 's': cfg.seed = (unsigned)atoi(optarg);   break;
        case 'Q': cfg.quiet = 1;                       break;
        default:  usage(argv[0]); return opt == 'h' ? 0 : 2;
        }
    }

    if (cfg.level < 1 || cfg.level > 5) {
        fprintf(stderr, "level must be 1..5\n");
        return 2;
    }
    apply_level_defaults(&cfg, &p, &c, &q, &m);

    long qmax = oq_system_max_msgs();
    if (p < 1 || p > MAX_PRODUCERS || c < 1 || c > MAX_CONSUMERS ||
        m < c || m > MAX_CONSUMERS || n < 1 || p * n > MAX_ORDERS ||
        q < 1 || q > qmax || cfg.dash_interval_ms < 100 || cfg.crash_every < 0) {
        fprintf(stderr,
                "invalid option values (producers 1..%d, consumers 1..%d, "
                "max>=consumers, orders<=%d total, queue 1..%ld)\n",
                MAX_PRODUCERS, MAX_CONSUMERS, MAX_ORDERS, qmax);
        return 2;
    }

    cfg.producers = p;
    cfg.consumers = c;
    cfg.max_consumers = m;
    cfg.orders_per_producer = n;
    cfg.queue_cap = q;

    setvbuf(stdout, NULL, _IOLBF, 0);
    return supervisor_run(&cfg);
}
