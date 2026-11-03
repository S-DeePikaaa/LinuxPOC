/* workers.c - Producer, consumer (failure recovery) and queue monitoring dashboard. */
#include "common.h"

/* ======== producer ======== */
/* producer.c - Generates orders and submits them to the bounded queue. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t p_g_stop = 0;

static void p_on_stop(int sig)
{
    (void)sig;
    p_g_stop = 1;
}

static void p_install_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = p_on_stop;          /* no SA_RESTART: blocking calls return */
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
}

static const char *prio_name(int p)
{
    return p == PRIO_EXPRESS ? "EXPRESS" : p == PRIO_BULK ? "BULK" : "NORMAL";
}

static void generate_order(order_msg_t *o, int producer_id,
                           const config_t *cfg, unsigned *seed)
{
    memset(o, 0, sizeof *o);
    o->producer_id = producer_id;
    o->customer_id = 1000 + rand_r(seed) % 50;
    o->product_id  = rand_r(seed) % MAX_PRODUCTS;
    o->quantity    = 1 + rand_r(seed) % 5;
    if (rand_r(seed) % 100 < 3)                  /* ~3% suspicious bulk buy */
        o->quantity = FRAUD_MAX_QTY + 5 + rand_r(seed) % 10;
    o->unit_price_cents = inventory_price_cents(o->product_id);
    o->created_at = (int64_t)time(NULL);

    if (cfg->use_priority) {                     /* Level 4 */
        int r = rand_r(seed) % 100;
        o->priority = (r < 15) ? PRIO_EXPRESS : (r < 35) ? PRIO_BULK : PRIO_NORMAL;
    } else {
        o->priority = PRIO_NORMAL;
    }
}

void producer_main(int producer_id, const config_t *cfg)
{
    p_install_handlers();

    mqd_t mq = oq_open(O_WRONLY);
    if (mq == (mqd_t)-1)
        return;

    unsigned seed = cfg->seed + (unsigned)producer_id * 7919u + (unsigned)getpid();
    log_msg("PRODUCER", "source #%d started, will submit %d orders",
            producer_id, cfg->orders_per_producer);

    for (int i = 0; i < cfg->orders_per_producer && !p_g_stop; i++) {
        order_msg_t o;
        generate_order(&o, producer_id, cfg, &seed);

        /* (1) wait for a free place - this is the bounded buffer. */
        int waited = 0;
        if (oq_acquire_slot(&p_g_stop, &waited) != 0)
            break;
        if (waited) {
            state_lock();
            g_state->stats.producer_blocked++;
            state_unlock();
        }

        /* (2) register the order. */
        state_lock();
        o.order_id = g_state->next_order_id++;
        g_state->status[o.order_id] = ORDER_SUBMITTED;
        g_state->stats.produced++;
        g_state->stats.by_priority[o.priority]++;
        state_unlock();

        /* (3) publish it and signal the consumers. */
        if (oq_send(mq, &o) != 0) {
            log_msg("PRODUCER", "send failed for order %u", o.order_id);
            continue;
        }

        char line[200];
        snprintf(line, sizeof line, "order=%u producer=%d customer=%d product=%d qty=%d prio=%s%s",
                 o.order_id, producer_id, o.customer_id, o.product_id, o.quantity,
                 prio_name(o.priority), waited ? " (waited: queue full)" : "");
        file_append_line(LOG_SUBMITTED, line);
        log_msg("PRODUCER", "src#%d -> order %u [%s] prod=%d qty=%d%s",
                producer_id, o.order_id, prio_name(o.priority), o.product_id,
                o.quantity, waited ? "  (queue was full, waited)" : "");

        usleep(PRODUCER_DELAY_US);
    }

    mq_close(mq);
    log_msg("PRODUCER", "source #%d finished", producer_id);
}

/* ======== consumer ======== */
/* consumer.c - Consumer worker with Consumer Failure Recovery.
 *
 * Every order moves through persistent stages stored in shared memory
 * (tracker). If a consumer dies, the supervisor starts a new consumer in
 * the same slot; it finds the in-flight order in the slot and resumes it
 * from the last completed stage.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t c_g_stop = 0;

static void c_on_stop(int sig)
{
    (void)sig;
    c_g_stop = 1;                       /* SIGTERM, SIGINT, SIGUSR1 (retire) */
}

static void c_install_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = c_on_stop;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGUSR1, &sa, NULL);
}

/* Failure injection used to demonstrate recovery (option -x). */
static void maybe_crash(int crash_every, unsigned *seed, uint32_t order_id,
                        const char *where)
{
    if (crash_every > 0 && (int)(rand_r(seed) % (unsigned)crash_every) == 0) {
        log_msg("CONSUMER", "!!! simulated crash while handling order %u (after %s)",
                order_id, where);
        raise(SIGKILL);
    }
}

/* Runs the pipeline, resuming from whatever stage is already recorded. */
static order_status_t process_order(const order_msg_t *o, unsigned *seed,
                                    int crash_every)
{
    order_status_t st = tracker_get(o->order_id);
    char text[160];

    if (ORDER_IS_TERMINAL(st))                    /* already done: skip */
        return st;

    /* --- Stage 1: fraud detection --- */
    if (st < ORDER_FRAUD_CLEARED) {
        char why[96];
        if (fraud_check(o, why, sizeof why)) {
            log_msg("ALERT", "CRITICAL: order %u rejected by fraud service (%s)",
                    o->order_id, why);
            snprintf(text, sizeof text, "order rejected: %s", why);
            notification_send(o, text);
            tracker_finish(o, ORDER_REJECTED_FRAUD);
            return ORDER_REJECTED_FRAUD;
        }
        tracker_set(o->order_id, ORDER_FRAUD_CLEARED);
        st = ORDER_FRAUD_CLEARED;
        maybe_crash(crash_every, seed, o->order_id, "fraud check");
    }

    /* --- Stage 2: payment --- */
    if (st < ORDER_PAID) {
        if (payment_charge(o, seed) != 0) {
            notification_send(o, "payment declined");
            tracker_finish(o, ORDER_FAILED_PAYMENT);
            return ORDER_FAILED_PAYMENT;
        }
        tracker_set(o->order_id, ORDER_PAID);
        st = ORDER_PAID;
        maybe_crash(crash_every, seed, o->order_id, "payment");
    }

    /* --- Stage 3: inventory (mutual exclusion inside) --- */
    if (st < ORDER_RESERVED) {
        if (inventory_reserve_order(o) != 0) {
            notification_send(o, "out of stock - payment refunded");
            return ORDER_FAILED_STOCK;
        }
        st = ORDER_RESERVED;
        maybe_crash(crash_every, seed, o->order_id, "inventory reservation");
    }

    /* --- Stage 4: shipping --- */
    char trk[32] = "";
    if (st < ORDER_SHIPPED) {
        shipping_dispatch(o, trk, sizeof trk);
        tracker_set(o->order_id, ORDER_SHIPPED);
        st = ORDER_SHIPPED;
        maybe_crash(crash_every, seed, o->order_id, "shipping");
    } else {
        snprintf(trk, sizeof trk, "TRK%08u", o->order_id);
    }

    /* --- Stage 5: notification + completion --- */
    snprintf(text, sizeof text, "order shipped, tracking %s", trk);
    notification_send(o, text);
    tracker_finish(o, ORDER_COMPLETED);
    return ORDER_COMPLETED;
}

static void handle_order(int slot, const order_msg_t *o, unsigned prio,
                         unsigned *seed, int crash_every, int recovered)
{
    order_status_t final = process_order(o, seed, crash_every);

    char line[200];
    snprintf(line, sizeof line, "order=%u status=%s consumer_slot=%d pid=%d%s",
             o->order_id, status_name(final), slot, (int)getpid(),
             recovered ? " (recovered)" : "");
    file_append_line(LOG_PROCESSED, line);

    log_msg("CONSUMER", "slot%d <- order %u [prio %u] => %s%s", slot,
            o->order_id, prio, status_name(final),
            recovered ? "  (recovered after crash)" : "");
}

void consumer_main(int slot, const config_t *cfg)
{
    c_install_handlers();

    mqd_t mq = oq_open(O_RDONLY);
    if (mq == (mqd_t)-1)
        return;

    consumer_slot_t *me = &g_state->slots[slot];
    me->pid = getpid();
    unsigned seed = cfg->seed * 31u + (unsigned)getpid() + (unsigned)slot;

    log_msg("CONSUMER", "worker slot %d started", slot);

    /* Consumer Failure Recovery: finish the order a dead predecessor held. */
    if (me->has_inflight) {
        order_msg_t o = me->inflight;
        log_msg("RECOVERY", "slot %d picked up unfinished order %u", slot, o.order_id);
        state_lock();
        g_state->stats.recovered++;
        state_unlock();
        handle_order(slot, &o, o.priority, &seed, cfg->crash_every, 1);
        me->has_inflight = 0;
    }

    while (!c_g_stop) {
        order_msg_t o;
        unsigned prio = 0;

        int rc = oq_receive(mq, &o, &prio, &c_g_stop);
        if (rc == 1)
            break;                                /* asked to stop */
        if (rc < 0) {
            usleep(10000);
            continue;
        }

        /* Keep a copy BEFORE freeing the buffer place, so it can't be lost. */
        me->inflight = o;
        __sync_synchronize();
        me->has_inflight = 1;
        oq_release_slot();

        state_lock();
        g_state->stats.received++;
        state_unlock();

        handle_order(slot, &o, prio, &seed, cfg->crash_every, 0);
        me->has_inflight = 0;
    }

    mq_close(mq);
    log_msg("CONSUMER", "worker slot %d exiting cleanly", slot);
}

/* ======== dashboard ======== */
/* dashboard.c - Periodically prints queue depth and statistics read from
 * shared memory, and runs the inventory synchronization check. */
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t d_g_stop = 0;

static void d_on_stop(int sig)
{
    (void)sig;
    d_g_stop = 1;
}

static void make_bar(char *bar, size_t width, long depth, int cap)
{
    size_t fill = (cap > 0 && depth > 0) ? (size_t)((depth * (long)width) / cap) : 0;
    if (fill > width)
        fill = width;
    for (size_t i = 0; i < width; i++)
        bar[i] = (i < fill) ? '#' : '.';
    bar[width] = '\0';
}

void dashboard_main(const config_t *cfg)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = d_on_stop;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    mqd_t mq = oq_open(O_RDONLY);
    if (mq == (mqd_t)-1)
        return;

    while (!d_g_stop) {
        for (int waited = 0; waited < cfg->dash_interval_ms && !d_g_stop; waited += 100)
            usleep(100 * 1000);
        if (d_g_stop)
            break;

        long depth = oq_depth(mq);
        int  bad   = inventory_verify();

        state_lock();
        stats_t s = g_state->stats;
        int consumers = g_state->active_consumers;
        int cap = g_state->queue_cap;
        state_unlock();

        char bar[21];
        make_bar(bar, 20, depth, cap);

        log_msg("DASHBOARD",
                "queue [%s] %ld/%d | produced %llu received %llu done %llu | "
                "fraud %llu pay-fail %llu no-stock %llu | consumers %d "
                "crashes %llu recovered %llu | blocked %llu | inventory %s",
                bar, depth, cap,
                (unsigned long long)s.produced, (unsigned long long)s.received,
                (unsigned long long)s.completed,
                (unsigned long long)s.rejected_fraud,
                (unsigned long long)s.failed_payment,
                (unsigned long long)s.failed_stock, consumers,
                (unsigned long long)s.consumer_crashes,
                (unsigned long long)s.recovered,
                (unsigned long long)s.producer_blocked,
                bad == 0 ? "in sync" : "MISMATCH");
    }

    mq_close(mq);
}
