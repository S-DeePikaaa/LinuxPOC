/*
 * common.h - Single header for the whole project.
 *
 * Producer-Consumer with POSIX Message Queues + Semaphores
 * E-Commerce Order Processing System
 *
 * Project files (6):
 *   Makefile       build (make, make test, make clean)
 *   common.h       constants, types and declarations of every module
 *   ipc.c          logger, file handling, shared memory, bounded buffer
 *                  (POSIX mqueue + semaphores), order tracking service
 *   services.c     inventory (+synchronization), fraud, payment, shipping,
 *                  notification services
 *   workers.c      producer, consumer (+failure recovery), queue
 *                  monitoring dashboard
 *   supervisor.c   main(), fork()/signals, dynamic process management,
 *                  consumer restart, final report
 *
 * Levels (-l N): 1 single P/C | 2 bounded buffer | 3 multiple P & C |
 *                4 priority messages | 5 preserve permissions
 * Build/run on Linux (or WSL2 on Windows):  make && ./ecom_orders -h
 */
#ifndef COMMON_H
#define COMMON_H


/* ======== common ======== */
#if defined(_WIN32)
#error "POSIX message queues are not available natively on Windows. Build inside WSL2 (Ubuntu) - see README.md"
#endif

#include <stdint.h>
#include <signal.h>
#include <time.h>

/* ---------- Limits ---------- */
#define MAX_PRODUCERS      8
#define MAX_CONSUMERS      8
#define MAX_PRODUCTS       10
#define MAX_ORDERS         2000
#define DEFAULT_QUEUE_CAP  8
#define INITIAL_STOCK      30      /* units per product at start-up */

/* ---------- Fraud rules ---------- */
#define FRAUD_LIMIT_CENTS  300000  /* order value above $3000 is suspicious */
#define FRAUD_MAX_QTY      25      /* more than 25 units is suspicious      */

/* ---------- Simulated work (microseconds) ---------- */
#define PRODUCER_DELAY_US  8000
#define STAGE_DELAY_US     4000

/* ---------- IPC object names and permissions (Level 5) ---------- */
#define MQ_NAME   "/ecom_orders"
#define SHM_NAME  "/ecom_state"
#define MQ_MODE   0660
#define SHM_MODE  0600
#define LOG_MODE  0640

/* ---------- Files (File Handling) ---------- */
#define LOG_SUBMITTED "orders_submitted.log"
#define LOG_PROCESSED "orders_processed.log"
#define LOG_NOTIFY    "notifications.log"
#define LOG_ARCHIVE   "orders_archive.log"

/* ---------- Level 4: message priorities (higher = served first) ---------- */
typedef enum {
    PRIO_BULK    = 0,
    PRIO_NORMAL  = 1,
    PRIO_EXPRESS = 2
} order_priority_t;

#define PRIO_LEVELS 3

/* ---------- Order life cycle (also used for crash recovery) ---------- */
typedef enum {
    ORDER_NONE = 0,
    ORDER_SUBMITTED,
    ORDER_FRAUD_CLEARED,
    ORDER_PAID,
    ORDER_RESERVED,
    ORDER_SHIPPED,
    ORDER_COMPLETED,        /* terminal states start here */
    ORDER_REJECTED_FRAUD,
    ORDER_FAILED_PAYMENT,
    ORDER_FAILED_STOCK
} order_status_t;

#define ORDER_IS_TERMINAL(s) ((s) >= ORDER_COMPLETED)

/* ---------- Message sent through the POSIX message queue ---------- */
typedef struct {
    uint32_t order_id;
    int32_t  producer_id;
    int32_t  customer_id;
    int32_t  product_id;
    int32_t  quantity;
    int32_t  unit_price_cents;
    int32_t  priority;
    int64_t  created_at;
} order_msg_t;

/* ---------- Run-time configuration ---------- */
typedef struct {
    int level;               /* 1..5 feature preset                         */
    int producers;
    int consumers;           /* initial (and minimum) consumers             */
    int max_consumers;       /* upper bound for dynamic scaling             */
    int orders_per_producer;
    int queue_cap;           /* bounded buffer size                         */
    int crash_every;         /* 0 = off, N = consumer crashes ~1 in N stage */
    int use_priority;        /* Level 4                                     */
    int dash_interval_ms;    /* queue monitoring dashboard period           */
    int quiet;               /* 1 = no dashboard                            */
    unsigned seed;
} config_t;

/* ======== logger ======== */
/* logger.h - Atomic, timestamped console logging usable from many processes. */
void log_msg(const char *tag, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

/* ======== file_utils ======== */
/* file_utils.h - File handling helpers (logs, permission-preserving copy). */
#include <sys/types.h>

/* Create/truncate a file and force its permission bits (defeats umask). */
int file_create_truncate(const char *path, mode_t mode);

/* Append one line (adds '\n') using O_APPEND; creates file with LOG_MODE. */
int file_append_line(const char *path, const char *line);

/* Return permission bits (e.g. 0640) of a path, or -1 on error. */
int file_get_mode(const char *path);

/* Level 5: copy src -> dst preserving mode, owner (best effort) and times. */
int file_copy_preserve(const char *src, const char *dst);

/* ======== shm_state ======== */
/*
 * shm_state.h - State shared by every process (POSIX shared memory).
 *
 * Holds: the bounded-buffer semaphores, a process-shared robust mutex,
 * statistics, inventory, the order-tracking array and consumer slots
 * used for failure recovery.
 */
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <sys/types.h>

typedef struct {
    uint64_t produced;
    uint64_t received;
    uint64_t completed;
    uint64_t rejected_fraud;
    uint64_t failed_payment;
    uint64_t failed_stock;
    uint64_t recovered;          /* orders re-processed after a crash   */
    uint64_t consumer_crashes;
    uint64_t producer_blocked;   /* times a producer hit a full buffer  */
    uint64_t critical_events;
    uint64_t revenue_cents;
    uint64_t by_priority[PRIO_LEVELS];
} stats_t;

/* One slot per consumer process; survives the consumer's death. */
typedef struct {
    pid_t       pid;
    int         active;
    int         retiring;        /* asked to leave by the supervisor   */
    int         has_inflight;
    order_msg_t inflight;        /* order held while being processed   */
} consumer_slot_t;

typedef struct {
    pthread_mutex_t lock;        /* process-shared, robust (mutual exclusion) */
    sem_t           sem_empty;   /* free places in the bounded buffer         */
    sem_t           sem_full;    /* messages waiting in the queue             */

    int      queue_cap;
    int      total_expected;
    uint32_t next_order_id;
    int      active_consumers;
    int      mq_mode;            /* actual permission bits, for the report */
    int      shm_mode;

    stats_t  stats;

    int      initial_stock[MAX_PRODUCTS];
    int      stock[MAX_PRODUCTS];
    int      sold[MAX_PRODUCTS];

    order_status_t  status[MAX_ORDERS + 1];   /* indexed by order id */
    consumer_slot_t slots[MAX_CONSUMERS];
} shared_state_t;

extern shared_state_t *g_state;

shared_state_t *state_create(const config_t *cfg);   /* supervisor only */
void            state_destroy(void);                /* supervisor only */

int  state_lock(void);
void state_unlock(void);

/* ======== order_queue ======== */
/*
 * order_queue.h - Bounded buffer built from a POSIX message queue
 * plus two counting semaphores (empty / full).
 *
 *   Producer: acquire_slot -> (track order) -> send
 *   Consumer: receive -> (save in-flight copy) -> release_slot
 *
 * Messages with a higher priority are delivered first (Level 4).
 */
#include <fcntl.h>
#include <mqueue.h>
#include <signal.h>

long   oq_system_max_msgs(void);                 /* kernel limit mq_maxmsg   */
mqd_t  oq_create(int capacity);                  /* supervisor               */
mqd_t  oq_open(int flags);                       /* any other process        */
void   oq_destroy(void);                         /* supervisor               */
long   oq_depth(mqd_t mq);                       /* messages waiting         */

/* Return: 0 ok, 1 interrupted by *stop, -1 error */
int  oq_acquire_slot(volatile sig_atomic_t *stop, int *had_to_wait);
int  oq_send(mqd_t mq, const order_msg_t *o);
int  oq_receive(mqd_t mq, order_msg_t *o, unsigned *prio,
                volatile sig_atomic_t *stop);
void oq_release_slot(void);

/* ======== tracker ======== */
/* tracker.h - Order Tracking Service (array in shared memory + linked list report). */
const char    *status_name(order_status_t s);
order_status_t tracker_get(uint32_t order_id);
void           tracker_set(uint32_t order_id, order_status_t s);

/* Move an order to a terminal state and update statistics atomically. */
void tracker_finish(const order_msg_t *o, order_status_t terminal);
void tracker_finish_locked(const order_msg_t *o, order_status_t terminal);

/* Linked list of orders that did not complete normally. */
typedef struct pending_node {
    uint32_t             order_id;
    order_status_t       status;
    struct pending_node *next;
} pending_node_t;

pending_node_t *tracker_collect_exceptions(void);
void            tracker_free_list(pending_node_t *head);

/* ======== inventory ======== */
/* inventory.h - Inventory service + Inventory Synchronization check. */
void inventory_init(void);                       /* called with g_state ready */
int  inventory_price_cents(int product_id);

/* Atomically reserve stock and update the order's status.
 * Returns 0 on success, -1 if out of stock (order marked FAILED_STOCK). */
int  inventory_reserve_order(const order_msg_t *o);

/* Inventory synchronization: initial == stock + sold for every product.
 * Returns the number of inconsistent products (0 = in sync). */
int  inventory_verify(void);

/* ======== fraud ======== */
/* fraud.h - Fraud Detection Service. */
#include <stddef.h>
/* Returns 1 if the order looks fraudulent (reason is filled), else 0. */
int fraud_check(const order_msg_t *o, char *reason, size_t reason_len);

/* ======== payment ======== */
/* payment.h - Payment service (simulated gateway). */
/* Returns 0 if the charge succeeded, -1 if declined. */
int payment_charge(const order_msg_t *o, unsigned *seed);

/* ======== shipping ======== */
/* shipping.h - Shipping service. */
#include <stddef.h>
/* Creates a shipment; the tracking number is derived from the order id,
 * so re-running it after a crash gives the same result (idempotent). */
void shipping_dispatch(const order_msg_t *o, char *tracking, size_t len);

/* ======== notification ======== */
/* notification.h - Customer notification service (writes notifications.log). */
void notification_send(const order_msg_t *o, const char *message);

/* ======== producer ======== */
/* producer.h - Order source process (web / mobile / marketplace). */
/* Runs in a child process created with fork(). Returns when done. */
void producer_main(int producer_id, const config_t *cfg);

/* ======== consumer ======== */
/* consumer.h - Order processing worker (fraud -> payment -> inventory ->
 * shipping -> notification) with crash recovery. */
/* Runs in a child process. `slot` indexes g_state->slots[]. */
void consumer_main(int slot, const config_t *cfg);

/* ======== dashboard ======== */
/* dashboard.h - Queue Monitoring Dashboard (runs as its own process). */
void dashboard_main(const config_t *cfg);

/* ======== supervisor ======== */
/* supervisor.h - Process supervisor: fork(), signals, scaling, recovery. */
/* Returns 0 when every order was accounted for and inventory is consistent. */
int supervisor_run(const config_t *cfg);

#endif /* COMMON_H */
