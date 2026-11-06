// tests.cpp - GoogleTest unit + integration tests for the order system.
// Built by `make gtest`; the C sources are compiled as C and linked in.
#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

extern "C" {
#include "common.h"
}

static config_t make_cfg(int producers, int consumers, int orders, int cap)
{
    config_t c;
    memset(&c, 0, sizeof c);
    c.level = 5;
    c.producers = producers;
    c.consumers = consumers;
    c.max_consumers = consumers + 1;
    c.orders_per_producer = orders;
    c.queue_cap = cap;
    c.use_priority = 1;
    c.dash_interval_ms = 1000;
    c.quiet = 1;
    c.seed = 7;
    return c;
}

static order_msg_t make_order(uint32_t id, int product, int qty, int prio = PRIO_NORMAL)
{
    order_msg_t o;
    memset(&o, 0, sizeof o);
    o.order_id = id;
    o.customer_id = 1001;
    o.product_id = product;
    o.quantity = qty;
    o.unit_price_cents = inventory_price_cents(product);
    o.priority = prio;
    return o;
}

/* Every test gets a fresh shared-memory segment and message queue. */
class OrderSystem : public ::testing::Test {
protected:
    mqd_t mq = (mqd_t)-1;
    config_t cfg;

    void SetUp() override
    {
        cfg = make_cfg(1, 1, 5, 4);
        ASSERT_NE(state_create(&cfg), nullptr);
        mq = oq_create(cfg.queue_cap);
        ASSERT_NE(mq, (mqd_t)-1);
    }
    void TearDown() override
    {
        if (mq != (mqd_t)-1)
            mq_close(mq);
        oq_destroy();
        state_destroy();
    }
};

/* ---------------- Fraud detection ---------------- */
TEST_F(OrderSystem, FraudAcceptsNormalOrder)
{
    char why[64];
    order_msg_t o = make_order(1, 0, 2);
    EXPECT_EQ(fraud_check(&o, why, sizeof why), 0);
}

TEST_F(OrderSystem, FraudRejectsLargeQuantity)
{
    char why[64];
    order_msg_t o = make_order(1, 8, FRAUD_MAX_QTY + 1);   /* cheap item, many units */
    EXPECT_EQ(fraud_check(&o, why, sizeof why), 1);
    EXPECT_NE(strstr(why, "quantity"), nullptr);
}

TEST_F(OrderSystem, FraudRejectsHighValue)
{
    char why[64];
    order_msg_t o = make_order(1, 3, FRAUD_MAX_QTY);       /* 25 x $129.99 > $3000 */
    EXPECT_EQ(fraud_check(&o, why, sizeof why), 1);
    EXPECT_NE(strstr(why, "value"), nullptr);
}

/* ---------------- Payment / shipping / notification ---------------- */
TEST_F(OrderSystem, PaymentDeclinesOnlySometimes)
{
    order_msg_t o = make_order(1, 0, 1);
    unsigned seed = 12345;
    int declined = 0;
    for (int i = 0; i < 150; i++)
        if (payment_charge(&o, &seed) != 0)
            declined++;
    EXPECT_GT(declined, 0);
    EXPECT_LT(declined, 50);
}

TEST_F(OrderSystem, ShippingTrackingNumberIsDeterministic)
{
    char a[32], b[32];
    order_msg_t o = make_order(42, 0, 1);
    shipping_dispatch(&o, a, sizeof a);
    shipping_dispatch(&o, b, sizeof b);
    EXPECT_STREQ(a, "TRK00000042");
    EXPECT_STREQ(a, b);                       /* idempotent for crash recovery */
}

TEST_F(OrderSystem, NotificationAppendsToLog)
{
    unlink(LOG_NOTIFY);
    order_msg_t o = make_order(9, 0, 1);
    notification_send(&o, "hello");
    FILE *f = fopen(LOG_NOTIFY, "r");
    ASSERT_NE(f, nullptr);
    char line[256] = "";
    ASSERT_NE(fgets(line, sizeof line, f), nullptr);
    fclose(f);
    EXPECT_NE(strstr(line, "order=9"), nullptr);
    EXPECT_NE(strstr(line, "hello"), nullptr);
    unlink(LOG_NOTIFY);
}

/* ---------------- Inventory ---------------- */
TEST_F(OrderSystem, InventoryPriceList)
{
    EXPECT_EQ(inventory_price_cents(0), 1999);
    EXPECT_EQ(inventory_price_cents(3), 12999);
    EXPECT_EQ(inventory_price_cents(13), 12999);          /* wraps modulo MAX_PRODUCTS */
}

TEST_F(OrderSystem, InventoryReserveReducesStock)
{
    order_msg_t o = make_order(1, 2, 5);
    ASSERT_EQ(inventory_reserve_order(&o), 0);
    EXPECT_EQ(g_state->stock[2], INITIAL_STOCK - 5);
    EXPECT_EQ(g_state->sold[2], 5);
    EXPECT_EQ(g_state->status[1], ORDER_RESERVED);
    EXPECT_EQ(inventory_verify(), 0);
}

TEST_F(OrderSystem, InventoryRefusesOversell)
{
    order_msg_t o = make_order(1, 2, INITIAL_STOCK + 1);
    EXPECT_EQ(inventory_reserve_order(&o), -1);
    EXPECT_EQ(g_state->stock[2], INITIAL_STOCK);          /* untouched */
    EXPECT_EQ(g_state->status[1], ORDER_FAILED_STOCK);
    EXPECT_EQ(g_state->stats.failed_stock, 1u);
    EXPECT_EQ(inventory_verify(), 0);
}

TEST_F(OrderSystem, InventoryVerifyDetectsCorruption)
{
    g_state->stock[4] -= 3;                               /* stock vanishes */
    EXPECT_EQ(inventory_verify(), 1);
    g_state->stock[4] = -1;                               /* negative stock */
    EXPECT_GE(inventory_verify(), 1);
}

/* ---------------- Order tracking ---------------- */
TEST_F(OrderSystem, TrackerStatusNames)
{
    EXPECT_STREQ(status_name(ORDER_NONE), "NONE");
    EXPECT_STREQ(status_name(ORDER_SUBMITTED), "SUBMITTED");
    EXPECT_STREQ(status_name(ORDER_FRAUD_CLEARED), "FRAUD_CLEARED");
    EXPECT_STREQ(status_name(ORDER_PAID), "PAID");
    EXPECT_STREQ(status_name(ORDER_RESERVED), "RESERVED");
    EXPECT_STREQ(status_name(ORDER_SHIPPED), "SHIPPED");
    EXPECT_STREQ(status_name(ORDER_COMPLETED), "COMPLETED");
    EXPECT_STREQ(status_name(ORDER_REJECTED_FRAUD), "REJECTED_FRAUD");
    EXPECT_STREQ(status_name(ORDER_FAILED_PAYMENT), "FAILED_PAYMENT");
    EXPECT_STREQ(status_name(ORDER_FAILED_STOCK), "FAILED_STOCK");
    EXPECT_STREQ(status_name((order_status_t)99), "?");
}

TEST_F(OrderSystem, TrackerSetGetAndTerminal)
{
    tracker_set(5, ORDER_PAID);
    EXPECT_EQ(tracker_get(5), ORDER_PAID);
    EXPECT_FALSE(ORDER_IS_TERMINAL(ORDER_SHIPPED));
    EXPECT_TRUE(ORDER_IS_TERMINAL(ORDER_COMPLETED));
    EXPECT_TRUE(ORDER_IS_TERMINAL(ORDER_FAILED_STOCK));
}

TEST_F(OrderSystem, TrackerFinishUpdatesStatistics)
{
    order_msg_t a = make_order(1, 0, 2), b = make_order(2, 0, 1),
                c = make_order(3, 0, 1), d = make_order(4, 0, 1);
    tracker_finish(&a, ORDER_COMPLETED);
    tracker_finish(&b, ORDER_REJECTED_FRAUD);
    tracker_finish(&c, ORDER_FAILED_PAYMENT);
    tracker_finish(&d, ORDER_PAID);                       /* non-terminal: no counter */
    EXPECT_EQ(g_state->stats.completed, 1u);
    EXPECT_EQ(g_state->stats.revenue_cents, 2u * 1999u);
    EXPECT_EQ(g_state->stats.rejected_fraud, 1u);
    EXPECT_EQ(g_state->stats.critical_events, 1u);
    EXPECT_EQ(g_state->stats.failed_payment, 1u);
}

TEST_F(OrderSystem, TrackerExceptionLinkedList)
{
    g_state->next_order_id = 5;                           /* ids 1..4 exist */
    g_state->status[1] = ORDER_COMPLETED;
    g_state->status[2] = ORDER_REJECTED_FRAUD;
    g_state->status[3] = ORDER_COMPLETED;
    g_state->status[4] = ORDER_PAID;

    pending_node_t *head = tracker_collect_exceptions();
    ASSERT_NE(head, nullptr);
    EXPECT_EQ(head->order_id, 2u);
    ASSERT_NE(head->next, nullptr);
    EXPECT_EQ(head->next->order_id, 4u);
    EXPECT_EQ(head->next->next, nullptr);
    tracker_free_list(head);
    tracker_free_list(nullptr);                           /* must be a safe no-op */
}

/* ---------------- Shared state / mutex ---------------- */
TEST_F(OrderSystem, SharedStateInitialValues)
{
    EXPECT_EQ(g_state->queue_cap, 4);
    EXPECT_EQ(g_state->total_expected, 5);
    EXPECT_EQ(g_state->next_order_id, 1u);
    EXPECT_EQ(g_state->shm_mode, SHM_MODE);               /* Level 5: permissions */
    EXPECT_EQ(g_state->mq_mode, MQ_MODE);
    for (int i = 0; i < MAX_PRODUCTS; i++)
        EXPECT_EQ(g_state->stock[i], INITIAL_STOCK);
}

TEST_F(OrderSystem, RobustMutexRecoversFromDeadOwner)
{
    pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        state_lock();                                     /* die holding the lock */
        CHILD_EXIT(0);
    }
    int st = 0;
    waitpid(pid, &st, 0);
    EXPECT_EQ(state_lock(), 0);                           /* EOWNERDEAD handled */
    state_unlock();
    EXPECT_EQ(state_lock(), 0);                           /* and it keeps working */
    state_unlock();
}

/* ---------------- Bounded buffer + priority ---------------- */
TEST_F(OrderSystem, QueueDeliversHighestPriorityFirst)
{
    volatile sig_atomic_t stop = 0;
    int waited;
    order_msg_t bulk = make_order(1, 0, 1, PRIO_BULK);
    order_msg_t norm = make_order(2, 0, 1, PRIO_NORMAL);
    order_msg_t expr = make_order(3, 0, 1, PRIO_EXPRESS);

    for (order_msg_t *o : {&bulk, &norm, &expr}) {
        ASSERT_EQ(oq_acquire_slot(&stop, &waited), 0);
        ASSERT_EQ(oq_send(mq, o), 0);
    }
    EXPECT_EQ(oq_depth(mq), 3);

    unsigned prio = 99;
    order_msg_t out;
    ASSERT_EQ(oq_receive(mq, &out, &prio, &stop), 0);
    EXPECT_EQ(out.order_id, 3u);
    EXPECT_EQ(prio, (unsigned)PRIO_EXPRESS);
    ASSERT_EQ(oq_receive(mq, &out, &prio, &stop), 0);
    EXPECT_EQ(out.order_id, 2u);
    ASSERT_EQ(oq_receive(mq, &out, &prio, &stop), 0);
    EXPECT_EQ(out.order_id, 1u);
    EXPECT_EQ(oq_depth(mq), 0);
}

TEST_F(OrderSystem, QueueFifoWithinSamePriority)
{
    volatile sig_atomic_t stop = 0;
    int waited;
    unsigned prio;
    order_msg_t out;
    for (uint32_t id = 1; id <= 3; id++) {
        order_msg_t o = make_order(id, 0, 1);
        ASSERT_EQ(oq_acquire_slot(&stop, &waited), 0);
        ASSERT_EQ(oq_send(mq, &o), 0);
    }
    for (uint32_t id = 1; id <= 3; id++) {
        ASSERT_EQ(oq_receive(mq, &out, &prio, &stop), 0);
        EXPECT_EQ(out.order_id, id);
    }
}

TEST_F(OrderSystem, BoundedBufferBlocksWhenFull)
{
    volatile sig_atomic_t stop = 0;
    int waited = 0;
    for (int i = 0; i < 4; i++) {                         /* capacity is 4 */
        order_msg_t o = make_order((uint32_t)i + 1, 0, 1);
        ASSERT_EQ(oq_acquire_slot(&stop, &waited), 0);
        EXPECT_EQ(waited, 0);
        ASSERT_EQ(oq_send(mq, &o), 0);
    }
    stop = 1;                                             /* a 5th producer would wait... */
    EXPECT_EQ(oq_acquire_slot(&stop, &waited), 1);        /* ...until told to stop */
    EXPECT_EQ(waited, 1);
}

TEST_F(OrderSystem, ReceiveOnEmptyQueueHonoursStopFlag)
{
    volatile sig_atomic_t stop = 1;
    order_msg_t out;
    unsigned prio;
    EXPECT_EQ(oq_receive(mq, &out, &prio, &stop), 1);
}

TEST_F(OrderSystem, ReleaseSlotMakesRoomAgain)
{
    volatile sig_atomic_t stop = 0;
    int waited;
    for (int i = 0; i < 4; i++) {
        order_msg_t o = make_order((uint32_t)i + 1, 0, 1);
        ASSERT_EQ(oq_acquire_slot(&stop, &waited), 0);
        ASSERT_EQ(oq_send(mq, &o), 0);
    }
    oq_release_slot();
    EXPECT_EQ(oq_acquire_slot(&stop, &waited), 0);
    EXPECT_EQ(waited, 0);
}

TEST(QueueLimits, SystemMaximumIsPositive)
{
    EXPECT_GE(oq_system_max_msgs(), 1);
}

TEST(QueueLimits, OpenMissingQueueFails)
{
    mq_unlink(MQ_NAME);
    EXPECT_EQ(oq_open(O_RDONLY), (mqd_t)-1);
}

/* ---------------- File handling (Level 5 permissions) ---------------- */
class Files : public ::testing::Test {
protected:
    const char *a = "gtest_tmp_a.log";
    const char *b = "gtest_tmp_b.log";
    void TearDown() override { unlink(a); unlink(b); }
};

TEST_F(Files, CreateForcesRequestedMode)
{
    mode_t old = umask(0277);                             /* hostile umask */
    ASSERT_EQ(file_create_truncate(a, LOG_MODE), 0);
    umask(old);
    EXPECT_EQ(file_get_mode(a), (int)LOG_MODE);
}

TEST_F(Files, AppendWritesLinesInOrder)
{
    unlink(a);
    ASSERT_EQ(file_append_line(a, "one"), 0);
    ASSERT_EQ(file_append_line(a, "two"), 0);
    FILE *f = fopen(a, "r");
    ASSERT_NE(f, nullptr);
    char l1[32], l2[32];
    ASSERT_NE(fgets(l1, sizeof l1, f), nullptr);
    ASSERT_NE(fgets(l2, sizeof l2, f), nullptr);
    fclose(f);
    EXPECT_STREQ(l1, "one\n");
    EXPECT_STREQ(l2, "two\n");
}

TEST_F(Files, CopyPreservesModeAndMtime)
{
    ASSERT_EQ(file_create_truncate(a, 0640), 0);
    ASSERT_EQ(file_append_line(a, "payload"), 0);
    chmod(a, 0604);
    struct timespec t[2] = {{1000000000, 0}, {1000000000, 0}};
    utimensat(AT_FDCWD, a, t, 0);

    ASSERT_EQ(file_copy_preserve(a, b), 0);
    struct stat sa, sb;
    ASSERT_EQ(stat(a, &sa), 0);
    ASSERT_EQ(stat(b, &sb), 0);
    EXPECT_EQ(sb.st_mode & 07777, sa.st_mode & 07777);
    EXPECT_EQ(sb.st_mtime, sa.st_mtime);
    EXPECT_EQ(sb.st_size, sa.st_size);
}

TEST_F(Files, ErrorsAreReported)
{
    EXPECT_EQ(file_copy_preserve("gtest_does_not_exist.log", b), -1);
    EXPECT_EQ(file_get_mode("gtest_does_not_exist.log"), -1);
    EXPECT_EQ(file_create_truncate("/nonexistent_dir/x.log", 0600), -1);
    EXPECT_EQ(file_append_line("/nonexistent_dir/x.log", "x"), -1);
}

TEST(Logger, HandlesLongMessages)
{
    std::string big(2000, 'x');
    log_msg("TEST", "short message %d", 1);
    log_msg("TEST", "%s", big.c_str());                   /* truncated, not overflowed */
    SUCCEED();
}

/* ---------------- Whole system (fork, signals, recovery) ---------------- */
TEST(Integration, SingleProducerSingleConsumer)           /* Level 1 */
{
    config_t c = make_cfg(1, 1, 8, 10);
    c.use_priority = 0;
    c.max_consumers = 1;
    EXPECT_EQ(supervisor_run(&c), 0);
}

TEST(Integration, SmallBoundedBuffer)                     /* Level 2 */
{
    config_t c = make_cfg(1, 1, 10, 2);
    c.use_priority = 0;
    c.max_consumers = 1;
    EXPECT_EQ(supervisor_run(&c), 0);
}

TEST(Integration, MultipleProducersAndConsumersWithPriority)   /* Level 3 + 4 */
{
    config_t c = make_cfg(3, 3, 8, 6);
    c.max_consumers = 3;
    EXPECT_EQ(supervisor_run(&c), 0);
}

TEST(Integration, AutoscalingAndPermissionReport)         /* dynamic process mgmt, Level 5 */
{
    config_t c = make_cfg(4, 1, 15, 4);
    c.max_consumers = 3;
    EXPECT_EQ(supervisor_run(&c), 0);
    EXPECT_EQ(file_get_mode(LOG_ARCHIVE), file_get_mode(LOG_SUBMITTED));
}

TEST(Integration, DashboardRuns)                          /* Queue Monitoring Dashboard */
{
    config_t c = make_cfg(2, 1, 20, 4);
    c.quiet = 0;
    c.dash_interval_ms = 100;
    EXPECT_EQ(supervisor_run(&c), 0);
}

TEST(Integration, ConsumerCrashRecovery)                  /* Consumer Failure Recovery */
{
    config_t c = make_cfg(2, 2, 15, 4);
    c.crash_every = 6;
    EXPECT_EQ(supervisor_run(&c), 0);
}
