/* services.c - Inventory, fraud, payment, shipping and notification services. */
#include "common.h"

/* ======== inventory ======== */
/* inventory.c - Stock reservation protected by the shared mutex. */
/* Static price list (cents). */
static const int PRICE_CENTS[MAX_PRODUCTS] = {
    1999, 4999, 799, 12999, 2499, 999, 7999, 3499, 599, 9999
};

void inventory_init(void)
{
    for (int i = 0; i < MAX_PRODUCTS; i++) {
        g_state->initial_stock[i] = INITIAL_STOCK;
        g_state->stock[i]         = INITIAL_STOCK;
        g_state->sold[i]          = 0;
    }
}

int inventory_price_cents(int product_id)
{
    return PRICE_CENTS[product_id % MAX_PRODUCTS];
}

int inventory_reserve_order(const order_msg_t *o)
{
    int p  = o->product_id % MAX_PRODUCTS;
    int rc = 0;

    state_lock();                          /* critical section begins */
    if (g_state->stock[p] >= o->quantity) {
        g_state->stock[p] -= o->quantity;
        g_state->sold[p]  += o->quantity;
        g_state->status[o->order_id] = ORDER_RESERVED;
    } else {
        tracker_finish_locked(o, ORDER_FAILED_STOCK);
        rc = -1;
    }
    state_unlock();                        /* critical section ends   */
    return rc;
}

int inventory_verify(void)
{
    int bad = 0;

    state_lock();
    for (int i = 0; i < MAX_PRODUCTS; i++) {
        if (g_state->stock[i] < 0 ||
            g_state->initial_stock[i] != g_state->stock[i] + g_state->sold[i])
            bad++;
    }
    state_unlock();
    return bad;
}

/* ======== fraud ======== */
/* fraud.c - Simple rule-based fraud detection. */
#include <stdio.h>
#include <unistd.h>

int fraud_check(const order_msg_t *o, char *reason, size_t reason_len)
{
    usleep(STAGE_DELAY_US / 2);

    long long total = (long long)o->quantity * o->unit_price_cents;

    if (o->quantity > FRAUD_MAX_QTY) {
        snprintf(reason, reason_len, "quantity %d exceeds limit %d",
                 o->quantity, FRAUD_MAX_QTY);
        return 1;
    }
    if (total > FRAUD_LIMIT_CENTS) {
        snprintf(reason, reason_len, "value $%lld.%02lld exceeds limit",
                 total / 100, total % 100);
        return 1;
    }
    reason[0] = '\0';
    return 0;
}

/* ======== payment ======== */
/* payment.c - Simulated payment gateway: ~8% of cards are declined. */
#include <stdlib.h>
#include <unistd.h>

int payment_charge(const order_msg_t *o, unsigned *seed)
{
    (void)o;
    usleep(STAGE_DELAY_US);
    return (rand_r(seed) % 100 < 8) ? -1 : 0;
}

/* ======== shipping ======== */
/* shipping.c - Shipping service. */
#include <stdio.h>
#include <unistd.h>

void shipping_dispatch(const order_msg_t *o, char *tracking, size_t len)
{
    usleep(STAGE_DELAY_US);
    snprintf(tracking, len, "TRK%08u", o->order_id);
}

/* ======== notification ======== */
/* notification.c - Appends customer notifications to a log file. */
#include <stdio.h>
#include <unistd.h>

void notification_send(const order_msg_t *o, const char *message)
{
    char line[256];

    usleep(STAGE_DELAY_US / 2);
    snprintf(line, sizeof line, "order=%u customer=%d : %s",
             o->order_id, o->customer_id, message);
    file_append_line(LOG_NOTIFY, line);
}
