// test.cpp - GoogleTest suite for the E-Commerce Order Processing System
//
// Covers the IPC building blocks the project relies on (POSIX message queue,
// semaphores, shared memory, robust mutex) and the behaviours built on top of
// them (bounded buffer, priority, atomic inventory, crash recovery).
//
// This file uses only POSIX APIs, so it builds without your headers.
// To test your own functions (oq_acquire_slot(), etc.), add tests in the
// "PROJECT HOOKS" section at the bottom.

#include <gtest/gtest.h>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <mqueue.h>
#include <pthread.h>
#include <semaphore.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

constexpr int kSlots = 4;
constexpr int kStock = 10;

// Mirrors the kind of state the project keeps in shared memory.
enum Stage { STAGE_NONE = 0, STAGE_FRAUD, STAGE_PAYMENT, STAGE_INVENTORY, STAGE_SHIPPING, STAGE_DONE };

struct ConsumerSlot {
    int  order_id;
    int  stage;      // last completed pipeline stage (checkpoint)
};

struct ShmState {
    pthread_mutex_t lock;
    int             stock;
    int             submitted;
    int             completed;
    ConsumerSlot    slot;
    sem_t           sem_empty;
    sem_t           sem_full;
};

class IpcTest : public ::testing::Test {
protected:
    std::string shm_name_;
    std::string mq_name_;
    ShmState*   shm_ = nullptr;

    void SetUp() override {
        const std::string tag = std::to_string(getpid());
        shm_name_ = "/ecom_test_shm_" + tag;
        mq_name_  = "/ecom_test_mq_" + tag;

        shm_unlink(shm_name_.c_str());
        mq_unlink(mq_name_.c_str());

        int fd = shm_open(shm_name_.c_str(), O_CREAT | O_RDWR, 0600);
        ASSERT_NE(fd, -1);
        ASSERT_EQ(ftruncate(fd, sizeof(ShmState)), 0);
        void* p = mmap(nullptr, sizeof(ShmState), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
        ASSERT_NE(p, MAP_FAILED);
        shm_ = static_cast<ShmState*>(p);
        std::memset(shm_, 0, sizeof(ShmState));

        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
        pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
        ASSERT_EQ(pthread_mutex_init(&shm_->lock, &attr), 0);
        pthread_mutexattr_destroy(&attr);

        ASSERT_EQ(sem_init(&shm_->sem_empty, 1, kSlots), 0);
        ASSERT_EQ(sem_init(&shm_->sem_full, 1, 0), 0);
        shm_->stock = kStock;
    }

    void TearDown() override {
        if (shm_ != nullptr) {
            sem_destroy(&shm_->sem_empty);
            sem_destroy(&shm_->sem_full);
            pthread_mutex_destroy(&shm_->lock);
            munmap(shm_, sizeof(ShmState));
        }
        shm_unlink(shm_name_.c_str());
        mq_unlink(mq_name_.c_str());
    }

    mqd_t OpenQueue(int flags) {
        struct mq_attr attr;
        std::memset(&attr, 0, sizeof(attr));
        attr.mq_maxmsg  = kSlots;
        attr.mq_msgsize = 64;
        return mq_open(mq_name_.c_str(), flags | O_CREAT, 0600, &attr);
    }
};

// ---------------------------------------------------------------- Semaphores

TEST_F(IpcTest, BoundedBufferBlocksWhenFull) {
    for (int i = 0; i < kSlots; ++i) {
        EXPECT_EQ(sem_trywait(&shm_->sem_empty), 0) << "slot " << i;
    }
    errno = 0;
    EXPECT_EQ(sem_trywait(&shm_->sem_empty), -1);
    EXPECT_EQ(errno, EAGAIN);  // no free slot -> producer would block
}

TEST_F(IpcTest, ReleasingSlotUnblocksProducer) {
    for (int i = 0; i < kSlots; ++i) sem_wait(&shm_->sem_empty);
    EXPECT_EQ(sem_post(&shm_->sem_empty), 0);   // consumer finished an order
    EXPECT_EQ(sem_trywait(&shm_->sem_empty), 0);
}

TEST_F(IpcTest, ConsumerBlocksWhenQueueEmpty) {
    errno = 0;
    EXPECT_EQ(sem_trywait(&shm_->sem_full), -1);
    EXPECT_EQ(errno, EAGAIN);
}

// ------------------------------------------------------------ Message queue

TEST_F(IpcTest, QueueDeliversHighestPriorityFirst) {
    mqd_t q = OpenQueue(O_RDWR);
    ASSERT_NE(q, (mqd_t)-1);

    ASSERT_EQ(mq_send(q, "low",    4, 1), 0);
    ASSERT_EQ(mq_send(q, "urgent", 7, 9), 0);
    ASSERT_EQ(mq_send(q, "normal", 7, 5), 0);

    char buf[64];
    unsigned prio = 0;
    ASSERT_GT(mq_receive(q, buf, sizeof(buf), &prio), 0);
    EXPECT_STREQ(buf, "urgent");
    EXPECT_EQ(prio, 9u);
    ASSERT_GT(mq_receive(q, buf, sizeof(buf), &prio), 0);
    EXPECT_STREQ(buf, "normal");
    ASSERT_GT(mq_receive(q, buf, sizeof(buf), &prio), 0);
    EXPECT_STREQ(buf, "low");

    mq_close(q);
}

TEST_F(IpcTest, QueueIsBoundedByMaxMsg) {
    mqd_t q = OpenQueue(O_RDWR | O_NONBLOCK);
    ASSERT_NE(q, (mqd_t)-1);

    for (int i = 0; i < kSlots; ++i) {
        ASSERT_EQ(mq_send(q, "order", 6, 0), 0);
    }
    errno = 0;
    EXPECT_EQ(mq_send(q, "overflow", 9, 0), -1);
    EXPECT_EQ(errno, EAGAIN);

    mq_close(q);
}

TEST_F(IpcTest, ProducerToConsumerAcrossProcesses) {
    mqd_t q = OpenQueue(O_RDWR);
    ASSERT_NE(q, (mqd_t)-1);

    pid_t pid = fork();
    ASSERT_NE(pid, -1);
    if (pid == 0) {  // producer
        sem_wait(&shm_->sem_empty);
        shm_->submitted++;
        mq_send(q, "order-42", 9, 0);
        sem_post(&shm_->sem_full);
        _exit(0);
    }

    sem_wait(&shm_->sem_full);   // consumer side
    char buf[64];
    ASSERT_GT(mq_receive(q, buf, sizeof(buf), nullptr), 0);
    EXPECT_STREQ(buf, "order-42");
    sem_post(&shm_->sem_empty);

    int st = 0;
    waitpid(pid, &st, 0);
    EXPECT_TRUE(WIFEXITED(st) && WEXITSTATUS(st) == 0);
    EXPECT_EQ(shm_->submitted, 1);

    mq_close(q);
}

// ------------------------------------------------------------ Shared memory

TEST_F(IpcTest, SharedMemoryVisibleAcrossFork) {
    pid_t pid = fork();
    ASSERT_NE(pid, -1);
    if (pid == 0) {
        shm_->completed = 7;
        _exit(0);
    }
    waitpid(pid, nullptr, 0);
    EXPECT_EQ(shm_->completed, 7);
}

// -------------------------------------------------------- Atomic inventory

TEST_F(IpcTest, ConcurrentReservationNeverOversells) {
    constexpr int kWorkers  = 4;
    constexpr int kAttempts = 10;   // 4 * 10 = 40 attempts for 10 units

    pid_t pids[kWorkers];
    for (int w = 0; w < kWorkers; ++w) {
        pids[w] = fork();
        ASSERT_NE(pids[w], -1);
        if (pids[w] == 0) {
            int reserved = 0;
            for (int i = 0; i < kAttempts; ++i) {
                int rc = pthread_mutex_lock(&shm_->lock);
                if (rc == EOWNERDEAD) pthread_mutex_consistent(&shm_->lock);
                if (shm_->stock > 0) {
                    shm_->stock--;
                    reserved++;
                }
                pthread_mutex_unlock(&shm_->lock);
            }
            _exit(reserved);   // report successes via exit code
        }
    }

    int total = 0;
    for (int w = 0; w < kWorkers; ++w) {
        int st = 0;
        waitpid(pids[w], &st, 0);
        ASSERT_TRUE(WIFEXITED(st));
        total += WEXITSTATUS(st);
    }
    EXPECT_EQ(total, kStock);      // exactly the available stock was sold
    EXPECT_EQ(shm_->stock, 0);     // and it never went negative
}

// ----------------------------------------------------- Fault tolerance

TEST_F(IpcTest, RobustMutexRecoversFromCrashedOwner) {
    pid_t pid = fork();
    ASSERT_NE(pid, -1);
    if (pid == 0) {
        pthread_mutex_lock(&shm_->lock);
        _exit(1);                  // crash while holding the lock
    }
    waitpid(pid, nullptr, 0);

    EXPECT_EQ(pthread_mutex_lock(&shm_->lock), EOWNERDEAD);
    EXPECT_EQ(pthread_mutex_consistent(&shm_->lock), 0);
    EXPECT_EQ(pthread_mutex_unlock(&shm_->lock), 0);
    EXPECT_EQ(pthread_mutex_lock(&shm_->lock), 0);   // usable again
    pthread_mutex_unlock(&shm_->lock);
}

TEST_F(IpcTest, RestartedConsumerResumesFromCheckpoint) {
    // Consumer #1 gets through fraud + payment, then crashes.
    pid_t pid = fork();
    ASSERT_NE(pid, -1);
    if (pid == 0) {
        shm_->slot.order_id = 101;
        shm_->slot.stage    = STAGE_FRAUD;
        shm_->slot.stage    = STAGE_PAYMENT;
        _exit(139);                // simulated crash mid-pipeline
    }
    int st = 0;
    waitpid(pid, &st, 0);          // supervisor detects the exit
    ASSERT_TRUE(WIFEXITED(st));
    ASSERT_NE(WEXITSTATUS(st), 0);

    // Consumer #2 picks up the in-flight order and resumes after the checkpoint.
    ASSERT_EQ(shm_->slot.order_id, 101);
    ASSERT_EQ(shm_->slot.stage, STAGE_PAYMENT);

    int resumed_from = shm_->slot.stage;
    for (int s = resumed_from + 1; s <= STAGE_SHIPPING; ++s) shm_->slot.stage = s;
    shm_->slot.stage = STAGE_DONE;
    shm_->completed++;

    EXPECT_EQ(shm_->slot.stage, STAGE_DONE);
    EXPECT_EQ(shm_->completed, 1);   // processed exactly once, not twice
}

// ------------------------------------------------------- PROJECT HOOKS
// Add tests for your own code here, e.g.:
//
//   extern "C" { #include "common.h" }
//   TEST(ProjectTest, AcquireSlotReturnsSuccess) { ... oq_acquire_slot() ... }
//
// Compile your C sources with -Dmain=ecom_application_main (see Makefile
// "gtest" target) so the supervisor's main() does not clash with gtest_main.

}  // namespace
