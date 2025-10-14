
#include "tasksys.h"
IRunnable::~IRunnable() {}
ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}
/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */
const char* TaskSystemSerial::name() {
    return "Serial";
}
TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}
TaskSystemSerial::~TaskSystemSerial() {}
void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}
TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}
void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}
/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */
const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}
TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads):
    ITaskSystem(num_threads), num_threads_(num_threads) {}
TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}
void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    struct Ctx { IRunnable* r; int num; int total; } ctx{runnable, num_threads_, num_total_tasks};
    auto thread_func = [ctx](int i) {
        while (i < ctx.total) {
            ctx.r->runTask(i, ctx.total);
            i += ctx.num;
        }
    };
    std::vector<std::thread> threads(num_threads_);
    for (int i = 0; i < num_threads_; ++i) {
        threads[i] = std::thread(thread_func, i);
    }
    for (int i = 0; i < num_threads_; ++i) {
        threads[i].join();
    }
}
TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}
void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}
/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */
const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
  : ITaskSystem(num_threads), num_threads_(num_threads) {
    start();
}
void TaskSystemParallelThreadPoolSpinning::start() {
    threads_.resize(num_threads_);
    for (int i = 0; i < num_threads_; ++i) {
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::threadLoop, this, i);
    }
}
void TaskSystemParallelThreadPoolSpinning::threadLoop(int i) {
    (void)i;
    for (;;) {
        if (terminate_.load(std::memory_order_acquire)) break;
    // Dynamic work distribution when a run is active
        if (has_work_.load(std::memory_order_acquire)) {
            for (;;) {
                int task_id = next_task_id_.fetch_add(1, std::memory_order_acq_rel);
                if (task_id >= total_tasks_) break;
                runnable_->runTask(task_id, total_tasks_);
                tasks_completed_.fetch_add(1, std::memory_order_release);
            }
            // No more dynamic tasks; fall through to yield briefly
            std::this_thread::yield();
            continue;
        }
    std::this_thread::yield();
    }
}
TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    terminate_.store(true, std::memory_order_release);
    for (int i = 0; i < num_threads_; ++i) {
        threads_[i].join();
    }
}
void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    // Prefer dynamic distribution for better load balance
    next_task_id_.store(0, std::memory_order_release);
    tasks_completed_.store(0, std::memory_order_release);
    has_work_.store(true, std::memory_order_release);
    // Wait until all dynamic tasks complete; faster exit
    while (tasks_completed_.load(std::memory_order_acquire) < num_total_tasks) {
        std::this_thread::yield();
    }
    // Clear flags for next run
    has_work_.store(false, std::memory_order_release);
    runnable_ = nullptr;
}
TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}
void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}
/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */
const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}
TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
  int num_threads): ITaskSystem(num_threads) , num_threads_(num_threads) {
    threads_.resize(num_threads_);
    for (int i = 0; i < num_threads_; ++i) {
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::threadLoop, this);
    }
}
void TaskSystemParallelThreadPoolSleeping::threadLoop() {
    for (;;) {
    std::unique_lock<std::mutex> lk(mtx_);
        cv_work_.wait(lk, [this]{ return has_work_.load(std::memory_order_acquire) || terminate_; });
        if (terminate_) break;
        lk.unlock();
    // Batch size helps reduce atomic contention for tiny tasks
        const int kBatch = batch_size_.load(std::memory_order_relaxed);
        for (;;) {
            int start = next_task_id_.fetch_add(kBatch, std::memory_order_relaxed);
            if (start >= total_tasks_) break;
            int end = start + kBatch;
            if (end > total_tasks_) end = total_tasks_;
            for (int id = start; id < end; ++id) {
                runnable_->runTask(id, total_tasks_);
            }
            int newly_done = end - start;
            int after = completed_tasks_.fetch_add(newly_done, std::memory_order_relaxed) + newly_done;
            if (after == total_tasks_) {
                has_work_.store(false, std::memory_order_release);
                cv_done_.notify_one();
            }
        }
    }
}
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lk(mtx_);
        terminate_ = true;
        cv_work_.notify_all();
    }
    for (int i = 0; i < num_threads_; ++i) {
        threads_[i].join();
    }
}
// not thread safe
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::unique_lock<std::mutex> lk{mtx_};
    total_tasks_ = num_total_tasks;
    runnable_ = runnable;
    if (total_tasks_ <= 0) {
        // Nothing to do
        return;
    }
    completed_tasks_.store(0, std::memory_order_release);
    next_task_id_.store(0, std::memory_order_release);
    // Choose an adaptive batch size based on tasks per thread
    int per_thread = (total_tasks_ + num_threads_ - 1) / num_threads_;
    int batch = 1;
    if (per_thread > 64) batch = 16;
    else if (per_thread > 16) batch = 8;
    else if (per_thread > 4) batch = 4;
    else batch = 1;
    batch_size_.store(batch, std::memory_order_relaxed);
    has_work_.store(true, std::memory_order_release);
    int kBatch = batch_size_.load(std::memory_order_relaxed);
    // For small workloads per thread, wake only the needed workers; otherwise wake all
    if (((total_tasks_ + num_threads_ - 1) / num_threads_) <= 8) {
        int need = (total_tasks_ + kBatch - 1) / kBatch;
        if (need > num_threads_) need = num_threads_;
        for (int i = 0; i < need; ++i) cv_work_.notify_one();
    } else {
        cv_work_.notify_all();
    }
    cv_done_.wait(lk, [this]{ return !has_work_.load(std::memory_order_acquire); });
}
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //
    return 0;
}
void TaskSystemParallelThreadPoolSleeping::sync() {
    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    return;
}
