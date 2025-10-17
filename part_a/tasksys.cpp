#include "tasksys.h"

#include <optional>
#include <iostream>

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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    maxNumThreads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void invoke_runnable(IRunnable* runnable, int threadId, int num_total_tasks, int num_threads) {
    int taskId = threadId;
    while (taskId < num_total_tasks) {
        runnable->runTask(taskId, num_total_tasks);
        taskId += num_threads;
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    vector<thread> threads(maxNumThreads);

    for (int threadId = 0; threadId < maxNumThreads; threadId++) {
        threads[threadId] = thread(invoke_runnable, runnable, threadId, num_total_tasks, maxNumThreads);
    }

    for (thread &th : threads) {
        th.join();
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    tasks_remaining_ = 0;
    num_total_tasks_ = 0;
    runnable_ = nullptr;
    tasks_complete_ = 0;

    threads = vector<thread>(num_threads);
    for (int i = 0; i < num_threads; i++) {
        threads[i] = thread(&TaskSystemParallelThreadPoolSpinning::threadFunc, this);
    }
}

void TaskSystemParallelThreadPoolSpinning::threadFunc() {
    // Thread function implementation goes here
    while (true) {
        if ( kill_threads_ )
            break;

        std::pair<int, bool> work_ticket = { 0, false };

        lock_.lock();

        if ( tasks_remaining_ > 0 ) {
            work_ticket = { tasks_remaining_--, true };
        }

        lock_.unlock();

        if ( work_ticket.second ) {
            runnable_->runTask(work_ticket.first - 1, num_total_tasks_);

            lock_.lock();
            tasks_complete_++;

            if ( tasks_complete_ == num_total_tasks_ )
                run_complete_.notify_all();
            
            lock_.unlock();
        }
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    kill_threads_ = true;
    for (thread &th : threads) {
        th.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    std::unique_lock<std::mutex> unique_lock(lock_);

    // Set up tasks.
    runnable_ = runnable;
    tasks_remaining_ = num_total_tasks;
    num_total_tasks_ = num_total_tasks;
    tasks_complete_ = 0;

    // Wait for tasks to finish.
    run_complete_.wait(unique_lock);

    // Clean up.
    runnable_ = nullptr;
    num_total_tasks_ = 0;
    tasks_complete_ = 0;

    unique_lock.unlock();
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads];
    finish_ = false;
    runnable_ = nullptr;
    num_total_tasks_ = 0;
    cur_task_ = 0;
    num_finished_task_ = 0;

    for (int i = 0; i < num_threads; ++i) {
        threads_[i] = std::thread([this] {
            std::unique_lock<std::mutex> lock(this->lock_);
            while (true) {
                // wait until there's work or shutdown
                this->runtracker_.wait(lock, [this] {
                    return this->finish_ || (this->runnable_ && this->cur_task_ < this->num_total_tasks_);
                });
                if (this->finish_) {
                    break;
                }
                // claim next task if available
                if (!this->runnable_ || this->cur_task_ >= this->num_total_tasks_) {
                    // spurious wake or race, continue waiting
                    continue;
                }
                int task_id = this->cur_task_++;
                IRunnable *r = this->runnable_;
                int total = this->num_total_tasks_;
                // if we've handed out the last task, clear runnable_ to block new claims until next run
                if (this->cur_task_ >= this->num_total_tasks_) {
                    this->runnable_ = nullptr;
                }
                // run task outside the lock
                lock.unlock();
                r->runTask(task_id, total);
                // after finishing, update completion count and notify if done
                lock.lock();
                int finished = ++this->num_finished_task_;
                if (finished >= total) {
                    this->cv_done_.notify_all();
                }
                // loop continues to wait for more work or shutdown
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> g(lock_);
        finish_ = true;
    }
    runtracker_.notify_all();
    for (int i = 0; i < num_threads_; ++i) {
        threads_[i].join();
    }
    delete[] threads_;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // Initialize this run
    {
        std::lock_guard<std::mutex> g(lock_);
        runnable_ = runnable;
        num_total_tasks_ = num_total_tasks;
        cur_task_ = 0;
        num_finished_task_ = 0;
    }
    // wake all workers to begin processing
    runtracker_.notify_all();
    // wait for completion of all tasks
    std::unique_lock<std::mutex> lk(lock_);
    cv_done_.wait(lk, [this] { return this->num_finished_task_ >= this->num_total_tasks_; });
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
