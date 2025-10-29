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
    while (!kill_threads_.load()) {
        int task_id = tasks_remaining_.fetch_sub(1) - 1;
        
        if (task_id >= 0) {
            int total = num_total_tasks_.load();
            IRunnable* r = runnable_;
            
            if (r) {
                r->runTask(task_id, total);
                
                int completed = tasks_complete_.fetch_add(1) + 1;
                if (completed == total) {
                    std::lock_guard<std::mutex> g(lock_);
                    run_complete_.notify_all();
                }
            }
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
    // Set up tasks
    runnable_ = runnable;
    num_total_tasks_.store(num_total_tasks);
    tasks_complete_.store(0);
    tasks_remaining_.store(num_total_tasks);

    // Wait for tasks to finish
    std::unique_lock<std::mutex> unique_lock(lock_);
    run_complete_.wait(unique_lock, [this] { 
        return tasks_complete_.load() >= num_total_tasks_.load(); 
    });

    // Clean up
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    threads_ = new std::thread[num_threads];
    finish_.store(false);
    runnable_ = nullptr;
    num_total_tasks_.store(0);
    cur_task_.store(0);
    num_finished_task_.store(0);

    for (int i = 0; i < num_threads; ++i) {
        threads_[i] = std::thread([this] {
            while (true) {
                int task_id = -1;
                IRunnable *r = nullptr;
                int total = 0;

                // try to claim a task atomically
                {
                    std::unique_lock<std::mutex> lock(this->lock_);
                    
                    // wait until there's work or shutdown
                    this->runtracker_.wait(lock, [this] {
                        return this->finish_.load() || 
                               (this->runnable_ && this->cur_task_.load() < this->num_total_tasks_.load());
                    });

                    if (this->finish_.load()) {
                        break;
                    }

                    // claim next task if available
                    if (this->runnable_) {
                        task_id = this->cur_task_.fetch_add(1);
                        total = this->num_total_tasks_.load();
                        if (task_id < total) {
                            r = this->runnable_;
                        }
                    }
                }

                // execute task outside the lock
                if (r && task_id < total) {
                    r->runTask(task_id, total);
                    
                    // update completion count
                    int finished = this->num_finished_task_.fetch_add(1) + 1;
                    if (finished >= total) {
                        std::lock_guard<std::mutex> g(this->lock_);
                        this->cv_done_.notify_all();
                    }
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> g(lock_);
        finish_.store(true);
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
        num_total_tasks_.store(num_total_tasks);
        cur_task_.store(0);
        num_finished_task_.store(0);
    }
    // wake all workers to begin processing
    runtracker_.notify_all();
    // wait for completion without busy-waiting
    std::unique_lock<std::mutex> lk(lock_);
    cv_done_.wait(lk, [this] { 
        return this->num_finished_task_.load() >= this->num_total_tasks_.load(); 
    });
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
