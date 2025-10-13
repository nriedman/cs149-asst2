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
        std::pair<int, bool> work_ticket = { 0, false };

        lock_.lock();

        if ( tasks_remaining_ > 0 ) {
            std::cout << "Taking work ticket: " << tasks_remaining_ << std::endl;
            work_ticket = { tasks_remaining_--, true };
        }

        lock_.unlock();

        if ( work_ticket.second ) {
            runnable_->runTask(work_ticket.first - 1, num_total_tasks_);

            lock_.lock();
            std::cout << "Work ticket: [" << work_ticket.first << "] done. " << tasks_complete_ + 1 << " tasks complete." << std::endl;
            tasks_complete_++;
            lock_.unlock();
        }
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    lock_.lock();

    runnable_ = runnable;
    tasks_remaining_ = num_total_tasks; 
    num_total_tasks_ = num_total_tasks;
    tasks_complete_ = 0;

    lock_.unlock();
    
    while ( true ) {
        lock_.lock();
        if ( tasks_complete_ == num_total_tasks_ ) {
            std::cout << "Done!" << std::endl;
            lock_.unlock();
            break;
        }
        lock_.unlock();
    }

    lock_.lock();

    runnable_ = nullptr;
    num_total_tasks_ = 0;
    tasks_complete_ = 0;

    lock_.unlock();
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
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
