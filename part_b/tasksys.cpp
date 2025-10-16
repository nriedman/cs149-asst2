#include "tasksys.h"

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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), threads_(num_threads) {
    global_lock_ = new std::mutex();
    cv_ = new std::condition_variable();

    for (int i = 0; i < num_threads; i++) {
        threads_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::thread_work_fn, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    kill_threads_.store(true);

    cv_->notify_all();

    for (std::thread& th : threads_) {
        th.join();
    }

    for (auto& entry : task_registry_) {
        delete entry.second;
    }

    delete global_lock_;
    delete cv_;
}

void TaskSystemParallelThreadPoolSleeping::thread_work_fn() {
    while(!kill_threads_) {
        // Wait for a work item to be on the queue.
        std::unique_lock<std::mutex> lk(*global_lock_);
        
        cv_->wait(lk, [this] { return !ready_queue_.empty() || kill_threads_; });

        if ( ready_queue_.empty() )
            continue;

        if (kill_threads_)
            break;
        
        // Take a work ticket
        TaskID id = ready_queue_.front();
        TaskState* task_state = task_registry_.at(id);
        int ticket = task_state->available_work_tickets.fetch_sub(1);

        if ( task_state->available_work_tickets == 0 )
            ready_queue_.pop();
                
        lk.unlock();

        // Do the work
        TaskRunArgs args = task_state->run_args;
        args.runnable->runTask(ticket - 1, args.num_total_tasks);

        lk.lock();

        // Update the completion state
        task_state->tasks_completed.fetch_add(1);

        bool task_complete = task_state->tasks_completed.load() == task_state->run_args.num_total_tasks;

        lk.unlock();

        if ( task_complete )
            mark_task_complete(task_state->id);
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    
    
    std::unique_lock<std::mutex> lk(*global_lock_);

    TaskID new_task_id = next_task_id_.fetch_add(1);
    TaskState* new_task_state = new TaskState(new_task_id, runnable, num_total_tasks);

    int num_blocking_deps = 0;

    task_registry_.insert({ new_task_id, new_task_state });

    for (const TaskID& dependency : deps) {
        task_registry_.at(dependency)->dependents.push_back(new_task_id);
        num_blocking_deps += !completed_tasks_.count(dependency);
    }

    new_task_state->num_blocking_dependencies = num_blocking_deps;

    lk.unlock();

    if ( num_blocking_deps == 0 )
        enqueue_tasks({ new_task_id });

    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lk(*global_lock_);
    while (completed_tasks_.size() < next_task_id_.load()) {
        cv_->wait(lk);
    }
}

void TaskSystemParallelThreadPoolSleeping::enqueue_tasks(const std::vector<TaskID>& tasks) {
    std::unique_lock<std::mutex> lk(*global_lock_);

    for ( const TaskID& taskID : tasks ) {
        ready_queue_.push(taskID);
    }

    if ( ready_queue_.size() == tasks.size() ) {
        lk.unlock();
        cv_->notify_all();
    }
}

void TaskSystemParallelThreadPoolSleeping::mark_task_complete(const TaskID taskID) {
    std::unique_lock<std::mutex> lk(*global_lock_);

    // Record the completion
    completed_tasks_.insert(taskID);

    // Collect batch of ready-to-go tasks
    std::vector<TaskID> unblocked_tasks;

    // Notify waiting dependencies
    for (TaskID& waiting_task : task_registry_.at(taskID)->dependents) {
        task_registry_.at(waiting_task)->num_blocking_dependencies.fetch_sub(1);

        if ( task_registry_.at(waiting_task)->num_blocking_dependencies == 0 )
            unblocked_tasks.push_back(waiting_task);
    }

    // If we're synced up, signal it!
    bool all_tasks_complete = completed_tasks_.size() == next_task_id_.load();

    lk.unlock();

    if ( all_tasks_complete ) {
        cv_->notify_all();
    }

    enqueue_tasks(unblocked_tasks);
}
