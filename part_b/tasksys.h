#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"

#include <thread>

#include <queue>
#include <mutex>
#include <atomic>
#include <condition_variable>

#include <vector>
#include <unordered_map>
#include <set>

struct TaskRunArgs {
    IRunnable* const runnable;
    const int num_total_tasks;

    TaskRunArgs(IRunnable* const runnable, const int num_total_tasks)
        : runnable(runnable), num_total_tasks(num_total_tasks) {}
};

struct TaskState {
    const TaskID id;
    const TaskRunArgs run_args;
    
    std::vector<TaskID> dependents {};

    std::atomic<int> num_blocking_dependencies { 0 };
    std::atomic<int> available_work_tickets;
    std::atomic<int> tasks_completed { 0 };

    TaskState(const TaskID id, IRunnable* const runnable, const int num_total_tasks)
        : id(id), run_args(runnable, num_total_tasks), available_work_tickets(num_total_tasks)
        {}
};

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
    private:
        // Thread pool
        std::vector<std::thread> threads_;
        void thread_work_fn();
        std::atomic<bool> kill_threads_ { false };

        // Sync
        std::mutex* global_lock_;
        std::condition_variable* cv_;

        // Global task registry
        std::atomic<unsigned TaskID> next_task_id_ { 0 };
        std::unordered_map<TaskID, TaskState*> task_registry_ {};
        
        // In progress
        void enqueue_tasks(const std::vector<TaskID>&);    // blocked -> in progress
        std::queue<TaskID> ready_queue_ {};

        // Finished
        void mark_task_complete(const TaskID);  // in progress -> complete
        std::set<TaskID> completed_tasks_ {};
};

#endif
