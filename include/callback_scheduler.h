#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_set>
#include <vector>
#include <thread>

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

class CallbackScheduler
{
public:
    using TaskId = std::uint64_t;

    CallbackScheduler();
    ~CallbackScheduler();

    CallbackScheduler(const CallbackScheduler&) = delete;
    CallbackScheduler& operator=(const CallbackScheduler&) = delete;

    CallbackScheduler(CallbackScheduler&&) = delete;
    CallbackScheduler& operator=(CallbackScheduler&&) = delete;

    TaskId Schedule(std::function<void()> callback, TimePoint when);
    bool Cancel(TaskId id);

private:
    struct Task
    {
        TaskId id;
        TimePoint when;
        std::function<void()> callback;

        bool operator>(const Task& other) const
        {
            return when > other.when;
        }
    };

    void workerLoop();

    std::priority_queue<Task, std::vector<Task>, std::greater<Task>> pendingTasks;
    std::unordered_set<TaskId> cancelledTasks;
    std::atomic<TaskId> nextId{1};
    std::atomic<TaskId> currentExecutingId{0};
    
    std::thread workerThread;
    std::mutex mutex;
    std::condition_variable cv;
    std::atomic<bool> stopFlag{false};
};
