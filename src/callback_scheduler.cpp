#include "callback_scheduler.h"
#include <thread>
#include <chrono>

using namespace std::chrono_literals;

CallbackScheduler::CallbackScheduler()
    : workerThread(&CallbackScheduler::workerLoop, this)
{
}

CallbackScheduler::~CallbackScheduler()
{
    stopFlag = true;
    cv.notify_one();
    
    if (workerThread.joinable())
    {
        workerThread.join();
    }
}

CallbackScheduler::TaskId CallbackScheduler::Schedule(std::function<void()> callback, TimePoint when)
{
    if (!callback)
    {
        return 0;
    }

    TaskId id = nextId++;
    
    Task task{id, when, std::move(callback)};
    
    {
        std::lock_guard<std::mutex> lock(mutex);
        pendingTasks.push(std::move(task));
    }
    
    cv.notify_one();
    
    return id;
}

bool CallbackScheduler::Cancel(TaskId id)
{
    std::lock_guard<std::mutex> lock(mutex);
    
    if (currentExecutingId == id)
    {
        return false;
    }
    
    auto result = cancelledTasks.insert(id);
    return result.second;
}

void CallbackScheduler::workerLoop()
{
    while (true)
    {
        std::unique_lock<std::mutex> lock(mutex);
        
        while (!stopFlag && pendingTasks.empty())
        {
            cv.wait(lock);
        }
        
        if (stopFlag && pendingTasks.empty())
        {
            break;
        }
        
        while (!pendingTasks.empty())
        {
            Task nextTask = pendingTasks.top();
            
            auto now = std::chrono::system_clock::now();
            
            if (nextTask.when <= now)
            {
                pendingTasks.pop();
                
                auto it = cancelledTasks.find(nextTask.id);
                if (it != cancelledTasks.end())
                {
                    cancelledTasks.erase(it);
                    continue;
                }
                
                currentExecutingId = nextTask.id;
                
                lock.unlock();
                
                try
                {
                    if (nextTask.callback)
                    {
                        nextTask.callback();
                    }
                }
                catch (...)
                {
                }
                
                lock.lock();
                
                currentExecutingId = 0;
            }
            else
            {
                auto waitDuration = nextTask.when - now;
                cv.wait_for(lock, waitDuration);
                break;
            }
        }
    }
}
