#include "callback_scheduler.h"
#include <thread>
#include <chrono>

using namespace std::chrono_literals;

// Добавляем реализацию конструктора
CallbackScheduler::CallbackScheduler()
    : workerThread(&CallbackScheduler::workerLoop, this)
{
}

// Добавляем реализацию деструктора
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
    // TODO(student): реализуйте планирование и выполнение callback в момент when.
    //
    // Минимальные требования:
    // - callback должен быть вызван не раньше when;
    // - необходимо поддерживать несколько запланированных callback;
    // - решение должно быть потокобезопасным;
    // - метод должен вернуть идентификатор задачи для последующей отмены.
    
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
    // TODO(student): отмените задачу по идентификатору id.
    // Возвращайте true, если задача была найдена и отменена, иначе false.
    
    std::lock_guard<std::mutex> lock(mutex);
    
    if (currentExecutingId == id)
    {
        return false;
    }
    
    auto result = cancelledTasks.insert(id);
    return result.second;
}

// Добавляем реализацию рабочего цикла
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
