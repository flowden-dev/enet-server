#pragma once

#include <array>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include <enet_server/protocol/Scheduling.hpp>
#include <enet_server/logging/Log.hpp>

namespace enet_server
{

class TaskExecutor
{

private:
	// priority execution schedule
	// current execution priority: 8x High, 4x Normal, 1x Low
	inline static constexpr std::array<ExecPriority, 13> m_ExecSchedule =
	{
		ExecPriority::High,   ExecPriority::High,
		ExecPriority::High,   ExecPriority::High,
		ExecPriority::High,   ExecPriority::High,
		ExecPriority::High,   ExecPriority::High,
		ExecPriority::Normal, ExecPriority::Normal,
		ExecPriority::Normal, ExecPriority::Normal,
		ExecPriority::Low
	};
	static_assert([] {
		bool high = false, normal = false, low = false;
		for (auto priority : m_ExecSchedule)
		{
			high |= priority == ExecPriority::High;
			normal |= priority == ExecPriority::Normal;
			low |= priority == ExecPriority::Low;
		}
		return high && normal && low;
	}(), "execution schedule must contain every priority");

private:
	std::string m_Name;
	std::mutex m_Mutex;
	std::vector<std::thread> m_Threads;

	std::deque<std::function<void()>> m_High;
	std::deque<std::function<void()>> m_Normal;
	std::deque<std::function<void()>> m_Low;

	std::condition_variable m_Cv;
	bool m_IsRunning = false;
	size_t m_NextScheduleIndex = 0;
	size_t m_MaxQueuedJobs = 65536;

public:
	TaskExecutor(const std::string& Name, size_t ThreadCount)
		: m_Name(Name) {
		try { start(ThreadCount); }
		catch (...) { stop(); throw; }
	}

	~TaskExecutor() { stop(); }

	TaskExecutor(const TaskExecutor&) = delete;
	TaskExecutor& operator=(const TaskExecutor&) = delete;

	bool enqueue(std::function<void()> Job, ExecPriority Priority = ExecPriority::Normal)
	{
		return try_enqueue(Job, Priority);
	}

	// preserve the caller's job on rejection so accepted delayed work can be retried
	bool try_enqueue(std::function<void()>& Job, ExecPriority Priority = ExecPriority::Normal)
	{
		if (!Job) return false;

		{
			std::lock_guard<std::mutex> lk(m_Mutex);
			if (!m_IsRunning) return false;
			if (m_High.size() + m_Normal.size() + m_Low.size() >= m_MaxQueuedJobs) return false;

			try
			{
				switch (Priority)
				{

				case ExecPriority::High:	m_High.push_back(std::move(Job)); break;
				case ExecPriority::Low:		m_Low.push_back(std::move(Job)); break;
				case ExecPriority::Normal:
				default:					m_Normal.push_back(std::move(Job)); break;

				}
			}
			catch (const std::bad_alloc&) { return false; }
		}

		m_Cv.notify_one();
		return true;
	}

	void stop()
	{
		{
			std::lock_guard<std::mutex> lk(m_Mutex);
			if (!m_IsRunning) return;

			m_IsRunning = false;
		}

		m_Cv.notify_all();

		for (auto& Thread : m_Threads)
			if (Thread.joinable()) Thread.join();

		m_Threads.clear();

		std::lock_guard<std::mutex> lk(m_Mutex);
		m_High.clear();
		m_Normal.clear();
		m_Low.clear();
		m_NextScheduleIndex = 0;
	}

private:
	void start(size_t ThreadCount)
	{
		if (!ThreadCount) ThreadCount = 1;

		std::lock_guard<std::mutex> lk(m_Mutex);
		if (m_IsRunning) return;

		m_IsRunning = true;
		m_Threads.reserve(ThreadCount);

		for (size_t i = 0; i != ThreadCount; ++i)
		{
			m_Threads.emplace_back([this]
				{
					try
					{
						while (1)
						{
							std::function<void()> Job;

							{
								std::unique_lock<std::mutex> lk(m_Mutex);
								m_Cv.wait(lk, [this] { return !m_IsRunning || HasJobsLocked(); });

								if (!m_IsRunning && !HasJobsLocked()) return;

								if (!TryPopNextLocked(Job)) continue;
							}

							try
							{
								if (Job) Job();
							}
							catch (const std::exception& e) { ENET_SERVER_LOG(ERROR) << "[" << m_Name << "] worker job exception: " << e.what(); }
							catch (...) { ENET_SERVER_LOG(ERROR) << "[" << m_Name << "] worker job unknown exception"; }
						}
					}
					catch (const std::exception& e) { ENET_SERVER_LOG(ERROR) << "[" << m_Name << "] worker terminated by exception: " << e.what(); }
					catch (...) { ENET_SERVER_LOG(ERROR) << "[" << m_Name << "] worker terminated by unknown exception"; }
				});
		}
	}

	bool HasJobsLocked() const noexcept { return !m_High.empty() || !m_Normal.empty() || !m_Low.empty(); }

	bool TryPopNextLocked(std::function<void()>& Job)
	{
		if (!HasJobsLocked()) return false;

		for (size_t i = 0; i != m_ExecSchedule.size(); ++i)
		{
			ExecPriority wanted = m_ExecSchedule[m_NextScheduleIndex];
			m_NextScheduleIndex = (m_NextScheduleIndex + 1) % m_ExecSchedule.size();

			auto& q = QueueForLocked(wanted);
			if (!q.empty())
			{
				Job = std::move(q.front());
				q.pop_front();
				return true;
			}
		}

		return false;
	}

	std::deque<std::function<void()>>& QueueForLocked(ExecPriority Priority) noexcept
	{
		switch (Priority)
		{

		case ExecPriority::High:   return m_High;
		case ExecPriority::Low:    return m_Low;
		case ExecPriority::Normal:
		default:                   return m_Normal;

		}
	}

};
}
