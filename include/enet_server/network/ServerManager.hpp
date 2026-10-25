#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>
#include <unordered_map>

#include <enet_server/protocol/Scheduling.hpp>
#include <enet/enet.h>
#include <enet_server/util/ReturnEx.hpp>

#include <enet_server/network/Server.hpp>
#include <enet_server/network/TaskExecutor.hpp>

namespace enet_server
{


class ServerManager
{

public:
	static ServerManager& Get();

public:
	std::shared_ptr<ReturnEx> Initialize(size_t IoTimeoutMinMs = 1, size_t IoTimeoutMaxMs = 16, size_t LogicWorkerCount = 0, size_t RealtimeWorkerCount = 1, size_t BackgroundWorkerCount = 0);
	void Shutdown();
	bool IsRunning() const noexcept { return m_IsRunning.load(std::memory_order_acquire); }

	void WakeIo() noexcept;

	std::shared_ptr<Server> CreateServer(ServerInfo& Info, std::shared_ptr<ReturnEx> &ResultOut);
	std::shared_ptr<Server> FindServer(TServerId Id) const;

	bool Send(TServerId Id, const std::shared_ptr<Connection>& Conn, ENetPacket* Packet, Network::Channel Channel = Network::Main);
	bool SendAndDc(TServerId Id, const std::shared_ptr<Connection>& Conn, ENetPacket* Packet, Network::Channel Channel = Network::Main);

	bool Disconnect(TServerId Id, const std::shared_ptr<Connection>& Conn);
	bool DisconnectLater(TServerId Id, const std::shared_ptr<Connection>& Conn);
	bool DisconnectWithReason(TServerId Id, const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason);

	bool PostRealtime(std::function<void()> Job, ExecPriority Priority = ExecPriority::High);
	bool PostLogic(std::function<void()> Job, ExecPriority Priority = ExecPriority::Normal);
	bool PostBackground(std::function<void()> Job, ExecPriority Priority = ExecPriority::Normal);

	bool PostRealtimeDelayed(std::chrono::steady_clock::duration Delay, std::function<void()> Job, ExecPriority Priority = ExecPriority::High);
	bool PostLogicDelayed(std::chrono::steady_clock::duration Delay, std::function<void()> Job, ExecPriority Priority = ExecPriority::Normal);

	bool CompleteAsync(TServerId Id, const std::shared_ptr<Connection>& Conn);


private:
	using IoSchedule = std::vector<std::shared_ptr<Server>>;

private:
	struct TimedJob
	{
		std::chrono::steady_clock::time_point DueAt{};
		WorkClass Lane = WorkClass::Logic;
		ExecPriority Priority = ExecPriority::Normal;
		std::function<void()> Job;
	};

private:
	using Clock = std::chrono::steady_clock;
	using ClockRep = Clock::duration::rep;

	static constexpr ClockRep NoTimedJobDue = (std::numeric_limits<ClockRep>::max)();

	struct TimedJobDueLater
	{
		bool operator()(const TimedJob& A, const TimedJob& B) const noexcept { return A.DueAt > B.DueAt; }
	};

	static ClockRep TimePointTick(Clock::time_point Tp) noexcept { return Tp.time_since_epoch().count(); }

private:
	ServerManager() = default;
	~ServerManager();

private:
	void IoLoop(size_t IoTimeOutMinMs, size_t IoTimeOutMaxMs);
	void WaitForIoWake(std::chrono::milliseconds Timeout, uint64_t ObservedWakeSeq);

	void RebuildIoScheduleLocked();

	bool RequestDisconnectCallback(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason);
	bool PostDisconnectCallbackJob(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason);
	void RunDisconnectCallback(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason);

	// delayed jobs are only for standalone realtime/logic work
	// do not use WorkClass::AsyncLogic here because it is packet/barrier-specific
	bool PostDelayed(WorkClass Lane, std::chrono::steady_clock::duration Delay, std::function<void()> Job, ExecPriority Priority);

	bool RunDueTimedJobs();

	bool ScheduleConnectionLane(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, WorkClass Lane);
	bool SchedulePlannedLanes(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, bool ScheduleRealtime, bool ScheduleLogic);
	void RunConnectionQueue(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, WorkClass ScheduledWork);

private:
	std::atomic<bool> m_IsRunning { false };
	std::atomic<bool> m_ShutdownStarted { false };

	std::thread m_IoThread;
	std::atomic<std::shared_ptr<TaskExecutor>> m_RealtimeWorkers;
	std::atomic<std::shared_ptr<TaskExecutor>> m_LogicWorkers;
	std::atomic<std::shared_ptr<TaskExecutor>> m_BackgroundWorkers;
	bool m_EnetInitialized = false;

	mutable std::shared_mutex m_MtxServers;
	std::vector<std::shared_ptr<Server>> m_Servers;
	std::unordered_map<TServerId, std::weak_ptr<Server>> m_ServersById;

	std::atomic<std::shared_ptr<const IoSchedule>> m_IoSchedule;

	std::mutex m_MtxIoWake;
	std::condition_variable m_CvIoWake;
	std::atomic<uint64_t> m_IoWakeSeq{ 0 };


	std::mutex m_MtxTimedJobs;
	std::vector<TimedJob> m_TimedJobs; // min-heap ordered by DueAt
	std::atomic<ClockRep> m_NextTimedJobDueTick{ NoTimedJobDue };
};
}
