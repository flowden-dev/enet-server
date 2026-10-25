#include <enet_server/logging/Log.hpp>

#include <algorithm>
#include <chrono>
#include <deque>
#include <new>

#ifdef DEV
#endif

#include <enet_server/util/ScopeExit.hpp>

#include <enet_server/network/HeaderReader.hpp>
#include <enet_server/network/ServerManager.hpp>

namespace enet_server
{

namespace
{

	// per-connection fairness:
	// while both queues have work and no async barrier is active,
	// execute up to 8 critical packets, then force 1 normal packet
	constexpr uint8_t MaxConsecutiveCriticalPerConnection = 8;

	struct SchedulePlan
	{
		bool Realtime = false;
		bool Logic = false;
	};

	void NormalizePacketTraits(PacketTraits& Traits) noexcept
	{
		switch (Traits.Work)
		{

		case WorkClass::Realtime:
		case WorkClass::Logic:
		case WorkClass::AsyncLogic:
			break;

		default:
			Traits.Work = WorkClass::Logic;
			break;

		}

		switch (Traits.Priority)
		{

		case PacketPriority::Critical:
		case PacketPriority::Normal:
		case PacketPriority::Low:
			break;

		default:
			Traits.Priority = PacketPriority::Normal;
			break;

		}

		// only realtime packets should be critical
		if (Traits.Work == WorkClass::Realtime)
			Traits.Priority = PacketPriority::Critical;
		else if (Traits.Priority == PacketPriority::Critical)
			Traits.Priority = PacketPriority::Normal;

		// prevent coalescing for async requests
		if (Traits.Work == WorkClass::AsyncLogic)
			Traits.Coalescable = false;
	}

#ifdef DEV
	bool ValidatePacketTraits(const PacketTraits& Traits) noexcept
	{
		switch (Traits.Work)
		{

		case WorkClass::Realtime:
			return Traits.Priority == PacketPriority::Critical;

		case WorkClass::Logic:
			return Traits.Priority != PacketPriority::Critical;

		case WorkClass::AsyncLogic:
			return Traits.Priority != PacketPriority::Critical && !Traits.Coalescable;

		default:
			return false;

		}
	}
#endif

	size_t TotalQueuedPacketsLocked(const Connection& Conn) { return Conn.Rx.CriticalQueue.size() + Conn.Rx.NormalQueue.size(); }

	std::deque<PendingPacket>& SelectPendingQueueLocked(Connection& Conn, WorkClass Work) { return Work == WorkClass::Realtime ? Conn.Rx.CriticalQueue : Conn.Rx.NormalQueue; }

	bool HasRealtimeSchedulableLocked(const Connection& Conn) noexcept { return !Conn.Rx.Closing && !Conn.Rx.CriticalQueue.empty(); }

	bool HasLogicSchedulableLocked(const Connection& Conn) noexcept { return !Conn.Rx.Closing && !Conn.Rx.HasAsyncBarrier && !Conn.Rx.NormalQueue.empty(); }

	bool LaneHasSchedulableLocked(const Connection& Conn, WorkClass Lane) noexcept { return Lane == WorkClass::Realtime ? HasRealtimeSchedulableLocked(Conn) : HasLogicSchedulableLocked(Conn); }

	void ClearLaneScheduledLocked(Connection& Conn, WorkClass Lane) noexcept
	{
		if (Lane == WorkClass::Realtime)
			Conn.Rx.RealtimeScheduled = false;
		else if (Lane == WorkClass::Logic)
			Conn.Rx.LogicScheduled = false;
	}

	bool TryGetPreferredLaneLocked(const Connection& Conn, WorkClass& PreferredLaneOut) noexcept
	{
		bool hasRealtime = HasRealtimeSchedulableLocked(Conn);
		bool hasLogic = HasLogicSchedulableLocked(Conn);
		if (!hasRealtime && !hasLogic) return false;

		if (hasRealtime && hasLogic)
		{
			PreferredLaneOut = Conn.Rx.ConsecutiveCriticalPicks >= MaxConsecutiveCriticalPerConnection ? WorkClass::Logic : WorkClass::Realtime;
			return true;
		}

		PreferredLaneOut = hasRealtime ? WorkClass::Realtime : WorkClass::Logic;
		return true;
	}

	std::deque<PendingPacket>* SelectNextSchedulableQueueForLaneLocked(Connection& Conn, WorkClass Lane)
	{
		if (Lane == WorkClass::Realtime)
		{
			if (Conn.Rx.CriticalQueue.empty())
				return nullptr;

			// stop realtime after configured streak if normal work is waiting and no async barrier blocks it
			if (!Conn.Rx.HasAsyncBarrier && !Conn.Rx.NormalQueue.empty() && Conn.Rx.ConsecutiveCriticalPicks >= MaxConsecutiveCriticalPerConnection)
				return nullptr;

			return &Conn.Rx.CriticalQueue;
		}

		if (Lane == WorkClass::Logic)
		{
			if (Conn.Rx.HasAsyncBarrier || Conn.Rx.NormalQueue.empty())
				return nullptr;

			return &Conn.Rx.NormalQueue;
		}

		return nullptr;
	}

	void CollectSchedulePlanLocked(Connection& Conn, SchedulePlan& Plan)
	{
		if (Conn.Rx.Closing || Conn.Rx.HandlerRunning) return;

		WorkClass preferredLane = WorkClass::Logic;
		if (!TryGetPreferredLaneLocked(Conn, preferredLane)) return;

		if (preferredLane == WorkClass::Realtime)
		{
			if (!Conn.Rx.RealtimeScheduled)
			{
				Conn.Rx.RealtimeScheduled = true;
				Plan.Realtime = true;
			}
		}
		else
		{
			if (!Conn.Rx.LogicScheduled)
			{
				Conn.Rx.LogicScheduled = true;
				Plan.Logic = true;
			}
		}
	}

	ExecPriority QueuePriorityForLaneLocked(const Connection& Conn, WorkClass Lane) noexcept
	{
		if (Lane == WorkClass::Realtime) return ExecPriority::High;
		if (!Conn.Rx.NormalQueue.empty()) return ToExecPriority(Conn.Rx.NormalQueue.front().Traits.Priority);

		return ExecPriority::Normal;
	}

	PendingPacket* FindCoalescablePacket(std::deque<PendingPacket>& Queue, const PendingPacket& Packet)
	{
		if (!Packet.Traits.Coalescable) return nullptr;

		for (auto it = Queue.rbegin(); it != Queue.rend(); ++it)
		{
			if (it->Id == Packet.Id && it->Channel == Packet.Channel)
				return &*it;
		}

		return nullptr;
	}

	bool ConsumeRateToken(Connection& Conn, const RateLimitRuntime& Limit, std::chrono::steady_clock::time_point Now) noexcept
	{
		auto& rate = Conn.Rate;

		if (!rate.Initialized)
		{
			rate.Initialized = true;
			rate.LastRefill = Now;
			rate.TokensScaled = Limit.CapacityScaled;
			rate.Remainder = 0;
		}

		if (Now > rate.LastRefill)
		{
			auto elapsedMsSigned = std::chrono::duration_cast<std::chrono::milliseconds>(Now - rate.LastRefill).count();

			if (elapsedMsSigned > 0)
			{
				uint64_t elapsedMs = static_cast<uint64_t>(elapsedMsSigned);

				if (elapsedMs > Limit.WindowMs)
					elapsedMs = Limit.WindowMs;

				// if already full, do not accumulate fractional credit
				if (rate.TokensScaled >= Limit.CapacityScaled)
				{
					rate.TokensScaled = Limit.CapacityScaled;
					rate.Remainder = 0;
					rate.LastRefill = Now;
				}
				else
				{
					uint64_t add = elapsedMs * Limit.RefillPerMsScaled;
					uint64_t remainderTotal = rate.Remainder + (elapsedMs * Limit.RefillRemainderPerMs);

					add += remainderTotal / Limit.WindowMs;
					uint64_t newRemainder = remainderTotal % Limit.WindowMs;

					uint64_t available = Limit.CapacityScaled - rate.TokensScaled;

					if (add >= available)
					{
						rate.TokensScaled = Limit.CapacityScaled;
						rate.Remainder = 0;
					}
					else
					{
						rate.TokensScaled += add;
						rate.Remainder = newRemainder;
					}

					rate.LastRefill = Now;
				}
			}
		}

		if (rate.TokensScaled < Limit.CostScaled)
			return false;

		rate.TokensScaled -= Limit.CostScaled;
		return true;
	}
}

ServerManager::~ServerManager() { Shutdown(); }

ServerManager& ServerManager::Get()
{
	static ServerManager instance;
	return instance;
}

std::shared_ptr<ReturnEx> ServerManager::Initialize(size_t IoTimeoutMinMs, size_t IoTimeoutMaxMs, size_t LogicWorkerCount, size_t RealtimeWorkerCount, size_t BackgroundWorkerCount)
{
	ENET_SERVER_LOG(INFO) << "[ServerManager] initializing now...";

	if (m_IsRunning.load()) return std::make_shared<ReturnEx>(1, "[ServerManager] already running");
	if (!IoTimeoutMinMs) return std::make_shared<ReturnEx>(2, "[ServerManager] min I/O timeout is 0");
	if (!IoTimeoutMaxMs) return std::make_shared<ReturnEx>(3, "[ServerManager] max I/O timeout is 0");
	if (IoTimeoutMinMs > IoTimeoutMaxMs) return std::make_shared<ReturnEx>(4, "[ServerManager] min I/O timeout is greater than max I/O timeout");

	m_ShutdownStarted.store(false, std::memory_order_release);

	{
		std::lock_guard<std::mutex> lock(m_MtxTimedJobs);
		m_TimedJobs.clear();
		m_NextTimedJobDueTick.store(NoTimedJobDue, std::memory_order_release);
	}

	if (enet_initialize() != 0) return std::make_shared<ReturnEx>(5, "[ServerManager] failed to initialize enet");
	m_EnetInitialized = true;

	try
	{

		if (!LogicWorkerCount)
		{
			auto hc = std::thread::hardware_concurrency();
			LogicWorkerCount = std::min<size_t>(hc > 2 ? hc - 2 : 1, 64);
		}
		if (!RealtimeWorkerCount) RealtimeWorkerCount = 1;
		if (!BackgroundWorkerCount) BackgroundWorkerCount = 1;

		m_RealtimeWorkers.store(std::make_shared<TaskExecutor>("RealtimeExecutor", RealtimeWorkerCount), std::memory_order_release);
		m_LogicWorkers.store(std::make_shared<TaskExecutor>("LogicExecutor", LogicWorkerCount), std::memory_order_release);
		m_BackgroundWorkers.store(std::make_shared<TaskExecutor>("BackgroundExecutor", BackgroundWorkerCount), std::memory_order_release);

		ENET_SERVER_LOG(INFO) << "[ServerManager] realtime workers: " << RealtimeWorkerCount;
		ENET_SERVER_LOG(INFO) << "[ServerManager] logic workers: " << LogicWorkerCount;
		ENET_SERVER_LOG(INFO) << "[ServerManager] background workers: " << BackgroundWorkerCount;

		m_IsRunning.store(true, std::memory_order_release);
		m_IoThread = std::thread([this, IoTimeoutMinMs, IoTimeoutMaxMs]
			{
				try
				{
					IoLoop(IoTimeoutMinMs, IoTimeoutMaxMs);
				}
				catch (const std::exception& e)
				{
					ENET_SERVER_LOG(ERROR) << "[ServerManager] I/O thread exception: " << e.what();
					m_IsRunning.store(false, std::memory_order_release);
				}
				catch (...)
				{
					ENET_SERVER_LOG(ERROR) << "[ServerManager] I/O thread unknown exception";
					m_IsRunning.store(false, std::memory_order_release);
				}
			});

		ENET_SERVER_LOG(INFO) << "[ServerManager] I/O thread started";
		return nullptr;
	}
	catch (const std::exception& error)
	{
		Shutdown();
		return std::make_shared<ReturnEx>(6, std::string("worker startup failed: ") + error.what());
	}
}

void ServerManager::Shutdown()
{
	if (m_ShutdownStarted.exchange(true)) return;

	ENET_SERVER_LOG(WARNING) << "[ServerManager] shutdown called";

	// stop I/O loop first so it cannot touch the workers anymore
	m_IsRunning.store(false, std::memory_order_release);
	WakeIo();

	{
		std::lock_guard<std::mutex> lock(m_MtxTimedJobs);
		m_TimedJobs.clear();
		m_NextTimedJobDueTick.store(NoTimedJobDue, std::memory_order_release);
	}

	m_IoSchedule.store(std::make_shared<IoSchedule>(), std::memory_order_release);

	if (m_IoThread.joinable()) m_IoThread.join();

	if (auto workers = m_RealtimeWorkers.exchange(nullptr)) workers->stop();
	if (auto workers = m_LogicWorkers.exchange(nullptr)) workers->stop();
	if (auto workers = m_BackgroundWorkers.exchange(nullptr)) workers->stop();

	// shutdown servers and destroy remaining queued packets
	// release the registry lock before packet free callbacks can query the manager
	std::vector<std::shared_ptr<Server>> servers;
	{
		std::unique_lock lk(m_MtxServers);
		servers.swap(m_Servers);
		m_ServersById.clear();
	}
	for (auto& s : servers) if (s) s->Shutdown();
	servers.clear();


	if (m_EnetInitialized) { enet_deinitialize(); m_EnetInitialized = false; }
}

void ServerManager::WakeIo() noexcept
{
	m_IoWakeSeq.fetch_add(1, std::memory_order_release);
	m_CvIoWake.notify_one();
}

std::shared_ptr<Server> ServerManager::CreateServer(ServerInfo& Info, std::shared_ptr<ReturnEx>& ResultOut)
{
	TServerId resolvedId = Info.Id;


	{
		std::unique_lock lk(m_MtxServers);

		if (auto existing = m_ServersById[resolvedId].lock())
		{
			ResultOut = std::make_shared<ReturnEx>(101, "[ServerManager] duplicate server id registration for '" + Info.Identifier + "'");
			return nullptr;
		}
	}

	auto srv = std::make_shared<Server>();

	ResultOut = srv->Initialize(Info);
	if (ResultOut && ResultOut->is_error()) return nullptr;

	{
		std::unique_lock lk(m_MtxServers);

		// re-check after initialization in case another thread registered meanwhile
		if (auto existing = m_ServersById[resolvedId].lock())
		{
			srv->Shutdown();
			ResultOut = std::make_shared<ReturnEx>(102, "[ServerManager] duplicate server id registration for '" + Info.Identifier + "'");
			return nullptr;
		}

		m_Servers.push_back(srv);
		m_ServersById[resolvedId] = srv;

		RebuildIoScheduleLocked();
	}

	WakeIo();
	return srv;
}

std::shared_ptr<Server> ServerManager::FindServer(TServerId Id) const
{


	std::shared_lock lk(m_MtxServers);
	auto it = m_ServersById.find(Id);
	return it == m_ServersById.end() ? nullptr : it->second.lock();
}

bool ServerManager::Send(TServerId Id, const std::shared_ptr<Connection>& Conn, ENetPacket* Packet, Network::Channel Channel)
{
	if (!Conn || !Packet) [[unlikely]]
	{
		if (Packet) enet_packet_destroy(Packet);
		return false;
	}

	if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] { enet_packet_destroy(Packet); return false; }

	auto srv = FindServer(Id);
	if (!srv || !srv->IsRunning()) [[unlikely]] { enet_packet_destroy(Packet); return false; }

	return srv->SendPacket(Conn, Packet, Channel);
}

bool ServerManager::SendAndDc(TServerId Id, const std::shared_ptr<Connection>& Conn, ENetPacket* Packet, Network::Channel Channel)
{
	if (!Conn || !Packet) [[unlikely]]
	{
		if (Packet) enet_packet_destroy(Packet);
		return false;
	}

	if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] { enet_packet_destroy(Packet); return false; }

	auto srv = FindServer(Id);
	if (!srv || !srv->IsRunning()) [[unlikely]] { enet_packet_destroy(Packet); return false; }

	return srv->SendPacketAndDc(Conn, Packet, Channel);
}

bool ServerManager::Disconnect(TServerId Id, const std::shared_ptr<Connection>& Conn)
{
	if (!Conn || !m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;

	auto srv = FindServer(Id);
	if (!srv || !srv->IsRunning()) [[unlikely]] return false;

	return srv->Disconnect(Conn);
}

bool ServerManager::DisconnectLater(TServerId Id, const std::shared_ptr<Connection>& Conn)
{
	if (!Conn || !m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;

	auto srv = FindServer(Id);
	if (!srv || !srv->IsRunning()) [[unlikely]] return false;

	return srv->DisconnectLater(Conn);
}

bool ServerManager::DisconnectWithReason(TServerId Id, const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason)
{
	if (!Conn || !m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;

	auto srv = FindServer(Id);
	if (!srv || !srv->IsRunning()) [[unlikely]] return false;

	return srv->DisconnectWithReason(Conn, Reason);
}

bool ServerManager::PostRealtime(std::function<void()> Job, ExecPriority Priority)
{
	if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;
	auto workers = m_RealtimeWorkers.load(std::memory_order_acquire);
	return workers && workers->enqueue(std::move(Job), Priority);
}

bool ServerManager::PostLogic(std::function<void()> Job, ExecPriority Priority)
{
	if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;
	auto workers = m_LogicWorkers.load(std::memory_order_acquire);
	return workers && workers->enqueue(std::move(Job), Priority);
}

bool ServerManager::PostBackground(std::function<void()> Job, ExecPriority Priority)
{
	if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;
	auto workers = m_BackgroundWorkers.load(std::memory_order_acquire);
	return workers && workers->enqueue(std::move(Job), Priority);
}

bool ServerManager::PostRealtimeDelayed(std::chrono::steady_clock::duration Delay, std::function<void()> Job, ExecPriority Priority)
{
	return PostDelayed(WorkClass::Realtime, Delay, std::move(Job), Priority);
}

bool ServerManager::PostLogicDelayed(std::chrono::steady_clock::duration Delay, std::function<void()> Job, ExecPriority Priority)
{
	return PostDelayed(WorkClass::Logic, Delay, std::move(Job), Priority);
}

bool ServerManager::CompleteAsync(TServerId Id, const std::shared_ptr<Connection>& Conn)
{
	if (!Conn) return false;
	if (!m_IsRunning.load(std::memory_order_acquire)) return false;


	auto srv = FindServer(Id);
	if (!srv || !srv->IsRunning()) return false;
	if (Conn->m_Owner.lock() != srv) return false;

	bool isCurrentConnection = srv->IsCurrentConnection(Conn);

	SchedulePlan plan;
	bool scheduleDisconnectCleanup = false;
	Network::DisconnectReason disconnectReason = Network::DisconnectReason::ClientDc;

	{
		std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
		if (!Conn->Rx.HasAsyncBarrier) return isCurrentConnection && !Conn->Rx.Closing;

		Conn->Rx.HasAsyncBarrier = false;

		if (Conn->Rx.DisconnectCallbackPending)
		{
			// a realtime handler is still active, its ExitHandler() will schedule disconnect cleanup
			if (Conn->Rx.HandlerRunning) return true;

			Conn->Rx.DisconnectCallbackPending = false;
			disconnectReason = Conn->Rx.DisconnectCallbackReason;
			Conn->Rx.HandlerRunning = true;

			scheduleDisconnectCleanup = true;
		}
		else
		{
			if (Conn->Rx.Closing || !isCurrentConnection)
				return false;

			CollectSchedulePlanLocked(*Conn, plan);
		}
	}

	if (scheduleDisconnectCleanup)
	{
		if (!PostDisconnectCallbackJob(srv, Conn, disconnectReason))
		{
			std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
			Conn->Rx.HandlerRunning = false;

			if (!m_ShutdownStarted.load(std::memory_order_acquire))
				ENET_SERVER_LOG_EVERY("network/disconnect_callback/enqueue_failed", ERROR, std::chrono::minutes(1)) << "[" << srv->Identifier() << "] failed to enqueue disconnect cleanup after async completion";

			return false;
		}

		return true;
	}

	if (!SchedulePlannedLanes(srv, Conn, plan.Realtime, plan.Logic))
	{
		srv->DisconnectWithReason(Conn, Network::DisconnectReason::ServerFullCapacity);
		return false;
	}

	return true;
}

void ServerManager::IoLoop(size_t IoTimeOutMinMs, size_t IoTimeOutMaxMs)
{
	ENetEvent evt{ };
	std::vector<Server::IoCommand> localIoCommands;
	std::vector<ENetHost*> noEventHosts;

	size_t idleRounds = 0;

	auto ComputeIoWaitMs = [&]() -> size_t
		{
			// if there is no work the sleep time gradually increases
			// timed jobs are considered to ensure that no sleeping happens during a due date
			size_t sleepMs = IoTimeOutMinMs;

			if (idleRounds > 2)
			{
				size_t shifts = idleRounds - 2;

				while (shifts-- && sleepMs < IoTimeOutMaxMs)
				{
					if (sleepMs > IoTimeOutMaxMs / 2)
					{
						sleepMs = IoTimeOutMaxMs;
						break;
					}

					sleepMs *= 2;
				}
			}

			if (sleepMs > IoTimeOutMaxMs)
				sleepMs = IoTimeOutMaxMs;

			ClockRep nextDue = m_NextTimedJobDueTick.load(std::memory_order_acquire);
			if (nextDue != NoTimedJobDue)
			{
				ClockRep nowTick = TimePointTick(Clock::now());
				if (nextDue <= nowTick) return 0;

				auto untilDue = Clock::duration(nextDue - nowTick);
				auto untilDueMs = std::chrono::duration_cast<std::chrono::milliseconds>(untilDue).count();

				if (untilDueMs <= 0)
					return 0;

				sleepMs = std::min<size_t>(sleepMs, static_cast<size_t>(untilDueMs));
			}

			return sleepMs;
		};

	while (m_IsRunning.load(std::memory_order_acquire))
	{
		uint64_t wakeSeqAtLoopStart = m_IoWakeSeq.load(std::memory_order_acquire);

		// run jobs that have been scheduled for a later time
		bool didWork = RunDueTimedJobs();

		auto schedule = m_IoSchedule.load(std::memory_order_acquire);
		if (!schedule || schedule->empty())
		{
			if (didWork)
				idleRounds = 0;
			else
			{
				++idleRounds;

				size_t sleepMs = ComputeIoWaitMs();
				if (sleepMs) WaitForIoWake(std::chrono::milliseconds(sleepMs), wakeSeqAtLoopStart);
			}

			continue;
		}

		// avoids that weighted schedule duplicates can rescan idle hosts
		noEventHosts.clear();
		noEventHosts.reserve(schedule->size());

		for (const auto& srv : *schedule)
		{
			if (!srv || !srv->IsRunning()) [[unlikely]] continue;

			ENetHost* host = srv->Host();
			if (!host) [[unlikely]] continue;

			const ServerInfo& cfg = srv->Config();

			bool skipEventPump = std::find(noEventHosts.begin(), noEventHosts.end(), host) != noEventHosts.end();
			if (!skipEventPump)
			{
				// retry cleanup without growing worker queues or accepting replacement connections
				for (size_t i = 0; i < srv->m_Disconnected.size();)
				{
					auto conn = srv->m_Disconnected[i];
					bool finished;
					{
						std::lock_guard lock(conn->Rx.Mutex);
						finished = conn->Rx.DisconnectCallbackFinished;
					}
					if (finished)
					{
						srv->m_Disconnected[i] = std::move(srv->m_Disconnected.back());
						srv->m_Disconnected.pop_back();
					}
					else
					{
						RequestDisconnectCallback(srv, conn, conn->LoadDisconnectReason());
						++i;
					}
				}

				bool hostRanOutOfEvents = false;

				size_t eventBudget = cfg.MaxEventsPerTurn;
				for (size_t count = 0; count != eventBudget; ++count)
				{
					int serviceResult = enet_host_service(host, &evt, 0);

					if (serviceResult < 0) [[unlikely]]
					{
						ENET_SERVER_LOG(ERROR) << "[ServerManager][" << srv->Identifier() << "] enet_host_service() failed; stopping I/O loop";
						m_IsRunning.store(false, std::memory_order_release);
						break;
					}

					if (serviceResult == 0)
					{
						hostRanOutOfEvents = true;
						break;
					}

					didWork = true;

					switch (evt.type)
					{
					case ENET_EVENT_TYPE_NONE: break;

					case ENET_EVENT_TYPE_CONNECT:
					{
						// detached connections keep their admission slot until application cleanup finishes
						if (srv->m_Connections.size() + srv->m_Disconnected.size() >= cfg.MaxConnections)
						{
							enet_peer_disconnect_now(evt.peer, 0);
							break;
						}
						std::shared_ptr<Connection> conn;

						try
						{
							conn = std::make_shared<Connection>();
							conn->m_Owner = srv;
							conn->AttachPeer(evt.peer);
							conn->RemoteAddress = Server::GetPeerAddressStr(evt.peer);

							std::unique_ptr<Server::PeerUserData> peerData(new (std::nothrow) Server::PeerUserData{ conn });
							if (!peerData) [[unlikely]] { enet_peer_disconnect(evt.peer, 0); break; }

							{
								std::unique_lock lock(srv->m_MtxConnections);

								auto [it, inserted] = srv->m_Connections.emplace(evt.peer, conn);
								if (!inserted) [[unlikely]] { enet_peer_disconnect(evt.peer, 0); break; }
							}

							evt.peer->data = peerData.release(); // transfer ownership

							// callback
							srv->m_ServInfo.OnConnect(conn);
						}
						catch (const std::exception& e)
						{
							std::string peerAddress(conn ? conn->RemoteAddress : Server::GetPeerAddressStr(evt.peer));
							ENET_SERVER_LOG(ERROR) << "[" << srv->Identifier() << "][connect] exception: '" << e.what() << "' for " << peerAddress;

							if (conn)
								srv->DisconnectWithReason(conn, Network::DisconnectReason::UnhandledException);
							else
								enet_peer_disconnect(evt.peer, 0);
						}
						catch (...)
						{
							std::string peerAddress(conn ? conn->RemoteAddress : Server::GetPeerAddressStr(evt.peer));
							ENET_SERVER_LOG(ERROR) << "[" << srv->Identifier() << "][connect] unknown exception for " << peerAddress;

							if (conn)
								srv->DisconnectWithReason(conn, Network::DisconnectReason::UnhandledException);
							else
								enet_peer_disconnect(evt.peer, 0);
						}
					} break;

					case ENET_EVENT_TYPE_RECEIVE:
					{
						std::shared_ptr<Connection> conn;

						if (evt.peer && evt.peer->data) [[likely]]
						{
							auto* peerData = static_cast<Server::PeerUserData*>(evt.peer->data);
							conn = peerData->Conn;
						}
						if (!conn) [[unlikely]]
						{
							std::shared_lock lk(srv->m_MtxConnections);
							auto it = srv->m_Connections.find(evt.peer);
							if (it != srv->m_Connections.end()) conn = it->second;
						}
						if (!conn) [[unlikely]] { enet_packet_destroy(evt.packet); break; }

						if (conn->IsClosing()) [[unlikely]] { enet_packet_destroy(evt.packet); break; }

						// rate limiting
						if (!ConsumeRateToken(*conn, srv->m_RateLimit, Clock::now())) [[unlikely]]
						{
							conn->SetDisconnectReason(Network::DisconnectReason::RateLimit);
							srv->MarkConnectionClosingAndDropQueued(conn);

							enet_packet_destroy(evt.packet);
							enet_peer_disconnect(evt.peer, 0);
							break;
						}

						// ensure valid packet size
						if (evt.packet->dataLength < cfg.MinPacketSize || evt.packet->dataLength > cfg.MaxPacketSize) [[unlikely]]
						{
							conn->SetDisconnectReason(Network::DisconnectReason::PacketSizeInvalid);
							srv->MarkConnectionClosingAndDropQueued(conn);

							enet_packet_destroy(evt.packet);
							enet_peer_disconnect(evt.peer, 0);
							break;
						}

						// peek packet id
						TPacketId packetId = 0;
						if (!PeekPacketHeader(evt.packet->data, evt.packet->dataLength, packetId)) [[unlikely]]
						{
							conn->SetDisconnectReason(Network::DisconnectReason::PacketHeaderInvalid);
							srv->MarkConnectionClosingAndDropQueued(conn);

							enet_packet_destroy(evt.packet);
							enet_peer_disconnect(evt.peer, 0);
							break;
						}

						PendingPacket queued;
						queued.Buffer = PacketBuffer(evt.packet);
						evt.packet = nullptr; // ownership transferred into PacketBuffer

						queued.Channel = evt.channelID;
						queued.Id = packetId;

						try
						{
							queued.Traits = srv->m_ServInfo.ClassifyPacket(packetId, evt.channelID);
						}
						catch (const std::exception& e)
						{
							ENET_SERVER_LOG(ERROR) << "[" << srv->Identifier() << "][recv] classify exception: '" << e.what() << "' for packet id: " << static_cast<unsigned int>(packetId) << " peer: " << conn->RemoteAddress;

							conn->SetDisconnectReason(Network::DisconnectReason::UnhandledException);
							srv->MarkConnectionClosingAndDropQueued(conn);

							enet_peer_disconnect(evt.peer, 0);
							break;
						}
						catch (...)
						{
							ENET_SERVER_LOG(ERROR) << "[" << srv->Identifier() << "][recv] classify unknown exception for packet id: " << static_cast<unsigned int>(packetId) << " peer: " << conn->RemoteAddress;

							conn->SetDisconnectReason(Network::DisconnectReason::UnhandledException);
							srv->MarkConnectionClosingAndDropQueued(conn);

							enet_peer_disconnect(evt.peer, 0);
							break;
						}

					#if defined(DEV)
						if (!ValidatePacketTraits(queued.Traits)) [[unlikely]]
							ENET_SERVER_LOG(WARNING) << "[" << srv->Identifier() << "][recv] invalid packet traits for id " << static_cast<unsigned int>(packetId);
					#endif

						NormalizePacketTraits(queued.Traits);

						bool shouldDisconnect = false;
						bool closedBeforeQueue = false;
						SchedulePlan plan;

						{
							std::lock_guard<std::mutex> lk(conn->Rx.Mutex);

							if (conn->Rx.Closing) [[unlikely]]
								closedBeforeQueue = true;
							else
							{
								auto& targetQueue = SelectPendingQueueLocked(*conn, queued.Traits.Work);

								if (PendingPacket* existing = FindCoalescablePacket(targetQueue, queued))
								{
									// replaces old packet, queue pressure does not change
									*existing = std::move(queued);
								}
								else
								{
									if (TotalQueuedPacketsLocked(*conn) >= srv->m_ServInfo.MaxConnectionPacketQueueSize) [[unlikely]]
										shouldDisconnect = true;
									else
									{
										// capacity guard
										size_t inFlight = srv->m_PendingPackets.fetch_add(1, std::memory_order_relaxed);
										if (inFlight >= srv->m_ServInfo.MaxPacketQueueSize) [[unlikely]]
										{
											srv->SubPendingPackets(1);
											shouldDisconnect = true;
										}
										else targetQueue.push_back(std::move(queued));
									}
								}

								if (!shouldDisconnect) [[likely]]
									CollectSchedulePlanLocked(*conn, plan);
							}
						}

						// queued.Buffer destructor destroys the packet
						if (closedBeforeQueue) [[unlikely]] break;

						if (shouldDisconnect) [[unlikely]]
						{
							conn->SetDisconnectReason(Network::DisconnectReason::ServerFullCapacity);
							srv->MarkConnectionClosingAndDropQueued(conn);

							enet_peer_disconnect(evt.peer, 0);
							break;
						}

						if (!SchedulePlannedLanes(srv, conn, plan.Realtime, plan.Logic)) [[unlikely]]
						{
							conn->SetDisconnectReason(Network::DisconnectReason::ServerFullCapacity);
							srv->MarkConnectionClosingAndDropQueued(conn);

							enet_peer_disconnect(evt.peer, 0);
							break;
						}
					} break;

					case ENET_EVENT_TYPE_DISCONNECT:
					{
						std::shared_ptr<Connection> conn;

						if (evt.peer && evt.peer->data) [[likely]]
						{
							auto* peerData = static_cast<Server::PeerUserData*>(evt.peer->data);
							conn = std::move(peerData->Conn);

							delete peerData;
							evt.peer->data = nullptr;
						}

						if (!conn) [[unlikely]]
						{
							std::shared_lock lk(srv->m_MtxConnections);
							auto it = srv->m_Connections.find(evt.peer);
							if (it != srv->m_Connections.end()) conn = it->second;
						}

						srv->MarkConnectionClosingAndDropQueued(conn);

						{
							std::unique_lock lk(srv->m_MtxConnections);
							auto it = srv->m_Connections.find(evt.peer);
							if (it != srv->m_Connections.end() && (!conn || it->second.get() == conn.get()))
								srv->m_Connections.erase(it);
						}

						Network::DisconnectReason reason = Network::DisconnectReason::ClientDc;

						if (conn) [[likely]]
						{
							conn->DetachPeer();
							reason = conn->LoadDisconnectReason();
							srv->m_Disconnected.push_back(conn);
						}

						// don't execute the disconnect callback handler in the I/O thread
						if (!RequestDisconnectCallback(srv, conn, reason))
						{
							if (!m_ShutdownStarted.load(std::memory_order_acquire))
								ENET_SERVER_LOG_EVERY("network/disconnect_callback/request_failed", ERROR, std::chrono::minutes(1)) << "[" << srv->Identifier() << "] failed to request disconnect cleanup";
						}
					} break;
					}
				}

				if (hostRanOutOfEvents)
					noEventHosts.push_back(host);
			}

			// stop after a fatal I/O failure
			if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] break;

			size_t ioBudget = cfg.MaxIoCommandsPerTurn;
			if (!ioBudget) ioBudget = 1;

			localIoCommands.clear();

			if (localIoCommands.capacity() < ioBudget)
				localIoCommands.reserve(ioBudget);

			{
				std::lock_guard<std::mutex> lk(srv->m_MtxIoCmd);

				while (localIoCommands.size() < ioBudget && !srv->m_IoCmdQueue.empty())
				{
					localIoCommands.emplace_back(std::move(srv->m_IoCmdQueue.front()));
					srv->m_IoCmdQueue.pop_front();
				}
			}

			if (!localIoCommands.empty())
				didWork = true;

			bool hostNeedsFlush = false;

			// I/O command processing
			for (Server::IoCommand& cmd : localIoCommands)
			{
				auto conn = cmd.Conn.lock();
				if (!conn) [[unlikely]]
				{
					if (cmd.Packet) enet_packet_destroy(cmd.Packet);
					continue;
				}

				ENetPeer* peer = conn->TransportPeer();
				if (!peer || peer->host != host) [[unlikely]]
				{
					if (cmd.Packet) enet_packet_destroy(cmd.Packet);
					continue;
				}

				bool isClosing = conn->IsClosing();

				// graceful close preserves sends accepted before its I/O command
				if (cmd.Op == Server::IoOp::Send && isClosing && !conn->Rx.DrainAcceptedSends.load(std::memory_order_acquire)) [[unlikely]]
				{
					if (cmd.Packet) enet_packet_destroy(cmd.Packet);
					continue;
				}

				auto* peerData = peer->data ? static_cast<Server::PeerUserData*>(peer->data) : nullptr;
				if (!peerData || peerData->Conn.get() != conn.get()) [[unlikely]]
				{
					if (cmd.Packet) enet_packet_destroy(cmd.Packet);
					continue;
				}

				switch (cmd.Op)
				{

				case Server::IoOp::Send:
				{
					if (enet_peer_send(peer, cmd.Channel, cmd.Packet) != 0) [[unlikely]]
						enet_packet_destroy(cmd.Packet);
					else
						hostNeedsFlush = true;
				} break;

				case Server::IoOp::SendAndDc:
				{
					if (cmd.Packet && enet_peer_send(peer, cmd.Channel, cmd.Packet) != 0) [[unlikely]]
						enet_packet_destroy(cmd.Packet);

					enet_peer_disconnect_later(peer, 0);
					hostNeedsFlush = true;
				} break;

				case Server::IoOp::Disconnect:
					enet_peer_disconnect(peer, 0);
					hostNeedsFlush = true;
					break;

				case Server::IoOp::DisconnectLater:
					enet_peer_disconnect_later(peer, 0);
					hostNeedsFlush = true;
					break;

				}
			}

			if (hostNeedsFlush) enet_host_flush(host);
		}

		// stop after a fatal I/O failure
		if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] break;

		// run jobs once more
		didWork |= RunDueTimedJobs();

		if (didWork)
			idleRounds = 0;
		else
		{
			++idleRounds;

			size_t sleepMs = ComputeIoWaitMs();
			if (sleepMs) WaitForIoWake(std::chrono::milliseconds(sleepMs), wakeSeqAtLoopStart);
		}
	}
}

void ServerManager::WaitForIoWake(std::chrono::milliseconds Timeout, uint64_t ObservedWakeSeq)
{
	if (Timeout.count() <= 0) return;

	// do not sleep if work arrived after the loop started deciding it was idle
	if (m_IoWakeSeq.load(std::memory_order_acquire) != ObservedWakeSeq) return;

	std::unique_lock<std::mutex> lk(m_MtxIoWake);

	m_CvIoWake.wait_for(lk, Timeout, [&]
		{
			if (!m_IsRunning.load(std::memory_order_acquire))
				return true;

			if (m_IoWakeSeq.load(std::memory_order_acquire) != ObservedWakeSeq)
				return true;

			ClockRep nextDue = m_NextTimedJobDueTick.load(std::memory_order_acquire);
			if (nextDue != NoTimedJobDue && nextDue <= TimePointTick(Clock::now()))
				return true;

			return false;
		});
}

void ServerManager::RebuildIoScheduleLocked()
{
	auto schedule = std::make_shared<IoSchedule>();
	size_t maxWeight = 1;

	for (const auto& srv : m_Servers)
	{
		if (!srv) continue;

		size_t weight = std::clamp<size_t>(srv->Config().IoWeight, 1, Network::ServerIoWeightMax);
		if (weight > maxWeight) maxWeight = weight;
	}

	schedule->reserve(m_Servers.size() * maxWeight);

	for (size_t round = 0; round != maxWeight; ++round)
	{
		for (const auto& srv : m_Servers)
		{
			if (!srv) continue;

			size_t weight = std::clamp<size_t>(srv->Config().IoWeight, 1, Network::ServerIoWeightMax);
			if (round < weight) schedule->push_back(srv);
		}
	}

	m_IoSchedule.store(std::move(schedule), std::memory_order_release);
}

bool ServerManager::RequestDisconnectCallback(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason)
{
	// don't call the disconnect handler if there is no connection
	if (!Srv || !Srv->m_ServInfo.OnDisconnect || !Conn) [[unlikely]] return true;

	bool shouldPostNow = false;

	{
		std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);

		// disconnect event should happen once, but keep this idempotent
		if (Conn->Rx.DisconnectCallbackFinished) return true;

		Conn->Rx.DisconnectCallbackReason = Reason;

		// wait for either active sync handler or active async barrier
		if (Conn->Rx.HandlerRunning || Conn->Rx.HasAsyncBarrier)
		{
			Conn->Rx.DisconnectCallbackPending = true;
			return true;
		}

		// reserve the per-connection execution gate for disconnect cleanup
		Conn->Rx.DisconnectCallbackPending = false;
		Conn->Rx.HandlerRunning = true;
		shouldPostNow = true;
	}

	if (!shouldPostNow)
		return true;

	if (!PostDisconnectCallbackJob(Srv, Conn, Reason))
	{
		std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
		Conn->Rx.HandlerRunning = false;
		return false;
	}

	return true;
}

bool ServerManager::PostDisconnectCallbackJob(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason)
{
	if (!Srv || !Srv->m_ServInfo.OnDisconnect) [[unlikely]]
		return true;

	auto job = [this, Srv, Conn, Reason] { RunDisconnectCallback(Srv, Conn, Reason); };
	return PostLogic(std::move(job), ExecPriority::Normal);
}

void ServerManager::RunDisconnectCallback(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason)
{
	if (!Srv || !Srv->m_ServInfo.OnDisconnect) [[unlikely]]
	{
		if (Conn)
		{
			std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
			Conn->Rx.HandlerRunning = false;
		}

		return;
	}

	try
	{
		Srv->m_ServInfo.OnDisconnect(Conn, Reason);
	}
	catch (const std::exception& e)
	{
		std::string addr(Conn ? Conn->RemoteAddress : "unknown");
		ENET_SERVER_LOG(ERROR) << "[" << Srv->Identifier() << "][disconnect] exception: '" << e.what() << "' for " << addr;
	}
	catch (...)
	{
		std::string addr(Conn ? Conn->RemoteAddress : "unknown");
		ENET_SERVER_LOG(ERROR) << "[" << Srv->Identifier() << "][disconnect] unknown exception for " << addr;
	}

	if (Conn)
	{
		std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);

		Conn->Rx.HandlerRunning = false;
		Conn->Rx.DisconnectCallbackPending = false;
		Conn->Rx.DisconnectCallbackFinished = true;
	}
	WakeIo();
}

bool ServerManager::PostDelayed(WorkClass Lane, std::chrono::steady_clock::duration Delay, std::function<void()> Job, ExecPriority Priority)
{
	if (!Job) return false;
	if (!m_IsRunning.load(std::memory_order_acquire)) return false;
	if (m_ShutdownStarted.load(std::memory_order_acquire)) return false;

	// delayed jobs are standalone executor jobs
	// WorkClass::AsyncLogic is packet/barrier-specific
	if (Lane != WorkClass::Realtime && Lane != WorkClass::Logic) return false;

	if (Delay < std::chrono::steady_clock::duration::zero())
		Delay = std::chrono::steady_clock::duration::zero();

	auto now = Clock::now();

	Clock::time_point dueAt;
	if (Delay > Clock::time_point::max() - now)
		dueAt = Clock::time_point::max();
	else
		dueAt = now + Delay;

	TimedJob timedJob;
	timedJob.DueAt = dueAt;
	timedJob.Lane = Lane;
	timedJob.Priority = Priority;
	timedJob.Job = std::move(Job);

	{
		std::lock_guard lock(m_MtxTimedJobs);

		if (!m_IsRunning.load(std::memory_order_acquire)) return false;
		if (m_ShutdownStarted.load(std::memory_order_acquire)) return false;

		if (m_TimedJobs.size() >= 65536) return false;
		m_TimedJobs.emplace_back(std::move(timedJob));
		std::push_heap(m_TimedJobs.begin(), m_TimedJobs.end(), TimedJobDueLater{ });

		m_NextTimedJobDueTick.store(TimePointTick(m_TimedJobs.front().DueAt), std::memory_order_release);

		WakeIo();
	}

	return true;
}

bool ServerManager::RunDueTimedJobs()
{
	constexpr size_t MaxTimedJobsPerTurn = 64;
	if (!m_IsRunning.load(std::memory_order_acquire) || m_ShutdownStarted.load(std::memory_order_acquire)) [[unlikely]]
		return false;

	auto now = Clock::now();
	ClockRep nowTick = TimePointTick(now);

	if (m_NextTimedJobDueTick.load(std::memory_order_acquire) > nowTick)
		return false;

	bool didWork = false;
	std::lock_guard<std::mutex> lock(m_MtxTimedJobs);
	for (size_t count = 0; count != MaxTimedJobsPerTurn && !m_TimedJobs.empty() && m_TimedJobs.front().DueAt <= now; ++count)
	{
		if (m_ShutdownStarted.load(std::memory_order_acquire)) break;
		std::pop_heap(m_TimedJobs.begin(), m_TimedJobs.end(), TimedJobDueLater{});
		auto& job = m_TimedJobs.back();
		auto workers = job.Lane == WorkClass::Realtime ? m_RealtimeWorkers.load(std::memory_order_acquire)
			: m_LogicWorkers.load(std::memory_order_acquire);
		if (workers && workers->try_enqueue(job.Job, job.Priority))
		{
			m_TimedJobs.pop_back();
			didWork = true;
		}
		else
		{
			// retain ownership and retry shortly; keep the timer queue bounded
			job.DueAt = now + std::chrono::milliseconds(1);
			std::push_heap(m_TimedJobs.begin(), m_TimedJobs.end(), TimedJobDueLater{});
		}
	}
	ClockRep nextDue = m_TimedJobs.empty() ? NoTimedJobDue : TimePointTick(m_TimedJobs.front().DueAt);
	m_NextTimedJobDueTick.store(nextDue, std::memory_order_release);
	return didWork;
}

bool ServerManager::ScheduleConnectionLane(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, WorkClass Lane)
{
	if (!Srv || !Conn) [[unlikely]] return false;
	if (Lane != WorkClass::Realtime && Lane != WorkClass::Logic) [[unlikely]] return false;

	ExecPriority priority = ExecPriority::Normal;

	{
		std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
		if (Conn->Rx.Closing) { ClearLaneScheduledLocked(*Conn, Lane); return true; }

		bool laneMarkedScheduled = Lane == WorkClass::Realtime ? Conn->Rx.RealtimeScheduled : Conn->Rx.LogicScheduled;
		if (!laneMarkedScheduled) return true;

		// if state changed since planning, just drop the stale schedule bit
		if (Conn->Rx.HandlerRunning || !LaneHasSchedulableLocked(*Conn, Lane)) { ClearLaneScheduledLocked(*Conn, Lane); return true; }

		priority = QueuePriorityForLaneLocked(*Conn, Lane);
	}

	auto Job = [this, Srv, Conn, Lane] { RunConnectionQueue(Srv, Conn, Lane); };

	bool isRealtimeLane = (Lane == WorkClass::Realtime);
	bool isQueued = isRealtimeLane ? PostRealtime(std::move(Job), priority) : PostLogic(std::move(Job), priority);

	if (!isQueued) [[unlikely]]
	{
		std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
		ClearLaneScheduledLocked(*Conn, Lane);
	}

	return isQueued;
}

bool ServerManager::SchedulePlannedLanes(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, bool ScheduleRealtime, bool ScheduleLogic)
{
	if (ScheduleRealtime && !ScheduleConnectionLane(Srv, Conn, WorkClass::Realtime)) return false;
	if (ScheduleLogic && !ScheduleConnectionLane(Srv, Conn, WorkClass::Logic)) return false;

	return true;
}

void ServerManager::RunConnectionQueue(const std::shared_ptr<Server>& Srv, const std::shared_ptr<Connection>& Conn, WorkClass ScheduledWork)
{
	if (!Srv || !Conn) [[unlikely]] return;
	if (ScheduledWork != WorkClass::Realtime && ScheduledWork != WorkClass::Logic) [[unlikely]] return;

	bool enteredHandler = false;
	SchedulePlan redirectPlan;

	auto ExitHandler = [&](bool AllowReschedule)
		{
			if (!enteredHandler) return;
			enteredHandler = false;

			SchedulePlan plan;
			bool mustCloseAndDrop = false;
			bool scheduleDisconnectCleanup = false;
			Network::DisconnectReason disconnectReason = Network::DisconnectReason::ClientDc;

			{
				std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
				Conn->Rx.HandlerRunning = false;

				if (Conn->Rx.DisconnectCallbackPending)
				{
					if (!Conn->Rx.HasAsyncBarrier)
					{
						Conn->Rx.DisconnectCallbackPending = false;
						disconnectReason = Conn->Rx.DisconnectCallbackReason;
						Conn->Rx.HandlerRunning = true;

						scheduleDisconnectCleanup = true;
					}
				}
				else if (!AllowReschedule || Conn->Rx.Closing)
					mustCloseAndDrop = true;
				else
					CollectSchedulePlanLocked(*Conn, plan);
			}

			if (scheduleDisconnectCleanup)
			{
				if (!PostDisconnectCallbackJob(Srv, Conn, disconnectReason))
				{
					std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
					Conn->Rx.HandlerRunning = false;

					if (!m_ShutdownStarted.load(std::memory_order_acquire))
						ENET_SERVER_LOG_EVERY("network/disconnect_callback/enqueue_failed", ERROR, std::chrono::minutes(1)) << "[" << Srv->Identifier() << "] failed to enqueue disconnect cleanup";
				}

				return;
			}

			if (mustCloseAndDrop)
			{
				Srv->MarkConnectionClosingAndDropQueued(Conn);
				return;
			}

			if (!SchedulePlannedLanes(Srv, Conn, plan.Realtime, plan.Logic))
				Srv->DisconnectWithReason(Conn, Network::DisconnectReason::ServerFullCapacity);
		};

	{
		std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);

		ClearLaneScheduledLocked(*Conn, ScheduledWork);

		if (Conn->Rx.Closing) [[unlikely]] return;
		if (Conn->Rx.HandlerRunning) return;

		WorkClass preferredLane = WorkClass::Logic;
		if (!TryGetPreferredLaneLocked(*Conn, preferredLane)) return;

		// stale queued job for the wrong lane -> do not run it
		// instead, plan the currently preferred lane
		if (preferredLane != ScheduledWork)
			CollectSchedulePlanLocked(*Conn, redirectPlan);
		else
		{
			Conn->Rx.HandlerRunning = true;
			enteredHandler = true;
		}
	}

	if (!enteredHandler)
	{
		if ((redirectPlan.Realtime || redirectPlan.Logic) && !SchedulePlannedLanes(Srv, Conn, redirectPlan.Realtime, redirectPlan.Logic))
			Srv->DisconnectWithReason(Conn, Network::DisconnectReason::ServerFullCapacity);

		return;
	}

	// the connection could have been replaced/removed before this job ran
	// check once here, mid-burst removal is caught by the per-iteration Closing check because disconnect always marks the connection closing before removing it
	if (!Srv->IsCurrentConnection(Conn)) [[unlikely]]
	{
		ExitHandler(false);
		return;
	}

	size_t burst = Srv->Config().MaxConnectionBurst;
	if (ScheduledWork == WorkClass::Realtime) burst = std::min<size_t>(burst, MaxConsecutiveCriticalPerConnection);
	if (!burst) burst = 1;

	for (size_t i = 0; i != burst; ++i)
	{
		PendingPacket queued;
		bool havePacket = false;
		bool preArmedAsyncBarrier = false;

		{
			std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
			if (Conn->Rx.Closing) [[unlikely]] break;

			std::deque<PendingPacket>* sourceQueue = SelectNextSchedulableQueueForLaneLocked(*Conn, ScheduledWork);
			if (!sourceQueue || sourceQueue->empty()) break;

			bool fromCritical = (sourceQueue == &Conn->Rx.CriticalQueue);

			queued = std::move(sourceQueue->front());
			sourceQueue->pop_front();
			havePacket = true;

			if (fromCritical)
			{
				if (Conn->Rx.ConsecutiveCriticalPicks < MaxConsecutiveCriticalPerConnection)
					++Conn->Rx.ConsecutiveCriticalPicks;
			}
			else Conn->Rx.ConsecutiveCriticalPicks = 0;

			if (queued.Traits.Work == WorkClass::AsyncLogic)
			{
				Conn->Rx.HasAsyncBarrier = true;
				preArmedAsyncBarrier = true;
			}
		}
		if (!havePacket) break;

		auto cancelPreArmedAsyncBarrier = [&]() noexcept
			{
				if (!preArmedAsyncBarrier)
					return;

				std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
				if (Conn->Rx.HasAsyncBarrier) Conn->Rx.HasAsyncBarrier = false;
			};

		auto pendingGuard = MakeScopeExit([srv = Srv.get()]() noexcept { srv->SubPendingPackets(1); });

		if (Conn->IsClosing())
		{
			cancelPreArmedAsyncBarrier();
			ExitHandler(false);
			return;
		}

		IncomingPacket packet(Conn, std::move(queued.Buffer), queued.Channel);

		try
		{
			bool stopAfterThisPacket = false;

		#ifdef DEV
			auto started = std::chrono::steady_clock::now();
		#endif

			HandleResult result = Srv->m_ServInfo.OnReceive(packet);

		#ifdef DEV
			auto elapsed = std::chrono::steady_clock::now() - started;

			if (queued.Traits.Work == WorkClass::Realtime)
			{
				if (elapsed > std::chrono::milliseconds(2)) [[unlikely]]
				{
					ENET_SERVER_LOG_EVERY("network/dev/realtime_speed", WARNING, std::chrono::seconds(10))
						<< "[" << Srv->Identifier() << "] slow realtime handler for id: "
						<< static_cast<unsigned int>(queued.Id)
						<< " peer: " << Conn->RemoteAddress;
				}
			}
			else if (queued.Traits.Work == WorkClass::AsyncLogic)
			{
				if (elapsed > std::chrono::milliseconds(5)) [[unlikely]]
				{
					ENET_SERVER_LOG_EVERY("network/dev/async_logic_speed", WARNING, std::chrono::seconds(10))
						<< "[" << Srv->Identifier() << "] slow async logic handler before async handoff for id: "
						<< static_cast<unsigned int>(queued.Id)
						<< " peer: " << Conn->RemoteAddress;
				}
			}
		#endif

			switch (result)
			{

			case HandleResult::Ok:
				cancelPreArmedAsyncBarrier();
				break;

			case HandleResult::Async:
			{
				if (queued.Traits.Work != WorkClass::AsyncLogic) [[unlikely]]
				{
					cancelPreArmedAsyncBarrier();

					ENET_SERVER_LOG(ERROR) << "[" << Srv->Identifier() << "] invalid async result for non-AsyncLogic packet id: " << static_cast<unsigned int>(queued.Id);

					Srv->DisconnectWithReason(Conn, Network::DisconnectReason::PacketHandlerError);
					ExitHandler(false);
					return;
				}

				if (!preArmedAsyncBarrier)
				{
					std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
					Conn->Rx.HasAsyncBarrier = true;
				}

				stopAfterThisPacket = true;
			}
			break;

			case HandleResult::Disconnect:
			default:
			{
				cancelPreArmedAsyncBarrier();

				Srv->DisconnectWithReason(Conn, Network::DisconnectReason::PacketHandlerError);
				ExitHandler(false);
				return;
			}
			}

			bool closeAfterPacket = false;
			{
				std::lock_guard<std::mutex> lk(Conn->Rx.Mutex);
				closeAfterPacket = Conn->Rx.Closing;

				if (!closeAfterPacket && ScheduledWork == WorkClass::Logic && !Conn->Rx.HasAsyncBarrier && !Conn->Rx.CriticalQueue.empty())
					stopAfterThisPacket = true;
			}

			if (closeAfterPacket)
			{
				ExitHandler(false);
				return;
			}

			if (stopAfterThisPacket) break;
		}
		catch (const std::exception& excp)
		{
			std::string peerAddress(Conn ? Conn->RemoteAddress : "unknown");
			ENET_SERVER_LOG(ERROR) << "[" << Srv->Identifier() << "][recv] exception: '" << excp.what() << "' for " << peerAddress;

			cancelPreArmedAsyncBarrier();
			Srv->DisconnectWithReason(Conn, Network::DisconnectReason::UnhandledException);
			ExitHandler(false);

			return;
		}
		catch (...)
		{
			std::string peerAddress(Conn ? Conn->RemoteAddress : "unknown");
			ENET_SERVER_LOG(ERROR) << "[" << Srv->Identifier() << "][recv] unknown exception for " << peerAddress;

			cancelPreArmedAsyncBarrier();
			Srv->DisconnectWithReason(Conn, Network::DisconnectReason::UnhandledException);
			ExitHandler(false);

			return;
		}
	}

	ExitHandler(true);
}

}
