#include <algorithm>
#include <limits>
#include <new>
#include <sstream>

#include <enet_server/logging/Log.hpp>

#include <enet_server/network/Server.hpp>
#include <enet_server/network/ServerManager.hpp>
#include <enet_server/util/ScopeExit.hpp>

namespace enet_server
{

Server::~Server() { Shutdown(); }

bool Server::SendPacket(const std::shared_ptr<Connection>& Conn, ENetPacket* Packet, Network::Channel Channel)
{
	if (!Packet) [[unlikely]] return false;
	// release rejected packets after queue locks so free callbacks may re-enter the server
	auto packetGuard = MakeScopeExit([&]() noexcept { enet_packet_destroy(Packet); });
	if (!Conn || !Conn->TransportPeer() || Conn->IsClosing()) [[unlikely]] return false;
	if (!TrackPendingSend(Conn, Packet)) [[unlikely]] return false;

	// current-connection validation is done later on the I/O thread
	// this keeps normal send hot-path cheap for worker threads

	bool wakeIo = false;
	{
		std::lock_guard<std::mutex> lk(m_MtxIoCmd);
		if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;
		if (Conn->IsClosing()) [[unlikely]] return false;

		// ensure outbound I/O commands are bounded
		if (m_IoCmdQueue.size() >= m_ServInfo.MaxPacketQueueSize) [[unlikely]] return false;

		wakeIo = m_IoCmdQueue.empty();
		try { m_IoCmdQueue.emplace_back(IoOp::Send, Conn, Packet, Channel); }
		catch (const std::bad_alloc&) { return false; }
		packetGuard.release();
	}

	// wake I/O without holding the queue mutex
	if (wakeIo) ServerManager::Get().WakeIo();

	return true;
}

bool Server::SendPacketAndDc(const std::shared_ptr<Connection>& Conn, ENetPacket* Packet, Network::Channel Channel)
{
	if (!Packet) [[unlikely]] return false;
	auto packetGuard = MakeScopeExit([&]() noexcept { enet_packet_destroy(Packet); });
	if (!Conn || !Conn->TransportPeer() || !IsCurrentConnection(Conn)) [[unlikely]] return false;
	if (!TrackPendingSend(Conn, Packet)) [[unlikely]] return false;

	bool wakeIo = false;
	{
		std::lock_guard<std::mutex> lk(m_MtxIoCmd);
		if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;

		wakeIo = m_IoCmdQueue.empty();
		try { m_IoCmdQueue.emplace_back(IoOp::SendAndDc, Conn, Packet, Channel); }
		catch (const std::bad_alloc&) { return false; }

		// reserve the command before closing so allocation failure leaves the connection usable
		if (!MarkConnectionClosingAndDropQueued(Conn, true))
		{
			m_IoCmdQueue.pop_back();
			return false;
		}
		packetGuard.release();
	}

	// wake I/O without holding the queue mutex
	if (wakeIo) ServerManager::Get().WakeIo();

	return true;
}

bool Server::Disconnect(const std::shared_ptr<Connection>& Conn)
{
	if (!Conn || !Conn->TransportPeer() || !IsCurrentConnection(Conn)) [[unlikely]] return false;

	// only schedule I/O command if it's the first close
	if (!MarkConnectionClosingAndDropQueued(Conn)) return true;

	bool wakeIo = false;
	{
		std::lock_guard<std::mutex> lk(m_MtxIoCmd);
		if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;

		wakeIo = m_IoCmdQueue.empty();
		m_IoCmdQueue.emplace_back(IoOp::Disconnect, Conn, nullptr, Network::Main);
	}

	// wake I/O without holding the queue mutex
	if (wakeIo) ServerManager::Get().WakeIo();

	return true;
}

bool Server::DisconnectWithReason(const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason)
{
	if (!Conn || !Conn->TransportPeer() || !IsCurrentConnection(Conn)) [[unlikely]] return false;

	// record once before triggering the disconnect, first specific reason wins
	Conn->SetDisconnectReason(Reason);

	return Disconnect(Conn);
}

bool Server::DisconnectLater(const std::shared_ptr<Connection>& Conn)
{
	if (!Conn || !Conn->TransportPeer() || !IsCurrentConnection(Conn)) [[unlikely]] return false;

	// only schedule I/O command if it's the first close
	if (!MarkConnectionClosingAndDropQueued(Conn, true)) return true;

	bool wakeIo = false;
	{
		std::lock_guard<std::mutex> lk(m_MtxIoCmd);
		if (!m_IsRunning.load(std::memory_order_acquire)) [[unlikely]] return false;

		wakeIo = m_IoCmdQueue.empty();
		m_IoCmdQueue.emplace_back(IoOp::DisconnectLater, Conn, nullptr, Network::Main);
	}

	// wake I/O without holding the queue mutex
	if (wakeIo) ServerManager::Get().WakeIo();

	return true;
}

const ServerInfo& Server::Config() const { return m_ServInfo; }

const std::string& Server::Identifier() const noexcept { return m_ServInfo.Identifier; }

TServerId Server::Id() const noexcept { return m_ServInfo.Id; }

bool Server::IsCurrentConnection(const std::shared_ptr<Connection>& Conn) const
{
	if (!Conn) [[unlikely]] return false;

	ENetPeer* peer = Conn->TransportPeer();
	if (!peer) [[unlikely]] return false;

	std::shared_lock lk(m_MtxConnections);

	auto it = m_Connections.find(peer);
	if (it == m_Connections.end()) [[unlikely]] return false;

	return it->second.get() == Conn.get();
}

bool Server::TrackPendingSend(const std::shared_ptr<Connection>& Conn, ENetPacket* Packet)
{
	if (Packet->dataLength > m_ServInfo.MaxPacketSize) return false;
	size_t pending = Conn->m_PendingSends.load(std::memory_order_relaxed);
	do
	{
		if (pending >= m_ServInfo.MaxConnectionSendQueueSize) return false;
	} while (!Conn->m_PendingSends.compare_exchange_weak(pending, pending + 1, std::memory_order_relaxed));

	auto* state = new (std::nothrow) PendingSend{Conn, Packet->freeCallback, Packet->userData};
	if (!state)
	{
		Conn->m_PendingSends.fetch_sub(1, std::memory_order_relaxed);
		return false;
	}
	Packet->freeCallback = &Server::FreePendingSend;
	Packet->userData = state;
	return true;
}

void ENET_CALLBACK Server::FreePendingSend(ENetPacket* Packet)
{
	std::unique_ptr<PendingSend> state(static_cast<PendingSend*>(Packet->userData));
	Packet->freeCallback = state->FreeCallback;
	Packet->userData = state->UserData;
	if (state->FreeCallback) state->FreeCallback(Packet);
	state->Conn->m_PendingSends.fetch_sub(1, std::memory_order_relaxed);
}

std::pair<std::string, enet_uint16> Server::GetPeerAddress(ENetPeer* Peer)
{
	if (!Peer) [[unlikely]] return { { }, 0 };

	char ip[64]{ };
	if (enet_address_get_host_ip(&Peer->address, ip, sizeof(ip)) != 0) [[unlikely]]
		return { "unknown", Peer->address.port };

	return { ip, Peer->address.port };
}

std::string Server::GetPeerAddressStr(ENetPeer* Peer)
{
	auto [ip, port] = GetPeerAddress(Peer);

	std::stringstream ss;
	ss << ip << ':' << port;

	return ss.str();
}

void Server::Shutdown()
{
	ENET_SERVER_LOG(WARNING) << "[" << m_ServInfo.Identifier << "] shutdown called";
	bool wasRunning = m_IsRunning.exchange(false, std::memory_order_acq_rel);
	std::deque<IoCommand> discardedCommands;

	if (wasRunning)
	{
		std::scoped_lock lk(m_MtxConnections, m_MtxIoCmd);

		for (auto& [peer, conn] : m_Connections)
		{
			if (conn) conn->DetachPeer();

			if (peer && peer->data)
			{
				delete static_cast<PeerUserData*>(peer->data);
				peer->data = nullptr;
			}

			if (conn)
			{
				std::lock_guard<std::mutex> rxlk(conn->Rx.Mutex);

				conn->Rx.Closing = true;
				conn->Rx.CriticalQueue.clear();
				conn->Rx.NormalQueue.clear();
				conn->Rx.RealtimeScheduled = false;
				conn->Rx.LogicScheduled = false;
				conn->Rx.HasAsyncBarrier = false;
				conn->Rx.ConsecutiveCriticalPicks = 0;
				// don't set HandlerRunning here, is owned by the worker that entered the handler
			}
		}

		m_Connections.clear();
		m_Disconnected.clear();

		discardedCommands.swap(m_IoCmdQueue);

		m_PendingPackets.store(0, std::memory_order_release);
	}

	for (auto& cmd : discardedCommands)
		if (cmd.Packet) enet_packet_destroy(cmd.Packet);

	if (m_Server)
	{
		enet_host_destroy(m_Server);
		m_Server = nullptr;
	}
}

std::shared_ptr<ReturnEx> Server::Initialize(ServerInfo& Info)
{
	constexpr int SocketBufferSize = 1024 * 1024; // 1 MiB should be reasonable for socket buffers

	if (Info.Identifier.empty()) Info.Identifier = "Server";
	if (!Info.MaxConnectionSendQueueSize) return std::make_shared<ReturnEx>(24, "connection send queue limit is 0");

	if (!Info.MaxConnections) return std::make_shared<ReturnEx>(2, "[" + Info.Identifier + "] max connections is 0");
	else if (!Info.ChannelCount) return std::make_shared<ReturnEx>(3, "[" + Info.Identifier + "] channel count is 0");
	else if (!Info.MaxRatePackets) return std::make_shared<ReturnEx>(5, "[" + Info.Identifier + "] max rate packet count is 0");
	else if (Info.MaxRateWindow.count() <= 0) return std::make_shared<ReturnEx>(6, "[" + Info.Identifier + "] max rate window is 0");
	else if (!Info.MaxPacketQueueSize) return std::make_shared<ReturnEx>(7, "[" + Info.Identifier + "] max packet queue count is 0");
	else if (!Info.MaxConnectionPacketQueueSize) return std::make_shared<ReturnEx>(8, "[" + Info.Identifier + "] max connection packet queue count is 0");
	else if (!Info.MaxConnectionBurst) return std::make_shared<ReturnEx>(9, "[" + Info.Identifier + "] max connection burst count is 0");
	else if (!Info.MaxEventsPerTurn) return std::make_shared<ReturnEx>(10, "[" + Info.Identifier + "] max events per turn is 0");
	else if (!Info.MaxIoCommandsPerTurn) return std::make_shared<ReturnEx>(11, "[" + Info.Identifier + "] max I/O commands per turn is 0");
	else if (!Info.IoWeight) return std::make_shared<ReturnEx>(12, "[" + Info.Identifier + "] I/O weight is 0");
	else if (Info.IoWeight > Network::ServerIoWeightMax) return std::make_shared<ReturnEx>(13, "[" + Info.Identifier + "] I/O weight is too high");
	else if (Info.MinPacketSize > Info.MaxPacketSize) return std::make_shared<ReturnEx>(14, "[" + Info.Identifier + "] min packet size is greater than max packet size");
	else if (Info.MaxPacketSize > Packet::MaxSize) return std::make_shared<ReturnEx>(15, "[" + Info.Identifier + "] max packet size is above application packet maximum");
	else if (Info.MinPacketSize < Packet::HeaderSize) return std::make_shared<ReturnEx>(16, "[" + Info.Identifier + "] min packet size is below packet header size");
	else if (Info.MaxPacketSize < Packet::HeaderSize) return std::make_shared<ReturnEx>(17, "[" + Info.Identifier + "] max packet size is below packet header size");
	else if (Info.MaxConnections > ENET_PROTOCOL_MAXIMUM_PEER_ID) return std::make_shared<ReturnEx>(18, "[" + Info.Identifier + "] max connections exceeds ENet peer limit");
	else if (Info.ChannelCount > ENET_PROTOCOL_MAXIMUM_CHANNEL_COUNT) return std::make_shared<ReturnEx>(19, "[" + Info.Identifier + "] channel count exceeds ENet channel limit");
	else if (Info.MaxConnectionPacketQueueSize > ((std::numeric_limits<size_t>::max)() / Info.MaxPacketSize)) return std::make_shared<ReturnEx>(20, "[" + Info.Identifier + "] connection packet queue byte limit would overflow");

	else if (!Info.OnConnect) return std::make_shared<ReturnEx>(21, "[" + Info.Identifier + "] no connect handler set");
	else if (!Info.OnDisconnect) return std::make_shared<ReturnEx>(22, "[" + Info.Identifier + "] no disconnect handler set");
	else if (!Info.OnReceive) return std::make_shared<ReturnEx>(23, "[" + Info.Identifier + "] no recv handler set");

	if (!Info.ClassifyPacket)
		Info.ClassifyPacket = [](TPacketId, enet_uint8) { return PacketTraits{ }; };


	Info.IoWeight = std::clamp<size_t>(Info.IoWeight, 1, Network::ServerIoWeightMax);

	if (Info.MaxRateWindow.count() > 86400) return std::make_shared<ReturnEx>(55, "rate window exceeds one day");
	auto windowMsSigned = std::chrono::duration_cast<std::chrono::milliseconds>(Info.MaxRateWindow).count();
	if (windowMsSigned <= 0) return std::make_shared<ReturnEx>(50, "[" + Info.Identifier + "] max rate window converted to 0ms");

	constexpr uint64_t tokenScale = RateLimitRuntime::TokenScale;
	constexpr uint64_t maxCapacityPackets = (std::numeric_limits<uint64_t>::max)() / tokenScale;

	if (Info.MaxRatePackets > maxCapacityPackets) return std::make_shared<ReturnEx>(51, "[" + Info.Identifier + "] max rate packet count is too high");

	m_ServInfo = Info;
	m_Disconnected.reserve(Info.MaxConnections);

	m_RateLimit.WindowMs = static_cast<uint64_t>(windowMsSigned);
	if (m_RateLimit.WindowMs > 0 && m_RateLimit.WindowMs > ((std::numeric_limits<uint64_t>::max)() / m_RateLimit.WindowMs)) return std::make_shared<ReturnEx>(52, "[" + Info.Identifier + "] max rate window is too large");

	m_RateLimit.CostScaled = tokenScale;
	m_RateLimit.CapacityScaled = static_cast<uint64_t>(Info.MaxRatePackets) * tokenScale;
	m_RateLimit.RefillPerMsScaled = m_RateLimit.CapacityScaled / m_RateLimit.WindowMs;
	m_RateLimit.RefillRemainderPerMs = m_RateLimit.CapacityScaled % m_RateLimit.WindowMs;

	ENetAddress address{ ENET_HOST_ANY, m_ServInfo.Port };
	if (enet_address_set_host_ip(&address, m_ServInfo.BindAddress.c_str()) != 0)
		return std::make_shared<ReturnEx>(54, "invalid IPv4 bind address");
	m_Server = enet_host_create(&address, m_ServInfo.MaxConnections, m_ServInfo.ChannelCount, 0, 0);
	if (!m_Server) return std::make_shared<ReturnEx>(53, "[" + Info.Identifier + "] failed to create host");
	m_ServInfo.Port = m_Server->address.port;

	if (enet_socket_set_option(m_Server->socket, ENET_SOCKOPT_RCVBUF, SocketBufferSize) != 0)
		ENET_SERVER_LOG(WARNING) << "[" << m_ServInfo.Identifier << "] failed to set receive socket buffer";

	if (enet_socket_set_option(m_Server->socket, ENET_SOCKOPT_SNDBUF, SocketBufferSize) != 0)
		ENET_SERVER_LOG(WARNING) << "[" << m_ServInfo.Identifier << "] failed to set send socket buffer";

	m_Server->maximumPacketSize = m_ServInfo.MaxPacketSize;

	// per-peer queued waiting data inside ENet
	// keep this bounded so fragmented/reliable abuse cannot build huge memory pressure
	size_t maxWaitingByQueue = m_ServInfo.MaxConnectionPacketQueueSize * m_ServInfo.MaxPacketSize;

	m_Server->maximumWaitingData = std::max<size_t>(m_ServInfo.MaxPacketSize, maxWaitingByQueue);

	m_IsRunning.store(true, std::memory_order_release);

	ENET_SERVER_LOG(INFO) << "[" << m_ServInfo.Identifier << "] now listening on port " << m_ServInfo.Port;
	return nullptr;
}

ENetHost* Server::Host() const noexcept { return m_Server; }

bool Server::IsRunning() const noexcept { return m_IsRunning.load(std::memory_order_acquire); }

std::shared_ptr<Connection> Server::FindConnection(ENetPeer* Peer) const
{
	if (!Peer) [[unlikely]] return nullptr;

	std::shared_lock lk(m_MtxConnections);

	auto it = m_Connections.find(Peer);
	if (it == m_Connections.end()) [[unlikely]] return nullptr;

	return it->second;
}

bool Server::MarkConnectionClosingAndDropQueued(const std::shared_ptr<Connection>& Conn, bool DrainAcceptedSends) noexcept
{
	if (!Conn) [[unlikely]] return false;

	DroppedPendingPackets dropped = Conn->MarkClosingAndExtractQueued(DrainAcceptedSends);
	size_t droppedCount = dropped.count();

	if (droppedCount)
		SubPendingPackets(droppedCount);

	return dropped.first_close();
}

void Server::SubPendingPackets(size_t Count) noexcept
{
	if (!Count) return;

	size_t cur = m_PendingPackets.load(std::memory_order_relaxed);

	while (cur != 0)
	{
		size_t next = (cur > Count) ? (cur - Count) : 0;

		if (m_PendingPackets.compare_exchange_weak(cur, next, std::memory_order_relaxed, std::memory_order_relaxed))
			return;
	}
}
}
