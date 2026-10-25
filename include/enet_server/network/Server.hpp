#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <thread>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include <enet_server/protocol/Packet.hpp>
#include <enet_server/protocol/IncomingPacket.hpp>
#include <enet_server/protocol/Network.hpp>
#include <enet_server/protocol/Scheduling.hpp>
#include <enet/enet.h>
#include <enet_server/util/ReturnEx.hpp>

#include <enet_server/network/Connection.hpp>

namespace enet_server
{

struct ServerInfo
{
	using ConnectHandler = std::function<void(std::shared_ptr<Connection>)>;
	using DisconnectHandler = std::function<void(std::shared_ptr<Connection>, Network::DisconnectReason)>;
	using RecvHandler = std::function<HandleResult(IncomingPacket&)>;
	using ClassifyHandler = std::function<PacketTraits(TPacketId, enet_uint8)>;

	std::string Identifier;
	TServerId Id = 0;


	std::string BindAddress = "127.0.0.1";
	unsigned short Port = 0;
	size_t MaxConnections  = 0;
	size_t ChannelCount = Network::DefaultChannelCount;

	size_t MaxRatePackets = 500;									// how many packets may be received within a time window
	std::chrono::seconds MaxRateWindow = std::chrono::seconds(60);	// time window for MaxRatePackets

	size_t MaxPacketQueueSize = 10000;								// total queued packets on the server
	size_t MaxConnectionPacketQueueSize = 128;						// max queued packets for one connection
	size_t MaxConnectionBurst = 16;									// packets handled before yielding worker

	size_t MaxEventsPerTurn = 32;									// max ENet events handled before next server gets a turn
	size_t MaxIoCommandsPerTurn = 64;								// max queued outbound I/O commands drained before yielding

	size_t IoWeight = 1;											// packet handling prioritization; example:
																	// a weight of 4 gives four polling turns for each turn of a server with weight 1

	size_t MinPacketSize = Packet::MinSize;
	size_t MaxPacketSize = Packet::MaxSize;
	size_t MaxConnectionSendQueueSize = 128;						// packets retained by the framework and ENet until released

	// OnConnect and ClassifyPacket run in the I/O thread
	// keep them fast and non-blocking!
	ConnectHandler OnConnect = nullptr;
	ClassifyHandler ClassifyPacket = nullptr;

	// runs on realtime/logic workers depending on PacketTraits
	RecvHandler OnReceive = nullptr;

	// runs on the logic worker and is serialized after active packet work
	DisconnectHandler OnDisconnect = nullptr;
};

struct RateLimitRuntime
{
	static constexpr uint64_t TokenScaleBits = 16;
	static constexpr uint64_t TokenScale = 1ull << TokenScaleBits;

	uint64_t CapacityScaled = 0;
	uint64_t CostScaled = TokenScale;

	uint64_t WindowMs = 1;
	uint64_t RefillPerMsScaled = 0;
	uint64_t RefillRemainderPerMs = 0;
};

class Server
{
	friend class ServerManager;

private:
	enum class IoOp : uint8_t { Send, SendAndDc, Disconnect, DisconnectLater };

	struct IoCommand
	{
		IoOp Op = IoOp::Send;
		std::weak_ptr<Connection> Conn;
		ENetPacket* Packet = nullptr;
		Network::Channel Channel = Network::Main;

		IoCommand() = default;

		IoCommand(IoOp OpIn, const std::shared_ptr<Connection>& ConnIn, ENetPacket* PacketIn, Network::Channel ChannelIn)
			: Op(OpIn), Conn(ConnIn), Packet(PacketIn), Channel(ChannelIn) { }
	};

public:
	Server() = default;
	~Server();

	Server(const Server&) = delete;
	Server& operator=(const Server&) = delete;

	bool SendPacket(const std::shared_ptr<Connection>& Conn, ENetPacket* Packet, Network::Channel Channel = Network::Main);
	bool SendPacketAndDc(const std::shared_ptr<Connection>& Conn, ENetPacket* Packet, Network::Channel Channel = Network::Main);
	bool Disconnect(const std::shared_ptr<Connection>& Conn);
	bool DisconnectWithReason(const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason);
	bool DisconnectLater(const std::shared_ptr<Connection>& Conn);

	const ServerInfo& Config() const;
	const std::string& Identifier() const noexcept;
	TServerId Id() const noexcept;

	bool IsCurrentConnection(const std::shared_ptr<Connection>& Conn) const;

private:
	struct PeerUserData { std::shared_ptr<Connection> Conn; };
	struct PendingSend
	{
		std::shared_ptr<Connection> Conn;
		ENetPacketFreeCallback FreeCallback;
		void* UserData;
	};

private:
	bool TrackPendingSend(const std::shared_ptr<Connection>& Conn, ENetPacket* Packet);
	static void ENET_CALLBACK FreePendingSend(ENetPacket* Packet);
	static std::pair<std::string, enet_uint16> GetPeerAddress(ENetPeer* Peer);
	static std::string GetPeerAddressStr(ENetPeer* Peer);

private:
	void Shutdown();
	std::shared_ptr<ReturnEx> Initialize(ServerInfo& Info);

	ENetHost* Host() const noexcept;
	bool IsRunning() const noexcept;

	std::shared_ptr<Connection> FindConnection(ENetPeer* Peer) const;
	bool MarkConnectionClosingAndDropQueued(const std::shared_ptr<Connection>& Conn, bool DrainAcceptedSends = false) noexcept; // returns true for first close
	void SubPendingPackets(size_t Count) noexcept;

private:
	ServerInfo m_ServInfo;
	RateLimitRuntime m_RateLimit;

	ENetHost* m_Server = nullptr;

	std::atomic<bool> m_IsRunning { false };
	std::atomic<size_t> m_PendingPackets { 0 };

	std::mutex m_MtxIoCmd;
	std::deque<IoCommand> m_IoCmdQueue;

	std::unordered_map<ENetPeer*, std::shared_ptr<Connection>> m_Connections;
	mutable std::shared_mutex m_MtxConnections;
	// I/O-owned cleanup backlog; counts against MaxConnections until callbacks finish
	std::vector<std::shared_ptr<Connection>> m_Disconnected;

};
}
