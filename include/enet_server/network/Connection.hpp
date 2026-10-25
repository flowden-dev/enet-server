#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include <enet_server/protocol/Network.hpp>
#include <enet_server/protocol/Packet.hpp>
#include <enet_server/protocol/Scheduling.hpp>
#include <enet/enet.h>
#include <enet_server/network/PacketBuffer.hpp>

namespace enet_server
{

struct PendingPacket
{
	PacketBuffer Buffer;
	enet_uint8 Channel = 0;
	TPacketId Id = 0;
	PacketTraits Traits { };
};

struct DroppedPendingPackets
{
	std::deque<PendingPacket> Critical;
	std::deque<PendingPacket> Normal;

	bool WasAlreadyClosing = false;

	size_t count() const noexcept { return Critical.size() + Normal.size(); }
	bool empty() const noexcept { return Critical.empty() && Normal.empty(); }
	bool first_close() const noexcept { return !WasAlreadyClosing; }
};

class Server;
class ServerManager;

class Connection
{
	friend class Server;
	friend class ServerManager;
public:
	// public-safe transport state check without exposing ENetPeer
	bool IsTransportAttached() const noexcept;


	DroppedPendingPackets MarkClosingAndExtractQueued(bool DrainAcceptedSends = false);
	void MarkClosing();

	bool IsClosing() const noexcept;

	// set-once disconnect reason (first specific reason wins; defaults to ClientDc)
	bool SetDisconnectReason(Network::DisconnectReason Reason) noexcept;
	Network::DisconnectReason LoadDisconnectReason() const noexcept;

public:
	std::string RemoteAddress;

	// ensure only one synchronous packet handler per connection runs at a time
	// background work may overlap with realtime handling
	struct
	{
		mutable std::mutex Mutex;
		std::deque<PendingPacket> CriticalQueue;
		std::deque<PendingPacket> NormalQueue;

		// once set, this connection is closing and no more packet work may start
		std::atomic<bool> Closing { false };
		std::atomic<bool> DrainAcceptedSends { false };

		// scheduled/queued worker jobs per lane
		bool RealtimeScheduled = false;
		bool LogicScheduled = false;

		// true while a packet handler or disconnect cleanup owns this connection's execution gate
		bool HandlerRunning = false;

		// while true, only realtime packets may continue
		bool HasAsyncBarrier = false;

		// if both queues have work and no async barrier is active:
		// execute up to 8 critical packets then force 1 normal packet
		uint8_t ConsecutiveCriticalPicks = 0;

		// disconnect cleanup is executed on logic workers, serialized after any active handler
		bool DisconnectCallbackPending = false;
		bool DisconnectCallbackFinished = false;
		Network::DisconnectReason DisconnectCallbackReason = Network::DisconnectReason::ClientDc;
	} Rx;

	struct
	{
		// rate-limiter implemented as token bucket
		// for instance, the client starts with 500 tokens
		// each accepted packet consumes 1 token
		// tokens refill over time
		// this design is good because clients can burst briefly, but cannot sustain more than the configured rate
		std::chrono::steady_clock::time_point LastRefill{ };
		uint64_t TokensScaled = 0;
		uint64_t Remainder = 0;
		bool Initialized = false;
	} Rate;

private:
	void AttachPeer(ENetPeer* Peer) noexcept;
	ENetPeer* DetachPeer() noexcept;
	ENetPeer* TransportPeer() const noexcept;

private:
	// set before publication; retains server identity after transport detach without a cycle
	std::weak_ptr<Server> m_Owner;
	std::atomic<ENetPeer*> m_Peer{ nullptr };
	std::atomic<size_t> m_PendingSends{ 0 };

	// -1 == unset, otherwise a Network::DisconnectReason value, first writer wins
	std::atomic<int> m_DisconnectReason { -1 };


};
}
