#include <enet_server/network/Connection.hpp>

namespace enet_server
{

bool Connection::IsTransportAttached() const noexcept { return m_Peer.load(std::memory_order_acquire) != nullptr; }

DroppedPendingPackets Connection::MarkClosingAndExtractQueued(bool DrainAcceptedSends)
{
	DroppedPendingPackets dropped;

	std::lock_guard<std::mutex> lk(Rx.Mutex);

	dropped.WasAlreadyClosing = Rx.Closing;
	if (!dropped.WasAlreadyClosing) Rx.DrainAcceptedSends.store(DrainAcceptedSends, std::memory_order_release);
	Rx.Closing = true;

	dropped.Critical.swap(Rx.CriticalQueue);
	dropped.Normal.swap(Rx.NormalQueue);

	Rx.RealtimeScheduled = false;
	Rx.LogicScheduled = false;

	// do not clear HasAsyncBarrier here
	// if a AsyncLogic packet already handed work to the background worker,
	// CompleteAsync() must release the barrier and trigger pending disconnect cleanup
	Rx.ConsecutiveCriticalPicks = 0;

	return dropped;
}

void Connection::MarkClosing()
{
	std::lock_guard<std::mutex> lk(Rx.Mutex);

	Rx.Closing = true;
	Rx.RealtimeScheduled = false;
	Rx.LogicScheduled = false;

	// do not clear HasAsyncBarrier here
	Rx.ConsecutiveCriticalPicks = 0;
}

bool Connection::IsClosing() const noexcept { return Rx.Closing.load(std::memory_order_acquire); }

bool Connection::SetDisconnectReason(Network::DisconnectReason Reason) noexcept
{
	int expected = -1;
	return m_DisconnectReason.compare_exchange_strong(expected, static_cast<int>(Reason), std::memory_order_acq_rel, std::memory_order_relaxed);
}

Network::DisconnectReason Connection::LoadDisconnectReason() const noexcept
{
	int v = m_DisconnectReason.load(std::memory_order_acquire);
	return v < 0 ? Network::DisconnectReason::ClientDc : static_cast<Network::DisconnectReason>(v);
}

void Connection::AttachPeer(ENetPeer* Peer) noexcept { m_Peer.store(Peer, std::memory_order_release); }

ENetPeer* Connection::DetachPeer() noexcept { return m_Peer.exchange(nullptr, std::memory_order_acq_rel); }

ENetPeer* Connection::TransportPeer() const noexcept { return m_Peer.load(std::memory_order_acquire); }
}
