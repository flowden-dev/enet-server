#include <enet_server/network/PacketSender.hpp>
#include <enet_server/network/ServerManager.hpp>
#include <enet_server/util/ScopeExit.hpp>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace enet_server;

namespace
{
	void Check(bool Value, const char* Message) { if (!Value) throw std::runtime_error(Message); }
	struct PacketCounters { std::atomic<int> Created{0}, Freed{0}; };
	void ENET_CALLBACK FreePacket(ENetPacket* Packet)
	{
		++static_cast<PacketCounters*>(Packet->userData)->Freed;
	}
	struct CallbackProbe
	{
		std::function<void()> OnFree;
		std::atomic<int> Calls{0};
		std::atomic<bool> Failed{false};
	};
	void ENET_CALLBACK FreeProbePacket(ENetPacket* Packet) noexcept
	{
		auto& probe = *static_cast<CallbackProbe*>(Packet->userData);
		++probe.Calls;
		try { probe.OnFree(); }
		catch (...) { probe.Failed = true; }
	}

	void Run()
	{
		PacketCounters counters;
		std::atomic<std::shared_ptr<Connection>> connection;
		auto& manager = ServerManager::Get();
		Check(!manager.Initialize(1, 2, 1, 1, 1), "manager initialization failed");
		auto shutdown = MakeScopeExit([&]() noexcept { manager.Shutdown(); });
		ServerInfo info;
		info.Id = 31;
		info.MaxConnections = 1;
		info.MaxConnectionSendQueueSize = 8;
		info.OnConnect = [&](auto Conn) { connection.store(Conn); };
		info.OnReceive = [](auto&) { return HandleResult::Ok; };
		info.OnDisconnect = [](auto, auto) { };
		std::shared_ptr<ReturnEx> error;
		auto server = manager.CreateServer(info, error);
		Check(server != nullptr, "server creation failed");
		std::unique_ptr<ENetHost, decltype(&enet_host_destroy)> client(enet_host_create(nullptr, 1, 2, 0, 0), enet_host_destroy);
		Check(client != nullptr, "client creation failed");
		ENetAddress address{};
		enet_address_set_host_ip(&address, "127.0.0.1");
		address.port = server->Config().Port;
		Check(enet_host_connect(client.get(), &address, 2, 0) != nullptr, "connect failed");
		int received = 0;
		auto waitFor = [&](auto Predicate) {
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
			while (!Predicate() && std::chrono::steady_clock::now() < deadline)
			{
				ENetEvent event{};
				int status = enet_host_service(client.get(), &event, 1);
				Check(status >= 0, "client service failed");
				if (status > 0 && event.type == ENET_EVENT_TYPE_RECEIVE) { ++received; enet_packet_destroy(event.packet); }
			}
			Check(Predicate(), "send limit test timed out");
		};
		waitFor([&] { return connection.load() != nullptr; });
		auto conn = connection.load();
		auto send = [&] {
			uint8_t bytes[]{0x45, 0x4E, 2, 0};
			auto* packet = enet_packet_create(bytes, sizeof(bytes), ENET_PACKET_FLAG_RELIABLE);
			Check(packet != nullptr, "packet allocation failed");
			++counters.Created;
			packet->userData = &counters;
			packet->freeCallback = FreePacket;
			return server->SendPacket(conn, packet);
		};

		// stop client polling so reliable replies remain unacknowledged inside ENet
		std::atomic<int> accepted{0};
		std::vector<std::jthread> producers;
		for (int i = 0; i != 4; ++i)
			producers.emplace_back([&] { for (int j = 0; j != 128; ++j) if (send()) ++accepted; });
		for (auto& producer : producers) producer.join();
		Check(accepted == 8, "outstanding sends exceeded the per-connection limit");
		Check(counters.Created - counters.Freed == 8, "rejected packets leaked or accepted packets lost ownership");
		PacketSender final;
		Check(final.begin(4, 0) != nullptr && !server->SendPacketAndDc(conn, final.finalize()), "final send bypassed the limit");
		Check(!conn->IsClosing(), "rejected final send left the connection closing");

		// acknowledgements return capacity; original callbacks receive their original userData
		// enet may acknowledge a datagram before all its receive events reach the client application
		waitFor([&] { return counters.Created == counters.Freed && received == 8; });
		Check(received == 8 && send(), "acknowledgements did not restore send capacity");
		waitFor([&] { return counters.Created == counters.Freed && received == 9; });
		Check(received == 9, "send after recovery was not delivered");

		// shared payload ownership must survive the tracking callback as well
		PacketSender sender;
		Check(sender.begin(2, 4) != nullptr, "shared packet allocation failed");
		auto shared = sender.finalize_shared();
		Check(server->SendPacket(conn, shared.make()), "shared send failed");
		shared = {};
		waitFor([&] { return received == 10; });
		Check(send(), "pending shutdown send failed");
		client.reset();
		manager.Shutdown();
		Check(counters.Created == counters.Freed, "shutdown did not release all tracked sends");
	}

	void RunPacketCallbacks()
	{
		std::mutex mutex;
		std::condition_variable cv;
		bool releaseIo = false;
		std::atomic<std::shared_ptr<Connection>> connection;
		CallbackProbe rejected, closed, pending;
		auto& manager = ServerManager::Get();
		Check(!manager.Initialize(1, 2, 1, 1, 1), "callback test initialization failed");
		auto shutdown = MakeScopeExit([&]() noexcept { manager.Shutdown(); });
		auto unblock = MakeScopeExit([&]() noexcept { { std::lock_guard lock(mutex); releaseIo = true; } cv.notify_all(); });
		ServerInfo info;
		info.Id = 32;
		info.MaxConnections = 1;
		info.MaxPacketQueueSize = 1;
		info.OnConnect = [&](auto Conn) {
			std::unique_lock lock(mutex);
			connection.store(Conn);
			cv.wait(lock, [&] { return releaseIo; });
		};
		info.OnReceive = [](auto&) { return HandleResult::Ok; };
		info.OnDisconnect = [](auto, auto) { };
		std::shared_ptr<ReturnEx> error;
		auto server = manager.CreateServer(info, error);
		Check(server != nullptr, "callback test server creation failed");
		std::unique_ptr<ENetHost, decltype(&enet_host_destroy)> client(enet_host_create(nullptr, 1, 2, 0, 0), enet_host_destroy);
		Check(client != nullptr, "callback test client creation failed");
		ENetAddress address{};
		enet_address_set_host_ip(&address, "127.0.0.1");
		address.port = server->Config().Port;
		Check(enet_host_connect(client.get(), &address, 2, 0) != nullptr, "callback test connection failed");
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
		while (!connection.load() && std::chrono::steady_clock::now() < deadline)
		{
			ENetEvent event{};
			int status = enet_host_service(client.get(), &event, 1);
			Check(status >= 0, "callback test client service failed");
			if (status > 0 && event.type == ENET_EVENT_TYPE_RECEIVE) enet_packet_destroy(event.packet);
		}
		auto conn = connection.load();
		Check(conn != nullptr, "callback test connection timed out");
		auto makePacket = [](CallbackProbe* Probe = nullptr) {
			uint8_t bytes[]{0x45, 0x4E, 2, 0};
			auto* packet = enet_packet_create(bytes, sizeof(bytes), ENET_PACKET_FLAG_RELIABLE);
			Check(packet != nullptr, "callback test packet allocation failed");
			if (Probe) { packet->userData = Probe; packet->freeCallback = FreeProbePacket; }
			return packet;
		};

		// keep the I/O queue full while a rejected packet's free callback sends again
		pending.OnFree = [&manager] { Check(!manager.FindServer(32), "shutdown registry was not cleared"); };
		rejected.OnFree = [server, conn, makePacket] {
			Check(!server->SendPacket(conn, makePacket()), "nested send bypassed the full queue");
		};
		Check(server->SendPacket(conn, makePacket(&pending)), "callback test initial send failed");
		Check(!server->SendPacket(conn, makePacket(&rejected)), "callback test full queue accepted a send");
		Check(rejected.Calls == 1 && !rejected.Failed, "rejected packet callback could not re-enter send");

		// exercise final-send rejection after another close already won
		Check(server->DisconnectLater(conn), "callback test close failed");
		closed.OnFree = [server, conn, makePacket] {
			Check(!server->SendPacketAndDc(conn, makePacket()), "nested final send bypassed closing");
		};
		Check(!server->SendPacketAndDc(conn, makePacket(&closed)), "callback test final send bypassed closing");
		Check(closed.Calls == 1 && !closed.Failed, "final packet callback could not re-enter send");
		{ std::lock_guard lock(mutex); releaseIo = true; }
		cv.notify_all();
		client.reset();
		manager.Shutdown();
		Check(pending.Calls == 1 && !pending.Failed, "shutdown packet callback could not query the manager");
	}
}

int main()
{
	try { Run(); Run(); RunPacketCallbacks(); std::cout << "send limit tests passed\n"; return 0; }
	catch (const std::exception& Error) { std::cerr << Error.what() << '\n'; return 1; }
}
