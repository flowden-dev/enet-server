#include <enet_server/network/HeaderReader.hpp>
#include <enet_server/network/PacketSender.hpp>
#include <enet_server/network/ServerManager.hpp>
#include <enet_server/util/ScopeExit.hpp>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stdexcept>

using namespace enet_server;

namespace
{
	void Check(bool Value, const char* Message) { if (!Value) throw std::runtime_error(Message); }

	struct SendOwnershipProbe
	{
		std::atomic<int> Freed{0};
		std::atomic<bool> Sent{false};
	};

	void ENET_CALLBACK FreeOwnershipPacket(ENetPacket* Packet)
	{
		auto& probe = *static_cast<SendOwnershipProbe*>(Packet->userData);
		probe.Sent = (Packet->flags & ENET_PACKET_FLAG_SENT) != 0;
		++probe.Freed;
	}

	void Run()
	{
		std::mutex mutex;
		std::condition_variable cv;
		bool releaseBackground = false;
		std::atomic<int> normalCount{0}, realtimeCount{0}, updates{0}, latest{0}, disconnects{0}, active{0};
		std::atomic<bool> started{false}, completionOk{false}, overlapped{false};
		SendOwnershipProbe ownership;
		std::shared_ptr<Connection> connection;
		std::shared_ptr<Server> server;
		std::shared_ptr<ReturnEx> error;
		ServerInfo info;
		info.Id = 50000;
		info.Identifier = "Test";
		info.Port = 0;
		info.MaxConnections = 8;
		auto& manager = ServerManager::Get();
		Check(!manager.Initialize(1, 2, 2, 1, 1), "manager initialization failed");
		// callbacks may use the state above until shutdown has drained all workers
		auto shutdown = MakeScopeExit([&]() noexcept { manager.Shutdown(); });
		auto release = MakeScopeExit([&]() noexcept { { std::lock_guard lock(mutex); releaseBackground = true; } cv.notify_all(); });
		info.OnConnect = [&](auto Conn) { std::lock_guard lock(mutex); connection = Conn; };
		info.OnDisconnect = [&](auto, auto) { if (active.load()) overlapped = true; ++disconnects; };
		info.ClassifyPacket = [](TPacketId Id, enet_uint8) {
			return PacketTraits{Id == 3 ? WorkClass::AsyncLogic : Id == 5 ? WorkClass::Realtime : WorkClass::Logic,
				PacketPriority::Normal, Id == 9};
		};
		info.OnReceive = [&](IncomingPacket& Packet) {
			if (active.fetch_add(1)) overlapped = true;
			auto exit = MakeScopeExit([&]() noexcept { --active; });
			TPacketId id = 0;
			if (!ReadPacketHeader(Packet, id)) return HandleResult::Disconnect;
			if (id == 3)
			{
				bool posted = manager.PostBackground([&, conn = Packet.Conn] {
					{ std::unique_lock lock(mutex); cv.wait(lock, [&] { return releaseBackground; }); }
					completionOk = manager.CompleteAsync(info.Id, conn);
				});
				started = posted;
				return posted ? HandleResult::Async : HandleResult::Disconnect;
			}
			if (id == 1) ++normalCount;
			else if (id == 5) ++realtimeCount;
			else if (id == 9)
			{
				uint32_t value = 0;
				if (!Packet.Buffer.read(value)) return HandleResult::Disconnect;
				latest = static_cast<int>(value); ++updates;
			}
			return HandleResult::Ok;
		};
		server = manager.CreateServer(info, error);
		Check(server != nullptr, "server creation failed");
		Check(manager.FindServer(info.Id) == server, "generic server id lookup failed");
		Check(!manager.CreateServer(info, error), "duplicate server id accepted");
		ServerInfo otherInfo = info;
		otherInfo.Id = info.Id + 1;
		auto otherServer = manager.CreateServer(otherInfo, error);
		Check(otherServer != nullptr, "second server creation failed");
		std::unique_ptr<ENetHost, decltype(&enet_host_destroy)> client(enet_host_create(nullptr, 1, 2, 0, 0), enet_host_destroy);
		Check(client != nullptr, "client creation failed");
		ENetAddress address{};
		enet_address_set_host_ip(&address, "127.0.0.1");
		address.port = server->Config().Port;
		auto* peer = enet_host_connect(client.get(), &address, 2, 0);
		Check(peer != nullptr, "client connection allocation failed");
		bool connected = false;
		auto waitFor = [&](auto Predicate) {
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
			while (!Predicate() && std::chrono::steady_clock::now() < deadline)
			{
				ENetEvent event{};
				int result = enet_host_service(client.get(), &event, 2);
				Check(result >= 0, "client service failed");
				if (result > 0 && event.type == ENET_EVENT_TYPE_CONNECT) connected = true;
				if (result > 0 && event.type == ENET_EVENT_TYPE_RECEIVE) enet_packet_destroy(event.packet);
			}
			Check(Predicate(), "network test timed out");
		};
		std::shared_ptr<Connection> current;
		waitFor([&] { std::lock_guard lock(mutex); current = connection; return connected && current != nullptr; });

		// a queued send must belong to the server that owns the connection
		uint8_t bytes[]{0x45, 0x4E, 2, 0};
		auto* foreignPacket = enet_packet_create(bytes, sizeof(bytes), ENET_PACKET_FLAG_RELIABLE);
		Check(foreignPacket != nullptr, "ownership packet allocation failed");
		foreignPacket->userData = &ownership;
		foreignPacket->freeCallback = FreeOwnershipPacket;
		(void)otherServer->SendPacket(current, foreignPacket);
		waitFor([&] { return ownership.Freed.load() == 1; });
		Check(!ownership.Sent.load(), "packet sent through a server that does not own the connection");
		auto send = [&](TPacketId Id, uint32_t Value = 0) {
			PacketSender sender;
			auto* writer = sender.begin(Id, Id == 9 ? 4 : 0);
			Check(writer != nullptr, "request allocation failed");
			if (Id == 9) writer->write<uint32_t>(Value);
			auto* packet = sender.finalize();
			Check(packet != nullptr, "request finalization failed");
			if (enet_peer_send(peer, 0, packet) != 0) { enet_packet_destroy(packet); Check(false, "request send failed"); }
		};
		send(3);
		waitFor([&] { return started.load(); });
		Check(!manager.CompleteAsync(otherInfo.Id, current), "async completion accepted the wrong server id");
		{
			std::lock_guard lock(current->Rx.Mutex);
			Check(current->Rx.HasAsyncBarrier, "wrong-server async completion released the barrier");
		}
		send(1);
		for (uint32_t i = 1; i <= 10; ++i) send(9, i);
		send(5);
		waitFor([&] { return realtimeCount.load() == 1; });
		Check(normalCount == 0 && updates == 0, "normal work crossed the async barrier");
		{ std::lock_guard lock(mutex); releaseBackground = true; }
		cv.notify_all();
		waitFor([&] { return completionOk.load() && normalCount.load() == 1 && updates.load() == 1; });
		Check(latest == 10, "coalescing did not preserve newest update");
		enet_peer_disconnect(peer, 0);
		waitFor([&] { return disconnects.load() == 1; });
		Check(!overlapped, "handlers or disconnect callbacks overlapped");
		std::shared_ptr<Connection> old;
		{ std::lock_guard lock(mutex); old = connection; }
		PacketSender stale;
		Check(stale.begin(2, 0) != nullptr && !server->SendPacket(old, stale.finalize()), "send to disconnected peer succeeded");
	}
}

int main()
{
	try { Run(); std::cout << "network scheduling tests passed\n"; return 0; }
	catch (const std::exception& Error) { std::cerr << Error.what() << '\n'; return 1; }
}
