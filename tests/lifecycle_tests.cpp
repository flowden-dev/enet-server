#include <enet_server/network/PacketSender.hpp>
#include <enet_server/network/ServerManager.hpp>
#include <enet_server/util/ScopeExit.hpp>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>

using namespace enet_server;

namespace
{
	void Check(bool Value, const char* Message) { if (!Value) throw std::runtime_error(Message); }

	struct Gate
	{
		std::mutex Mutex;
		std::condition_variable Cv;
		std::atomic<bool> Entered{false};
		bool Released = false;
		void Wait()
		{
			std::unique_lock lock(Mutex);
			Entered = true;
			Cv.wait(lock, [&] { return Released; });
		}
		void Release() { { std::lock_guard lock(Mutex); Released = true; } Cv.notify_all(); }
	};

	void Run(const std::string& Mode)
	{
		Gate workerGate, ioGate, logicGate;
		std::atomic<int> disconnected{0}, received{0}, completed{0}, connects{0}, active{0};
		std::atomic<bool> delayed{false}, timerPassed{false};
		std::atomic<bool> backgroundActive{false}, completionReturned{false}, overlapped{false};
		bool cleanup = Mode == "cleanup" || Mode == "cleanup-active" || Mode == "cleanup-async";
		std::atomic<std::shared_ptr<Connection>> connection;
		auto& manager = ServerManager::Get();
		Check(!manager.Initialize(1, 2, 1, 1, 1), "initialization failed");
		auto shutdown = MakeScopeExit([&]() noexcept { manager.Shutdown(); });
		auto unblock = MakeScopeExit([&]() noexcept { workerGate.Release(); ioGate.Release(); logicGate.Release(); });
		ServerInfo info;
		info.Id = 7;
		info.MaxConnections = 1;
		info.OnConnect = [&](auto Conn) {
			connection.store(Conn);
			++connects;
			// hold one I/O turn to make sends preceding graceful close reproducible
			if (Mode == "graceful" || Mode == "send-close" || Mode == "immediate") ioGate.Wait();
		};
		info.OnDisconnect = [&](auto, auto) {
			if (active.load() || backgroundActive.load()) overlapped = true;
			++disconnected;
		};
		info.ClassifyPacket = [&](TPacketId, enet_uint8) {
			return PacketTraits{Mode == "cleanup-async" ? WorkClass::AsyncLogic : WorkClass::Logic};
		};
		info.OnReceive = [&](auto& Packet) {
			++active;
			auto exit = MakeScopeExit([&]() noexcept { --active; });
			if (Mode == "cleanup-active") workerGate.Wait();
			if (Mode == "cleanup-async")
			{
				bool posted = manager.PostBackground([&, conn = Packet.Conn] {
					backgroundActive = true;
					workerGate.Wait();
					backgroundActive = false;
					manager.CompleteAsync(info.Id, conn);
					completionReturned = true;
				});
				return posted ? HandleResult::Async : HandleResult::Disconnect;
			}
			return HandleResult::Ok;
		};
		std::shared_ptr<ReturnEx> error;
		auto server = manager.CreateServer(info, error);
		Check(server != nullptr, "server creation failed");
		std::unique_ptr<ENetHost, decltype(&enet_host_destroy)> client(enet_host_create(nullptr, 1, 2, 0, 0), enet_host_destroy);
		Check(client != nullptr, "client creation failed");
		ENetAddress address{};
		enet_address_set_host_ip(&address, "127.0.0.1");
		address.port = server->Config().Port;
		auto* peer = enet_host_connect(client.get(), &address, 2, 0);
		Check(peer != nullptr, "connection allocation failed");
		bool connected = false, clientDisconnected = false;
		auto waitFor = [&](auto Predicate) {
			auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
			while (!Predicate() && std::chrono::steady_clock::now() < deadline)
			{
				ENetEvent event{};
				int result = enet_host_service(client.get(), &event, 1);
				Check(result >= 0, "client service failed");
				if (result > 0 && event.type == ENET_EVENT_TYPE_CONNECT) connected = true;
				if (result > 0 && event.type == ENET_EVENT_TYPE_DISCONNECT) clientDisconnected = true;
				if (result > 0 && event.type == ENET_EVENT_TYPE_RECEIVE)
				{
					++received;
					enet_packet_destroy(event.packet);
				}
			}
			Check(Predicate(), "lifecycle test timed out");
		};
		waitFor([&] { return connected && connection.load() != nullptr; });
		auto conn = connection.load();
		if (Mode == "graceful" || Mode == "send-close" || Mode == "immediate")
		{
			waitFor([&] { return ioGate.Entered.load(); });
			PacketSender sender;
			Check(sender.begin(2, 0) != nullptr && server->SendPacket(conn, sender.finalize()), "initial send failed");
			if (Mode == "graceful") Check(server->DisconnectLater(conn), "graceful close failed");
			else if (Mode == "immediate") Check(server->Disconnect(conn), "immediate close failed");
			else Check(sender.begin(4, 0) != nullptr && server->SendPacketAndDc(conn, sender.finalize()), "send and close failed");
			Check(sender.begin(6, 0) != nullptr && !server->SendPacket(conn, sender.finalize()), "send accepted after closing");
			ioGate.Release();
			waitFor([&] { return clientDisconnected && disconnected == 1; });
			Check(received == (Mode == "immediate" ? 0 : Mode == "graceful" ? 1 : 2), "disconnect did not preserve its send policy");
			return;
		}

		if (Mode == "cleanup-active" || Mode == "cleanup-async")
		{
			PacketSender sender;
			Check(sender.begin(1, 0) != nullptr, "request allocation failed");
			auto* packet = sender.finalize();
			Check(packet != nullptr, "request finalization failed");
			if (enet_peer_send(peer, 0, packet) != 0) { enet_packet_destroy(packet); Check(false, "request send failed"); }
		}
		else Check(manager.PostLogic([&] { workerGate.Wait(); }), "worker blocker failed");
		waitFor([&] { return workerGate.Entered.load(); });
		if (Mode == "cleanup-async")
		{
			Check(manager.PostLogic([&] { logicGate.Wait(); }), "logic blocker failed");
			waitFor([&] { return logicGate.Entered.load(); });
		}
		int accepted = 0;
		while (manager.PostLogic([&] { ++completed; }))
		{
			++accepted;
			Check(accepted <= 65536, "worker queue is unbounded");
		}
		Check(accepted == 65536, "unexpected worker queue capacity");
		if (cleanup)
		{
			enet_peer_disconnect(peer, 0);
			waitFor([&] { return clientDisconnected && !conn->IsTransportAttached(); });
			Check(disconnected == 0, "cleanup crossed an active handler, barrier, or full queue");
			// the old connection retains its slot while cleanup is blocked
			clientDisconnected = false;
			Check(enet_host_connect(client.get(), &address, 2, 0) != nullptr, "replacement connection allocation failed");
			waitFor([&] { return clientDisconnected; });
			Check(connects == 1, "cleanup backlog did not bound connection admission");
		}
		else
		{
			Check(manager.PostLogicDelayed(std::chrono::milliseconds(0), [&] { delayed = true; }), "delayed job rejected");
			Check(manager.PostRealtimeDelayed(std::chrono::milliseconds(20), [&] { timerPassed = true; }), "timer marker rejected");
			waitFor([&] { return timerPassed.load(); });
		}
		workerGate.Release();
		if (Mode == "cleanup-async") waitFor([&] { return completionReturned.load(); });
		logicGate.Release();
		waitFor([&] { return completed == accepted; });
		// allow retries while keeping the assertion independent of queue-drain timing
		auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
		waitFor([&] { return (cleanup ? disconnected == 1 : delayed.load()) || std::chrono::steady_clock::now() >= deadline; });
		Check(cleanup ? disconnected == 1 : delayed.load(),
			cleanup ? "disconnect callback lost under queue saturation" : "accepted delayed job lost under queue saturation");
		Check(!overlapped, "cleanup overlapped a handler or unfinished async work");
		if (cleanup)
		{
			Check(enet_host_connect(client.get(), &address, 2, 0) != nullptr, "reconnection allocation failed");
			waitFor([&] { return connects == 2; });
		}
	}
}

int main(int Argc, char** Argv)
{
	try
	{
		Check(Argc == 2, "expected test mode");
		Run(Argv[1]);
		Run(Argv[1]);
		std::cout << Argv[1] << " lifecycle test passed\n";
		return 0;
	}
	catch (const std::exception& Error) { std::cerr << Error.what() << '\n'; return 1; }
}
