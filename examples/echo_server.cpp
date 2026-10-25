#include <csignal>
#include <iostream>
#include <string>
#include <thread>

#include <enet_server/config/ServerConfig.hpp>
#include <enet_server/network/ConnectionStore.hpp>
#include <enet_server/network/HeaderReader.hpp>
#include <enet_server/network/PacketDispatcher.hpp>
#include <enet_server/network/PacketSender.hpp>
#include <enet_server/network/ServerManager.hpp>
#include <enet_server/util/ScopeExit.hpp>

using namespace enet_server;

namespace
{
	volatile std::sig_atomic_t g_Stop = 0;
	void OnSignal(int) { g_Stop = 1; }

	struct ClientState
	{
		std::shared_ptr<Connection> Conn;
		uint64_t Requests = 0;
	};

	bool SendReply(const std::shared_ptr<Server>& ServerPtr, const std::shared_ptr<Connection>& Conn, TPacketId Id, const std::string& Text)
	{
		PacketSender sender;
		auto* writer = sender.begin(Id, Text.size());
		if (!writer) return false;
		writer->write(Text.data(), Text.size());
		return ServerPtr->SendPacket(Conn, sender.finalize());
	}

	class EchoApplication
	{
		using Context = PacketContext<EchoApplication, ClientState>;
	public:
		EchoApplication()
		{
			m_Dispatch.set(1, &OnEcho);
			m_Dispatch.set(3, &OnAsyncEcho);
			m_Dispatch.set(5, &OnPing);
		}
		void Connect(const std::shared_ptr<Connection>& Conn)
		{
			auto state = std::make_shared<ClientState>();
			state->Conn = Conn;
			if (!m_Clients.add(state)) ServerManager::Get().Disconnect(1, Conn);
		}
		void Disconnect(const std::shared_ptr<Connection>& Conn, Network::DisconnectReason Reason)
		{
			m_Clients.remove(Conn);
			std::cout << Conn->RemoteAddress << ": " << Network::DisconnectReasonToString(Reason) << '\n';
		}
		HandleResult Receive(IncomingPacket& Packet)
		{
			TPacketId id = 0;
			if (!ReadPacketHeader(Packet, id)) return HandleResult::Disconnect;
			auto state = m_Clients.find(Packet.Conn);
			if (!state || Packet.Buffer.size_remaining() > 1024) return HandleResult::Disconnect;
			++state->Requests;
			Context context{*this, state, Packet};
			switch (m_Dispatch.dispatch(id, context))
			{
			case decltype(m_Dispatch)::Result::Ok: return HandleResult::Ok;
			case decltype(m_Dispatch)::Result::Async: return HandleResult::Async;
			default: return HandleResult::Disconnect;
			}
		}
	private:
		static HandleResult OnEcho(Context& Ctx)
		{
			std::string text;
			if (!Ctx.Packet.Buffer.read(text, Ctx.Packet.Buffer.size_remaining())) return HandleResult::Disconnect;
			auto server = ServerManager::Get().FindServer(1);
			return server && SendReply(server, Ctx.Packet.Conn, 2, text) ? HandleResult::Ok : HandleResult::Disconnect;
		}
		static HandleResult OnAsyncEcho(Context& Ctx)
		{
			std::string text;
			if (!Ctx.Packet.Buffer.read(text, Ctx.Packet.Buffer.size_remaining())) return HandleResult::Disconnect;
			auto server = ServerManager::Get().FindServer(1);
			if (!server) return HandleResult::Disconnect;
			auto conn = Ctx.Packet.Conn;
			bool queued = ServerManager::Get().PostBackground([server, conn, text = std::move(text)] {
				auto complete = MakeScopeExit([&]() noexcept { ServerManager::Get().CompleteAsync(1, conn); });
				// simulate work that must not block a packet worker
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
				if (!conn->IsClosing() && !SendReply(server, conn, 4, text)) server->Disconnect(conn);
			});
			return queued ? HandleResult::Async : HandleResult::Disconnect;
		}
		static HandleResult OnPing(Context& Ctx)
		{
			if (Ctx.Packet.Buffer.size_remaining()) return HandleResult::Disconnect;
			auto server = ServerManager::Get().FindServer(1);
			return server && SendReply(server, Ctx.Packet.Conn, 6, "pong") ? HandleResult::Ok : HandleResult::Disconnect;
		}
		ConnectionStore<ClientState> m_Clients;
		PacketDispatcher<Context, 1, 5> m_Dispatch;
	};
}

int main(int Argc, char** Argv)
{
	try
	{
		ServerConfig config(Argc > 1 ? Argv[1] : "server.conf");
		ServerSettings settings;
		if (auto error = config.Load(settings)) { std::cerr << error->what() << '\n'; return 1; }
		EchoApplication application;
		settings.Server.OnConnect = [&](auto Conn) { application.Connect(Conn); };
		settings.Server.OnDisconnect = [&](auto Conn, auto Reason) { application.Disconnect(Conn, Reason); };
		settings.Server.OnReceive = [&](IncomingPacket& Packet) { return application.Receive(Packet); };
		settings.Server.ClassifyPacket = [](TPacketId Id, enet_uint8) {
			return PacketTraits{Id == 5 ? WorkClass::Realtime : Id == 3 ? WorkClass::AsyncLogic : WorkClass::Logic};
		};
		auto& manager = ServerManager::Get();
		if (auto error = manager.Initialize(settings.IoTimeoutMinMs, settings.IoTimeoutMaxMs,
			settings.LogicWorkers, settings.RealtimeWorkers, settings.BackgroundWorkers))
		{
			std::cerr << error->what() << '\n'; return 1;
		}
		auto shutdown = MakeScopeExit([&]() noexcept { manager.Shutdown(); });
		std::shared_ptr<ReturnEx> error;
		auto server = manager.CreateServer(settings.Server, error);
		if (!server) { std::cerr << (error ? error->what() : "server creation failed") << '\n'; return 1; }
		std::signal(SIGINT, OnSignal);
		std::signal(SIGTERM, OnSignal);
		std::cout << "Echo server on " << settings.Server.BindAddress << ':' << settings.Server.Port << "; Ctrl+C to stop\n";
		while (!g_Stop && manager.IsRunning()) std::this_thread::sleep_for(std::chrono::milliseconds(50));
		return manager.IsRunning() ? 0 : 1;
	}
	catch (const std::exception& Error) { std::cerr << Error.what() << '\n'; return 1; }
}
