#include <enet/enet.h>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// this client uses only the public enet api
int main(int Argc, char** Argv)
{
	std::string hostName = Argc > 1 ? Argv[1] : "127.0.0.1";
	std::string portText = Argc > 2 ? Argv[2] : "7777";
	std::string text = Argc > 3 ? Argv[3] : "hello";
	std::string mode = Argc > 4 ? Argv[4] : "echo";
	unsigned int port = 0;
	auto [end, error] = std::from_chars(portText.data(), portText.data() + portText.size(), port);
	if (error != std::errc{} || end != portText.data() + portText.size() || !port || port > 65535 || text.size() > 1024 ||
		(mode != "echo" && mode != "async" && mode != "ping"))
	{
		std::cerr << "usage: echo_client [host] [port] [message] [echo|async|ping]\n";
		return 1;
	}
	if (enet_initialize() != 0) { std::cerr << "ENet initialization failed\n"; return 1; }
	struct RuntimeGuard { ~RuntimeGuard() { enet_deinitialize(); } } runtime;
	std::unique_ptr<ENetHost, decltype(&enet_host_destroy)> host(enet_host_create(nullptr, 1, 2, 0, 0), enet_host_destroy);
	if (!host) return 1;
	ENetAddress address{};
	address.port = static_cast<enet_uint16>(port);
	if (enet_address_set_host(&address, hostName.c_str()) != 0) { std::cerr << "invalid host\n"; return 1; }
	ENetPeer* peer = enet_host_connect(host.get(), &address, 2, 0);
	if (!peer) return 1;
	uint8_t requestId = mode == "async" ? 3 : mode == "ping" ? 5 : 1;
	if (mode == "ping") text.clear();
	bool sent = false;
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
	while (std::chrono::steady_clock::now() < deadline)
	{
		ENetEvent event{};
		int result = enet_host_service(host.get(), &event, 25);
		if (result < 0) break;
		if (!result) continue;
		if (event.type == ENET_EVENT_TYPE_CONNECT && !sent)
		{
			std::vector<uint8_t> bytes{0x45, 0x4E, requestId, 0};
			bytes.insert(bytes.end(), text.begin(), text.end());
			ENetPacket* packet = enet_packet_create(bytes.data(), bytes.size(), ENET_PACKET_FLAG_RELIABLE);
			if (!packet) return 1;
			if (enet_peer_send(peer, requestId == 5 ? 1 : 0, packet) != 0) { enet_packet_destroy(packet); return 1; }
			sent = true;
		}
		else if (event.type == ENET_EVENT_TYPE_RECEIVE)
		{
			std::unique_ptr<ENetPacket, decltype(&enet_packet_destroy)> packet(event.packet, enet_packet_destroy);
			if (packet->dataLength < 4 || packet->data[0] != 0x45 || packet->data[1] != 0x4E ||
				packet->data[2] != requestId + 1 || packet->data[3] != 0) return 1;
			std::string reply(reinterpret_cast<char*>(packet->data + 4), packet->dataLength - 4);
			if (reply != (mode == "ping" ? "pong" : text)) return 1;
			std::cout << "reply: " << reply << '\n';
			enet_peer_disconnect(peer, 0);
			auto disconnectDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
			while (std::chrono::steady_clock::now() < disconnectDeadline)
			{
				ENetEvent disconnect{};
				int status = enet_host_service(host.get(), &disconnect, 10);
				if (status < 0 || (status > 0 && disconnect.type == ENET_EVENT_TYPE_DISCONNECT)) break;
				if (status > 0 && disconnect.type == ENET_EVENT_TYPE_RECEIVE) enet_packet_destroy(disconnect.packet);
			}
			return 0;
		}
		else if (event.type == ENET_EVENT_TYPE_DISCONNECT) break;
	}
	std::cerr << "connection or reply timed out\n";
	return 1;
}
