#include <enet/enet.h>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace
{
	void Check(bool Value, const char* Message) { if (!Value) throw std::runtime_error(Message); }

	// expose unwritten reassembly bytes deterministically
	void* ENET_CALLBACK Allocate(size_t Size)
	{
		void* data = std::malloc(Size);
		if (data) std::memset(data, 0xA5, Size);
		return data;
	}
	void ENET_CALLBACK Free(void* Data) { std::free(Data); }
	void ENET_CALLBACK NoMemory() { std::abort(); }

	void Run(bool Reliable)
	{
		ENetAddress address{};
		Check(enet_address_set_host_ip(&address, "127.0.0.1") == 0, "address setup failed");
		using Host = std::unique_ptr<ENetHost, decltype(&enet_host_destroy)>;
		Host server(enet_host_create(&address, 1, 2, 0, 0), enet_host_destroy);
		Host client(enet_host_create(nullptr, 1, 2, 0, 0), enet_host_destroy);
		Check(server && client, "host creation failed");
		address.port = server->address.port;
		auto* peer = enet_host_connect(client.get(), &address, 2, 0);
		Check(peer != nullptr, "connection allocation failed");
		bool connected = false, accepted = false;
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
		while ((!connected || !accepted) && std::chrono::steady_clock::now() < deadline)
		{
			ENetEvent event{};
			int status = enet_host_service(client.get(), &event, 1);
			Check(status >= 0, "client service failed");
			if (status > 0 && event.type == ENET_EVENT_TYPE_CONNECT) connected = true;
			status = enet_host_service(server.get(), &event, 1);
			Check(status >= 0, "server service failed");
			if (status > 0 && event.type == ENET_EVENT_TYPE_CONNECT) accepted = true;
		}
		Check(connected && accepted, "connection timed out");

		// ordinary fragmented packets must still arrive byte-for-byte
		std::array<unsigned char, 8192> expected{};
		for (size_t i = 0; i != expected.size(); ++i) expected[i] = static_cast<unsigned char>(i);
		auto* packet = enet_packet_create(expected.data(), expected.size(), Reliable ? ENET_PACKET_FLAG_RELIABLE : ENET_PACKET_FLAG_UNRELIABLE_FRAGMENT);
		Check(packet != nullptr, "packet allocation failed");
		if (enet_peer_send(peer, 0, packet) != 0) { enet_packet_destroy(packet); Check(false, "valid fragment send failed"); }
		bool delivered = false;
		deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
		while (!delivered && std::chrono::steady_clock::now() < deadline)
		{
			ENetEvent event{};
			int status = enet_host_service(client.get(), &event, 1);
			Check(status >= 0, "valid fragment client service failed");
			status = enet_host_service(server.get(), &event, 1);
			Check(status >= 0, "valid fragment server service failed");
			if (status > 0 && event.type == ENET_EVENT_TYPE_RECEIVE)
			{
				std::unique_ptr<ENetPacket, decltype(&enet_packet_destroy)> received(event.packet, enet_packet_destroy);
				Check(received->dataLength == expected.size() && std::memcmp(received->data, expected.data(), expected.size()) == 0,
					"valid fragments changed payload");
				delivered = true;
			}
		}
		Check(delivered, "valid fragments timed out");
		enet_uint16 reliableSequence = peer->channels[0].outgoingReliableSequenceNumber;
		enet_uint16 startSequence = Reliable ? reliableSequence + 1 : peer->channels[0].outgoingUnreliableSequenceNumber + 1;

		// two distinct fragment numbers overlap the header, leaving four payload bytes unwritten
		for (enet_uint32 i = 0; i != 2; ++i)
		{
			ENetProtocolHeader header{};
			header.peerID = ENET_HOST_TO_NET_16(peer->outgoingPeerID |
				(peer->outgoingSessionID << ENET_PROTOCOL_HEADER_SESSION_SHIFT) | ENET_PROTOCOL_HEADER_FLAG_SENT_TIME);
			header.sentTime = ENET_HOST_TO_NET_16(enet_time_get() & 0xFFFF);
			ENetProtocolSendFragment fragment{};
			fragment.header.command = Reliable ? static_cast<int>(ENET_PROTOCOL_COMMAND_SEND_FRAGMENT) | ENET_PROTOCOL_COMMAND_FLAG_ACKNOWLEDGE
				: ENET_PROTOCOL_COMMAND_SEND_UNRELIABLE_FRAGMENT;
			fragment.header.channelID = 0;
			fragment.header.reliableSequenceNumber = ENET_HOST_TO_NET_16(Reliable ? startSequence + i : reliableSequence);
			fragment.startSequenceNumber = ENET_HOST_TO_NET_16(startSequence);
			fragment.dataLength = ENET_HOST_TO_NET_16(4);
			fragment.fragmentCount = ENET_HOST_TO_NET_32(2);
			fragment.fragmentNumber = ENET_HOST_TO_NET_32(i);
			fragment.totalLength = ENET_HOST_TO_NET_32(8);
			fragment.fragmentOffset = 0;
			std::array<unsigned char, sizeof(header) + sizeof(fragment) + 4> bytes{};
			std::memcpy(bytes.data(), &header, sizeof(header));
			std::memcpy(bytes.data() + sizeof(header), &fragment, sizeof(fragment));
			unsigned char payload[]{0x45, 0x4E, 1, 0};
			std::memcpy(bytes.data() + sizeof(header) + sizeof(fragment), payload, sizeof(payload));
			ENetBuffer buffer{};
			buffer.data = bytes.data(); buffer.dataLength = bytes.size();
			Check(enet_socket_send(client->socket, &address, &buffer, 1) == static_cast<int>(bytes.size()), "fragment send failed");
		}

		deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
		while (std::chrono::steady_clock::now() < deadline)
		{
			ENetEvent event{};
			int status = enet_host_service(server.get(), &event, 1);
			Check(status >= 0, "fragment service failed");
			if (status > 0 && event.type == ENET_EVENT_TYPE_RECEIVE)
			{
				std::unique_ptr<ENetPacket, decltype(&enet_packet_destroy)> packet(event.packet, enet_packet_destroy);
				Check(packet->dataLength == 8, "unexpected reassembly size");
				for (size_t i = 4; i != 8; ++i)
					Check(packet->data[i] == 0, "fragment reassembly exposed unwritten heap bytes");
				return;
			}
		}
		throw std::runtime_error("fragment delivery timed out");
	}
}

int main()
{
	ENetCallbacks callbacks{Allocate, Free, NoMemory};
	if (enet_initialize_with_callbacks(ENET_VERSION, &callbacks) != 0) return 1;
	struct Runtime { ~Runtime() { enet_deinitialize(); } } runtime;
	try
	{
		Run(true);
		Run(false);
		std::cout << "fragment initialization tests passed\n";
		return 0;
	}
	catch (const std::exception& Error) { std::cerr << Error.what() << '\n'; return 1; }
}
