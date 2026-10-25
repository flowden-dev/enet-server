#include <enet_server/network/HeaderReader.hpp>
#include <enet_server/network/PacketSender.hpp>
#include <enet_server/network/PacketCompression.hpp>
#include <enet_server/config/ServerConfig.hpp>
#include <enet_server/util/ScopeExit.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>

using namespace enet_server;

int main()
{
	try
	{
		PacketSender sender;
		auto* writer = sender.begin(1, 4);
		if (!writer) throw std::runtime_error("packet allocation failed");
		writer->write<uint32_t>(42);
		PacketBuffer buffer(sender.finalize());
		IncomingPacket packet(nullptr, std::move(buffer));
		TPacketId id = 0;
		uint32_t value = 0;
		if (!ReadPacketHeader(packet, id) || id != 1 || !packet.Buffer.read(value) || value != 42 || packet.Buffer.size_remaining())
			throw std::runtime_error("packet round-trip failed");
		if (packet.Buffer.read(value)) throw std::runtime_error("truncated read accepted");
		if (sender.begin(1, Packet::MaxSize)) throw std::runtime_error("oversized packet accepted");
		uint8_t invalid[]{0, 0, 1, 0};
		if (PeekPacketHeader(invalid, 4, id) || PeekPacketHeader(invalid, 3, id) || PeekPacketHeader(nullptr, 4, id))
			throw std::runtime_error("malformed header accepted");
		auto* sharedWriter = sender.begin(7, 4);
		if (!sharedWriter) throw std::runtime_error("shared allocation failed");
		sharedWriter->write<uint32_t>(123);
		auto shared = sender.finalize_shared();
		auto first = shared.make();
		auto second = shared.make();
		if (!first || !second) throw std::runtime_error("shared packet creation failed");
		shared = {};
		if (first->data != second->data) throw std::runtime_error("shared payload copied");
		enet_packet_destroy(first);
		IncomingPacket surviving(nullptr, PacketBuffer(second));
		if (!ReadPacketHeader(surviving, id) || !surviving.Buffer.read(value) || value != 123)
			throw std::runtime_error("shared packet lifetime failed");
		auto path = std::filesystem::temp_directory_path() / ("enet-server-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".conf");
		auto cleanup = MakeScopeExit([&]() noexcept { std::error_code error; std::filesystem::remove(path, error); });
		ServerConfig config(path.string());
		ServerSettings settings;
		if (config.Load(settings) || settings.Server.Port != 7777) throw std::runtime_error("default configuration failed");
		for (const char* invalidConfig : {"port=-1\n", "port=65536\n", "port=12oops\n", "port=7777\nport=8888\n", "max_connections=999999\n"})
		{
			{ std::ofstream file(path); file << invalidConfig; }
			if (!config.Load(settings)) throw std::runtime_error("invalid configuration accepted");
		}
#ifdef ENET_SERVER_WITH_LZ4
		for (std::string text : {std::string{}, std::string("short"), std::string(8192, 'x')})
		{
			auto* payload = sender.begin(11, text.size());
			if (!payload) throw std::runtime_error("compression allocation failed");
			payload->write(text.data(), text.size());
			if (!PacketCompression::Compress(*payload)) throw std::runtime_error("compression failed");
			PacketBuffer compressed(sender.finalize(), Packet::HeaderSize);
			ByteReader decoded;
			std::string output;
			if (!PacketCompression::Decompress(compressed, decoded) || !decoded.read(output, decoded.size_remaining()) || output != text)
				throw std::runtime_error("compression round-trip failed");
		}
		uint8_t malformed[]{1, 0xff, 0xff, 0xff, 0xff};
		ByteReader input(malformed, sizeof(malformed)), decoded;
		if (PacketCompression::Decompress(input, decoded)) throw std::runtime_error("oversized decompression accepted");
#endif
		std::cout << "core tests passed\n";
		return 0;
	}
	catch (const std::exception& Error) { std::cerr << Error.what() << '\n'; return 1; }
}
