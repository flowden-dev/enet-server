#include <enet_server/config/ServerConfig.hpp>

namespace enet_server
{
const std::vector<std::pair<std::string, std::string>>& ServerConfig::default_config() const
{
	static const std::vector<std::pair<std::string, std::string>> defaults {
		{"bind_address", "127.0.0.1"}, {"port", "7777"}, {"max_connections", "128"},
		{"logic_workers", "2"}, {"realtime_workers", "1"}, {"background_workers", "1"},
		{"io_timeout_min_ms", "1"}, {"io_timeout_max_ms", "8"},
		{"max_rate_packets", "1000"}, {"rate_window_seconds", "10"},
		{"max_packet_queue", "10000"}, {"max_connection_queue", "128"},
		{"max_connection_send_queue", "128"},
		{"max_connection_burst", "16"}, {"max_events_per_turn", "32"},
		{"max_io_commands_per_turn", "64"}, {"io_weight", "1"}
	};
	return defaults;
}

std::shared_ptr<ReturnEx> ServerConfig::Load(ServerSettings& Out)
{
	if (auto error = read()) return error;
	ServerSettings settings;
	settings.Server.Identifier = "Echo";
	settings.Server.Id = 1;
	auto readRange = [&](const char* Key, size_t Min, size_t Max, size_t& Value) {
		return try_get(Key, Value) && Value >= Min && Value <= Max;
	};
	size_t port = 0, window = 0;
	if (!try_get("bind_address", settings.Server.BindAddress) || settings.Server.BindAddress.empty() ||
		!readRange("port", 1, 65535, port) ||
		!readRange("max_connections", 1, ENET_PROTOCOL_MAXIMUM_PEER_ID, settings.Server.MaxConnections) ||
		!readRange("logic_workers", 1, 64, settings.LogicWorkers) ||
		!readRange("realtime_workers", 1, 64, settings.RealtimeWorkers) ||
		!readRange("background_workers", 1, 64, settings.BackgroundWorkers) ||
		!readRange("io_timeout_min_ms", 1, 1000, settings.IoTimeoutMinMs) ||
		!readRange("io_timeout_max_ms", settings.IoTimeoutMinMs, 1000, settings.IoTimeoutMaxMs) ||
		!readRange("max_rate_packets", 1, 1000000, settings.Server.MaxRatePackets) ||
		!readRange("rate_window_seconds", 1, 86400, window) ||
		!readRange("max_packet_queue", 1, 1000000, settings.Server.MaxPacketQueueSize) ||
		!readRange("max_connection_queue", 1, 100000, settings.Server.MaxConnectionPacketQueueSize) ||
		!readRange("max_connection_send_queue", 1, 100000, settings.Server.MaxConnectionSendQueueSize) ||
		!readRange("max_connection_burst", 1, 1000, settings.Server.MaxConnectionBurst) ||
		!readRange("max_events_per_turn", 1, 10000, settings.Server.MaxEventsPerTurn) ||
		!readRange("max_io_commands_per_turn", 1, 10000, settings.Server.MaxIoCommandsPerTurn) ||
		!readRange("io_weight", 1, Network::ServerIoWeightMax, settings.Server.IoWeight))
		return std::make_shared<ReturnEx>(1, "invalid server configuration: check numeric ranges and bind address");
	settings.Server.Port = static_cast<uint16_t>(port);
	settings.Server.MaxRateWindow = std::chrono::seconds(window);
	Out = std::move(settings);
	return nullptr;
}
}
