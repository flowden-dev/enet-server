#pragma once

#include <string>
#include <enet_server/config/ConfigFile.hpp>
#include <enet_server/network/Server.hpp>

namespace enet_server
{
	struct ServerSettings
	{
		ServerInfo Server;
		size_t LogicWorkers = 2;
		size_t RealtimeWorkers = 1;
		size_t BackgroundWorkers = 1;
		size_t IoTimeoutMinMs = 1;
		size_t IoTimeoutMaxMs = 8;
	};

	class ServerConfig : public ConfigFile
	{
	public:
		explicit ServerConfig(std::string Path = "server.conf") : m_Path(std::move(Path)) { }
		std::shared_ptr<ReturnEx> Load(ServerSettings& Out);
		std::string filename() const override { return m_Path; }

	protected:
		std::string header() const override { return "# enet-server configuration\n# restart the server after editing"; }
		std::string footer() const override { return {}; }
		const std::vector<std::pair<std::string, std::string>>& default_config() const override;

	private:
		std::string m_Path;
	};
}
