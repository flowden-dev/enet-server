#include <enet_server/config/ServerConfig.hpp>
#include <enet_server/util/ScopeExit.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#endif

using namespace enet_server;

int main()
{
	try
	{
		auto path = std::filesystem::temp_directory_path() / ("enet-config-io-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
		std::filesystem::create_directory(path);
		auto cleanup = MakeScopeExit([&]() noexcept { std::error_code error; std::filesystem::remove_all(path, error); });
		ServerSettings settings;
		ServerConfig directory(path.string());
		if (!directory.Load(settings)) throw std::runtime_error("directory was accepted as a configuration file");
		ServerConfig missingParent((path / "absent" / "server.conf").string());
		if (!missingParent.Load(settings)) throw std::runtime_error("unwritable configuration path was accepted");
		auto filePath = path / "server.conf";
		ServerConfig fresh(filePath.string());
		if (fresh.Load(settings) || !std::filesystem::is_regular_file(filePath) || settings.Server.MaxConnectionSendQueueSize != 128)
			throw std::runtime_error("default configuration creation failed");
		// configuration files may move between Windows and Linux
		for (const char* newline : {"\n", "\r\n"})
		{
			{
				std::ofstream file(filePath, std::ios::binary | std::ios::trunc);
				file << "# line endings" << newline << newline << " \t" << newline << "port=4321" << newline;
			}
			if (fresh.Load(settings) || settings.Server.Port != 4321)
				throw std::runtime_error("configuration with blank LF or CRLF lines was rejected");
		}
#ifndef _WIN32
		// root can read mode-0222 files; exercise this path when normal file permissions apply
		if (geteuid() != 0)
		{
			{ std::ofstream file(filePath); file << "port=4321\n"; }
			if (chmod(filePath.c_str(), 0222) != 0) throw std::runtime_error("could not set test file permissions");
			auto restore = MakeScopeExit([&]() noexcept { (void)chmod(filePath.c_str(), 0600); });
			if (!fresh.Load(settings)) throw std::runtime_error("unreadable file was accepted");
			if (chmod(filePath.c_str(), 0600) != 0) throw std::runtime_error("could not restore test file permissions");
			std::ifstream file(filePath);
			std::string contents;
			std::getline(file, contents);
			if (contents != "port=4321") throw std::runtime_error("unreadable file was overwritten");
		}
		else std::cout << "unreadable-file permission check skipped for root\n";

#endif
		std::cout << "configuration I/O tests passed\n";
		return 0;
	}
	catch (const std::exception& Error) { std::cerr << Error.what() << '\n'; return 1; }
}
