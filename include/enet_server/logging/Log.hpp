#pragma once

#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

namespace enet_server
{
	enum class LogLevel { Info, Warning, Error };

	class LogLine
	{
	public:
		explicit LogLine(LogLevel Level, bool Enabled = true) : m_Level(Level), m_Enabled(Enabled) { }
		~LogLine() noexcept
		{
			if (!m_Enabled) return;
			try
			{
				static std::mutex outputMutex;
				std::lock_guard lock(outputMutex);
				const char* level = m_Level == LogLevel::Error ? "error" : m_Level == LogLevel::Warning ? "warning" : "info";
				std::clog << '[' << level << "] " << m_Stream.str() << '\n';
			}
			catch (...) { }
		}
		LogLine(const LogLine&) = delete;
		LogLine& operator=(const LogLine&) = delete;

		template<typename T>
		LogLine& operator<<(T&& Value) { if (m_Enabled) m_Stream << std::forward<T>(Value); return *this; }

	private:
		LogLevel m_Level;
		bool m_Enabled;
		std::ostringstream m_Stream;
	};

	inline bool ShouldLog(const char* Key, std::chrono::steady_clock::duration Interval)
	{
		static std::mutex mutex;
		static std::unordered_map<std::string, std::chrono::steady_clock::time_point> deadlines;
		std::lock_guard lock(mutex);
		auto now = std::chrono::steady_clock::now();
		auto& deadline = deadlines[Key];
		if (now < deadline) return false;
		deadline = now + Interval;
		return true;
	}
}

// token pasting keeps platform macros such as ERROR from expanding the level argument
#define ENET_SERVER_LOG_LEVEL_INFO ::enet_server::LogLevel::Info
#define ENET_SERVER_LOG_LEVEL_WARNING ::enet_server::LogLevel::Warning
#define ENET_SERVER_LOG_LEVEL_ERROR ::enet_server::LogLevel::Error
#define ENET_SERVER_LOG(Level) ::enet_server::LogLine(ENET_SERVER_LOG_LEVEL_##Level)
#define ENET_SERVER_LOG_EVERY(Key, Level, Interval) ::enet_server::LogLine(ENET_SERVER_LOG_LEVEL_##Level, ::enet_server::ShouldLog(Key, Interval))
