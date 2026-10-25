#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include <enet_server/network/Connection.hpp>

namespace enet_server
{

class Connection;

template<typename TClientState>
class ConnectionStore
{

private:
	mutable std::shared_mutex m_Mutex;
	std::unordered_map<const Connection*, std::shared_ptr<TClientState>> m_Index;

public:
	bool add(const std::shared_ptr<TClientState>& State)
	{
		if (!State || !State->Conn || !State->Conn->IsTransportAttached() || State->Conn->IsClosing()) return false;

		const Connection* key = State->Conn.get();

		std::unique_lock lock(m_Mutex);
		m_Index[key] = State;

		return true;
	}

	std::shared_ptr<TClientState> find(const std::shared_ptr<Connection>& Conn) const
	{
		return Conn ? find(Conn.get()) : nullptr;
	}
	std::shared_ptr<TClientState> find(const Connection* Conn) const
	{
		if (!Conn) return nullptr;

		std::shared_lock lock(m_Mutex);

		auto it = m_Index.find(Conn);
		if (it == m_Index.end()) return nullptr;

		return it->second;
	}

	std::shared_ptr<TClientState> remove(const std::shared_ptr<Connection>& Conn)
	{
		return Conn ? remove(Conn.get()) : nullptr;
	}
	std::shared_ptr<TClientState> remove(const Connection* Conn)
	{
		if (!Conn) return nullptr;

		std::unique_lock lock(m_Mutex);

		auto it = m_Index.find(Conn);
		if (it == m_Index.end()) return nullptr;

		auto out = std::move(it->second);
		m_Index.erase(it);

		return out;
	}

	std::vector<std::shared_ptr<TClientState>> snapshot() const
	{
		std::shared_lock lock(m_Mutex);

		std::vector<std::shared_ptr<TClientState>> out;
		out.reserve(m_Index.size());

		for (const auto& [conn, state] : m_Index)
			out.push_back(state);

		return out;
	}

	size_t size() const
	{
		std::shared_lock lock(m_Mutex);
		return m_Index.size();
	}

	bool empty() const
	{
		std::shared_lock lock(m_Mutex);
		return m_Index.empty();
	}
};
}
