#pragma once

#include <type_traits>
#include <utility>

namespace enet_server
{

/*++

use like this:

auto pendingGuard = MakeScopeExit([srv = Srv.get()]() noexcept
{
	srv->SubPendingPackets(1);
});

--*/

template<typename Fn>
class ScopeExit
{
private:
	Fn m_Fn;
	bool m_Active = true;

public:
	explicit ScopeExit(Fn&& F) noexcept(std::is_nothrow_move_constructible_v<Fn>)
		: m_Fn(std::move(F)) { }

	~ScopeExit() noexcept { if (m_Active) m_Fn(); }

	ScopeExit(const ScopeExit&) = delete;
	ScopeExit& operator=(const ScopeExit&) = delete;

	ScopeExit(ScopeExit&& Other) noexcept(std::is_nothrow_move_constructible_v<Fn>)
		: m_Fn(std::move(Other.m_Fn)), m_Active(Other.m_Active)
	{
		Other.m_Active = false;
	}

	void release() noexcept { m_Active = false; }
};

template<typename Fn>
ScopeExit<Fn> MakeScopeExit(Fn&& F) { return ScopeExit<Fn>(std::forward<Fn>(F)); }
}
