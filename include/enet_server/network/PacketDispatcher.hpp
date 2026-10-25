#pragma once

#include <array>

#include <enet_server/protocol/Packet.hpp>
#include <enet_server/protocol/Scheduling.hpp>

namespace enet_server
{

template<typename TContext, TPacketId RangeBegin, TPacketId RangeEnd>
class PacketDispatcher
{
	static_assert(RangeEnd >= RangeBegin, "invalid packet range");

public:
	using Handler = HandleResult(*)(TContext&);

	static constexpr uint16_t Begin = RangeBegin;
	static constexpr uint16_t End = RangeEnd;
	static constexpr size_t Count = static_cast<size_t>(RangeEnd - RangeBegin + 1);

	enum class Result
	{
		InvalidId,
		NoHandler,
		Ok,
		Async,
		Disconnect
	};

private:
	std::array<Handler, Count> m_Handler{ };

public:
	void set(TPacketId Id, Handler Fn)
	{
		if (Id < Begin || Id > End) return;

		m_Handler[Id - Begin] = Fn;
	}

	Result dispatch(TPacketId Id, TContext& Ctx) const
	{
		if (Id < Begin || Id > End) [[unlikely]] return Result::InvalidId;

		Handler fn = m_Handler[Id - Begin];
		if (!fn) [[unlikely]] return Result::NoHandler;

		switch (fn(Ctx))
		{

		case HandleResult::Ok:			return Result::Ok;
		case HandleResult::Async:		return Result::Async;
		case HandleResult::Disconnect:	return Result::Disconnect;
		default:						return Result::Disconnect;

		}
	}
};
}
