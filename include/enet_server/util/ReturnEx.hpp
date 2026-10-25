#pragma once

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

namespace enet_server
{

using TReturnErr = unsigned int;
class ReturnEx
{

private:
	TReturnErr m_code = 0; // >0 = error
	std::shared_ptr<std::string> m_info; // lazily allocated on first info()

public:
	ReturnEx(TReturnErr code) : m_code(code) { }
	ReturnEx(const std::string& info) : m_info(std::make_shared<std::string>(info)) { }
	ReturnEx(TReturnErr code, const std::string& info) : m_code(code), m_info(std::make_shared<std::string>(info)) { }

	ReturnEx() = default;
	ReturnEx(const ReturnEx&) = default;
	ReturnEx(ReturnEx&&) = default;
	ReturnEx& operator=(const ReturnEx&) = default;
	ReturnEx& operator=(ReturnEx&&) = default;

	ReturnEx& operator=(TReturnErr code)
	{
		m_code = code;
		return *this;
	}
	ReturnEx& operator=(const std::string& info)
	{
		this->info(info);
		return *this;
	}

	void code(TReturnErr code) { m_code = code; }
	TReturnErr code() const { return m_code; }

	void info(const std::string& i)
	{
		if (!m_info) m_info = std::make_shared<std::string>(i);
		else *m_info = i;
	}
	const std::string& info() const
	{
		static const std::string empty{ };
		return m_info ? *m_info : empty;
	}

	void set(TReturnErr c, const std::string& i) { code(c); info(i); }
	bool is_error() const { return m_code != 0; }

	std::string what() const
	{
		std::stringstream ss;
		bool hasCode = m_code != 0;

		if (hasCode) ss << "[E: " << m_code << "]";
		if (m_info && !m_info->empty())
		{
			if (hasCode) ss << ' ';
			ss << *m_info;
		}
		return ss.str();
	}

	void clear()
	{
		m_code = 0;
		if (m_info) m_info->clear();
	}
};
}
