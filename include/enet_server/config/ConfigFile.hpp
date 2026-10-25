#pragma once

#include <charconv>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include <enet_server/util/ReturnEx.hpp>

namespace enet_server
{

class ConfigFile
{

public:
    std::shared_ptr<ReturnEx> read();

    template<typename T>
    bool try_get(const std::string& param, T& out) const
    {
        auto it = m_index.find(param);
        if (it == m_index.end()) return false;

        return ParseValue(it->second, out);
    }

    template<typename T>
    T get(const std::string& param) const
    {
        T val { };
        (void)try_get(param, val);
        return val;
    }

    const std::map<std::string, std::string>& index() const { return m_index; }
    size_t count() const { return m_index.size(); }

public:
    virtual std::string filename() const = 0;
    virtual ~ConfigFile() = default;

protected:
    virtual std::string header() const = 0;
    virtual std::string footer() const = 0;
    virtual const std::vector<std::pair<std::string, std::string>>& default_config() const = 0;

private:
    std::shared_ptr<ReturnEx> create();
    void parse_line(std::map<std::string, std::string>& out, const std::string& line, unsigned int line_number, std::shared_ptr<ReturnEx> result_state) const;

private:
    template<typename T>
    static bool ParseValue(const std::string& text, T& out)
    {
        if constexpr (std::is_same_v<T, std::string>)
        {
            out = text;
            return true;
        }
        else if constexpr (std::is_same_v<T, bool>)
        {
            if (text == "1" || text == "true" || text == "TRUE" || text == "True")
            {
                out = true;
                return true;
            }
            if (text == "0" || text == "false" || text == "FALSE" || text == "False")
            {
                out = false;
                return true;
            }

            return false;
        }
        else if constexpr (std::is_integral_v<T>)
            return ParseIntegral(text, out);
        else
        {
            std::stringstream ss(text);

            T value{ };
            ss >> value;
            if (ss.fail()) return false;

            ss >> std::ws;
            if (!ss.eof()) return false;

            out = value;
            return true;
        }
    }

    template<typename T>
    static bool ParseIntegral(const std::string& text, T& out)
    {
        static_assert(std::is_integral_v<T>, "T must be an integral type");
        static_assert(!std::is_same_v<T, bool>, "bool is handled separately");

        if (text.empty()) return false;

        const char* begin = text.data();
        const char* end = begin + text.size();

        if constexpr (std::is_signed_v<T>)
        {
            long long value = 0;
            auto [ptr, ec] = std::from_chars(begin, end, value, 10);

            if (ec != std::errc { } || ptr != end)
                return false;

            if (value < static_cast<long long>((std::numeric_limits<T>::min)()) || value > static_cast<long long>((std::numeric_limits<T>::max)()))
                return false;

            out = static_cast<T>(value);
            return true;
        }
        else
        {
            unsigned long long value = 0;
            auto [ptr, ec] = std::from_chars(begin, end, value, 10);

            if (ec != std::errc{} || ptr != end)
                return false;

            if (value > static_cast<unsigned long long>((std::numeric_limits<T>::max)()))
                return false;

            out = static_cast<T>(value);
            return true;
        }
    }

    private:
        std::map<std::string, std::string> m_index;

};

template<>
inline bool ConfigFile::try_get<std::string>(const std::string& param, std::string& out) const
{
    auto it = m_index.find(param);
    if (it == m_index.end()) return false;

    out = it->second;
    return true;
}

template<>
inline std::string ConfigFile::get<std::string>(const std::string& param) const
{
    std::string out;
    (void)try_get<std::string>(param, out);
    return out;
}
}
