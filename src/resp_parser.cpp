#include "resp_parser.hpp"
#include <cstring>
#include <cstdlib>

namespace kvstore {

std::string RespObject::serialize() const {
    std::string out;
    switch (type) {
        case RespType::SimpleString:
            out = "+" + str_val + "\r\n";
            break;
        case RespType::Error:
            out = "-" + str_val + "\r\n";
            break;
        case RespType::Integer:
            out = ":" + std::to_string(int_val) + "\r\n";
            break;
        case RespType::BulkString:
            out = "$" + std::to_string(str_val.size()) + "\r\n" + str_val + "\r\n";
            break;
        case RespType::Null:
            out = "$-1\r\n";
            break;
        case RespType::Array:
            out = "*" + std::to_string(array_val.size()) + "\r\n";
            for (const auto& elem : array_val) {
                out += elem.serialize();
            }
            break;
    }
    return out;
}

std::optional<RespObject> RespParser::parse(const std::string& raw_input, size_t& bytes_consumed) {
    bytes_consumed = 0;
    if (raw_input.empty()) return std::nullopt;
    return parse_internal(raw_input.data(), raw_input.size(), bytes_consumed);
}

std::optional<RespObject> RespParser::parse_internal(const char* data, size_t len, size_t& offset) {
    if (offset >= len) return std::nullopt;

    char type_marker = data[offset++];
    
    // Find next CRLF
    auto find_crlf = [&](size_t start) -> size_t {
        for (size_t i = start; i + 1 < len; ++i) {
            if (data[i] == '\r' && data[i + 1] == '\n') {
                return i;
            }
        }
        return std::string::npos;
    };

    if (type_marker == '+' || type_marker == '-' || type_marker == ':') {
        size_t crlf = find_crlf(offset);
        if (crlf == std::string::npos) return std::nullopt;

        std::string line(data + offset, crlf - offset);
        offset = crlf + 2;

        if (type_marker == '+') return RespObject::SimpleString(line);
        if (type_marker == '-') return RespObject::Error(line);
        return RespObject::Integer(std::stoll(line));
    } 
    else if (type_marker == '$') {
        size_t crlf = find_crlf(offset);
        if (crlf == std::string::npos) return std::nullopt;

        std::string len_str(data + offset, crlf - offset);
        offset = crlf + 2;
        int64_t bulk_len = std::stoll(len_str);

        if (bulk_len < 0) {
            return RespObject::NullBulkString();
        }

        if (offset + bulk_len + 2 > len) return std::nullopt; // Incomplete data

        std::string payload(data + offset, bulk_len);
        offset += bulk_len + 2; // skip payload and \r\n
        return RespObject::BulkString(payload);
    } 
    else if (type_marker == '*') {
        size_t crlf = find_crlf(offset);
        if (crlf == std::string::npos) return std::nullopt;

        std::string count_str(data + offset, crlf - offset);
        offset = crlf + 2;
        int64_t count = std::stoll(count_str);

        if (count < 0) return RespObject::NullBulkString();

        std::vector<RespObject> elements;
        elements.reserve(count);

        for (int64_t i = 0; i < count; ++i) {
            auto elem = parse_internal(data, len, offset);
            if (!elem.has_value()) return std::nullopt;
            elements.push_back(std::move(*elem));
        }
        return RespObject::Array(elements);
    }

    // Fallback: Inline command (e.g. plain text "PING\r\n" or "SET foo bar\r\n")
    --offset;
    size_t crlf = find_crlf(offset);
    if (crlf == std::string::npos) return std::nullopt;

    std::string line(data + offset, crlf - offset);
    offset = crlf + 2;

    std::vector<RespObject> elements;
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
        elements.push_back(RespObject::BulkString(token));
    }
    return RespObject::Array(elements);
}

} // namespace kvstore
