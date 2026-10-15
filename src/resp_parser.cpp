// ==============================================================================
// Distributed In-Memory Key-Value Store & Cache (C++17)
// File: resp_parser.cpp
//
// Description:
//   Zero-copy serializer and deserializer for RESP2 (Redis Serialization
//   Protocol Version 2), following the official Redis RFC 6379 specification.
// ==============================================================================

#include "resp_parser.hpp"
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <charconv>

namespace kvstore {

// ------------------------------------------------------------------------------
// RespObject::serialize_to()
// Appends serialized RESP wire bytes directly to an outbound buffer without
// intermediate heap copies, enabling batched pipeline response flushes.
// ------------------------------------------------------------------------------
void RespObject::serialize_to(std::string& out) const {
    switch (type) {
        case RespType::SimpleString:
            out += "+";
            out += str_val;
            out += "\r\n";
            break;

        case RespType::Error:
            out += "-";
            out += str_val;
            out += "\r\n";
            break;

        case RespType::Integer:
            out += ":";
            out += std::to_string(int_val);
            out += "\r\n";
            break;

        case RespType::BulkString:
            out += "$";
            out += std::to_string(str_val.size());
            out += "\r\n";
            out += str_val;
            out += "\r\n";
            break;

        case RespType::Null:
            out += "$-1\r\n";
            break;

        case RespType::NullArray:
            out += "*-1\r\n";
            break;

        case RespType::Array:
            out += "*";
            out += std::to_string(array_val.size());
            out += "\r\n";
            for (const auto& elem : array_val) {
                elem.serialize_to(out);
            }
            break;
    }
}

std::string RespObject::serialize() const {
    std::string out;
    serialize_to(out);
    return out;
}

// ------------------------------------------------------------------------------
// RespParser::parse()
// Zero-copy stream parsing entry point over std::string_view.
// ------------------------------------------------------------------------------
std::optional<RespObject> RespParser::parse(std::string_view raw_input, size_t& bytes_consumed) {
    bytes_consumed = 0;
    if (raw_input.empty()) return std::nullopt;

    size_t offset = 0;
    auto res = parse_internal(raw_input, offset);
    if (res.has_value()) {
        bytes_consumed = offset;
    } else {
        bytes_consumed = 0; // Partial frame; leave cursor unchanged
    }
    return res;
}

namespace {

bool parse_resp_int64(std::string_view sv, int64_t& out) {
    if (sv.empty()) return false;
    const char* first = sv.data();
    const char* last = sv.data() + sv.size();
    if (*first == '+') {
        first++;
        if (first == last) return false;
    }
    auto [ptr, ec] = std::from_chars(first, last, out);
    return (ec == std::errc() && ptr == last);
}

} // anonymous namespace

// ------------------------------------------------------------------------------
// RespParser::parse_internal()
// Recursive-descent parser engine operating statelessly over std::string_view.
// ------------------------------------------------------------------------------
std::optional<RespObject> RespParser::parse_internal(std::string_view input, size_t& offset) {
    if (offset >= input.size()) return std::nullopt;

    char type_marker = input[offset++];

    auto find_crlf = [&](size_t start) -> size_t {
        for (size_t i = start; i + 1 < input.size(); ++i) {
            if (input[i] == '\r' && input[i + 1] == '\n') {
                return i;
            }
        }
        return std::string_view::npos;
    };

    // --------------------------------------------------------------------------
    // CASE A: Simple Types -> Simple String (+), Error (-), Integer (:)
    // --------------------------------------------------------------------------
    if (type_marker == '+' || type_marker == '-' || type_marker == ':') {
        size_t crlf = find_crlf(offset);
        if (crlf == std::string_view::npos) return std::nullopt;

        std::string_view line = input.substr(offset, crlf - offset);
        offset = crlf + 2;

        if (type_marker == '+') return RespObject::SimpleString(line);
        if (type_marker == '-') return RespObject::Error(line);

        int64_t val = 0;
        if (!parse_resp_int64(line, val)) {
            return std::nullopt;
        }
        return RespObject::Integer(val);
    }
    // --------------------------------------------------------------------------
    // CASE B: Bulk String ($) -> "$<len>\r\n<payload>\r\n"
    // --------------------------------------------------------------------------
    else if (type_marker == '$') {
        size_t crlf = find_crlf(offset);
        if (crlf == std::string_view::npos) return std::nullopt;

        std::string_view len_view = input.substr(offset, crlf - offset);
        offset = crlf + 2;
        int64_t bulk_len = 0;
        if (!parse_resp_int64(len_view, bulk_len)) {
            return std::nullopt;
        }

        if (bulk_len < 0) {
            return RespObject::NullBulkString();
        }

        if (bulk_len > MAX_BULK_LEN) {
            return std::nullopt;
        }

        size_t data_len = static_cast<size_t>(bulk_len);
        if (offset + data_len + 2 > input.size()) {
            return std::nullopt; // Partial payload in TCP window
        }

        if (input[offset + data_len] != '\r' || input[offset + data_len + 1] != '\n') {
            return std::nullopt;
        }

        std::string_view payload = input.substr(offset, data_len);
        offset += data_len + 2;
        return RespObject::BulkString(payload);
    }
    // --------------------------------------------------------------------------
    // CASE C: Array (*) -> "*<count>\r\n[elements...]"
    // --------------------------------------------------------------------------
    else if (type_marker == '*') {
        size_t crlf = find_crlf(offset);
        if (crlf == std::string_view::npos) return std::nullopt;

        std::string_view count_view = input.substr(offset, crlf - offset);
        offset = crlf + 2;
        int64_t count = 0;
        if (!parse_resp_int64(count_view, count)) {
            return std::nullopt;
        }

        if (count < 0) {
            return RespObject::NullArray();
        }

        if (count > MAX_ARRAY_COUNT) {
            return std::nullopt;
        }

        std::vector<RespObject> elements;
        elements.reserve(static_cast<size_t>(count));
        for (int64_t i = 0; i < count; ++i) {
            auto elem = parse_internal(input, offset);
            if (!elem.has_value()) {
                return std::nullopt;
            }
            elements.push_back(std::move(*elem));
        }
        return RespObject::Array(std::move(elements));
    }
    // --------------------------------------------------------------------------
    // CASE D: Inline Plain-Text Fallback (e.g., "PING\r\n" or "SET foo bar\r\n")
    // --------------------------------------------------------------------------
    else {
        size_t crlf = find_crlf(offset - 1);
        if (crlf == std::string_view::npos) return std::nullopt;

        std::string_view line = input.substr(offset - 1, crlf - (offset - 1));
        offset = crlf + 2;

        std::vector<RespObject> tokens;
        size_t start = 0;
        while (start < line.size()) {
            while (start < line.size() && line[start] == ' ') ++start;
            if (start >= line.size()) break;
            size_t end = start;
            while (end < line.size() && line[end] != ' ') ++end;
            tokens.push_back(RespObject::BulkString(line.substr(start, end - start)));
            start = end;
        }
        if (tokens.empty()) return std::nullopt;
        return RespObject::Array(std::move(tokens));
    }
}

} // namespace kvstore
