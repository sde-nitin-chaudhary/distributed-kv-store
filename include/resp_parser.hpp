#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <sstream>
#include <optional>
#include <stdexcept>
#include <cstdint>

namespace kvstore {

enum class RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null,
    NullArray
};

// ==============================================================================
// RespObject: In-memory representation of a Redis RESP2 data type.
// Supports both standalone serialization and high-performance zero-copy buffer
// appending via `serialize_to(std::string& out)`.
// ==============================================================================
struct RespObject {
    RespType type{RespType::Null};
    std::string str_val;
    int64_t int_val{0};
    std::vector<RespObject> array_val;

    static RespObject SimpleString(std::string_view val) {
        RespObject obj;
        obj.type = RespType::SimpleString;
        obj.str_val = std::string(val);
        return obj;
    }

    static RespObject Error(std::string_view msg) {
        RespObject obj;
        obj.type = RespType::Error;
        obj.str_val = std::string(msg);
        return obj;
    }

    static RespObject Integer(int64_t val) {
        RespObject obj;
        obj.type = RespType::Integer;
        obj.int_val = val;
        return obj;
    }

    static RespObject BulkString(std::string_view val) {
        RespObject obj;
        obj.type = RespType::BulkString;
        obj.str_val = std::string(val);
        return obj;
    }

    static RespObject NullBulkString() {
        RespObject obj;
        obj.type = RespType::Null;
        return obj;
    }

    static RespObject NullArray() {
        RespObject obj;
        obj.type = RespType::NullArray;
        return obj;
    }

    static RespObject Array(std::vector<RespObject> elements) {
        RespObject obj;
        obj.type = RespType::Array;
        obj.array_val = std::move(elements);
        return obj;
    }

    std::string serialize() const;
    void serialize_to(std::string& out) const;
};

// ==============================================================================
// RespParser: High-Performance Zero-Copy RESP2 Streaming Parser
//
// Key Upgrades:
//   - Stateless zero-copy cursor parsing across `std::string_view` slices.
//   - Zero heap reallocations when advancing through pipelined socket buffers.
//   - Full backwards compatibility with existing `const std::string&` callers.
// ==============================================================================
class RespParser {
public:
    static constexpr int64_t MAX_BULK_LEN = 536870912; // 512 MB per Redis specification
    static constexpr int64_t MAX_ARRAY_COUNT = 1048576; // 1,048,576 elements max

    // Zero-copy std::string_view entry point
    static std::optional<RespObject> parse(std::string_view raw_input, size_t& bytes_consumed);

    // Overload for std::string
    static std::optional<RespObject> parse(const std::string& raw_input, size_t& bytes_consumed) {
        return parse(std::string_view(raw_input), bytes_consumed);
    }

private:
    static std::optional<RespObject> parse_internal(std::string_view input, size_t& offset);
};

} // namespace kvstore
