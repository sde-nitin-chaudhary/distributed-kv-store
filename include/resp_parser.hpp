#pragma once

#include <string>
#include <vector>
#include <sstream>
#include <optional>
#include <stdexcept>

namespace kvstore {

enum class RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null
};

struct RespObject {
    RespType type;
    std::string str_val;
    int64_t int_val{0};
    std::vector<RespObject> array_val;

    static RespObject SimpleString(const std::string& val) {
        RespObject obj;
        obj.type = RespType::SimpleString;
        obj.str_val = val;
        return obj;
    }

    static RespObject Error(const std::string& msg) {
        RespObject obj;
        obj.type = RespType::Error;
        obj.str_val = msg;
        return obj;
    }

    static RespObject Integer(int64_t val) {
        RespObject obj;
        obj.type = RespType::Integer;
        obj.int_val = val;
        return obj;
    }

    static RespObject BulkString(const std::string& val) {
        RespObject obj;
        obj.type = RespType::BulkString;
        obj.str_val = val;
        return obj;
    }

    static RespObject NullBulkString() {
        RespObject obj;
        obj.type = RespType::Null;
        return obj;
    }

    static RespObject Array(const std::vector<RespObject>& elements) {
        RespObject obj;
        obj.type = RespType::Array;
        obj.array_val = elements;
        return obj;
    }

    std::string serialize() const;
};

class RespParser {
public:
    static std::optional<RespObject> parse(const std::string& raw_input, size_t& bytes_consumed);

private:
    static std::optional<RespObject> parse_internal(const char* data, size_t len, size_t& offset);
};

} // namespace kvstore
