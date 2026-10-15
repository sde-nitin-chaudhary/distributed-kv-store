#include "resp_parser.hpp"
#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <cstdint>

void test_simple_string() {
    std::cout << "[TEST] Running test_simple_string...\n";
    std::string input = "+OK\r\n";
    size_t consumed = 0;
    auto obj = kvstore::RespParser::parse(input, consumed);

    assert(obj.has_value());
    assert(obj->type == kvstore::RespType::SimpleString);
    assert(obj->str_val == "OK");
    assert(consumed == 5);
    assert(obj->serialize() == "+OK\r\n");

    std::cout << "[PASS] test_simple_string passed!\n";
}

void test_error() {
    std::cout << "[TEST] Running test_error...\n";
    std::string input = "-ERR unknown command 'FOO'\r\n";
    size_t consumed = 0;
    auto obj = kvstore::RespParser::parse(input, consumed);

    assert(obj.has_value());
    assert(obj->type == kvstore::RespType::Error);
    assert(obj->str_val == "ERR unknown command 'FOO'");
    assert(consumed == input.size());
    assert(obj->serialize() == input);

    std::cout << "[PASS] test_error passed!\n";
}

void test_integer() {
    std::cout << "[TEST] Running test_integer...\n";
    std::string input = ":1000\r\n";
    size_t consumed = 0;
    auto obj = kvstore::RespParser::parse(input, consumed);

    assert(obj.has_value());
    assert(obj->type == kvstore::RespType::Integer);
    assert(obj->int_val == 1000);
    assert(consumed == 7);
    assert(obj->serialize() == ":1000\r\n");

    // Negative integer
    input = ":-42\r\n";
    obj = kvstore::RespParser::parse(input, consumed);
    assert(obj.has_value());
    assert(obj->int_val == -42);
    assert(consumed == 6);

    std::cout << "[PASS] test_integer passed!\n";
}

void test_bulk_string() {
    std::cout << "[TEST] Running test_bulk_string...\n";
    std::string input = "$6\r\nfoobar\r\n";
    size_t consumed = 0;
    auto obj = kvstore::RespParser::parse(input, consumed);

    assert(obj.has_value());
    assert(obj->type == kvstore::RespType::BulkString);
    assert(obj->str_val == "foobar");
    assert(consumed == 12);
    assert(obj->serialize() == input);

    // Empty bulk string
    input = "$0\r\n\r\n";
    obj = kvstore::RespParser::parse(input, consumed);
    assert(obj.has_value());
    assert(obj->str_val.empty());
    assert(consumed == 6);
    assert(obj->serialize() == input);

    // Binary safe with internal null byte
    std::string binary_data = "foo\0bar";
    std::string bin_input = "$" + std::to_string(binary_data.size()) + "\r\n" + binary_data + "\r\n";
    obj = kvstore::RespParser::parse(bin_input, consumed);
    assert(obj.has_value());
    assert(obj->str_val == binary_data);

    std::cout << "[PASS] test_bulk_string passed!\n";
}

void test_null_bulk_and_array() {
    std::cout << "[TEST] Running test_null_bulk_and_array...\n";
    std::string input = "$-1\r\n";
    size_t consumed = 0;
    auto obj = kvstore::RespParser::parse(input, consumed);

    assert(obj.has_value());
    assert(obj->type == kvstore::RespType::Null);
    assert(consumed == 5);
    assert(obj->serialize() == "$-1\r\n");

    // Null array
    input = "*-1\r\n";
    obj = kvstore::RespParser::parse(input, consumed);
    assert(obj.has_value());
    assert(obj->type == kvstore::RespType::NullArray);
    assert(consumed == 5);
    assert(obj->serialize() == "*-1\r\n");

    std::cout << "[PASS] test_null_bulk_and_array passed!\n";
}

void test_array_command() {
    std::cout << "[TEST] Running test_array_command...\n";
    std::string input = "*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nNitin\r\n";
    size_t consumed = 0;
    auto obj = kvstore::RespParser::parse(input, consumed);

    assert(obj.has_value());
    assert(obj->type == kvstore::RespType::Array);
    assert(obj->array_val.size() == 3);
    assert(obj->array_val[0].str_val == "SET");
    assert(obj->array_val[1].str_val == "user");
    assert(obj->array_val[2].str_val == "Nitin");
    assert(consumed == input.size());
    assert(obj->serialize() == input);

    std::cout << "[PASS] test_array_command passed!\n";
}

void test_stream_framing_partial() {
    std::cout << "[TEST] Running test_stream_framing_partial...\n";
    // Incomplete array length
    std::string input = "*2\r\n$3\r";
    size_t consumed = 0;
    auto obj = kvstore::RespParser::parse(input, consumed);
    assert(!obj.has_value());
    assert(consumed == 0);

    // Incomplete payload (declared 6 bytes, only 3 sent)
    input = "$6\r\nfoo";
    obj = kvstore::RespParser::parse(input, consumed);
    assert(!obj.has_value());
    assert(consumed == 0);

    // Corrupted trailing CRLF after payload
    input = "$3\r\nfooXX";
    obj = kvstore::RespParser::parse(input, consumed);
    assert(!obj.has_value());
    assert(consumed == 0);

    std::cout << "[PASS] test_stream_framing_partial passed!\n";
}

void test_pipelining() {
    std::cout << "[TEST] Running test_pipelining...\n";
    std::string cmd1 = "*1\r\n$4\r\nPING\r\n";
    std::string cmd2 = "*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n";
    std::string combined = cmd1 + cmd2;

    size_t consumed1 = 0;
    auto obj1 = kvstore::RespParser::parse(combined, consumed1);
    assert(obj1.has_value());
    assert(consumed1 == cmd1.size());
    assert(obj1->array_val[0].str_val == "PING");

    // Second command parsed from remainder of buffer
    std::string remainder = combined.substr(consumed1);
    size_t consumed2 = 0;
    auto obj2 = kvstore::RespParser::parse(remainder, consumed2);
    assert(obj2.has_value());
    assert(consumed2 == cmd2.size());
    assert(obj2->array_val[0].str_val == "GET");
    assert(obj2->array_val[1].str_val == "user");

    std::cout << "[PASS] test_pipelining passed!\n";
}

void test_inline_command() {
    std::cout << "[TEST] Running test_inline_command...\n";
    std::string input = "PING\r\n";
    size_t consumed = 0;
    auto obj = kvstore::RespParser::parse(input, consumed);

    assert(obj.has_value());
    assert(obj->type == kvstore::RespType::Array);
    assert(obj->array_val.size() == 1);
    assert(obj->array_val[0].str_val == "PING");
    assert(consumed == input.size());

    // Multi-token inline
    input = "SET score 100\r\n";
    obj = kvstore::RespParser::parse(input, consumed);
    assert(obj.has_value());
    assert(obj->array_val.size() == 3);
    assert(obj->array_val[0].str_val == "SET");
    assert(obj->array_val[1].str_val == "score");
    assert(obj->array_val[2].str_val == "100");

    std::cout << "[PASS] test_inline_command passed!\n";
}

void test_zero_copy_string_view_cursor() {
    std::cout << "[TEST] Running test_zero_copy_string_view_cursor...\n";
    std::string stream = "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n+OK\r\n:12345\r\n";
    size_t cursor = 0;
    std::vector<kvstore::RespObject> parsed_objects;

    while (cursor < stream.size()) {
        std::string_view view(stream.data() + cursor, stream.size() - cursor);
        size_t consumed = 0;
        auto obj = kvstore::RespParser::parse(view, consumed);
        if (!obj.has_value() || consumed == 0) break;
        cursor += consumed;
        parsed_objects.push_back(std::move(*obj));
    }

    assert(cursor == stream.size());
    assert(parsed_objects.size() == 4);
    assert(parsed_objects[0].type == kvstore::RespType::Array);
    assert(parsed_objects[1].type == kvstore::RespType::Array);
    assert(parsed_objects[2].type == kvstore::RespType::SimpleString);
    assert(parsed_objects[3].type == kvstore::RespType::Integer);
    assert(parsed_objects[3].int_val == 12345);

    std::cout << "[PASS] test_zero_copy_string_view_cursor passed!\n";
}

void test_serialize_to_coalescing() {
    std::cout << "[TEST] Running test_serialize_to_coalescing...\n";
    std::string batch_out;
    auto obj1 = kvstore::RespObject::SimpleString("OK");
    auto obj2 = kvstore::RespObject::Integer(42);
    auto obj3 = kvstore::RespObject::BulkString("payload");

    obj1.serialize_to(batch_out);
    obj2.serialize_to(batch_out);
    obj3.serialize_to(batch_out);

    std::string expected = "+OK\r\n:42\r\n$7\r\npayload\r\n";
    assert(batch_out == expected);

    std::cout << "[PASS] test_serialize_to_coalescing passed!\n";
}

void test_malformed_numeric_rejection() {
    std::cout << "[TEST] Running test_malformed_numeric_rejection...\n";
    size_t consumed = 0;

    // Trailing junk after integer: :12x\r\n
    auto r1 = kvstore::RespParser::parse(std::string_view(":12x\r\n"), consumed);
    assert(!r1.has_value());

    // Trailing junk in bulk string length: $3x\r\nfoo\r\n
    auto r2 = kvstore::RespParser::parse(std::string_view("$3x\r\nfoo\r\n"), consumed);
    assert(!r2.has_value());

    // Trailing junk in array count: *2x\r\n+OK\r\n+OK\r\n
    auto r3 = kvstore::RespParser::parse(std::string_view("*2x\r\n+OK\r\n+OK\r\n"), consumed);
    assert(!r3.has_value());

    // Empty integer: :\r\n
    auto r4 = kvstore::RespParser::parse(std::string_view(":\r\n"), consumed);
    assert(!r4.has_value());

    std::cout << "[PASS] test_malformed_numeric_rejection passed!\n";
}

int main() {
    std::cout << "==================================================\n";
    std::cout << "  Redis RESP2 Protocol Parser Unit Test Suite     \n";
    std::cout << "==================================================\n";

    test_simple_string();
    test_error();
    test_integer();
    test_bulk_string();
    test_null_bulk_and_array();
    test_array_command();
    test_stream_framing_partial();
    test_pipelining();
    test_inline_command();
    test_zero_copy_string_view_cursor();
    test_serialize_to_coalescing();
    test_malformed_numeric_rejection();

    std::cout << "==================================================\n";
    std::cout << "  All 12 RESP2 Parser Unit Tests Passed Cleanly!  \n";
    std::cout << "==================================================\n";
    return 0;
}
