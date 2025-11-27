#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "parquet/arrow/writer.h"
#include "parquet/api/writer.h"
#include "parquet/stream_writer.h"
#include <stdio.h>
#include <iostream>
#include <cerrno>
#include "custom_types.h"


// Function to convert the variant content to a printable string
std::string variant_to_string(const DataTypeOrVoid& data) {
    // 1. Use std::visit with a lambda visitor.
    // The lambda is called with the *actual* type held by the variant.
    return std::visit([](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;

        // 2. Handle the std::monostate (empty/void) case separately.
        if constexpr (std::is_same_v<T, std::monostate>) {
            return "Void/Empty";
        }
        // 3. Handle the __int128_t case (requires custom string conversion).
        else if constexpr (std::is_same_v<T, __int128_t>) {
            // __int128_t doesn't have native ostream support, so we need a helper.
            // A full implementation is complex, but for printing, we'll simplify.
            // For a robust solution, you'd convert to a string using a library or custom logic.
            // For this example, we'll just indicate the type.
            return "__int128_t (custom print required)"; 
        }
        // 4. Handle all other supported types (doubles, ints, floats, bools).
        else {
            // Use a stringstream to convert the value to a string.
            std::ostringstream oss;
            oss << arg;
            return oss.str();
        }
    }, data);
}

std::array<unsigned char, 4> extract_32_bits(const unsigned char data[8], int start_bit) {
    // We use std::array<unsigned char, 4> as the return type for safe return-by-value.
    std::array<unsigned char, 4> extracted_data;
    
    // 1. Calculate the starting byte index and the bit offset within that byte
    const int start_byte = start_bit / 8;
    const int bit_offset = start_bit % 8; // Number of low-order bits to discard
    
    // The number of bits to take from the 'next' byte
    const int spill_bits = 8 - bit_offset;

    // Loop 4 times to extract 4 bytes (32 bits)
    for (int i = 0; i < 4; ++i) {
        // Source byte 1 (the primary byte)
        unsigned char byte1 = data[start_byte + i];
        
        // Source byte 2 (the spillover byte)
        // Check to make sure we don't read past the end of the 8-byte array (data[7])
        unsigned char byte2 = (start_byte + i + 1 < 8) ? data[start_byte + i + 1] : 0x00;

        unsigned char extracted_byte;

        if (bit_offset == 0) {
            // Case 1: Clean byte boundary (aligned)
            extracted_byte = byte1;
        } else {
            // Case 2: Unaligned access (bits span two source bytes)
            
            // a) Get the high bits from the current byte1, shifting them to the right.
            unsigned char high_bits = byte1 >> bit_offset;

            // b) Get the low bits from the next byte2, shifting them to the left.
            unsigned char low_bits = byte2 << spill_bits;
            
            // c) Combine them using bitwise OR
            extracted_byte = high_bits | low_bits;
        }

        // Store the resulting byte in the returned array
        extracted_data[i] = extracted_byte;
    }
    
    return extracted_data;
}

float le_uint32_to_float(uint32_t le_value) {
    // Step 1: Break into bytes (little-endian: DCBA)
    std::array<unsigned char, 4> bytes = {
        static_cast<unsigned char>(le_value & 0xFF),         // D (LSB)
        static_cast<unsigned char>((le_value >> 8) & 0xFF),  // C
        static_cast<unsigned char>((le_value >> 16) & 0xFF), // B
        static_cast<unsigned char>((le_value >> 24) & 0xFF)  // A (MSB)
    };

    // Step 2: Reverse for system endianness if needed
    // We want bytes in system-native order
    std::array<unsigned char, 4> native_bytes = {
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
        bytes[0], bytes[1], bytes[2], bytes[3]  // Already little-endian
#else
        bytes[3], bytes[2], bytes[1], bytes[0]  // Convert to big-endian system
#endif
    };

    // Step 3: Copy bytes into float safely
    float f;
    std::memcpy(&f, native_bytes.data(), sizeof(f));
    return f;
}

int find_index_by_name(const std::vector<SignalTypeOrderTracker>& vec, const std::string& target_name) {
    // 1. Use std::find_if to get an iterator to the first matching element.
    auto it = std::find_if(vec.begin(), vec.end(), 
        [&target_name](const SignalTypeOrderTracker& d) {
            return d.signal_name == target_name;
        }
    );

    // 2. Check if the element was found.
    if (it != vec.end()) {
        // 3. Use std::distance to calculate the index from the start (begin()).
        return std::distance(vec.begin(), it);
    } else {
        // Return a sentinel value (e.g., -1) if the element is not found.
        return -1;
    }
}

std::shared_ptr<arrow::DataType> map_parquet_to_arrow(parquet::Type::type t) { // Used for parquet reading
    switch (t) {
        case parquet::Type::BOOLEAN: return arrow::boolean();
        case parquet::Type::INT32:   return arrow::int32();
        case parquet::Type::INT64:   return arrow::int64();
        case parquet::Type::INT96:   return arrow::timestamp(arrow::TimeUnit::NANO);
        case parquet::Type::FLOAT:   return arrow::float32();
        case parquet::Type::DOUBLE:  return arrow::float64();
        default:                     return nullptr; // unsupported type
    }
}

DataTypeOrVoid scalar_to_variant(const std::shared_ptr<arrow::Scalar>& scalar) {
    if (!scalar->is_valid) {
        return std::monostate{};
    }

    switch (scalar->type->id()) {
        case arrow::Type::BOOL: {
            auto s = std::dynamic_pointer_cast<arrow::BooleanScalar>(scalar);
            return s->value;
        }
        case arrow::Type::INT32: {
            auto s = std::dynamic_pointer_cast<arrow::Int32Scalar>(scalar);
            return s->value;
        }
        case arrow::Type::INT64: {
            auto s = std::dynamic_pointer_cast<arrow::Int64Scalar>(scalar);
            return s->value;
        }
        case arrow::Type::FLOAT: {
            auto s = std::dynamic_pointer_cast<arrow::FloatScalar>(scalar);
            return s->value;
        }
        case arrow::Type::DOUBLE: {
            auto s = std::dynamic_pointer_cast<arrow::DoubleScalar>(scalar);
            return s->value;
        }
        default:
            // If you want, you can handle INT96 (timestamps) or cast to int128_t
            std::cerr << "Unsupported Arrow type: " << scalar->type->ToString() << "\n";
            return std::monostate{};
    }
}
