#ifndef CUSTOM_TYPES_H
#define CUSTOM_TYPES_H

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "parquet/arrow/writer.h"
#include "parquet/api/writer.h"
#include "parquet/stream_writer.h"


using canid_t = uint32_t;

// keeps track of indecies and types of signals.. used to put signals in the correct row in parquet by index
struct SignalTypeOrderTracker
{
    // The name of the CAN signal (e.g., "Engine_Speed")
    std::string signal_name; 
    
    // Arrow style type of the signal (will either be float, or int... but maybe we'll extend to treat bool as bool and not int in the future)
    parquet::Type::type arrow_type; 
};

// Stores monostate (for null column, or double, or 32/64/128 signed int, or float/double, or bool) to closely match parquet types
using DataTypeOrVoid = std::variant<std::monostate, double, int32_t, int64_t, __int128_t, float, bool>;


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

#endif