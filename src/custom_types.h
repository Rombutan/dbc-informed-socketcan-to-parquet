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
    
    // Parquet style type of the signal
    parquet::Type::type parquet_type; 

    // Arrow type
    std::shared_ptr<arrow::DataType> arrow_datatype;
};

using ArrowSchemaList = std::vector<SignalTypeOrderTracker>;

// Stores monostate (for null column, or double, or 32/64/128 signed int, or float/double, or bool) to closely match parquet types
using DataTypeOrVoid = std::variant<std::monostate, double, int32_t, int64_t, __int128_t, float, bool>;

std::string variant_to_string(const DataTypeOrVoid& data);

std::array<unsigned char, 4> extract_32_bits(const unsigned char data[8], int start_bit);

float le_uint32_to_float(uint32_t le_value);

int find_index_by_name(const std::vector<SignalTypeOrderTracker>& vec, const std::string& target_name);

DataTypeOrVoid scalar_to_variant(const std::shared_ptr<arrow::Scalar>& scalar);

std::shared_ptr<arrow::DataType> map_parquet_to_arrow(parquet::Type::type t);

#endif