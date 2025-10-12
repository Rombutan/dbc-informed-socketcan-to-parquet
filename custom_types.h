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



#endif