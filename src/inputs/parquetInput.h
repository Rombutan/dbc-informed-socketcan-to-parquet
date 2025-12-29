#pragma once
#include <parquet/arrow/reader.h>
#include <memory>
#include <string>
#include "custom_types.h"

class ParquetInput{
public:
    // Fills arrow table object from parquet file
    // @param fileName name of parquet file
    ParquetInput(std::string fileName);

    // Does nothing
    // Exists only to allow creation of emptey class
    ParquetInput();

    // Gets row from arrow table as an arrow array
    // Returns last row if `rowNumber` is too large, and sets `EOI`
    // @param rowNumber row index
    // @param EOI End of input, set true when there is no more to read
    std::shared_ptr<arrow::Array> emitRow(int rowNumber, bool * const &EOI);

private:
    // Arrow table. Stores entire contents of input parquet
    std::shared_ptr<arrow::Table> table;

    // Length of table
    int numRows;

    // To throw error if `emitRow` is called before initialization
    bool initialized = false;
};