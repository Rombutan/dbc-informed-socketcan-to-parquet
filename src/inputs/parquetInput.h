#pragma once
#include <parquet/arrow/reader.h>
#include <memory>
#include <string>

class ParquetInput{
public:
    // Fills arrow table object from parquet file
    // @param fileName name of parquet file
    ParquetInput(std::string fileName);

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
};