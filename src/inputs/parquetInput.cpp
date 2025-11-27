#include <parquet/arrow/reader.h>
#include <memory>
#include <string>
#include <iostream>
#include "arrow/io/file.h"
#include <arrow/table.h>
#include "parquetInput.h"

ParquetInput::ParquetInput(std::string fileName){
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(fileName));

    // Create Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    PARQUET_THROW_NOT_OK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &parquet_reader)
    );

    // Read entire file into an Arrow Table
    PARQUET_THROW_NOT_OK(parquet_reader->ReadTable(&table));

    std::cout << "Read table with " << table->num_rows()
            << " rows and " << table->num_columns() << " columns.\n";
}