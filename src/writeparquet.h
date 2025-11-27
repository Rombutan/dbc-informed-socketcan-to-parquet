#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <iostream>
#include <vector>
#include <variant>
#include <string>

// Type alias for your nested variant
using CellVariant = std::variant<
    std::monostate,
    double,
    int32_t,
    int64_t,
    __int128_t,
    float,
    bool
>;

using Row = std::vector<CellVariant>;
using TableData = std::vector<Row>;

// Convert your nested variant into per-column Arrow arrays
std::shared_ptr<arrow::Array>
BuildColumnArray(const std::vector<CellVariant>& column_data,
                 const std::shared_ptr<arrow::Field>& field)
{
    using namespace arrow;
    switch (field->type()->id()) {
        case Type::DOUBLE: {
            DoubleBuilder builder;
            for (auto& v : column_data) {
                if (auto p = std::get_if<double>(&v)) builder.Append(*p);
                else builder.AppendNull();
            }
            std::shared_ptr<Array> out;
            builder.Finish(&out);
            return out;
        }

        case Type::FLOAT: {
            FloatBuilder builder;
            for (auto& v : column_data) {
                if (auto p = std::get_if<float>(&v)) builder.Append(*p);
                else builder.AppendNull();
            }
            std::shared_ptr<Array> out;
            builder.Finish(&out);
            return out;
        }

        case Type::INT32: {
            Int32Builder builder;
            for (auto& v : column_data) {
                if (auto p = std::get_if<int32_t>(&v)) builder.Append(*p);
                else builder.AppendNull();
            }
            std::shared_ptr<Array> out;
            builder.Finish(&out);
            return out;
        }

        case Type::INT64: {
            Int64Builder builder;
            for (auto& v : column_data) {
                if (auto p = std::get_if<int64_t>(&v)) builder.Append(*p);
                // handle __int128_t by casting
                else if (auto p2 = std::get_if<__int128_t>(&v)) builder.Append((int64_t)*p2);
                else builder.AppendNull();
            }
            std::shared_ptr<Array> out;
            builder.Finish(&out);
            return out;
        }

        case Type::BOOL: {
            BooleanBuilder builder;
            for (auto& v : column_data) {
                if (auto p = std::get_if<bool>(&v)) builder.Append(*p);
                else builder.AppendNull();
            }
            std::shared_ptr<Array> out;
            builder.Finish(&out);
            return out;
        }

        default:
            throw std::runtime_error("Unsupported Arrow type in schema.");
    }
}

// Convert your row-major vector-of-vectors into Arrow table
std::shared_ptr<arrow::Table>
BuildArrowTable(const TableData& data,
                const std::shared_ptr<arrow::Schema>& schema)
{
    const size_t ncols = schema->num_fields();
    const size_t nrows = data.size();

    // Extract column-wise data
    std::vector<std::vector<CellVariant>> cols(ncols);
    for (const auto& row : data) {
        if (row.size() != ncols)
            throw std::runtime_error("Row width != schema column count");

        for (size_t c = 0; c < ncols; ++c)
            cols[c].push_back(row[c]);
    }

    // Build Arrow arrays for each column
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(ncols);

    for (size_t c = 0; c < ncols; ++c) {
        auto arr = BuildColumnArray(cols[c], schema->field(c));
        arrays.push_back(arr);
    }

    return arrow::Table::Make(schema, arrays);
}

// Write Arrow table to Parquet
void WriteParquet(const std::shared_ptr<arrow::Table>& table,
                  const std::string& filename)
{
    auto outfile_result = arrow::io::FileOutputStream::Open(filename);
    if (!outfile_result.ok()) throw std::runtime_error(outfile_result.status().ToString());

    std::shared_ptr<arrow::io::FileOutputStream> outfile = *outfile_result;

    parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                               outfile, /*chunk_size=*/1024);
}