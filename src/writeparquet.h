/// THIS SHIT IS VIBE CODE 10000 I HATE WRITTING BOILERPLATE

#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <iostream>
#include <vector>
#include <variant>
#include <string>
#include "custom_types.h"

using ValueVariant = DataTypeOrVoid;

// Make vector of array builders... the array builders are in effect the in process table until it has all elements
inline std::vector<std::shared_ptr<arrow::ArrayBuilder>>
CreateBuildersFromSchema(const std::shared_ptr<arrow::Schema>& schema,
                         arrow::MemoryPool* pool = arrow::default_memory_pool()) {
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    builders.reserve(schema->num_fields());

    for (const auto& field : schema->fields()) {
        std::shared_ptr<arrow::ArrayBuilder> builder;
        auto type = field->type();

        if (type->id() == arrow::Type::DOUBLE) {
            builder = std::make_shared<arrow::DoubleBuilder>(pool);
        } else if (type->id() == arrow::Type::INT32) {
            builder = std::make_shared<arrow::Int32Builder>(pool);
        } else if (type->id() == arrow::Type::INT64) {
            builder = std::make_shared<arrow::Int64Builder>(pool);
        } else if (type->id() == arrow::Type::FLOAT) {
            builder = std::make_shared<arrow::FloatBuilder>(pool);
        } else if (type->id() == arrow::Type::BOOL) {
            builder = std::make_shared<arrow::BooleanBuilder>(pool);
        } else if (type->id() == arrow::Type::DECIMAL128) {
            builder = std::make_shared<arrow::Decimal128Builder>(type, pool);
        } else {
            throw std::runtime_error("Unsupported Arrow type in schema");
        }

        builders.push_back(builder);
    }

    return builders;
}


inline arrow::Status SetValueAt(
    std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders,
    int field_index,
    const ValueVariant& value,
    int64_t row_index,
    std::vector<ValueVariant>& lastValues)
{
    if (field_index < 0 || field_index >= builders.size()) {
        return arrow::Status::Invalid("Invalid field index");
    }

    auto builder = builders[field_index];

    // Pad nulls if needed
    int64_t current_len = builder->length();
    if (row_index > current_len) {
        ARROW_RETURN_NOT_OK(builder->AppendNulls(row_index - current_len));
    }

    // Now row_index == builder->length() OR row_index < length (overwrite not supported)
    if (row_index < builder->length()) {
        return arrow::Status::Invalid(
            "Attempting to set a value at an already appended index (append-only builder)"
        );
    }

    // Insert into Last Value vector
    lastValues[field_index] = value;

    // Append appropriate type
    if (std::holds_alternative<std::monostate>(value)) {
        return builder->AppendNull();
    }

    switch (builder->type()->id()) {
        case arrow::Type::DOUBLE:
            return std::static_pointer_cast<arrow::DoubleBuilder>(builder)
                ->Append(std::get<double>(value));

        case arrow::Type::INT32:
            return std::static_pointer_cast<arrow::Int32Builder>(builder)
                ->Append(std::get<int32_t>(value));

        case arrow::Type::INT64:
            return std::static_pointer_cast<arrow::Int64Builder>(builder)
                ->Append(std::get<int64_t>(value));

        case arrow::Type::FLOAT:
            return std::static_pointer_cast<arrow::FloatBuilder>(builder)
                ->Append(std::get<float>(value));

        case arrow::Type::BOOL:
            return std::static_pointer_cast<arrow::BooleanBuilder>(builder)
                ->Append(std::get<bool>(value));

        case arrow::Type::DECIMAL128: {
            __int128_t v = std::get<__int128_t>(value);
            arrow::Decimal128 decimal((int64_t)(v >> 64), (uint64_t)v);
            return std::static_pointer_cast<arrow::Decimal128Builder>(builder)
                ->Append(decimal);
        }

        default:
            return arrow::Status::Invalid("Unsupported type in SetValueAt()");
    }
}
inline arrow::Result<std::shared_ptr<arrow::Table>>
FinishTable(
    const std::shared_ptr<arrow::Schema>& schema,
    std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders)
{
    if (schema->num_fields() != builders.size()) {
        return arrow::Status::Invalid("Schema/builder count mismatch");
    }

    // ------------------------------
    // 1. Find the required final length
    // ------------------------------
    int64_t final_length = 0;
    for (auto& b : builders) {
        final_length = std::max(final_length, b->length());
    }

    // ------------------------------
    // 2. Pad each builder to final_length
    // ------------------------------
    for (auto& b : builders) {
        int64_t curr = b->length();
        if (curr < final_length) {
            int64_t deficit = final_length - curr;
            ARROW_RETURN_NOT_OK(b->AppendNulls(deficit));
        }
    }

    // ------------------------------
    // 3. Finish arrays
    // ------------------------------
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(builders.size());

    for (auto& b : builders) {
        std::shared_ptr<arrow::Array> arr;
        ARROW_RETURN_NOT_OK(b->Finish(&arr));

        if (arr->length() != final_length) {
            return arrow::Status::Invalid("Array length mismatch after finishing");
        }

        arrays.push_back(arr);
    }

    // ------------------------------
    // 4. Build table
    // ------------------------------
    auto table = arrow::Table::Make(schema, arrays);
    ARROW_RETURN_NOT_OK(table->Validate());

    return table;
}





inline arrow::Status AppendTableToParquet(
    const std::shared_ptr<arrow::Table>& table,
    const std::string& path,
    std::unique_ptr<parquet::arrow::FileWriter>& writer,
    std::shared_ptr<arrow::io::FileOutputStream>& outfile)  // keep stream alive
{
    if (!writer) {
        // First call: open file and create writer
        ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(path));

        // Required writer properties
        auto props = parquet::WriterProperties::Builder().build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().build();

        // Create the writer
        ARROW_RETURN_NOT_OK(parquet::arrow::FileWriter::Open(
            *table->schema(),                // const arrow::Schema&
            arrow::default_memory_pool(),
            outfile,                         // shared_ptr<OutputStream>
            props,                           // WriterProperties
            arrow_props,                     // ArrowWriterProperties
            &writer                          // out: unique_ptr<FileWriter>
        ));
    }

    // Write a row group containing all rows in this table
    return writer->WriteTable(*table, table->num_rows()+1);
}
inline arrow::Status CloseParquetWriter(std::unique_ptr<parquet::arrow::FileWriter>& writer) {
    if (writer) {
        ARROW_RETURN_NOT_OK(writer->Close());
        writer.reset();
    }
    return arrow::Status::OK();
}