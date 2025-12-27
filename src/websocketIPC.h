#pragma once
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>

#include <iostream>
#include <memory>
#include <string>

namespace asio  = boost::asio;
namespace beast = boost::beast;
namespace ws    = beast::websocket;
using tcp        = asio::ip::tcp;

// Serialize an Arrow Table to an in-memory buffer containing an Arrow IPC RecordBatch stream.
// Returns non-null buffer on success or an arrow::Status error.
inline arrow::Result<std::shared_ptr<arrow::Buffer>>
SerializeTableToIpcBuffer(const std::shared_ptr<arrow::Table>& table) {
    if (!table) {
        return arrow::Status::Invalid("Null table");
    }

    // Create an in-memory output stream
    ARROW_ASSIGN_OR_RAISE(auto out_stream, arrow::io::BufferOutputStream::Create());

    // Create a RecordBatchWriter (IPC stream writer)
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ipc::RecordBatchWriter> writer,
        arrow::ipc::MakeStreamWriter(out_stream, table->schema())
    );

    // Use TableBatchReader to iterate record batches from the table
    arrow::TableBatchReader reader(*table);

    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;

        // Try to read next batch
        ARROW_RETURN_NOT_OK(reader.ReadNext(&batch));

        // Null batch means EOF
        if (!batch) {
            break;
        }

        // Write batch
        ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    }

    // Close writer (flushes any metadata)
    ARROW_RETURN_NOT_OK(writer->Close());

    // Finish the buffer and return it
    ARROW_ASSIGN_OR_RAISE(auto buffer, out_stream->Finish());
    return buffer;
}

// Send the given Arrow IPC buffer over a websocket (binary message) to ws://host:port/target
// This function blocks while connecting and writing.
inline beast::error_code
SendBufferOverWebSocket(const std::string& host,
                        const std::string& port,
                        const std::string& target,
                        const std::shared_ptr<arrow::Buffer>& buffer)
{
    if (!buffer) {
        return beast::error_code(beast::errc::invalid_argument, beast::system_category());
    }

    try {
        asio::io_context ioc;

        // Resolve host:port
        tcp::resolver resolver{ioc};
        auto const results = resolver.resolve(host, port);

        // Connect socket
        tcp::socket socket{ioc};
        asio::connect(socket, results.begin(), results.end());

        // Create websocket stream and perform handshake
        ws::stream<tcp::socket> ws{std::move(socket)};

        // Set a reasonable timeout (optional)
        ws.set_option(ws::stream_base::timeout::suggested(beast::role_type::client));

        // Perform the websocket handshake; host header should include port if it's nonstandard
        std::string host_header = host + ":" + port;
        ws.handshake(host_header, target);

        // Send binary message with the whole buffer
        ws.binary(true);
        // beast::multi_buffer or asio::buffer can be used; use asio::buffer with raw pointer
        beast::flat_buffer send_buf;
        // We can write directly from the buffer data
        ws.write(asio::buffer(buffer->data(), buffer->size()));

        // Optionally receive a response (here we just close)
        beast::error_code ec;
        ws.close(ws::close_code::normal, ec);
        return ec;
    } catch (const beast::system_error& se) {
        return se.code();
    } catch (const std::exception& ex) {
        // map to generic error
        return beast::error_code(beast::errc::make_error_code(beast::errc::io_error));
    }
}