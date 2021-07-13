#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>

using namespace std::chrono;

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/reader.h>
#include "parquet/arrow/writer.h"

using arrow::Result;
using arrow::Status;

namespace {

    Result<std::unique_ptr<parquet::arrow::FileReader>> OpenReader() {
        arrow::fs::LocalFileSystem file_system;
        ARROW_ASSIGN_OR_RAISE(auto input, file_system.OpenInputFile("/home/emulebest/CLionProjects/parquet_bench/sample_mb.parquet"));

        parquet::ArrowReaderProperties arrow_reader_properties =
                parquet::default_arrow_reader_properties();

        arrow_reader_properties.set_pre_buffer(true);
        arrow_reader_properties.set_use_threads(true);

        parquet::ReaderProperties reader_properties =
                parquet::default_reader_properties();

        // Open Parquet file reader
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        auto reader_builder = parquet::arrow::FileReaderBuilder();
        reader_builder.properties(arrow_reader_properties);
        ARROW_RETURN_NOT_OK(reader_builder.Open(std::move(input), reader_properties));
        ARROW_RETURN_NOT_OK(reader_builder.Build(&arrow_reader));

        return arrow_reader;
    }


    Status write_parquet_file(arrow::Table* table) {
        const arrow::Table& tbl = *table;
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_ASSIGN_OR_RAISE(outfile,
                              arrow::io::FileOutputStream::Open("./parquet-arrow-example.parquet"));
        // The last argument to the function call is the size of the RowGroup in
        // the parquet file. Normally you would choose this to be rather large but
        // for the example, we use a small value to have multiple RowGroups.
        PARQUET_THROW_NOT_OK(
                parquet::arrow::WriteTable(tbl, arrow::default_memory_pool(), outfile, 10000000));
        return Status::OK();
    }

    Status RunMain(int argc, char **argv) {
        // Read entire file as a single Arrow table
        std::shared_ptr<arrow::Table> table;
        ARROW_ASSIGN_OR_RAISE(auto arrow_reader, OpenReader());
        ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));
        for (auto i = 0; i < 10; i++) {
            auto t1 = std::chrono::high_resolution_clock::now();
            write_parquet_file(table.get());
//            std::cout << table->num_rows() << "," << table->num_columns() << std::endl;
            auto t2 = std::chrono::high_resolution_clock::now();
            auto ms_int =
                    std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1);
            std::cout << "Time taken to write parquet file is : " << ms_int.count()
                      << "ms\n";
        }

        return Status::OK();
    }

} // namespace

int main(int argc, char **argv) {
    Status st = RunMain(argc, argv);
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    return 0;
}