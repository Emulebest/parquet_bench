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

    std::shared_ptr<arrow::Table> CreateTable() {
        std::shared_ptr<arrow::Field> name, age, id, weight, sex, day;
        std::shared_ptr<arrow::Schema> schema;
        name = arrow::field("name", arrow::utf8());
        age = arrow::field("age", arrow::int32());
        id = arrow::field("id", arrow::int64());
        weight = arrow::field("weight", arrow::float32());
        sex = arrow::field("sex", arrow::boolean());
        day = arrow::field("day", arrow::int32());
        schema = arrow::schema({name, age, id, weight, sex, day});
        // build the writer properties
        parquet::WriterProperties::Builder builder;
        auto properties = builder.build();

        // my current best attempt at converting the Arrow schema to a Parquet schema
        std::shared_ptr<parquet::SchemaDescriptor> parquet_schema;
        arrow::StringBuilder nameBuilder;
        std::shared_ptr<arrow::Array> nameArray;
        arrow::Int32Builder ageBuilder, dayBuilder;
        std::shared_ptr<arrow::Array> dayArray;
        std::shared_ptr<arrow::Array> ageArray;
        arrow::Int64Builder idBuilder;
        std::shared_ptr<arrow::Array> idArray;
        arrow::FloatBuilder weightBuilder;
        std::shared_ptr<arrow::Array> weightArray;
        arrow::BooleanBuilder sexBuilder;
        std::shared_ptr<arrow::Array> sexArray;
        for (int i = 0; i < 30000000; i++) {
            nameBuilder.Append("StudentName");
            ageBuilder.Append(22);
            dayBuilder.Append(5);
            idBuilder.Append(i);
            weightBuilder.Append(50.2);
            sexBuilder.Append(true);
        }
        nameBuilder.Finish(&nameArray);
        ageBuilder.Finish(&ageArray);
        dayBuilder.Finish(&dayArray);
        idBuilder.Finish(&idArray);
        weightBuilder.Finish(&weightArray);
        sexBuilder.Finish(&sexArray);
        arrow::PrettyPrint(*idArray, 4, &std::cout);
        return arrow::Table::Make(schema, {nameArray, ageArray, idArray, weightArray, sexArray, dayArray}); // parquet_schema is now populated
    }


    Status write_parquet_file(arrow::Table* table) {
        const arrow::Table& tbl = *table;
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_ASSIGN_OR_RAISE(outfile,
                              arrow::io::FileOutputStream::Open("./parquet-arrow-example123.parquet"));
        // The last argument to the function call is the size of the RowGroup in
        // the parquet file. Normally you would choose this to be rather large but
        // for the example, we use a small value to have multiple RowGroups.
        PARQUET_THROW_NOT_OK(
                parquet::arrow::WriteTable(tbl, arrow::default_memory_pool(), outfile, 10000000));
        return Status::OK();
    }

    Status RunMain(int argc, char **argv) {
        // Read entire file as a single Arrow table
        auto table = CreateTable();
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