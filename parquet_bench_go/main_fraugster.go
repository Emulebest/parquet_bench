package main

//
//import (
//	goparquet "github.com/fraugster/parquet-go"
//	"github.com/fraugster/parquet-go/parquet"
//	"github.com/fraugster/parquet-go/parquetschema"
//	"io"
//	"log"
//	"os"
//	"time"
//)
//
//func timeTrack(start time.Time, name string) {
//	elapsed := time.Since(start)
//	log.Printf("%s took %s", name, elapsed)
//}
//
//func loadParquet(start time.Time, f *os.File) []map[string]interface{} {
//	defer timeTrack(start, "read")
//	defer f.Close()
//	fr, err := goparquet.NewFileReader(f)
//	if err != nil {
//		panic(err)
//	}
//
//	data := make([]map[string]interface{}, fr.NumRows())
//	count := 0
//	for {
//		row, err := fr.NextRow()
//		if err == io.EOF {
//			break
//		}
//		if err != nil {
//			panic(err)
//		}
//		data = append(data, row)
//		count++
//	}
//	return data
//}
//
//func writeParquet(start time.Time, data []map[string]interface{}) {
//	defer timeTrack(start, "write")
//	f, err := os.OpenFile("output.parquet", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
//	if err != nil {
//		log.Fatalf("Opening output file failed: %v", err)
//	}
//	defer f.Close()
//	schemaDef, err := parquetschema.ParseSchemaDefinition(
//		`message test {
//			required binary Region (STRING);
//			required binary Country (STRING);
//			required binary Item (STRING);
//		}`)
//	fw := goparquet.NewFileWriter(f,
//		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
//		goparquet.WithSchemaDefinition(schemaDef),
//	)
//	count := 0
//	for _, input := range data {
//		count++
//		if err := fw.AddData(input); err != nil {
//			log.Fatalf("Failed to add input %v to parquet file: %v", input, err)
//		}
//	}
//	println(count)
//	if err := fw.Close(); err != nil {
//		log.Fatalf("Closing parquet file writer failed: %v", err)
//	}
//}
//
//func main() {
//	println("Starting")
//	time.Sleep(5 * time.Second)
//	start := time.Now()
//	r, err := os.Open("./sample_mb.parquet")
//	if err != nil {
//		panic(err)
//	}
//	data := loadParquet(start, r)
//	for i := 0; i < 10; i++ {
//		start := time.Now()
//		writeParquet(start, data)
//	}
//}
