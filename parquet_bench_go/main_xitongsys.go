package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func writePar() {
	start := time.Now()
	bucket := "devrev-test-bucket"
	timeout := 10 * time.Minute

	//var AppFs = afero.NewMemMapFs()

	//preader, pwriter := io.Pipe()

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1"),
	})

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	//svc := s3.New(sess)

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	if cancelFn != nil {
		defer cancelFn()
	}

	defer timeTrack(start, "write")
	w, err := os.Create("flat.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	fmt.Println("Before create writer")
	//write
	pw, err := writer.NewParquetWriterFromWriter(w, new(Student), 100)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	fmt.Println("hello :)")
	pw.PageSize = 64 * 1024
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 30_000_000
	for i := 0; i < num; i++ {
		stu := Student{
			Name:   "StudentName",
			Age:    int32(20 + i%5),
			Id:     int64(i),
			Weight: float32(50.0 + float32(i)*0.1),
			Sex:    bool(i%2 == 0),
			Day:    int32(time.Now().Unix() / 3600 / 24),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")

	w.Close()

	file, _ := os.Open("flat.parquet")

	uploader := s3manager.NewUploader(sess)
	// Uploads the object to S3. The Context will interrupt the request if the
	// timeout expires.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		ACL:    aws.String("public-read"),
		Key:    aws.String("test"),
		Body:   file,
	})
	if err != nil {
		panic(err)
	}
	file.Close()
	//preader.Close()
	//pwriter.Close()
}

type Student struct {
	Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
	Id      int64   `parquet:"name=id, type=INT64"`
	Weight  float32 `parquet:"name=weight, type=FLOAT"`
	Sex     bool    `parquet:"name=sex, type=BOOLEAN"`
	Day     int32   `parquet:"name=day, type=INT32, convertedtype=DATE"`
	Ignored int32   //without parquet tag and won't write
}

func main() {
	//var err error
	writePar()

	///read
	//fr, err := local.NewLocalFileReader("flat.parquet")
	//if err != nil {
	//	log.Println("Can't open file")
	//	return
	//}

	//pr, err := reader.NewParquetReader(fr, new(Student), 4)
	//if err != nil {
	//	log.Println("Can't create parquet reader", err)
	//	return
	//}
	//num := int(pr.GetNumRows())
	//for i := 0; i < num/10; i++ {
	//	if i%2 == 0 {
	//		pr.SkipRows(10) //skip 10 rows
	//		continue
	//	}
	//	stus := make([]Student, 10) //read 10 rows
	//	if err = pr.Read(&stus); err != nil {
	//		log.Println("Read error", err)
	//	}
	//}
	//
	//pr.ReadStop()
	//fr.Close()

}
