package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"log"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

const bucketName = "kubernetes-jenkins"

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func download(objectPath string) ([]byte, error) {
	context := context.Background()
	client, err := storage.NewClient(context, option.WithoutAuthentication())
	handle(err)

	bucket := client.Bucket(bucketName)

	remoteFile := bucket.Object(objectPath).ReadCompressed(true)
	reader, err := remoteFile.NewReader(context)
	handle(err)

	defer timeTrack(time.Now(), "download")
	localBytes, err := ioutil.ReadAll(reader)
	return localBytes, err
}

func decompress(bts []byte) (*gzip.Reader, error) {
	defer timeTrack(time.Now(), "decompress")
	reader, err := gzip.NewReader(bytes.NewReader(bts))
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func handle(err error) {
	if err == nil {
		return
	}
	log.Panic(err)
}

func readFromLocalFile(filename string) io.Reader {
	bts, err := ioutil.ReadFile(filename)
	handle(err)
	return bytes.NewReader(bts)
}
