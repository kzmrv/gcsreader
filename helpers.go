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

func download(objectPath string) (*storage.Reader, error) {
	context := context.Background()
	client, err := storage.NewClient(context, option.WithoutAuthentication())
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)

	remoteFile := bucket.Object(objectPath).ReadCompressed(true)
	reader, err := remoteFile.NewReader(context)
	if err != nil {
		return nil, err
	}

	return reader, err
}

func decompress(reader *storage.Reader) (*gzip.Reader, error) {
	newReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return newReader, nil
}

func handle(err error) {
	if err == nil {
		return
	}
	log.Panic(err)
}

// For benchmarking purposes
func downloadFull(objectPath string) ([]byte, error) {
	context := context.Background()
	client, err := storage.NewClient(context, option.WithoutAuthentication())
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)

	remoteFile := bucket.Object(objectPath).ReadCompressed(true)
	reader, err := remoteFile.NewReader(context)
	if err != nil {
		return nil, err
	}

	defer timeTrack(time.Now(), "download")
	localBytes, err := ioutil.ReadAll(reader)
	return localBytes, err
}

func readFromLocalFile(filename string) io.Reader {
	bts, err := ioutil.ReadFile(filename)
	handle(err)
	return bytes.NewReader(bts)
}
