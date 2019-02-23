package main

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"time"

	"cloud.google.com/go/storage"
	gzip "github.com/klauspost/pgzip"
	"google.golang.org/api/option"
)

const bucketName = "kubernetes-jenkins"

func download(objectPath string) (io.Reader, error) {
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

	return newTrackingReader(reader, "download"), err
}

func decompress(reader io.Reader) (io.Reader, error) {
	newReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return newTrackingReader(newReader, "decompress"), nil
}

func handle(err error) {
	if err == nil {
		return
	}
	log.Panic(err)
}

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
