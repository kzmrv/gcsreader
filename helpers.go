package main

import (
	"context"
	"io"
	"log"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	gzip "github.com/klauspost/pgzip"
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
