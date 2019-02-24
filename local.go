package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

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

const testFilePath = "c:\\temp\\kube-apiserver.log"

func testDownloadToLocalFile() {
	path := testLogPath
	r, err := downloadAndDecompress(path)
	bytess, err := ioutil.ReadAll(r)
	err = ioutil.WriteFile(testFilePath, bytess, 0644)
	handle(err)
}

func testParsingOnLocalFile() {
	regex, _ := regexp.Compile(testTargetSubstring)
	reader := bufio.NewReader(readFromLocalFile(testFilePath))
	parsed, _ := processLines(reader, regex)
	log.Println(parsed)
}
