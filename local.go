package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

const testFilePath = "c:\\temp\\kube-apiserver.log"

// We assume read from file is fast - according to 'testRead' it takes ~1s on uncompressed file
func runBenchmarks() {
	testDecompress()
	testDownload()
	testParse()
	testFileRead()
}

func testDecompress() {
	defer timeTrack(time.Now(), "decompress")
	r := readFromLocalFile(testFilePath + ".gz")
	r, err := decompress(r)
	io.Copy(ioutil.Discard, r)
	handle(err)
}

func testDownload() {
	defer timeTrack(time.Now(), "download")

	r, err := download(testLogPath)
	handle(err)
	io.Copy(ioutil.Discard, r)
}

func testParse() {
	defer timeTrack(time.Now(), "parse")
	regex, _ := regexp.Compile(testTargetSubstring)
	reader := bufio.NewReader(readFromLocalFile(testFilePath))
	parsed, _ := processLines(reader, regex)
	if len(parsed) == 0 {
	}
}

func testFileRead() {
	defer timeTrack(time.Now(), "read")
	r := readFromLocalFile(testFilePath)
	io.Copy(ioutil.Discard, r)
}

func saveCompressed() {
	bts, err := downloadFull(testLogPath)
	handle(err)
	err = ioutil.WriteFile(testFilePath+".gz", bts, 0644)
}

func downloadFull(objectPath string) ([]byte, error) {
	context := context.Background()
	client, err := storage.NewClient(context, option.WithoutAuthentication())
	if err != nil {
		return nil, err
	}

	defer timeTrack(time.Now(), "download")
	bucket := client.Bucket(bucketName)

	remoteFile := bucket.Object(objectPath).ReadCompressed(true)
	reader, err := remoteFile.NewReader(context)
	if err != nil {
		return nil, err
	}

	localBytes, err := ioutil.ReadAll(reader)
	return localBytes, err
}

func readFromLocalFile(filename string) io.Reader {
	bts, err := ioutil.ReadFile(filename)
	handle(err)
	return bytes.NewReader(bts)
}
