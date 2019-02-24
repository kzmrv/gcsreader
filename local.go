package main

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"time"
)

// These methods are only meant to be used for local machine benchmarking, so they may be poor optimized or violate good style
const testFilePath = "c:\\temp\\kube-apiserver.log"

func setupForLocal() {
	downloadToFiles()
}

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

func downloadToFiles() {
	reader, err := download(testLogPath)
	handle(err)
	bts, err := ioutil.ReadAll(reader)
	handle(err)

	ioutil.WriteFile(testFilePath+".gz", bts, 0644)
	handle(err)

	r, err := decompress(bytes.NewReader(bts))
	handle(err)
	fullFile, err := os.OpenFile(testFilePath, os.O_RDWR|os.O_CREATE, 0644)
	handle(err)
	_, err = io.Copy(fullFile, r)
	handle(err)
}

func readFromLocalFile(filename string) io.Reader {
	bts, err := ioutil.ReadFile(filename)
	handle(err)
	return bytes.NewReader(bts)
}
