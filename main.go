package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	//gzip "github.com/klauspost/pgzip"
)

const bucketName = "kubernetes-jenkins"

type logEntry struct {
	log  string
	time string
}

// TODO #1 Setup pgzip
// TODO #2 Make line processing parallel, optimize it
// TODO #3 Split code in different files
// TODO #4 Convert manual test methods below into automated tests

func main() {
	logPath, targetSubstring := setupFromConsole()
	regex, _ := regexp.Compile(targetSubstring)

	r, err := downloadAndDecompress(logPath)
	bytess, err := ioutil.ReadAll(r)
	reader := bufio.NewReader(bytes.NewReader(bytess))
	parsed, _ := processLines(reader, regex)

	if len(parsed) == 0 {
		fmt.Printf("No lines found")
	} else {
		for _, line := range parsed {
			fmt.Println(*line)
		}
	}
}

const testLogPath = "logs/ci-kubernetes-e2e-gce-scale-performance/290/artifacts/gce-scale-cluster-master/kube-apiserver-audit.log-20190102-1546444805.gz"
const testTargetSubstring = "\"auditID\":\"07ff64df-fcfe-4cdc-83a5-0c6a09237698\""

func setupFromConsole() (string, string) {
	var logPath, targetSubstring string
	if len(os.Args) < 2 || os.Args[1] == "" {
		logPath = testLogPath
	} else {
		logPath = os.Args[1]
	}

	if len(os.Args) < 3 || os.Args[2] == "" {
		targetSubstring = testTargetSubstring
	} else {
		targetSubstring = os.Args[2]
	}

	return logPath, targetSubstring
}

func processLines(reader *bufio.Reader, regex *regexp.Regexp) ([]*logEntry, error) {
	var result []*logEntry
	defer timeTrack(time.Now(), "filtering and parsing")
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		isMatched, entry, err := processLine(line, regex)
		if isMatched && err == nil {
			result = append(result, entry)
		}
	}
	return result, nil
}

func processLine(line []byte, regex *regexp.Regexp) (bool, *logEntry, error) {
	if !regex.Match(line) {
		return false, nil, nil
	}

	parsed, err := parseLine(string(line))
	return true, parsed, err
}

// TODO Parse as bytes if needed
func parseLine(line string) (*logEntry, error) {
	const startMarker = "ReceivedTimestamp\":\""
	const endMarker = "\",\"stageTimestamp"
	start := strings.Index(line, startMarker)
	end := strings.Index(line, endMarker)
	if start == -1 {
		return &logEntry{}, &parseLineFailedError{line}
	}
	if end == -1 {
		return &logEntry{}, &parseLineFailedError{line}
	}
	timestamp := line[(start + len(startMarker)):end]
	return &logEntry{log: line, time: timestamp}, nil
}

type parseLineFailedError struct {
	line string
}

func (e *parseLineFailedError) Error() string {
	return "Failed to parse line: " + e.line
}

func downloadAndDecompress(objectPath string) (*gzip.Reader, error) {
	bts, err := download(objectPath)
	handle(err)

	decompressed, err := decompress(bts)
	handle(err)
	return decompressed, nil
}

func decompress(bts []byte) (*gzip.Reader, error) {
	defer timeTrack(time.Now(), "decompress")
	reader, err := gzip.NewReader(bytes.NewReader(bts))
	if err != nil {
		return nil, err
	}
	return reader, nil
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

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
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

const testFilePath = "c:\\temp\\kube-apiserver.log"

func testDownloadToLocalFile() {
	path := testLogPath
	bytess := downloadAndDecompress(path)
	err := ioutil.WriteFile(testFilePath, bytess, 0644)
	handle(err)
}

func testParsingOnLocalFile() {
	regex, _ := regexp.Compile(testTargetSubstring)
	reader := bufio.NewReader(readFromLocalFile(testFilePath))
	parsed, _ := processLines(reader, regex)
	log.Println(parsed)
}
