package main

import (
	"bufio"
	//"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"time"
	//gzip "github.com/klauspost/pgzip"
)

func main() {
	defer timeTrack(time.Now(), "total run")
	defer durations.printAll()
	logPath, targetSubstring := setupFromConsole()
	regex, _ := regexp.Compile(targetSubstring)

	r, err := downloadAndDecompress(logPath)
	handle(err)
	reader := bufio.NewReader(r)
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

func downloadAndDecompress(objectPath string) (io.Reader, error) {
	reader, err := download(objectPath)
	if err != nil {
		return nil, err
	}

	decompressed, err := decompress(reader)
	if err != nil {
		return nil, err
	}
	return decompressed, nil
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

func (e *parseLineFailedError) Error() string {
	return "Failed to parse line: " + e.line
}

type parseLineFailedError struct {
	line string
}

type logEntry struct {
	log  string
	time string
}
