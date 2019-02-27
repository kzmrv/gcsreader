package main

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/kubernetes/klog"
)

func main() {
	defer timeTrack(time.Now(), "total run")
	defer durations.printAll()
	run()
}

func run() {
	logPath, targetSubstring := setupFromConsole()
	regex, _ := regexp.Compile(targetSubstring)

	r, err := downloadAndDecompress(logPath)
	handle(err)
	parsed, _ := processLines(r, regex)

	if len(parsed) == 0 {
		klog.Info("No lines found")
	} else {
		for _, line := range parsed {
			klog.Infoln(*line)
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

func processLines(reader io.Reader, regex *regexp.Regexp) ([]*logEntry, error) {
	var result []*logEntry
	for {
		r := bufio.NewReader(reader)
		line, err := r.ReadBytes('\n')
		time := time.Now()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		isMatched, entry, err := processLine(line, regex)
		if err != nil {
			klog.Warningln(err, line) // TODO There is a problem that files finish with incompleted line
		}
		if isMatched && err == nil {
			result = append(result, entry)
		}
		timeTrackIncremental(time, "parsing")
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
