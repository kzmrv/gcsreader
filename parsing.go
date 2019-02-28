package main

import (
	"bufio"
	"io"
	"regexp"
	"strings"

	"github.com/kubernetes/klog"
)

func processLines(reader io.Reader, regex *regexp.Regexp) ([]*logEntry, error) {
	var result []*logEntry
	r := bufio.NewReader(reader)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		matched, entry, err := processLine(line, regex)
		if err != nil {
			// TODO There is a problem that files finish with incomplete line
			klog.Errorf("%s error parsing line %s", err, line)
		}
		if matched && err == nil {
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