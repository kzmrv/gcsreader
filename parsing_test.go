package main

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"testing"
)

func Test_ShouldMatchSingleLine(t *testing.T) {
	data, err := ioutil.ReadFile(filepath.Join("testdata", "kube-apiserver-cut.log"))
	regex := regexp.MustCompile("\"auditID\":\"39aec93e-031b-4002-8c0a-4ddcd92e250b\"")
	if err != nil {
		t.Fatal(err)
	}
	lines, err := processLines(bytes.NewReader(data), regex)
	if err != nil {
		t.Fatal(err)
	}

	if len(lines) != 1 {
		t.Fatalf("Expected 1 line, got %v", len(lines))
	}
}

func Test_ShouldParseLine(t *testing.T) {
	data, err := ioutil.ReadFile(filepath.Join("testdata", "kube-apiserver-cut.log"))
	lineStr := string(data[:983])
	t.Log(lineStr)
	line, err := parseLine(lineStr)
	if err != nil {
		t.Fatal(err)
	}

	const expectedTime = "2019-01-02T15:01:16.105964Z"
	if line.time != expectedTime {
		t.Fatalf("Expected time %s received %s", expectedTime, line.time)
	}

	if line.log != lineStr {
		t.Fatalf("Expected log %s received %s", lineStr, line.log)
	}
}
