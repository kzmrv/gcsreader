package main

import (
	"context"
	"io"
	"os"
	"regexp"

	"cloud.google.com/go/storage"
	gzip "github.com/klauspost/pgzip"
	"github.com/kubernetes/klog"
	"google.golang.org/api/option"
)

func main() {
	run()
}

func run() {
	logPath, targetSubstring := setupFromConsole()
	regex := regexp.MustCompile(targetSubstring)

	r, err := downloadAndDecompress(logPath)
	if err != nil {
		klog.Fatal(err)
	}
	parsed, err := processLines(r, regex)
	if err != nil {
		klog.Fatal(err)
	}

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

	return reader, err
}

func decompress(reader io.Reader) (io.Reader, error) {
	newReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	return newReader, nil
}
