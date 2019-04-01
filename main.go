/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"io"
	"net"
	"os"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	ts "github.com/golang/protobuf/ptypes/timestamp"
	gzip "github.com/klauspost/pgzip"
	pb "github.com/kzmrv/mixer/proto"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	log "k8s.io/klog"
)

const (
	port = ":17654"
)

type server struct{}

func main() {
	log.InitFlags(nil)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Infof("Listening on port: %v", port)
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *server) DoWork(ctx context.Context, in *pb.Work) (*pb.WorkResult, error) {
	defer timeTrack(time.Now(), "Call duration")
	log.Infof("Received: file %v, substring %v", in.File, in.TargetSubstring)

	regex := regexp.MustCompile(in.TargetSubstring)

	r, err := downloadAndDecompress(in.File)
	if err != nil {
		log.Fatal(err)
	}
	parsed, err := processLines(r, regex)
	if err != nil {
		log.Fatal(err)
	}

	lines := make([]*pb.LogLine, len(parsed))
	for i, p := range parsed {
		lines[i] = &pb.LogLine{Entry: *p.log, Timestamp: &ts.Timestamp{Seconds: p.time.Unix(), Nanos: int32(p.time.Nanosecond())}}
	}

	log.Infof("Finished with %v lines", len(lines))
	return &pb.WorkResult{Logs: lines}, nil
}

func runDetached() error {
	logPath, targetSubstring := setupFromConsole()
	regex := regexp.MustCompile(targetSubstring)

	r, err := downloadAndDecompress(logPath)
	if err != nil {
		return err
	}
	parsed, err := processLines(r, regex)
	if err != nil {
		return err
	}

	if len(parsed) == 0 {
		log.Info("No lines found")
	} else {
		for _, line := range parsed {
			log.Infoln(*line)
		}
	}
	return nil
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
