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
	"path/filepath"
	"regexp"
	"strings"
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

const buffer = 100000

func (*server) DoWork(in *pb.Work, srv pb.Worker_DoWorkServer) error {
	defer timeTrack(time.Now(), "Call duration")
	log.Infof("Received: file %v, substring %v", in.File, in.TargetSubstring)

	r, err := downloadAndDecompress(in.File)
	if err != nil {
		return err
	}
	ch := make(chan *lineEntry, buffer)
	regex, err := regexp.Compile(in.TargetSubstring)
	if err != nil {
		return err
	}
	go processLines(r, regex, ch)
	sendLines(ch, srv)

	return nil
}

func sendLines(ch chan *lineEntry, srv pb.Worker_DoWorkServer) {
	lineCounter := 0
	const batchSize = 100
	for hasMoreBatches := true; hasMoreBatches; {
		batches := make([]*pb.LogLine, batchSize)
		i := 0
		for i < batchSize {
			line, hasMore := <-ch
			if line.err == io.EOF || !hasMore {
				hasMoreBatches = false
				break
			}
			if line.err != nil {
				log.Errorf("Failed to parse line with error %v", line.err)
				continue
			}

			entry := line.logEntry
			pbLine := &pb.LogLine{
				Entry:     *entry.log,
				Timestamp: &ts.Timestamp{Seconds: entry.time.Unix(), Nanos: int32(entry.time.Nanosecond())}}

			batches[i] = pbLine
			i++
		}

		if i != 0 {
			err := srv.Send(&pb.WorkResult{LogLines: batches[:i]})
			if err != nil {
				log.Errorf("Failed to send result with: %v", err)
			}
			lineCounter += i
		}
	}

	log.Infof("Finished with %v lines", lineCounter)
}

func downloadAndDecompress(objectPath string) (io.Reader, error) {
	//return loadFromLocalFS(objectPath)
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

func loadFromLocalFS(objectPath string) (io.Reader, error) {
	const folder = "/Downloads/kubernetes-jenkins-310"
	idx := strings.LastIndex(objectPath, "/") + 1
	fileName := strings.TrimSuffix(objectPath[idx:], ".gz")
	home, _ := os.UserHomeDir()
	return os.Open(filepath.Join(home, folder, fileName))
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

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Infof("%s took %s", name, elapsed)
}
