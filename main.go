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

const buffer = 100000

func (*server) DoWork(in *pb.Work, srv pb.Worker_DoWorkServer) error {
	defer timeTrack(time.Now(), "Call duration")
	log.Infof("Received: file %v, substring %v", in.File, in.TargetSubstring)
	regex := regexp.MustCompile(in.TargetSubstring)

	r, err := downloadAndDecompress(in.File)
	if err != nil {
		log.Fatal(err)
	}
	ch := make(chan *lineEntry, buffer)
	go processLines(r, regex, ch)

	counter := 0
	for {
		line, hasMore := <-ch
		if line.err == io.EOF || !hasMore {
			break
		}
		if line.err != nil {
			log.Errorf("Failed to parse line with error %v", line.err)
			continue
		}
		entry := line.logEntry
		pbLine := &pb.LogLine{Entry: *entry.log, Timestamp: &ts.Timestamp{Seconds: entry.time.Unix(), Nanos: int32(entry.time.Nanosecond())}}
		counter++

		err := srv.Send(&pb.WorkResult{LogLine: pbLine})
		if err != nil {
			log.Errorf("Failed to send result with: %v", err)
		}
	}

	log.Infof("Finished with %v lines", counter)
	return nil
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

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Infof("%s took %s", name, elapsed)
}
