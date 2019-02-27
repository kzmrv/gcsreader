package main

import (
	"io"
	"sync"
	"time"

	"github.com/kubernetes/klog"
)

var durations = newDurationMap()
var trackDurations = false

type trackingReader struct {
	inner io.Reader
	name  string
}

func newTrackingReader(reader io.Reader, name string) io.Reader {
	return &trackingReader{
		inner: reader,
		name:  name}
}

func (reader *trackingReader) Read(bts []byte) (int, error) {
	defer timeTrackIncremental(time.Now(), reader.name)
	n, err := reader.inner.Read(bts)
	return n, err
}

func timeTrack(start time.Time, name string) {
	if !trackDurations {
		return
	}
	elapsed := time.Since(start)
	klog.Infof("%s took %s", name, elapsed)
}

func timeTrackIncremental(start time.Time, name string) {
	if !trackDurations {
		return
	}

	elapsed := time.Since(start)
	durations.increase(name, elapsed)
}

// Use this instead of sync.map to keep code cleaner
type durationMap struct {
	mx sync.Mutex
	m  map[string]time.Duration
}

func newDurationMap() *durationMap {
	return &durationMap{m: map[string]time.Duration{}}
}

func (mp *durationMap) increase(key string, value time.Duration) {
	mp.mx.Lock()
	defer mp.mx.Unlock()
	mp.m[key] += value
}

func (mp *durationMap) printAll() {
	for name, elapsed := range mp.m {
		klog.Infof("%s took %s", name, elapsed)
	}
}
