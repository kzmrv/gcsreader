package main

import (
	"log"
	"sync"
	"time"
)

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
		log.Printf("%s took %s", name, elapsed)
	}
}
