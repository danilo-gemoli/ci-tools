package main

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type prowjobs struct {
	mu              sync.Mutex
	data            map[string]string
	jobsStoragePath string
}

func newProwjobs(jobsStoragePath string) *prowjobs {
	var loadedJobs map[string]string
	if err := readGob(jobsStoragePath, &loadedJobs); err != nil {
		logrus.Errorf("falling back to empty map, error reading Gob file: %v", err)
		loadedJobs = make(map[string]string)
	}
	return &prowjobs{
		data:            loadedJobs,
		mu:              sync.Mutex{},
		jobsStoragePath: jobsStoragePath,
	}
}

func (pjs *prowjobs) regenerate(prowjobs map[string]string) {
	pjs.mu.Lock()
	defer pjs.mu.Unlock()
	pjs.data = make(map[string]string)
	for key, value := range prowjobs {
		pjs.data[key] = value
	}
}

func (pjs *prowjobs) get(pj string) string {
	pjs.mu.Lock()
	defer pjs.mu.Unlock()

	cluster, exists := pjs.data[pj]
	if exists {
		return cluster
	}
	return ""
}
