// Copyright © 2017 Douglas Chimento <dchimento@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logzio

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"github.com/beeker1121/goque"
	"github.com/logzio/logzio-go/inMemoryQueue"
	"github.com/shirou/gopsutil/disk"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"go.uber.org/atomic"
)

const (
	maxSize               = 3 * 1024 * 1024 // 3 mb
	sendSleepingBackoff   = time.Second * 2
	sendRetries           = 4
	defaultHost           = "https://listener.logz.io:8071"
	defaultDrainDuration  = 5 * time.Second
	defaultDiskThreshold  = 95.0 // represent % of the disk
	defaultCheckDiskSpace = true
	defaultQueueMaxLength = 9 * 1024 * 1024 // 9 mb
	defaultMaxLogCount    = 500000

	httpError = -1
)

// Sender Alias to LogzioSender
type Sender LogzioSender

// LogzioSender instance of the
type LogzioSender struct {
	queue          genericQueue
	drainDuration  time.Duration
	buf            *bytes.Buffer
	draining       atomic.Bool
	mux            sync.Mutex
	token          string
	url            string
	debug          io.Writer
	diskThreshold  float32
	checkDiskSpace bool
	//fullDisk          bool
	checkDiskDuration time.Duration
	dir               string
	httpClient        *http.Client
	httpTransport     *http.Transport
	compress          bool
	droppedLogs       int
	// In memory Queue
	inMemoryQueue    bool
	inMemoryCapacity uint64
	logCountLimit    int
}

// SenderOptionFunc options for logz
type SenderOptionFunc func(*LogzioSender) error

// New creates a new Logzio sender with a token and options
func New(token string, options ...SenderOptionFunc) (*LogzioSender, error) {
	l := &LogzioSender{
		buf:            bytes.NewBuffer(make([]byte, maxSize)),
		drainDuration:  defaultDrainDuration,
		url:            fmt.Sprintf("%s/?token=%s", defaultHost, token),
		token:          token,
		dir:            fmt.Sprintf("%s%s%s%s%d", os.TempDir(), string(os.PathSeparator), "logzio-buffer", string(os.PathSeparator), time.Now().UnixNano()),
		diskThreshold:  defaultDiskThreshold,
		checkDiskSpace: defaultCheckDiskSpace,
		//fullDisk:          false,
		checkDiskDuration: 5 * time.Second,
		compress:          true,
		droppedLogs:       0,
		// In memory queue
		inMemoryQueue:    false,
		inMemoryCapacity: defaultQueueMaxLength,
		logCountLimit:    defaultMaxLogCount,
	}
	tlsConfig := &tls.Config{}
	transport := &http.Transport{
		Proxy:           http.ProxyFromEnvironment,
		TLSClientConfig: tlsConfig,
	}
	// in case server side is sleeping - wait 10s instead of waiting for him to wake up
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Second * 10,
	}
	l.httpClient = client
	l.httpTransport = transport

	for _, option := range options {
		if err := option(l); err != nil {
			return nil, err
		}
	}

	if l.inMemoryQueue {
		// Init in memory queue
		q := inMemoryQueue.NewConcurrentQueue(l.logCountLimit)
		l.queue = q
		l.checkDiskSpace = false
	} else {
		// Init disk queue
		q, err := goque.OpenQueue(l.dir)
		if err != nil {
			return nil, err
		}
		l.queue = q
		//go l.isEnoughDiskSpace()
	}
	go l.start()
	return l, nil
}

// SetlogCountLimit to change the default limit
func SetlogCountLimit(limit int) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.logCountLimit = limit
		return nil
	}
}

// SetinMemoryCapacity to change the default capacity
func SetinMemoryCapacity(size uint64) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.inMemoryCapacity = size
		return nil
	}
}

func SetCompress(b bool) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.compress = b
		return nil
	}
}

// SetInMemoryQueue to change the default disk queue
func SetInMemoryQueue(b bool) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.inMemoryQueue = b
		return nil
	}
}

// SetTempDirectory Use this temporary dir
func SetTempDirectory(dir string) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.dir = dir
		return nil
	}
}

// SetUrl set the url which maybe different from the defaultUrl
func SetUrl(url string) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.url = fmt.Sprintf("%s/?token=%s", url, l.token)
		l.debugLog("logziosender.go: Setting url to %s\n", l.url)
		return nil
	}
}

// SetDebug mode and send logs to this writer
func SetDebug(debug io.Writer) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.debug = debug
		return nil
	}
}

// SetDrainDuration to change the interval between drains
func SetDrainDuration(duration time.Duration) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.drainDuration = duration
		return nil
	}
}

// SetCheckDiskSpace to check if it crosses the maximum allowed disk usage
func SetCheckDiskSpace(check bool) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.checkDiskSpace = check
		return nil
	}
}

// SetDrainDiskThreshold to change the maximum used disk space
func SetDrainDiskThreshold(th int) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.diskThreshold = float32(th)
		return nil
	}
}

func (l *LogzioSender) isEnoughDiskSpace() bool {
	//<-time.After(l.checkDiskDuration)
	if l.checkDiskSpace {
		diskStat, err := disk.Usage(l.dir)
		if err != nil {
			l.debugLog("logziosender.go: failed to get disk usage: %v\n", err)
			l.checkDiskSpace = false
			return false
		}

		usage := float32(diskStat.UsedPercent)
		if usage > l.diskThreshold {
			l.debugLog("Logz.io: Dropping logs, as FS used space on %s is %g percent,"+
				" and the drop threshold is %g percent\n",
				l.dir, usage, l.diskThreshold)
			l.droppedLogs++
			return false
		} else {
			return true
		}
	} else {
		return true
	}

}
func (l *LogzioSender) isEnoughMemory(dataSize uint64) bool {
	usage := l.queue.Length()
	if usage+dataSize >= l.inMemoryCapacity {
		l.debugLog("Logz.io: Dropping logs, the max capacity is %d and %d is requested, Request size: %d\n", l.inMemoryCapacity, usage+dataSize, dataSize)
		l.droppedLogs++
		return false
	} else {
		return true
	}
}

// Send the payload to logz.io
func (l *LogzioSender) Send(payload []byte) error {
	if !l.inMemoryQueue && l.isEnoughDiskSpace() {
		_, err := l.queue.Enqueue(payload)
		return err
	} else if l.inMemoryQueue && l.isEnoughMemory(uint64(len(payload))) {
		_, err := l.queue.Enqueue(payload)
		return err
	}
	return nil
}

func (l *LogzioSender) start() {
	l.drainTimer()
}

// Stop will close the LevelDB queue and do a final drain
func (l *LogzioSender) Stop() {
	defer l.queue.Close()
	l.Drain()
}

func (l *LogzioSender) makeHttpRequest(data bytes.Buffer, attempt int, c bool) int {
	var lost string
	if l.droppedLogs > 0 {
		lost = fmt.Sprintf("1/NN:%d", l.droppedLogs)
	} else {
		lost = "0"
	}
	req, err := http.NewRequest("POST", l.url, &data)
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("logzio-shipper", fmt.Sprintf("logzio-go/v1.0.0/%d/%s", attempt, lost))
	if c {
		req.Header.Add("Content-Encoding", "gzip")
	}
	resp, err := l.httpClient.Do(req)
	if err != nil {
		//l.debugLog("logziosender.go: Error sending logs to %s %s\n", l.url, err)
		return httpError
	}

	defer resp.Body.Close()
	statusCode := resp.StatusCode
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		l.debugLog("Error reading response body: %v", err)
	}
	if statusCode == 200 {
		l.droppedLogs = 0
	}
	return statusCode

}

func (l *LogzioSender) tryToSendLogs(attempt int) int {
	if l.compress {
		var compressedBuf bytes.Buffer
		compr := gzip.NewWriter(&compressedBuf)
		compr.Write(l.buf.Bytes())
		compr.Close()
		return l.makeHttpRequest(compressedBuf, attempt, true)
	} else {
		return l.makeHttpRequest(*l.buf, attempt, false)
	}
}

func (l *LogzioSender) drainTimer() {
	for {
		time.Sleep(l.drainDuration)
		l.Drain()
	}
}

func (l *LogzioSender) shouldRetry(statusCode int) bool {
	retry := true
	switch statusCode {
	case http.StatusBadRequest:
		l.debugLog("Got HTTP %d bad request, skip retry\n", statusCode)
		retry = false
	case http.StatusNotFound:
		l.debugLog("Got HTTP %d not found, skip retry\n", statusCode)
		retry = false
	case http.StatusUnauthorized:
		l.debugLog("Got HTTP %d unauthorized, skip retry\n", statusCode)
		retry = false
	case http.StatusForbidden:
		l.debugLog("Got HTTP %d forbidden, skip retry\n", statusCode)
		retry = false
	case http.StatusOK:
		retry = false
	}
	return retry
}

// Drain - Send remaining logs
func (l *LogzioSender) Drain() {
	if l.draining.Load() {
		l.debugLog("logziosender.go: Already draining\n")
	}
	l.mux.Lock()
	l.debugLog("logziosender.go: draining queue\n")
	defer l.mux.Unlock()
	l.draining.Toggle()
	defer l.draining.Toggle()

	l.buf.Reset()
	var reDrain bool = true
	for l.queue.Length() > 0 && reDrain {
		bufSize := l.dequeueUpToMaxBatchSize()
		if bufSize > 0 {
			backOff := sendSleepingBackoff
			toBackOff := false
			for attempt := 0; attempt < sendRetries; attempt++ {
				if toBackOff {
					l.debugLog("logziosender.go: failed to send logs, trying again in %v\n", backOff)
					time.Sleep(backOff)
					backOff *= 2
				}
				statusCode := l.tryToSendLogs(attempt)
				if l.shouldRetry(statusCode) {
					toBackOff = true
					if attempt == (sendRetries - 1) {
						l.requeue()
						reDrain = false
					}
				} else {
					reDrain = true
					break
				}
			}
		}
	}

}

func (l *LogzioSender) dequeueUpToMaxBatchSize() int {
	var (
		bufSize int
		err     error
	)
	for bufSize < maxSize && err == nil {
		item, err := l.queue.Dequeue()
		if err != nil {
			l.debugLog("queue state: %s\n", err)
		}
		if item != nil {
			// NewLine is appended tp item.Value
			if len(item.Value)+bufSize+1 > maxSize {
				break
			}
			bufSize += len(item.Value)
			l.debugLog("logziosender.go: Adding item with size %d (total buffSize: %d)\n", len(item.Value), bufSize)
			_, err := l.buf.Write(append(item.Value, '\n'))
			if err != nil {
				l.errorLog("error writing to buffer %s", err)
			}
		} else {
			break
		}
	}
	return bufSize
}

// Sync drains the queue
func (l *LogzioSender) Sync() error {
	l.Drain()
	return nil
}

func (l *LogzioSender) requeue() {
	l.debugLog("logziosender.go: Requeue %s", l.buf.String())
	err := l.Send(l.buf.Bytes())
	if err != nil {
		l.errorLog("could not requeue logs %s", err)
	}
}

func (l *LogzioSender) debugLog(format string, a ...interface{}) {
	if l.debug != nil {
		fmt.Fprintf(l.debug, format, a...)
	}
}

func (l *LogzioSender) errorLog(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func (l *LogzioSender) Write(p []byte) (n int, err error) {
	return len(p), l.Send(p)
}

// CloseIdleConnections to close all remaining open connections
func (l *LogzioSender) CloseIdleConnections() {
	l.httpTransport.CloseIdleConnections()
}
