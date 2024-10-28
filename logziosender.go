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
	"github.com/shirou/gopsutil/v3/disk"
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
	defaultQueueMaxLength = 20 * 1024 * 1024 // 20 mb
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
	dir            string
	httpClient     *http.Client
	httpTransport  *http.Transport
	compress       bool
	droppedLogs    int
	isOpen         bool
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
		compress:       true,
		droppedLogs:    0,
		isOpen:         false,
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
	}
	go l.start()
	return l, nil
}

// SetHttpClient to change the default http client
func SetHttpClient(client *http.Client) SenderOptionFunc {
	return func(l *LogzioSender) error {
		l.httpClient = client
		return nil
	}
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
		l.url = url
		if l.token != "" {
			l.url = fmt.Sprintf("%s/?token=%s", url, l.token)
		}
		l.debugLog("sender: Setting url to %s\n", l.url)
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
func (l *LogzioSender) getIsOpen() bool {
	l.mux.Lock()
	defer l.mux.Unlock()
	return l.isOpen
}

func (l *LogzioSender) isEnoughDiskSpace() bool {
	if l.checkDiskSpace {
		diskStat, err := disk.Usage(l.dir)
		if err != nil {
			l.debugLog("sender: failed to get disk usage: %v\n", err)
			l.checkDiskSpace = false
			return false
		}

		usage := float32(diskStat.UsedPercent)
		if usage > l.diskThreshold {
			l.debugLog("sender: Dropping logs, as FS used space on %s is %g percent,"+
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
		l.debugLog("sender: Dropping logs, the max capacity is %d and %d is requested, Request size: %d\n", l.inMemoryCapacity, usage+dataSize, dataSize)
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
	l.mux.Lock()
	l.isOpen = true
	l.mux.Unlock()
	l.drainTimer()
}

// Stop will close the LevelDB queue and do a final drain
func (l *LogzioSender) Stop() {
	defer l.queue.Close()
	l.Drain()
	l.mux.Lock()
	l.isOpen = false
	l.mux.Unlock()
}

func (l *LogzioSender) makeHttpRequest(data bytes.Buffer, attempt int, c bool) int {
	var lost string
	if l.droppedLogs > 0 {
		lost = fmt.Sprintf("1/NN:%d", l.droppedLogs)
	} else {
		lost = "0"
	}
	req, err := http.NewRequest("POST", l.url, &data)
	if err != nil {
		l.debugLog("sender: Error creating HTTP request for %s %s\n", l.url, err)
		return httpError
	}
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("logzio-shipper", fmt.Sprintf("logzio-go/v1.0.0/%d/%s", attempt, lost))
	if c {
		req.Header.Add("Content-Encoding", "gzip")
	}
	l.debugLog("sender: Sending bulk of %v bytes\n", l.buf.Len())
	resp, err := l.httpClient.Do(req)
	if err != nil {
		l.debugLog("sender: Error sending logs to %s %s\n", l.url, err)
		return httpError
	}

	defer resp.Body.Close()
	statusCode := resp.StatusCode
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		l.debugLog("sender: Error reading response body: %v", err)
	}
	l.debugLog("sender: Response status code: %v \n", statusCode)
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
	for l.getIsOpen() {
		time.Sleep(l.drainDuration)
		l.Drain()
	}
}

func (l *LogzioSender) shouldRetry(statusCode int) bool {
	retry := true
	switch statusCode {
	case http.StatusBadRequest:
		l.debugLog("sender: Got HTTP %d bad request, skip retry\n", statusCode)
		retry = false
	case http.StatusNotFound:
		l.debugLog("sender: Got HTTP %d not found, skip retry\n", statusCode)
		retry = false
	case http.StatusUnauthorized:
		l.debugLog("sender: Got HTTP %d unauthorized, skip retry\n", statusCode)
		retry = false
	case http.StatusForbidden:
		l.debugLog("sender: Got HTTP %d forbidden, skip retry\n", statusCode)
		retry = false
	case http.StatusOK:
		retry = false
	}
	return retry
}

// Drain - Send remaining logs
func (l *LogzioSender) Drain() {
	if l.draining.Load() {
		l.debugLog("sender: Already draining\n")
		return
	}
	l.mux.Lock()
	defer l.mux.Unlock()
	l.draining.Toggle()
	defer l.draining.Toggle()
	var reDrain = true
	for l.queue.Length() > 0 && reDrain {
		l.buf.Reset()
		l.dequeueUpToMaxBatchSize()
		if len(l.buf.Bytes()) > 0 {
			backOff := sendSleepingBackoff
			toBackOff := false
			for attempt := 0; attempt < sendRetries; attempt++ {
				if toBackOff {
					l.debugLog("sender: failed to send logs, trying again in %v\n", backOff)
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

func (l *LogzioSender) dequeueUpToMaxBatchSize() {
	var (
		err error
	)
	for l.buf.Len() < maxSize && err == nil {
		item, err := l.queue.Dequeue()
		if err != nil {
			l.debugLog("sender: queue state: %s\n", err)
		}
		if item != nil {
			// NewLine is appended tp item.Value
			if len(item.Value)+l.buf.Len()+1 >= maxSize {
				l.queue.Enqueue(item.Value)
				break
			}
			_, err := l.buf.Write(append(item.Value, '\n'))
			if err != nil {
				l.errorLog("sender: error writing to buffer %s", err)
			}
		} else {
			break
		}
	}
}

// Sync drains the queue
func (l *LogzioSender) Sync() error {
	l.Drain()
	return nil
}

func (l *LogzioSender) requeue() {
	l.debugLog("sender: Requeue %s", l.buf.String())
	err := l.Send(l.buf.Bytes())
	if err != nil {
		l.errorLog("sender: could not requeue logs %s", err)
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

// AwaitDrain waits for the sender to finish flushing all data up to a provided timeout
func (l *LogzioSender) AwaitDrain(timeout time.Duration) bool {
	l.Drain()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeoutChan := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			if !l.draining.Load() {
				return true // nothing to drain
			}
		case <-timeoutChan:
			l.errorLog("Timed out while waiting for draining to complete\n")
			return false
		}
	}
}
