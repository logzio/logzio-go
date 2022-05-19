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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// Utils
const (
	defaultQueueSize = 40 * 1024 * 1024
)

func TestLogzioSender_SetUrl(t *testing.T) {
	l, err := New(
		"",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetInMemoryQueue(true),
		SetinMemoryCapacity(500),
		SetDrainDuration(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if l.url != "http://localhost:12345" {
		t.Fatalf("url should be http://localhost:12345, actual: %s", l.url)
	}
	l2, err := New(
		"token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetInMemoryQueue(true),
		SetinMemoryCapacity(500),
		SetDrainDuration(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if l2.url != "http://localhost:12345/?token=token" {
		t.Fatalf("url should be http://localhost:12345/?token=token, actual: %s", l.url)
	}
}

// In memory queue tests
func TestLogzioSender_inMemoryRetries(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	defer ts.Close()
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetDrainDuration(time.Minute*10),
		SetInMemoryQueue(true),
		SetinMemoryCapacity(defaultQueueSize),
	)
	if err != nil {
		t.Fatal(err)
	}
	l.Send([]byte("blah"))
	l.Drain()
	item, err := l.queue.Dequeue()
	// expected msg to be in queue after max retries
	if item == nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	item, err = l.queue.Dequeue()
	// expected queue to be empty - only one requeue executed
	if err == nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	l.Stop()
}

func TestLogzioSender_InMemoryCapacityLimit(t *testing.T) {
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetInMemoryQueue(true),
		SetinMemoryCapacity(500),
		SetDrainDuration(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	l.Send(make([]byte, 1000))
	item, err := l.queue.Dequeue()
	if item != nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}

	l.Send(make([]byte, 200))
	l.Send(make([]byte, 400))
	item, err = l.queue.Dequeue()
	item, err = l.queue.Dequeue()
	if item != nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	l.Stop()

}

func TestLogzioSender_InMemorySend(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	defer ts.Close()
	l, err := New("fake-token",
		SetUrl(ts.URL),
		SetinMemoryCapacity(defaultQueueSize),
		SetInMemoryQueue(true),
		SetDrainDuration(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		l.Send([]byte("blah"))
	}
	if l.queue.Length() != 4*100 {
		t.Fatalf("Expected size: %d\n Actual size: %d\n", 4*100, l.queue.Length())
	}
	l.Drain()
	time.Sleep(200 * time.Millisecond)
	item, err := l.queue.Dequeue()
	if item != nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	l.Stop()
}

func TestLogzioSender_InMemoryDrain(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	defer ts.Close()
	l, err := New("fake-token",
		SetUrl(ts.URL),
		SetinMemoryCapacity(defaultQueueSize),
		SetInMemoryQueue(true),
		SetDebug(os.Stderr),
		SetDrainDuration(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	// 4000000 bytes = ~ 4mb, tests two batches in one drain
	for i := 0; i < 1000; i++ {
		l.Send(make([]byte, 4000))
	}
	l.Drain()
	time.Sleep(time.Second * 10)
	item, err := l.queue.Dequeue()
	if item != nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	l.Stop()
}

func TestLogzioSender_ShouldRetry(t *testing.T) {
	//var sent = make([]byte, 1024)
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetDrainDuration(time.Minute*10),
		SetInMemoryQueue(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Stop()
	retry := l.shouldRetry(200)
	if retry != false {
		t.Fatalf("Should be false")
	}
	retry = l.shouldRetry(404)
	if retry != false {
		t.Fatalf("Should be false")
	}
	retry = l.shouldRetry(401)
	if retry != false {
		t.Fatalf("Should be false")
	}
	retry = l.shouldRetry(403)
	if retry != false {
		t.Fatalf("Should be false")
	}
	retry = l.shouldRetry(400)
	if retry != false {
		t.Fatalf("Should be false")
	}
	retry = l.shouldRetry(500)
	if retry != true {
		t.Fatalf("Should be true")
	}
}

func TestLogzioSender_InMemoryDelayStart(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	defer ts.Close()
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetInMemoryQueue(true),
		SetCompress(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	l.Send([]byte("blah"))
	time.Sleep(200 * time.Millisecond)
	l.Drain()
	ts.Start()
	SetUrl(ts.URL)(l)
	l.Drain()
	time.Sleep(500 * time.Millisecond)
	sentMsg := string(sent[0:5])
	if len(sentMsg) != 5 {
		t.Fatalf("Wrong len of msg %d", len(sentMsg))
	}
	if sentMsg != "blah\n" {
		t.Fatalf("%s != %s ", sent, sentMsg)
	}
	l.Stop()
}

func TestLogzioSender_InMemoryUnauth(t *testing.T) {
	var sent = make([]byte, 1024)
	cnt := 0
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cnt++
		if cnt == 2 {
			w.WriteHeader(http.StatusAccepted)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}
		r.Body.Read(sent)
	}))
	ts.Start()
	defer ts.Close()
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetCompress(false),
		SetDrainDuration(time.Minute),
		SetUrl(ts.URL),
		SetInMemoryQueue(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	l.Write([]byte("blah"))
	time.Sleep(200 * time.Millisecond)
	l.Sync()
	time.Sleep(100 * time.Millisecond)
	l.Drain()
	time.Sleep(100 * time.Millisecond)
	sentMsg := string(sent[0:5])
	if len(sentMsg) != 5 {
		t.Fatalf("Wrong len of msg %d", len(sentMsg))
	}

	if sentMsg != "blah\n" {
		t.Fatalf("%s != %s ", string(sent), string(sentMsg))
	}
	l.Stop()
}

func TestLogzioSender_InMemoryWrite(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	ts.Start()
	defer ts.Close()
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetDrainDuration(time.Minute),
		SetUrl(ts.URL),
		SetCompress(false),
		SetInMemoryQueue(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	l.Write([]byte("blah"))
	time.Sleep(200 * time.Millisecond)
	l.Sync()
	sentMsg := string(sent[0:5])
	if len(sentMsg) != 5 {
		t.Fatalf("Wrong len of msg %d", len(sentMsg))
	}
	if sentMsg != "blah\n" {
		t.Fatalf("%s != %s ", string(sent), string(sentMsg))
	}
	l.Stop()
}

//dequeueUpToMaxBatchSize
func TestLogzioSender_DequeueUpToMaxBatchSize(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	ts.Start()
	defer ts.Close()
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetDrainDuration(time.Hour),
		SetUrl(ts.URL),
		SetInMemoryQueue(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		l.Send(make([]byte, 33000))
	}
	l.dequeueUpToMaxBatchSize()
	item, err := l.queue.Dequeue()
	if item == nil {
		t.Fatalf("Queue not suposed to bee empty")
	}
	if uint64(len(l.buf.Bytes())) > 3*1024*1024 {
		t.Fatalf("%d > %d", len(l.buf.Bytes()), 3*1024*1024)
	}

	l.Stop()
}

// Disk memory tests
func TestLogzioSender_Retries(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	defer ts.Close()
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetDrainDuration(time.Minute*10),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)
	defer l.Stop()
	l.Send([]byte("blah"))
	l.Drain()
	item, err := l.queue.Dequeue()
	// expected msg to be in queue after max retries
	if item == nil || item.ID != 2 {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	item, err = l.queue.Dequeue()
	// expected queue to be empty - only one requeue executed
	if err == nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
}

func TestLogzioSender_Send(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	defer ts.Close()

	l, err := New("fake-token",
		SetUrl(ts.URL),
		SetCompress(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)

	l.Send([]byte("blah"))
	l.Drain()
	time.Sleep(200 * time.Millisecond)
	sentMsg := string(sent[0:5])
	if sentMsg != "blah\n" {
		t.Fatalf("%s != %s ", sent, sentMsg)
	}

}

func TestLogzioSender_DelayStart(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	defer ts.Close()
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetCompress(false),
		SetUrl("http://localhost:12345"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)

	l.Send([]byte("blah"))
	time.Sleep(200 * time.Millisecond)
	l.Drain()
	ts.Start()
	SetUrl(ts.URL)(l)
	l.Drain()
	time.Sleep(500 * time.Millisecond)
	sentMsg := string(sent[0:5])
	if len(sentMsg) != 5 {
		t.Fatalf("Wrong len of msg %d", len(sentMsg))
	}
	if sentMsg != "blah\n" {
		t.Fatalf("%s != %s ", sent, sentMsg)
	}
}

func TestLogzioSender_TmpDir(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	ts.Start()
	defer ts.Close()
	tmp := fmt.Sprintf("%s/%d", os.TempDir(), time.Now().Nanosecond())
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetTempDirectory(tmp),
		SetCompress(false),
		SetDrainDuration(time.Minute),
		SetUrl(ts.URL),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)

	l.Send([]byte("blah"))
	time.Sleep(200 * time.Millisecond)
	l.Drain()
	sentMsg := string(sent[0:5])
	if len(sentMsg) != 5 {
		t.Fatalf("Wrong len of msg %d", len(sentMsg))
	}
	if sentMsg != "blah\n" {
		t.Fatalf("%s != %s ", string(sent), string(sentMsg))
	}
}

func TestLogzioSender_Write(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	ts.Start()
	defer ts.Close()
	tmp := fmt.Sprintf("%s/%d", os.TempDir(), time.Now().Nanosecond())
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetTempDirectory(tmp),
		SetCompress(false),
		SetDrainDuration(time.Minute),
		SetUrl(ts.URL),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)

	l.Write([]byte("blah"))
	time.Sleep(200 * time.Millisecond)
	l.Sync()
	sentMsg := string(sent[0:5])
	if len(sentMsg) != 5 {
		t.Fatalf("Wrong len of msg %d", len(sentMsg))
	}
	if sentMsg != "blah\n" {
		t.Fatalf("%s != %s ", string(sent), string(sentMsg))
	}
}

func TestLogzioSender_RestoreQueue(t *testing.T) {
	var sent = make([]byte, 1024)
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		r.Body.Read(sent)
	}))
	defer ts.Close()
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetDrainDuration(time.Minute*10),
		SetTempDirectory("./data"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)

	l.Send([]byte("blah"))
	l.Stop()

	// open queue again - same dir
	l, err = New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetDrainDuration(time.Minute*10),
		SetTempDirectory("./data"),
	)
	if err != nil {
		t.Fatal(err)
	}

	item, err := l.queue.Dequeue()
	if string(item.Value) != "blah\n" {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}

}

func TestLogzioSender_Unauth(t *testing.T) {
	var sent = make([]byte, 1024)
	cnt := 0
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cnt++
		if cnt == 2 {
			w.WriteHeader(http.StatusAccepted)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}
		r.Body.Read(sent)
	}))
	ts.Start()
	defer ts.Close()
	tmp := fmt.Sprintf("%s/%d", os.TempDir(), time.Now().Nanosecond())
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetTempDirectory(tmp),
		SetCompress(false),
		SetDrainDuration(time.Minute),
		SetUrl(ts.URL),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)

	l.Write([]byte("blah"))
	time.Sleep(200 * time.Millisecond)
	l.Sync()
	time.Sleep(100 * time.Millisecond)
	l.Drain()
	time.Sleep(100 * time.Millisecond)
	sentMsg := string(sent[0:5])
	if len(sentMsg) != 5 {
		t.Fatalf("Wrong len of msg %d", len(sentMsg))
	}
	if sentMsg != "blah\n" {
		t.Fatalf("%s != %s ", string(sent), string(sentMsg))
	}
}

func TestLogzioSender_CountDropped(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetDrainDiskThreshold(0),
		SetDrainDuration(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)
	l.Send([]byte("blah"))
	l.Send([]byte("blah"))
	l.Send([]byte("blah"))
	if l.droppedLogs != 3 {
		t.Fatalf("items should have been dropped")
	}
	l.diskThreshold = 98
	l.Send([]byte("blah"))
	l.Send([]byte("blah"))
	l.Drain()
	l.url = ts.URL
	l.Drain()
	if l.droppedLogs != 0 {
		t.Fatalf("should be 0 after export")
	}
	item, err := l.queue.Dequeue()
	if item != nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	l.Stop()
}

func TestLogzioSender_ThresholdLimit(t *testing.T) {
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetDrainDiskThreshold(0),
		SetDrainDuration(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)
	l.Send([]byte("blah"))
	item, err := l.queue.Dequeue()
	if item != nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	l.Stop()
}

func TestLogzioSender_ThresholdLimitWithoutCheck(t *testing.T) {
	l, err := New(
		"fake-token",
		SetDebug(os.Stderr),
		SetUrl("http://localhost:12345"),
		SetDrainDiskThreshold(0),
		SetCheckDiskSpace(false),
		SetDrainDuration(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(l.dir)

	l.Send([]byte("blah"))
	item, err := l.queue.Dequeue()
	if item == nil {
		t.Fatalf("Unexpect item in the queue - %s", string(item.Value))
	}
	l.Stop()

}

func BenchmarkLogzioSender(b *testing.B) {
	b.ReportAllocs()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	l, _ := New("fake-token", SetUrl(ts.URL),
		SetDrainDuration(time.Hour))
	defer ts.Close()
	defer l.Stop()
	msg := []byte("test")
	for i := 0; i < b.N; i++ {
		l.Send(msg)
	}
}

func BenchmarkLogzioSenderInmemory(b *testing.B) {
	b.ReportAllocs()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	l, _ := New("fake-token", SetUrl(ts.URL),
		SetDrainDuration(time.Hour),
		SetInMemoryQueue(true),
		SetlogCountLimit(6000000),
	)

	defer ts.Close()
	defer l.Stop()
	msg := []byte("test")
	for i := 0; i < b.N; i++ {
		l.Send(msg)
	}
}

//E2E test
func TestLogzioSender_E2E(t *testing.T) {
	l, err := New("fake",
		SetInMemoryQueue(true),
		SetUrl("https://listener.logz.io:8071"),
		SetDrainDuration(time.Second*5),
		SetinMemoryCapacity(300*1024*1024),
		SetlogCountLimit(2000000),
		SetDebug(os.Stderr),
	)
	if err != nil {
		t.Fatal(err)
	}
	msg := `{"traceID":"0000000000000001","operationName":"o3","spanID":"2a3ad4a54c048830","references":[],"startTime":1632401226891238,"startTimeMillis":1632401226891,"duration":0,"logs":[],"process":{"serviceName":"testService","tags":[]},"type":"jaegerSpan"}`
	for i := 0; i < 10000; i++ {
		err = l.Send([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
	}
	l.Stop() //logs are buffered on disk. Stop will drain the buffer
}
