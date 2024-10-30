# Logzio Golang API client

Sends logs to [logz.io](https://logz.io) over HTTP. It is a low level lib that can to be integrated with other logging libs.

[comment]: <> ([![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Go Report][report-img]][report])

## Prerequisites
go 1.x

## Installation
```shell
$ go get -u github.com/logzio/logzio-go
```
Logzio golang api client offers two queue implementations that you can use:
## Disk queue
Logzio go client uses [goleveldb](https://github.com/syndtr/goleveldb) and [goqueue](github.com/beeker1121/goque) as a persistent storage.
Every 5 seconds logs are sent to logz.io (if any are available)

## In memory queue
You can see the logzio go client queue implementation in `inMemoryQueue.go` file. The in memory queue is initialized with 500k log count limit and 20mb capacity by default.
You can use the `SetinMemoryCapacity()` and `SetlogCountLimit()` functions to override default settings.


## Quick Start

### Disk queue
```go
package main

import (
  "fmt"
  "github.com/logzio/logzio-go"
  "os"
  "time"
)

func main() {
  l, err := logzio.New(
  		"fake-token",
	  logzio.SetDebug(os.Stderr),
	  logzio.SetUrl("http://localhost:12345"),
	  logzio.SetDrainDuration(time.Minute*10),
	  logzio.SetTempDirectory("myQueue"),
	  logzio.SetDrainDiskThreshold(99),
  	) // token is required
  if err != nil {
    panic(err)
  }
  msg := fmt.Sprintf("{ \"%s\": \"%s\"}", "message", time.Now().UnixNano())

  err = l.Send([]byte(msg))
  if err != nil {
     panic(err)
  }

  l.Stop() //logs are buffered on disk. Stop will drain the buffer
}
```

### In memory queue
```go
package main

import (
  "fmt"
  "github.com/logzio/logzio-go"
  "os"
  "time"
)

func main() {
  l, err := logzio.New(
  		"fake-token",
	  logzio.SetDebug(os.Stderr),
	  logzio.SetUrl("http://localhost:12345"),
	  logzio.SetInMemoryQueue(true),
	  logzio.SetinMemoryCapacity(24000000),
	  logzio.SetlogCountLimit(6000000),
  	) // token is required
  if err != nil {
    panic(err)
  }
  msg := fmt.Sprintf("{ \"%s\": \"%s\"}", "message", time.Now().UnixNano())

  err = l.Send([]byte(msg))
  if err != nil {
     panic(err)
  }

  l.Stop() 
}
```

## Usage

- Set url mode:
    `logzio.New(token, SetUrl(ts.URL))`

- Set drain duration (flush logs on disk):
    `logzio.New(token, SetDrainDuration(time.Hour))`

- Set debug mode:
    `logzio.New(token, SetDebug(os.Stderr))`

- Set queue dir:
    `logzio.New(token, SetSetTempDirectory(os.Stderr))`

- Set the sender to check if it crosses the maximum allowed disk usage:
    `logzio.New(token, SetCheckDiskSpace(true))`

- Set disk queue threshold, once the threshold is crossed the sender will not enqueue the received logs:
    `logzio.New(token, SetDrainDiskThreshold(99))`

- Set the sender to Use in memory queue:
  `logzio.New(token, SetInMemoryQueue(true))`

- Set the sender to Use in memory queue with log count limit and capacity:
  `logzio.New(token,
  SetInMemoryQueue(true),
  SetinMemoryCapacity(500),
  SetlogCountLimit(6000000),
  )`
  
## Data compression
All bulks are compressed with gzip by default to disable compressing initialize the client with `SetCompress(false)`:
```go
  logzio.New(token,
	  SetCompress(false),
  )
```

## Tests

```shell
$ go test -v
```



## Contributing
 All PRs are welcome

## Authors

* **Douglas Chimento**  - [dougEfresh](https://github.com/dougEfresh)
* **Ido Halevi**  - [idohalevi](https://github.com/idohalevi)

## Maintainers
* **Yotam Loewenbach**  - [yotamloe](https://github.com/yotamloe)


## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* [logzio-java-sender](https://github.com/logzio/logzio-java-sender)


## Changelog 
- v1.0.9
    - Update dependencies:
      - `github.com/shirou/gopsutil/v3`: `v3.24.2` -> `v3.24.5`
    - Handle http.NewRequest error
- v1.0.8
    - Add `AwaitDrain` function to support ensuring all logs are drained in synchronous runtimes
- v1.0.7
    - Added set http client option (@orelazar1)
    - Update dependencies:
      - `github.com/shirou/gopsutil/v3`: `v3.22.3` -> `v3.24.2`
      - `go.uber.org/atomic`: `v1.9.0` -> `v1.11.0`
    - Update go version `1.15` -> `1.20`
- v1.0.6
    - Change logging
    - Remove draining queue log
- v1.0.5
    - Change `token` query parameter to optional for generic use
    - Changed logging levels
- v1.0.4
    - Update gopsutil version (v3.21.6 -> v3.22.3)
- v1.0.3
    - Adjust buffer clearance
    - Changed logging format
- v1.0.2
    - Update dependencies
- v1.0.1
    - Add gzip compression
    - Add option for in Memory queue

