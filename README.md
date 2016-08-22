# go-workq [![Build Status](https://travis-ci.org/iamduo/go-workq.svg?branch=master)](https://travis-ci.org/iamduo/go-workq) [![Coverage Status](https://coveralls.io/repos/github/iamduo/go-workq/badge.svg?branch=master)](https://coveralls.io/github/iamduo/go-workq?branch=master) ![GitHub Logo](https://img.shields.io/badge/status-alpha-yellow.svg)


Go client for [Workq](https://github.com/iamduo/workq).

**Table of Contents**

- [Connecting](#connecting)
- [Closing active connection](#closing-active-connection)
- [Client Commands](#client-commands)
  - [Add](#add)
  - [Run](#run)
  - [Schedule](#schedule)
  - [Result](#result)
- [Worker Commands](#worker-commands)
  - [Lease](#lease)
  - [Complete](#complete)
  - [Fail](#fail)
- [Adminstrative Commands](#adminstrative-commands)
  - [Delete](#delete)
  - [Inspect](#inspect)

## Connection Management

### Connecting

```go
client, err := workq.Connect("localhost:9922")
if err != nil {
  // ...
}
```

### Closing active connection

```go
err := client.Close()
if err != nil {
  // ...
}
```

## Commands [![Protocol Doc](https://img.shields.io/badge/protocol-doc-516EA9.svg)](https://github.com/iamduo/workq/blob/master/doc/protocol.md#commands) [![GoDoc](https://godoc.org/github.com/iamduo/go-workq?status.svg)](https://godoc.org/github.com/iamduo/go-workq)

### Client Commands

#### Add

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#add) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Add)

Add a background job. The result can be retrieved through the ["result"](#result) command.

```go
job := &workq.BgJob{
	ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c4",
	Name: "ping",
	TTR: 5000,       // 5 second time-to-run limit
  TTL: 60000,      // Expire after 60 seconds
	Payload: []byte("Ping!"),
	Priority: 10,    // @OPTIONAL Numeric priority, default 0.
	MaxAttempts: 3,  // @OPTIONAL Absolute max num of attempts.
	MaxFails: 1,     // @OPTIONAL Absolute max number of failures.
}
err := client.Add(job)
if err != nil {
	// ...
}
```

#### Run

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#run) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Run)

Run a job and wait for its result.

```go
job := &workq.FgJob{
	ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c4",
	Name: "ping",
	TTR: 5000,          // 5 second time-to-run limit
	Timeout: 60000, // Wait up to 60 seconds for a worker to pick up.
	Payload: []byte("Ping!"),
	Priority: 10,       // @OPTIONAL Numeric priority, default 0.
}
result, err := client.Run(job)
if err != nil {
  // ...
}

fmt.Printf("Success: %t, Result: %s", result.Success, result.Result)
```

#### Schedule

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#schedule) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Schedule)

Schedule a job at a UTC time. The result can be retrieved through the ["result"](#result) command.

```go
job := &workq.ScheduledJob{
	ID: "6ba7b810-9dad-11d1-80b4-00c04fd430c4",
	Name: "ping",
	Time:    "2016-12-01T00:00:00Z", // Start job at this UTC time.
	TTL: 60000,                      // Expire after 60 seconds
	TTR: 5000,                       // 5 second time-to-run limit
	Payload: []byte("Ping!"),
	Priority: 10,                    // @OPTIONAL Numeric priority, default 0.
  MaxAttempts: 3,                  // @OPTIONAL Absolute max num of attempts.
  MaxFails: 1,                     // @OPTIONAL Absolute max number of failures.
}
err := client.Schedule(job)
if err != nil {
	// ...
}
```

#### Result

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#result) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Result)

Get a job result previously executed by [Run](#run) or [Schedule](#schedule) commands.

```go
// Get a job result, waiting up to 60 seconds if the job is still executing.
result, err := client.Result("6ba7b810-9dad-11d1-80b4-00c04fd430c4", 60000)
if err != nil {
	// ...
}

fmt.Printf("Success: %t, Result: %s", result.Success, result.Result)
```

### Worker Commands

#### Lease

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#lease) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Lease)

Lease a job within a set of one or more names with a wait-timeout (milliseconds).

```go
// Lease the first available job in "ping1", "ping2", "ping3"
// waiting up to 60 seconds.
job, err := client.Lease([]string{"ping1", "ping2", "ping3"}, 60000)
if err != nil {
	// ...
}

fmt.Printf("Leased Job: ID: %s, Name: %s, Payload: %s", job.ID, job.Name, job.Payload)
```

#### Complete

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#complete) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Complete)

Mark a job successfully completed with a result.

```go
err := client.Complete("6ba7b810-9dad-11d1-80b4-00c04fd430c4", []byte("Pong!"))
if err != nil {
	// ...
}
```

#### Fail

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#fail) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Fail)

Mark a job failed with a result.

```go
err := client.Fail("6ba7b810-9dad-11d1-80b4-00c04fd430c4", []byte("Failed-Pong!"))
if err != nil {
	// ...
}
```

### Adminstrative Commands

#### Delete

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#delete) | [Go Doc](https://godoc.org/github.com/iamduo/go-workq#Client.Delete)


```go
err := client.Delete("6ba7b810-9dad-11d1-80b4-00c04fd430c4")
if err != nil {
	// ...
}
```


#### Inspect

[Protocol Doc](https://github.com/iamduo/workq/blob/master/doc/protocol.md#inspect)

Inspect commands not yet supported yet.
