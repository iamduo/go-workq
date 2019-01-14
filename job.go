package workq

import "time"

// FgJob is executed by the "run" command.
// Describes a foreground job specification.
type FgJob struct {
	ID       string
	Name     string
	TTR      int
	Timeout  int // Milliseconds to wait for job completion.
	Payload  []byte
	Priority int // Numeric priority
}

// BgJob is executed by the "add" command.
// Describes a background job specification.
type BgJob struct {
	ID          string
	Name        string
	TTR         int // Time-to-run
	TTL         int // Time-to-live
	Payload     []byte
	Priority    int // Numeric priority
	MaxAttempts int // Absoulute max num of attempts.
	MaxFails    int // Absolute max number of failures.
}

// ScheduledJob is executed by the "schedule" command.
// Describes a scheduled job specification.
type ScheduledJob struct {
	ID          string
	Name        string
	TTR         int
	TTL         int
	Payload     []byte
	Time        string
	Priority    int // Numeric priority
	MaxAttempts int // Absoulute max num of attempts.
	MaxFails    int // Absolute max number of failures.
}

// LeasedJob is returned by the "lease" command.
type LeasedJob struct {
	ID      string
	Name    string
	TTR     int
	Payload []byte
}

// JobResult is returned by the "run" & "result" commands.
type JobResult struct {
	Success bool
	Result  []byte
}

// InspectedJob is returned by the "inspect jobs" command.
type InspectedJob struct {
	BgJob
	Attempts int // Number of already made attempts.
	Fails int // Number of already occured fails.
	State int // Current state of the job.
	Created time.Time // Time of job creation
}