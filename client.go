// Package workq implements Workq protocol commands:
// https://github.com/iamduo/workq/blob/master/doc/protocol.md#commands
package workq

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/satori/go.uuid"
	"time"
)

var (
	// ErrMalformed is returned when responses from workq can not be parsed
	// due to unrecognized responses.
	ErrMalformed             = errors.New("Malformed response")
	ErrPayloadMustFollowSize = errors.New("Payload must immediately follow payload size when inspecting jobs")
)

const (
	// Max Data Block that can be read within a response, 1 MiB.
	maxDataBlock = 1048576

	// Line terminator in string form.
	crnl       = "\r\n"
	termLen    = 2
	// prefix of payload line when inspecting jobs
	payloadKey = "payload "

	// Time format for any date times. Compatible with time.Format.
	TimeFormat = "2006-01-02T15:04:05Z"
)

// Client represents a single connection to Workq.
type Client struct {
	conn   net.Conn
	rdr    *bufio.Reader
	parser *responseParser
}

// Connect to a Workq server returning a Client
func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return NewClient(conn), nil
}

// NewClient returns a Client from a net.Conn.
func NewClient(conn net.Conn) *Client {
	rdr := bufio.NewReader(conn)
	return &Client{
		conn:   conn,
		rdr:    rdr,
		parser: &responseParser{rdr: rdr},
	}
}

// "add" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#add
//
// Add background job
// Returns ResponseError for Workq response errors.
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
func (c *Client) Add(j *BgJob) error {
	var flagsPad string
	var flags []string
	if j.Priority != 0 {
		flags = append(flags, fmt.Sprintf("-priority=%d", j.Priority))
	}
	if j.MaxAttempts != 0 {
		flags = append(flags, fmt.Sprintf("-max-attempts=%d", j.MaxAttempts))
	}
	if j.MaxFails != 0 {
		flags = append(flags, fmt.Sprintf("-max-fails=%d", j.MaxFails))
	}
	if len(flags) > 0 {
		flagsPad = " "
	}
	r := []byte(fmt.Sprintf(
		"add %s %s %d %d %d%s"+crnl+"%s"+crnl,
		j.ID,
		j.Name,
		j.TTR,
		j.TTL,
		len(j.Payload),
		flagsPad+strings.Join(flags, " "),
		j.Payload,
	))
	_, err := c.conn.Write(r)
	if err != nil {
		return NewNetError(err.Error())
	}

	return c.parser.parseOk()
}

// "run" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#run
//
// Submit foreground job and wait for result.
// Returns ResponseError for Workq response errors
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
func (c *Client) Run(j *FgJob) (*JobResult, error) {
	var flags string
	if j.Priority != 0 {
		flags = fmt.Sprintf(" -priority=%d", j.Priority)
	}
	r := []byte(fmt.Sprintf(
		"run %s %s %d %d %d%s"+crnl+"%s"+crnl,
		j.ID,
		j.Name,
		j.TTR,
		j.Timeout,
		len(j.Payload),
		flags,
		j.Payload,
	))

	_, err := c.conn.Write(r)
	if err != nil {
		return nil, NewNetError(err.Error())
	}

	count, err := c.parser.parseOkWithReply()
	if err != nil {
		return nil, err
	}

	if count != 1 {
		return nil, ErrMalformed
	}

	return c.parser.readResult()
}

// "schedule" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#schedule
//
// Schedule job at future UTC time.
// Returns ResponseError for Workq response errors.
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
func (c *Client) Schedule(j *ScheduledJob) error {
	var flagsPad string
	var flags []string
	if j.Priority != 0 {
		flags = append(flags, fmt.Sprintf("-priority=%d", j.Priority))
	}
	if j.MaxAttempts != 0 {
		flags = append(flags, fmt.Sprintf("-max-attempts=%d", j.MaxAttempts))
	}
	if j.MaxFails != 0 {
		flags = append(flags, fmt.Sprintf("-max-fails=%d", j.MaxFails))
	}
	if len(flags) > 0 {
		flagsPad = " "
	}
	r := []byte(fmt.Sprintf(
		"schedule %s %s %d %d %s %d%s"+crnl+"%s"+crnl,
		j.ID,
		j.Name,
		j.TTR,
		j.TTL,
		j.Time,
		len(j.Payload),
		flagsPad+strings.Join(flags, " "),
		j.Payload,
	))
	_, err := c.conn.Write(r)
	if err != nil {
		return NewNetError(err.Error())
	}

	return c.parser.parseOk()
}

// "result" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#result
//
// Fetch job result, @see PROTOCOL_DOC
// Returns ResponseError for Workq response errors.
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
func (c *Client) Result(id string, timeout int) (*JobResult, error) {
	r := []byte(fmt.Sprintf(
		"result %s %d"+crnl,
		id,
		timeout,
	))
	_, err := c.conn.Write(r)
	if err != nil {
		return nil, NewNetError(err.Error())
	}

	count, err := c.parser.parseOkWithReply()
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, ErrMalformed
	}

	return c.parser.readResult()
}

// "lease" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#lease
//
// Lease a job, waiting for available jobs until timeout, @see PROTOCOL_DOC
// Returns ResponseError for Workq response errors.
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
func (c *Client) Lease(names []string, timeout int) (*LeasedJob, error) {
	r := []byte(fmt.Sprintf(
		"lease %s %d"+crnl,
		strings.Join(names, " "),
		timeout,
	))

	_, err := c.conn.Write(r)
	if err != nil {
		return nil, NewNetError(err.Error())
	}

	count, err := c.parser.parseOkWithReply()
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, ErrMalformed
	}

	return c.parser.readLeasedJob()
}

// "complete" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#complete
//
// Mark job successfully complete, @see PROTOCOL_DOC
// Returns ResponseError for Workq response errors.
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
func (c *Client) Complete(id string, result []byte) error {
	r := []byte(fmt.Sprintf(
		"complete %s %d"+crnl+"%s"+crnl,
		id,
		len(result),
		result,
	))
	_, err := c.conn.Write(r)
	if err != nil {
		return NewNetError(err.Error())
	}

	return c.parser.parseOk()
}

// "fail" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#fail
//
// Mark job as failure.
// Returns ResponseError for Workq response errors.
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
func (c *Client) Fail(id string, result []byte) error {
	r := []byte(fmt.Sprintf(
		"fail %s %d"+crnl+"%s"+crnl,
		id,
		len(result),
		result,
	))
	_, err := c.conn.Write(r)
	if err != nil {
		return NewNetError(err.Error())
	}

	return c.parser.parseOk()
}

// "delete" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#delete
//
// Delete job.
// Returns ResponseError for Workq response errors.
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
func (c *Client) Delete(id string) error {
	r := []byte(fmt.Sprintf(
		"delete %s"+crnl,
		id,
	))
	_, err := c.conn.Write(r)
	if err != nil {
		return NewNetError(err.Error())
	}

	return c.parser.parseOk()
}

// "inspect jobs" command: https://github.com/iamduo/workq/blob/master/doc/protocol.md#inspect-foreground-or-background-jobs-by-name
//
// Inspect foreground or background jobs by name, @see PROTOCOL_DOC
// Returns ResponseError for Workq response errors.
// Returns NetError on any network errors.
// Returns ErrMalformed if response can't be parsed.
// Returns ErrPayloadMustFollowSize if payload is not directly preceded by payload size in key value list.
func (c *Client) InspectJobs(name string, cursorOffset int, limit int) ([]*InspectedJob, error) {
	r := []byte(fmt.Sprintf(
		"inspect jobs %s %d %d"+crnl,
		name,
		cursorOffset,
		limit,
	))
	_, err := c.conn.Write(r)
	if err != nil {
		return nil, NewNetError(err.Error())
	}

	count, err := c.parser.parseOkWithReply()
	if err != nil {
		return nil, err
	}

	return c.parser.readInspectedJobs(count)
}

type responseParser struct {
	rdr *bufio.Reader
}

// Close client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Parse "OK\r\n" response.
func (p *responseParser) parseOk() error {
	line, err := p.readLine()
	if err != nil {
		return err
	}

	if len(line) < 3 {
		return ErrMalformed
	}

	sign := string(line[0])
	if sign == "+" && string(line[1:3]) == "OK" && len(line) == 3 {
		return nil
	}

	if sign != "-" {
		return ErrMalformed
	}

	err, _ = p.errorFromLine(line)
	return err
}

// Parse "OK <reply-count>\r\n" response.
func (p *responseParser) parseOkWithReply() (int, error) {
	line, err := p.readLine()
	if err != nil {
		return 0, err
	}

	if len(line) < 5 {
		return 0, ErrMalformed
	}

	sign := string(line[0])
	if sign == "+" && string(line[1:3]) == "OK" {
		count, err := strconv.Atoi(string(line[4:]))
		if err != nil {
			return 0, ErrMalformed
		}

		return count, nil
	}

	if sign != "-" {
		return 0, ErrMalformed
	}

	err, _ = p.errorFromLine(line)
	return 0, err
}

// Read valid line terminated by "\r\n"
func (p *responseParser) readLine() ([]byte, error) {
	line, err := p.rdr.ReadBytes(byte('\n'))
	if err != nil {
		return nil, NewNetError(err.Error())
	}

	if len(line) < termLen {
		return nil, ErrMalformed
	}

	if len(line) >= termLen {
		if line[len(line)-termLen] != '\r' {
			return nil, ErrMalformed
		}

		line = line[:len(line)-termLen]
	}

	return line, nil
}

// Read data block up to size terminated by "\r\n"
func (p *responseParser) readBlock(size int) ([]byte, error) {
	if size < 0 || size > maxDataBlock {
		return nil, ErrMalformed
	}

	block := make([]byte, size)
	n, err := io.ReadAtLeast(p.rdr, block, size)
	if n != size || err != nil {
		return nil, ErrMalformed
	}

	b := make([]byte, termLen)
	n, err = p.rdr.Read(b)
	if err != nil || n != termLen || string(b) != crnl {
		// Size does not match end of line.
		// Trailing garbage is not allowed.
		return nil, ErrMalformed
	}

	return block, nil
}

// Read job result consisting of 2 separate terminated lines.
// "<id> <success> <result-length>\r\n
// <result-block>\r\n"
func (p *responseParser) readResult() (*JobResult, error) {
	line, err := p.readLine()
	split := strings.Split(string(line), " ")
	if len(split) != 3 {
		return nil, ErrMalformed
	}

	if split[1] != "0" && split[1] != "1" {
		return nil, ErrMalformed
	}

	result := &JobResult{}
	if split[1] == "1" {
		result.Success = true
	}

	resultLen, err := strconv.ParseUint(split[2], 10, 64)
	if err != nil {
		return nil, ErrMalformed
	}

	result.Result, err = p.readBlock(int(resultLen))
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Read leased job consisting of 2 separate terminated lines.
// "<id> <name> <payload-length>\r\n
// <payload-block>\r\n"
func (p *responseParser) readLeasedJob() (*LeasedJob, error) {
	line, err := p.readLine()
	split := strings.Split(string(line), " ")
	if len(split) != 4 {
		return nil, ErrMalformed
	}

	j := &LeasedJob{}
	j.ID, err = idFromString(split[0])
	if err != nil {
		return nil, err
	}

	j.Name, err = nameFromString(split[1])
	if err != nil {
		return nil, err
	}

	ttr, err := strconv.ParseInt(split[2], 10, 64)
	if err != nil {
		return nil, ErrMalformed
	}

	j.TTR = int(ttr)

	payloadLen, err := strconv.ParseUint(split[3], 10, 64)
	if err != nil {
		return nil, ErrMalformed
	}

	j.Payload, err = p.readBlock(int(payloadLen))
	if err != nil {
		return nil, err
	}

	return j, nil
}

// Read inspected jobs.
// <id> <key-count>\r\n
// <key> <value>\r\n
// ... Repeats up to <key-count>
// ... Repeats up to <reply-count>
func (p *responseParser) readInspectedJobs(replyCount int) ([]*InspectedJob, error) {
	var jobs []*InspectedJob
	for i := 0; i < replyCount; i++ {
		job, err := p.parseInspectedJob()
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if len(jobs) != replyCount {
		return nil, ErrMalformed
	}

	// Check for unexpected trailing bytes
	block := make([]byte, 1)
	_, err := io.ReadAtLeast(p.rdr, block, 1)
	if err == nil {
		return nil, ErrMalformed
	}

	return jobs, nil
}

// Parse a single job from an inspected job response.
// <id> <key-count>\r\n
// <key> <value>\r\n
// ... Repeats up to <key-count>
func (p *responseParser) parseInspectedJob() (*InspectedJob, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, ErrMalformed
	}
	split := strings.Split(string(line), " ")
	if len(split) != 2 {
		return nil, ErrMalformed
	}

	j := &InspectedJob{}

	j.ID, err = idFromString(split[0])
	if err != nil {
		return nil, err
	}

	keyCount, err := strconv.Atoi(split[1])
	if err != nil {
		return nil, ErrMalformed
	}

	for k := 0; k < keyCount; k++ {
		line, err := p.readLine()
		if err != nil {
			return nil, ErrMalformed
		}

		split := strings.Split(string(line), " ")
		if len(split) != 2 {
			return nil, ErrMalformed
		}

		switch split[0] {
		case "name":
			j.Name = split[1]
		case "ttr":
			ttr, err := strconv.ParseUint(split[1], 10, 32)
			if err != nil {
				return nil, ErrMalformed
			}
			j.TTR = int(ttr)
		case "ttl":
			ttl, err := strconv.ParseUint(split[1], 10, 64)
			if err != nil {
				return nil, ErrMalformed
			}
			j.TTL = int(ttl)
		case "payload":
			// The payload line is always read by the payload-size section.
			// Encountering it here means that the order of keys is incorrect.
			return nil, ErrPayloadMustFollowSize
		case "payload-size":
			payloadSize, err := strconv.ParseUint(split[1], 10, 64)
			if err != nil {
				return nil, err
			}
			// Payload line has to immediately follow payload-size line because the entire
			// payload must be read as bytes regardless of the newlines it may contain.
			b := make([]byte, len(payloadKey))
			n, err := p.rdr.Read(b)
			if err != nil || n != len(payloadKey) || string(b) != payloadKey {
				return nil, ErrPayloadMustFollowSize
			}
			j.Payload, err = p.readBlock(int(payloadSize))
			if err != nil {
				return nil, err
			}
			k++ // because payload line has been processed outside of loop
		case "max-attempts":
			maxAttempts, err := strconv.ParseUint(split[1], 10, 8)
			if err != nil {
				return nil, ErrMalformed
			}
			j.MaxAttempts = int(maxAttempts)
		case "attempts":
			attempts, err := strconv.ParseUint(split[1], 10, 8)
			if err != nil {
				return nil, ErrMalformed
			}
			j.Attempts = int(attempts)
		case "max-fails":
			maxFails, err := strconv.ParseUint(split[1], 10, 8)
			if err != nil {
				return nil, ErrMalformed
			}
			j.MaxFails = int(maxFails)
		case "fails":
			fails, err := strconv.ParseUint(split[1], 10, 8)
			if err != nil {
				return nil, ErrMalformed
			}
			j.Fails = int(fails)
		case "priority":
			priority, err := strconv.ParseInt(split[1], 10, 32)
			if err != nil {
				return nil, ErrMalformed
			}
			j.Priority = int(priority)
		case "state":
			state, err := strconv.ParseUint(split[1], 10, 8)
			if err != nil {
				return nil, ErrMalformed
			}
			j.State = int(state)
		case "created":
			var created time.Time
			created, err = time.Parse(time.RFC3339, split[1])
			if err != nil {
				return nil, ErrMalformed
			}
			j.Created = created
		default:
			return nil, ErrMalformed
		}
	}
	return j, nil
}

// Parse an error from "-CODE TEXT"
func (p *responseParser) errorFromLine(line []byte) (error, bool) {
	split := strings.SplitN(string(line), " ", 2)
	if len(split[0]) <= 1 {
		return ErrMalformed, false
	}

	code := split[0][1:]
	var text string
	if len(split) == 2 {
		if len(split[1]) == 0 {
			return ErrMalformed, false
		}

		text = split[1]
	}

	return NewResponseError(code, text), true
}

// Return a valid ID string
// Returns ErrMalformed if not a valid UUID.
func idFromString(s string) (string, error) {
	_, err := uuid.FromString(s)
	if err != nil {
		return "", ErrMalformed
	}

	return s, nil
}

var nameRe = regexp.MustCompile("^[a-zA-Z0-9_.-]*$")

// Return a valid name string
// Returns ErrMalformed if name is not alphanumeric + special chars: "_", ".", "-"
func nameFromString(name string) (string, error) {
	l := len(name)
	if l > 0 && l <= 128 && nameRe.MatchString(name) {
		return name, nil
	}

	return "", ErrMalformed
}
