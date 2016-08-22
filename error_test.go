package workq

import (
	"testing"
)

func TestResponseError(t *testing.T) {
	err := NewResponseError("CODE", "TEXT")
	rerr := err.(*ResponseError)
	if err.Error() != "CODE TEXT" || rerr.Code() != "CODE" || rerr.Text() != "TEXT" {
		t.Fatalf("Error mismatch, err=%+v", rerr)
	}
}

func TestNetError(t *testing.T) {
	err := NewNetError("bad")
	_, ok := err.(*NetError)
	if err.Error() != "Net Error: bad" || !ok {
		t.Fatalf("Error mismatch, err=%s", err)
	}
}
