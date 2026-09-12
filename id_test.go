package trail

import (
	"strings"
	"sync"
	"testing"
)

func TestIDRoundTrip(t *testing.T) {
	flow := NewFlow()
	if flow.IsZero() {
		t.Fatal("NewFlow returned zero")
	}
	text := flow.String()
	if len(text) != 26 || text != strings.ToLower(text) {
		t.Fatalf("unexpected text encoding %q", text)
	}
	parsed, err := ParseFlowID(strings.ToUpper(text))
	if err != nil || parsed != flow {
		t.Fatalf("round trip: parsed=%v err=%v", parsed, err)
	}
	var decoded FlowID
	if err := decoded.UnmarshalText([]byte(text)); err != nil || decoded != flow {
		t.Fatalf("text unmarshal: decoded=%v err=%v", decoded, err)
	}
}

func TestInvalidIDs(t *testing.T) {
	for _, text := range []string{"", "0123", "z1234567890123456789012345", "0123456789012345678901234i"} {
		if _, err := ParseEventID(text); err == nil {
			t.Errorf("ParseEventID(%q) succeeded", text)
		}
	}
}

func TestIDsUniqueConcurrently(t *testing.T) {
	const goroutines, perGoroutine = 16, 1000
	ids := make(chan FlowID, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				ids <- NewFlow()
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[FlowID]struct{}, goroutines*perGoroutine)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate ID %s", id)
		}
		seen[id] = struct{}{}
	}
}

func TestIDTypesRemainDistinct(t *testing.T) {
	flow := NewFlow()
	execution := NewExecution()
	if [16]byte(flow) == [16]byte(execution) {
		t.Fatal("different ID calls returned the same value")
	}
}
