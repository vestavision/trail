package trail

import (
	"crypto/rand"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
)

const crockfordAlphabet = "0123456789abcdefghjkmnpqrstvwxyz"

var (
	ErrInvalidID = errors.New("trail: invalid ID")
	idOnce       sync.Once
	idPrefix     [10]byte
	idCounter    atomic.Uint64
)

// EventID uniquely identifies an event.
type EventID [16]byte

// FlowID correlates events belonging to one business flow.
type FlowID [16]byte

// ExecutionID correlates events produced by one job, run, or manual operation.
type ExecutionID [16]byte

func nextID() [16]byte {
	idOnce.Do(func() {
		if _, err := rand.Read(idPrefix[:]); err != nil {
			panic("trail: cannot initialize ID generator: " + err.Error())
		}
	})
	n := idCounter.Add(1)
	if n >= 1<<48 {
		panic("trail: ID counter exhausted")
	}
	var id [16]byte
	copy(id[:10], idPrefix[:])
	id[10] = byte(n >> 40)
	id[11] = byte(n >> 32)
	id[12] = byte(n >> 24)
	id[13] = byte(n >> 16)
	id[14] = byte(n >> 8)
	id[15] = byte(n)
	return id
}

// NewFlow returns a new stateless flow identifier.
func NewFlow() FlowID { return FlowID(nextID()) }

// NewExecution returns a new stateless execution identifier.
func NewExecution() ExecutionID { return ExecutionID(nextID()) }

func newEventID() EventID { return EventID(nextID()) }

func encodeID(id [16]byte) string {
	var out [26]byte
	x := new(big.Int).SetBytes(id[:])
	mask := big.NewInt(31)
	for i := len(out) - 1; i >= 0; i-- {
		var digit big.Int
		digit.And(x, mask)
		out[i] = crockfordAlphabet[digit.Uint64()]
		x.Rsh(x, 5)
	}
	return string(out[:])
}

func decodeID(text string) ([16]byte, error) {
	var id [16]byte
	if len(text) != 26 {
		return id, ErrInvalidID
	}
	var x big.Int
	for i := 0; i < len(text); i++ {
		v, ok := crockfordValue(text[i])
		if !ok || (i == 0 && v > 7) {
			return id, ErrInvalidID
		}
		x.Lsh(&x, 5)
		x.Or(&x, new(big.Int).SetUint64(uint64(v)))
	}
	b := x.Bytes()
	copy(id[len(id)-len(b):], b)
	return id, nil
}

func crockfordValue(c byte) (byte, bool) {
	if c >= 'A' && c <= 'Z' {
		c += 'a' - 'A'
	}
	if c >= '0' && c <= '9' {
		return c - '0', true
	}
	for i := byte(10); i < 32; i++ {
		if crockfordAlphabet[i] == c {
			return i, true
		}
	}
	return 0, false
}

func (id EventID) String() string     { return encodeID([16]byte(id)) }
func (id FlowID) String() string      { return encodeID([16]byte(id)) }
func (id ExecutionID) String() string { return encodeID([16]byte(id)) }

func (id EventID) IsZero() bool     { return id == EventID{} }
func (id FlowID) IsZero() bool      { return id == FlowID{} }
func (id ExecutionID) IsZero() bool { return id == ExecutionID{} }

func ParseEventID(s string) (EventID, error) {
	id, err := decodeID(s)
	return EventID(id), err
}

func ParseFlowID(s string) (FlowID, error) {
	id, err := decodeID(s)
	return FlowID(id), err
}

func ParseExecutionID(s string) (ExecutionID, error) {
	id, err := decodeID(s)
	return ExecutionID(id), err
}

func (id EventID) MarshalText() ([]byte, error)     { return []byte(id.String()), nil }
func (id FlowID) MarshalText() ([]byte, error)      { return []byte(id.String()), nil }
func (id ExecutionID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

func (id *EventID) UnmarshalText(text []byte) error {
	v, err := ParseEventID(string(text))
	if err == nil {
		*id = v
	}
	return err
}

func (id *FlowID) UnmarshalText(text []byte) error {
	v, err := ParseFlowID(string(text))
	if err == nil {
		*id = v
	}
	return err
}

func (id *ExecutionID) UnmarshalText(text []byte) error {
	v, err := ParseExecutionID(string(text))
	if err == nil {
		*id = v
	}
	return err
}
