package postgres

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/vestavision/trail/retention"
)

func TestRetentionCursorRoundTrip(t *testing.T) {
	at := time.Unix(123, 456).UTC()
	id := bytes.Repeat([]byte{7}, 16)
	cursor := retentionCursor(at, id)
	gotAt, gotID, err := parseRetentionCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if !gotAt.Equal(at) || !bytes.Equal(gotID, id) {
		t.Fatalf("at=%v id=%x", gotAt, gotID)
	}
}

func TestDeleteRequiresArchiveReceiptBeforeDatabaseAccess(t *testing.T) {
	store := &Store{}
	_, err := store.DeleteRetentionEvents(context.Background(), retention.DeleteRequest{Events: []retention.Event{{ID: "00"}}})
	if err == nil {
		t.Fatal("expected archive receipt error")
	}
}
func TestRetentionCursorRejectsInvalid(t *testing.T) {
	if _, _, err := parseRetentionCursor("invalid"); err == nil {
		t.Fatal("expected invalid cursor error")
	}
}
