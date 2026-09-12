package convention

import "testing"

func TestCanonicalStatuses(t *testing.T) {
	for _, status := range []Status{StatusRunning, StatusSucceeded, StatusFailed, StatusCancelled} {
		if !IsCanonicalStatus(string(status)) {
			t.Fatalf("status %q is not canonical", status)
		}
	}
	if IsCanonicalStatus("awaiting_review") {
		t.Fatal("custom status reported as canonical")
	}
}
