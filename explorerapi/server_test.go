package explorerapi

import (
	"net/http/httptest"
	"testing"

	"github.com/vestavision/trail/storage"
)

func TestOverviewActivityFilter(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/overview/activity?from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z&interval=hour&service=order-worker", nil)
	f, err := overviewActivityFilter(r)
	if err != nil {
		t.Fatal(err)
	}
	if f.Interval != storage.ActivityHour || f.Service != "order-worker" {
		t.Fatalf("unexpected filter: %+v", f)
	}
}

func TestOverviewActivityFilterRejectsInvalidRange(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/overview/activity?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z", nil)
	if _, err := overviewActivityFilter(r); err == nil {
		t.Fatal("expected invalid range error")
	}
}

func TestOverviewActivityFilterRejectsInvalidInterval(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/overview/activity?interval=minute", nil)
	if _, err := overviewActivityFilter(r); err == nil {
		t.Fatal("expected invalid interval error")
	}
}
