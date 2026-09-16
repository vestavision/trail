package explorerapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vestavision/trail"
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

func TestScopeAllowedRequiresExactTypedScopeWhenRequested(t *testing.T) {
	business := trail.Scope{Type: "business", ID: "b1"}
	if !scopeAllowed(trail.Scope{}, business) || !scopeAllowed(business, business) {
		t.Fatal("global or exact scope should be allowed")
	}
	if scopeAllowed(business, trail.Scope{Type: "business", ID: "b2"}) || scopeAllowed(business, trail.Scope{}) {
		t.Fatal("mismatched or unscoped record must be denied")
	}
}

func TestFiltersIncludeScopeAndOperationalFields(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/v1/events?scope_type=tenant&scope_id=t1&provider=bank&http_method=POST&http_status=503&has_error=true", nil)
	f := eventFilter(r)
	if f.Scope.Type != "tenant" || f.Scope.ID != "t1" || f.Provider != "bank" || f.HTTPMethod != "POST" || f.HTTPStatus != 503 || f.HasError == nil || !*f.HasError {
		t.Fatalf("filter = %+v", f)
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
