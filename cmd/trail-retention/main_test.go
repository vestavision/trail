package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retention.json")
	data := []byte(`{"event_policies":[{"name":"http","match":{"kind":"provider.http","level":"warn","http_status_class":5},"ttl":"24h"}],"payload_policies":[{"name":"xml","match":{"role":"response","content_type":"application/xml"},"ttl":"48h"}],"payload_grace_period":"2h","payload_concurrency":2}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.EventPolicies) != 1 || cfg.EventPolicies[0].Match.Kind != "provider.http" || cfg.EventPolicies[0].TTL != 24*time.Hour {
		t.Fatalf("event config=%+v", cfg.EventPolicies)
	}
	if len(cfg.PayloadPolicies) != 1 || cfg.PayloadPolicies[0].Match.Role != "response" || cfg.PayloadGracePeriod != 2*time.Hour || cfg.PayloadConcurrency != 2 {
		t.Fatalf("payload config=%+v", cfg)
	}
}

func TestLoadConfigRequiresEventPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(path, []byte(`{"payload_classes":{"short":"24h"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("expected safe configuration error")
	}
}
