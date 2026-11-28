package envfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLookup(t *testing.T) {
	t.Setenv("TRAIL_T_PLAIN", "a")
	if v, ok, err := Lookup("TRAIL_T_PLAIN"); v != "a" || !ok || err != nil {
		t.Fatalf("plain: %q %v %v", v, ok, err)
	}
	p := filepath.Join(t.TempDir(), "s")
	if err := os.WriteFile(p, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TRAIL_T_SECRET_FILE", p)
	if v, ok, err := Lookup("TRAIL_T_SECRET"); v != "from-file" || !ok || err != nil {
		t.Fatalf("file: %q %v %v", v, ok, err)
	}
	t.Setenv("TRAIL_T_SECRET", "x")
	if _, _, err := Lookup("TRAIL_T_SECRET"); err == nil {
		t.Fatal("expected conflict error")
	}
	t.Setenv("TRAIL_T_MISSING_FILE", filepath.Join(t.TempDir(), "nope"))
	if _, _, err := Lookup("TRAIL_T_MISSING"); err == nil {
		t.Fatal("expected read error")
	}
	if _, ok, err := Lookup("TRAIL_T_UNSET"); ok || err != nil {
		t.Fatalf("unset: %v %v", ok, err)
	}
}
