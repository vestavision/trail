// Package envfile resolves configuration from the environment, with the
// common <NAME>_FILE convention: when NAME_FILE is set, the value is read
// from that file (trailing newline trimmed). This lets secrets be delivered
// as mounted files (Docker/Swarm secrets, Kubernetes volumes, an identity
// agent) instead of environment variables. Setting both is an error.
package envfile

import (
	"fmt"
	"os"
	"strings"
)

// Lookup mirrors os.LookupEnv with NAME_FILE support.
func Lookup(name string) (string, bool, error) {
	path, hasFile := os.LookupEnv(name + "_FILE")
	value, hasValue := os.LookupEnv(name)
	if !hasFile || path == "" {
		return value, hasValue, nil
	}
	if hasValue && value != "" {
		return "", false, fmt.Errorf("%s and %s_FILE are both set", name, name)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false, fmt.Errorf("read %s_FILE: %w", name, err)
	}
	return strings.TrimRight(string(b), "\r\n"), true, nil
}

// Get mirrors os.Getenv with NAME_FILE support. A misconfigured or
// unreadable NAME_FILE stops the process: silently falling back to an empty
// credential would be worse.
func Get(name string) string {
	v, _, err := Lookup(name)
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(2)
	}
	return v
}
