package trail

// Scope identifies the tenant or security boundary that owns an event.
// Scope is deliberately authorization-neutral: applications decide which
// authenticated principals may query a given scope.
type Scope struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// IsZero reports whether no scope is attached.
func (s Scope) IsZero() bool { return s.Type == "" && s.ID == "" }
