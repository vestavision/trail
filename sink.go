package trail

// Sink consumes batches synchronously. Ownership transfers to Trail after a
// successful Init; Trail calls Close exactly once after draining the writer.
type Sink interface {
	WriteBatch(Batch) error
	Close() error
}
