package join

import "testing"

// DropRecord makes the next join leave one record out, for the App test
// of the INTERNAL path in package join_test.
func DropRecord(t *testing.T) {
	dropRecord.Store(true)
	t.Cleanup(func() { dropRecord.Store(false) })
}
