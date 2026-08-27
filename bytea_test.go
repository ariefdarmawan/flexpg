package flexpg

import "testing"

func TestValueToSQLValueBytea(t *testing.T) {
	query := &Query{}
	got := query.ValueToSQlValue([]byte{0x00, 0x27, 0xff, 0x41})
	const want = "decode('0027ff41', 'hex')"

	if got != want {
		t.Fatalf("ValueToSQlValue([]byte) = %q, want %q", got, want)
	}
}
