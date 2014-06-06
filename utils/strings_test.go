package utils

import (
	"testing"
)

func TestStringSliceEquals(t *testing.T) {
	if !StringSliceEquals([]string{}, []string{}) {
		t.Fatalf("Expect %+v == %+v", []string{}, []string{})
	}

	if StringSliceEquals([]string{}, nil) {
		t.Fatalf("Expect %+v != %+v", []string{}, nil)
	}

	if StringSliceEquals(nil, []string{}) {
		t.Fatalf("Expect %+v != %+v", nil, []string{})
	}

	if StringSliceEquals([]string{"a", "b"}, []string{"a", "b", "c"}) {
		t.Fatalf("Expect %+v != %+v", []string{"a", "b"}, []string{"a", "b", "c"})
	}

	if StringSliceEquals([]string{"a", "b", "c"}, []string{"a", "b"}) {
		t.Fatalf("Expect %+v != %+v", []string{"a", "b", "c"}, []string{"a", "b"})
	}

	if !StringSliceEquals([]string{"a", "b", "c"}, []string{"a", "b", "c"}) {
		t.Fatalf("Expect %+v == %+v", []string{"a", "b", "c"}, []string{"a", "b", "c"})
	}
}
