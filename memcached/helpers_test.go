package memcached

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func noError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func isError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
}

func isErrorIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("got %v, want errors.Is(..., %v)", err, target)
	}
}

func equal(t *testing.T, want, got any) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("want %#v, got %#v", want, got)
	}
}

func isTrue(t *testing.T, got bool) {
	t.Helper()
	if !got {
		t.Fatal("want true")
	}
}

func isFalse(t *testing.T, got bool) {
	t.Helper()
	if got {
		t.Fatal("want false")
	}
}

func isNil(t *testing.T, got any) {
	t.Helper()
	if !isNilValue(got) {
		t.Fatalf("want nil, got %#v", got)
	}
}

func notNil(t *testing.T, got any) {
	t.Helper()
	if isNilValue(got) {
		t.Fatal("want non-nil")
	}
}

func notEmpty(t *testing.T, got string) {
	t.Helper()
	if got == "" {
		t.Fatal("want non-empty string")
	}
}

func notZero[T comparable](t *testing.T, got T) {
	t.Helper()
	var zero T
	if got == zero {
		t.Fatal("want non-zero")
	}
}

func hasLen(t *testing.T, got any, n int) {
	t.Helper()
	gotLen := reflect.ValueOf(got).Len()
	if gotLen != n {
		t.Fatalf("want len %d, got %d", n, gotLen)
	}
}

func contains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("%q does not contain %q", s, sub)
	}
}

func lessOrEqual[T int | int64](t *testing.T, got, wantMax T) {
	t.Helper()
	if got > wantMax {
		t.Fatalf("got %v, want <= %v", got, wantMax)
	}
}

func greater[T ~int | ~int64](t *testing.T, got, wantMin T) {
	t.Helper()
	if got <= wantMin {
		t.Fatalf("got %v, want > %v", got, wantMin)
	}
}

func same(t *testing.T, want, got any) {
	t.Helper()
	if reflect.ValueOf(want).Pointer() != reflect.ValueOf(got).Pointer() {
		t.Fatal("want same pointer")
	}
}

func isNilValue(got any) bool {
	if got == nil {
		return true
	}
	v := reflect.ValueOf(got)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
