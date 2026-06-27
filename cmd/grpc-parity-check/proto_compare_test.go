package main

import (
	"encoding/binary"
	"math"
	"testing"
)

// --- protobuf wire-format encoding helpers (test-only) ---

func encVarint(v uint64) []byte {
	var out []byte
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if v == 0 {
			return out
		}
	}
}

func fieldFloat32(field int, v float32) []byte {
	out := encVarint(uint64(field)<<3 | 5)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], math.Float32bits(v))
	return append(out, b[:]...)
}

func fieldFloat64(field int, v float64) []byte {
	out := encVarint(uint64(field)<<3 | 1)
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], math.Float64bits(v))
	return append(out, b[:]...)
}

func fieldVarint(field int, v uint64) []byte {
	return append(encVarint(uint64(field)<<3), encVarint(v)...)
}

func fieldBytes(field int, payload []byte) []byte {
	out := encVarint(uint64(field)<<3 | 2)
	out = append(out, encVarint(uint64(len(payload)))...)
	return append(out, payload...)
}

func TestDecodeVarint(t *testing.T) {
	cases := []struct {
		name    string
		in      []byte
		wantVal uint64
		wantN   int
	}{
		{"single byte", []byte{0x01}, 1, 1},
		{"zero", []byte{0x00}, 0, 1},
		{"two byte 300", []byte{0xAC, 0x02}, 300, 2},
		{"max single", []byte{0x7F}, 127, 1},
		{"empty", []byte{}, 0, 0},
		{"overflow >10 bytes", []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, 0, 0},
		{"truncated continuation", []byte{0x80}, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			val, n := decodeVarint(tc.in)
			if val != tc.wantVal || n != tc.wantN {
				t.Fatalf("decodeVarint(%v) = (%d,%d), want (%d,%d)", tc.in, val, n, tc.wantVal, tc.wantN)
			}
		})
	}
}

func TestExtractTopLevelFloatsScalars(t *testing.T) {
	var msg []byte
	msg = append(msg, fieldVarint(1, 42)...)     // skipped
	msg = append(msg, fieldFloat32(2, 1.5)...)   // extracted
	msg = append(msg, fieldFloat64(3, 2.25)...)  // extracted
	msg = append(msg, fieldFloat32(4, -3.75)...) // extracted

	got := extractTopLevelFloats(msg)
	want := []float64{1.5, 2.25, -3.75}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if math.Abs(got[i]-want[i]) > 1e-9 {
			t.Fatalf("idx %d: got %v want %v", i, got[i], want[i])
		}
	}
}

func TestExtractTopLevelFloatsWalksOneLevel(t *testing.T) {
	// A repeated submessage that directly holds a float is walked one level deep.
	inner := fieldFloat32(1, 9.5)
	deepInner := fieldBytes(1, fieldFloat32(1, 100.0)) // float two levels down
	msg := append(fieldBytes(5, inner), fieldBytes(6, deepInner)...)

	got := extractTopLevelFloats(msg)
	if len(got) != 1 || math.Abs(got[0]-9.5) > 1e-9 {
		t.Fatalf("expected exactly the one-level-deep float [9.5], got %v", got)
	}
}

func TestExtractTopLevelFloatsFiltersNaNInfHuge(t *testing.T) {
	var msg []byte
	msg = append(msg, fieldFloat64(1, math.NaN())...)
	msg = append(msg, fieldFloat64(2, math.Inf(1))...)
	msg = append(msg, fieldFloat64(3, 1e16)...) // |v| >= 1e15 filtered
	msg = append(msg, fieldFloat64(4, 3.0)...)  // kept

	got := extractTopLevelFloats(msg)
	if len(got) != 1 || got[0] != 3.0 {
		t.Fatalf("expected only [3.0] after filtering, got %v", got)
	}
}

func TestCompareProtoFloatsIdentical(t *testing.T) {
	msg := append(fieldFloat32(1, 1.0), fieldFloat64(2, 2.0)...)
	match, maxDiff, total, mismatch := compareProtoFloats(msg, msg, 1e-6)
	if !match || maxDiff != 0 || total != 2 || mismatch != 0 {
		t.Fatalf("identical: match=%v maxDiff=%v total=%d mismatch=%d", match, maxDiff, total, mismatch)
	}
}

func TestCompareProtoFloatsWithinTolerance(t *testing.T) {
	a := fieldFloat64(1, 1.000000)
	b := fieldFloat64(1, 1.000001)
	match, _, total, mismatch := compareProtoFloats(a, b, 1e-3)
	if !match || total != 1 || mismatch != 0 {
		t.Fatalf("within tolerance should match: match=%v total=%d mismatch=%d", match, total, mismatch)
	}
}

func TestCompareProtoFloatsBeyondTolerance(t *testing.T) {
	a := fieldFloat64(1, 1.0)
	b := fieldFloat64(1, 2.0)
	match, maxDiff, _, mismatch := compareProtoFloats(a, b, 1e-3)
	if match || mismatch != 1 || math.Abs(maxDiff-1.0) > 1e-9 {
		t.Fatalf("beyond tolerance should mismatch: match=%v maxDiff=%v mismatch=%d", match, maxDiff, mismatch)
	}
}

func TestCompareProtoFloatsDifferentCounts(t *testing.T) {
	a := append(fieldFloat64(1, 1.0), fieldFloat64(2, 2.0)...)
	b := fieldFloat64(1, 1.0)
	match, _, total, mismatch := compareProtoFloats(a, b, 1e-6)
	if match || total != 1 || mismatch != 1 {
		t.Fatalf("count mismatch: match=%v total=%d mismatch=%d", match, total, mismatch)
	}
}

func TestAbsHelper(t *testing.T) {
	if abs(-3) != 3 || abs(3) != 3 || abs(0) != 0 {
		t.Fatalf("abs broken")
	}
}
