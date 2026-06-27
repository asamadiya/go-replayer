package main

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestDiffSamplerExactMinMax(t *testing.T) {
	d := newDiffSampler(rand.New(rand.NewSource(1)))
	for _, v := range []float64{0.3, 0.1, 0.9, 0.5} {
		d.add(v)
	}
	if d.count != 4 || d.min != 0.1 || d.max != 0.9 {
		t.Fatalf("count/min/max = %d/%v/%v", d.count, d.min, d.max)
	}
}

func TestDiffSamplerReservoirBounded(t *testing.T) {
	d := newDiffSampler(rand.New(rand.NewSource(1)))
	const extra = 500
	for i := 0; i < maxDiffSamples+extra; i++ {
		d.add(float64(i))
	}
	if len(d.reservoir) != maxDiffSamples {
		t.Fatalf("reservoir grew past cap: %d", len(d.reservoir))
	}
	if d.count != int64(maxDiffSamples+extra) {
		t.Fatalf("count not exact: %d", d.count)
	}
	if d.min != 0 || d.max != float64(maxDiffSamples+extra-1) {
		t.Fatalf("min/max wrong: %v/%v", d.min, d.max)
	}
}

func writeParityRec(b *[]byte, method string, payload []byte) {
	var mlen [4]byte
	binary.BigEndian.PutUint32(mlen[:], uint32(len(method)))
	*b = append(*b, mlen[:]...)
	*b = append(*b, method...)
	var plen [4]byte
	binary.BigEndian.PutUint32(plen[:], uint32(len(payload)))
	*b = append(*b, plen[:]...)
	*b = append(*b, payload...)
}

func TestParityLoadRequestsValid(t *testing.T) {
	var data []byte
	writeParityRec(&data, "/m1", []byte{1, 2})
	writeParityRec(&data, "/m2", []byte{3})
	p := filepath.Join(t.TempDir(), "r.bin")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	reqs, err := loadRequests(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reqs) != 2 || reqs[0].method != "/m1" || reqs[1].method != "/m2" {
		t.Fatalf("bad parse: %+v", reqs)
	}
}

func TestParityLoadRequestsEmptyFileErrors(t *testing.T) {
	p := filepath.Join(t.TempDir(), "empty.bin")
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRequests(p); err == nil {
		t.Fatal("empty file must error, not silently report zero requests")
	}
}

func TestParityLoadRequestsTruncatedErrors(t *testing.T) {
	var data []byte
	writeParityRec(&data, "/m1", []byte{1, 2, 3})
	data = data[:len(data)-2] // truncate the payload
	p := filepath.Join(t.TempDir(), "trunc.bin")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRequests(p); err == nil {
		t.Fatal("truncated file must error")
	}
}

func TestParityLoadRequestsRejectsHugeMethodLen(t *testing.T) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 0xFFFFFFFF)
	p := filepath.Join(t.TempDir(), "huge.bin")
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRequests(p); err == nil {
		t.Fatal("absurd method length must error, not allocate")
	}
}

func TestCompareProtoFloatsNoComparableFloatsIsMismatch(t *testing.T) {
	// Two byte-different messages whose only fields are varints (skipped by the
	// extractor) yield no comparable floats. This must be reported as a
	// MISMATCH, never a silent pass.
	a := fieldVarint(1, 100)
	b := fieldVarint(1, 200)
	match, _, total, mismatch := compareProtoFloats(a, b, 1e-6)
	if match || total != 0 || mismatch == 0 {
		t.Fatalf("non-float byte difference must mismatch: match=%v total=%d mismatch=%d", match, total, mismatch)
	}
}

func TestExtractFloatsMalformedLengthNoPanic(t *testing.T) {
	// A length-delimited field whose varint length is ~2^64 but with only a few
	// trailing bytes. The bounds check must reject it before the int conversion
	// rather than panicking on an out-of-range / negative slice index.
	tag := encVarint(uint64(5)<<3 | 2)
	hugeLen := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}
	msg := append(append(tag, hugeLen...), 0x01, 0x02)
	// Must not panic.
	_ = extractTopLevelFloats(msg)
	_ = extractShallowFloats(msg)
}

func TestCompareProtoFloatsUnequalCountsMismatch(t *testing.T) {
	a := append(fieldFloat64(1, 1.0), fieldFloat64(2, 2.0)...)
	b := fieldFloat64(1, 1.0)
	match, _, _, mismatch := compareProtoFloats(a, b, 1e-6)
	if match || mismatch == 0 {
		t.Fatalf("unequal float counts must mismatch: match=%v mismatch=%d", match, mismatch)
	}
}
