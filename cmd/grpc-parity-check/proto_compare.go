package main

import (
	"encoding/binary"
	"math"
)

// extractTopLevelFloats extracts float32/float64 values from the TOP level of a protobuf
// message only. Does NOT recurse into length-delimited submessages to avoid false positives.
// For nested scores, it walks one level of repeated message fields.
func extractTopLevelFloats(data []byte) []float64 {
	var floats []float64
	offset := 0
	for offset < len(data) {
		tag, n := decodeVarint(data[offset:])
		if n == 0 {
			break
		}
		offset += n
		wireType := tag & 0x07

		switch wireType {
		case 0: // varint - skip
			_, n := decodeVarint(data[offset:])
			if n == 0 {
				return floats
			}
			offset += n
		case 1: // 64-bit (double)
			if offset+8 > len(data) {
				return floats
			}
			bits := binary.LittleEndian.Uint64(data[offset : offset+8])
			val := math.Float64frombits(bits)
			if !math.IsNaN(val) && !math.IsInf(val, 0) && math.Abs(val) < 1e15 {
				floats = append(floats, val)
			}
			offset += 8
		case 2: // length-delimited - walk ONE level deeper for repeated messages
			length, n := decodeVarint(data[offset:])
			if n == 0 {
				return floats
			}
			offset += n
			if offset+int(length) > len(data) {
				return floats
			}
			// Walk this submessage for float fields (one level only)
			subFloats := extractShallowFloats(data[offset : offset+int(length)])
			floats = append(floats, subFloats...)
			offset += int(length)
		case 5: // 32-bit (float)
			if offset+4 > len(data) {
				return floats
			}
			bits := binary.LittleEndian.Uint32(data[offset : offset+4])
			val := math.Float32frombits(bits)
			if !math.IsNaN(float64(val)) && !math.IsInf(float64(val), 0) && math.Abs(float64(val)) < 1e15 {
				floats = append(floats, float64(val))
			}
			offset += 4
		default:
			return floats
		}
	}
	return floats
}

// extractShallowFloats extracts only direct float32/float64 fields from a protobuf message.
// Does not recurse further.
func extractShallowFloats(data []byte) []float64 {
	var floats []float64
	offset := 0
	for offset < len(data) {
		tag, n := decodeVarint(data[offset:])
		if n == 0 {
			break
		}
		offset += n
		wireType := tag & 0x07

		switch wireType {
		case 0: // varint
			_, n := decodeVarint(data[offset:])
			if n == 0 {
				return floats
			}
			offset += n
		case 1: // double
			if offset+8 > len(data) {
				return floats
			}
			bits := binary.LittleEndian.Uint64(data[offset : offset+8])
			val := math.Float64frombits(bits)
			if !math.IsNaN(val) && !math.IsInf(val, 0) && math.Abs(val) < 1e15 {
				floats = append(floats, val)
			}
			offset += 8
		case 2: // length-delimited - skip (don't recurse further)
			length, n := decodeVarint(data[offset:])
			if n == 0 {
				return floats
			}
			offset += n
			offset += int(length)
		case 5: // float
			if offset+4 > len(data) {
				return floats
			}
			bits := binary.LittleEndian.Uint32(data[offset : offset+4])
			val := math.Float32frombits(bits)
			if !math.IsNaN(float64(val)) && !math.IsInf(float64(val), 0) && math.Abs(float64(val)) < 1e15 {
				floats = append(floats, float64(val))
			}
			offset += 4
		default:
			return floats
		}
	}
	return floats
}

func decodeVarint(data []byte) (uint64, int) {
	var val uint64
	for i, b := range data {
		if i >= 10 {
			return 0, 0
		}
		val |= uint64(b&0x7F) << (7 * uint(i))
		if b&0x80 == 0 {
			return val, i + 1
		}
	}
	return 0, 0
}

// compareProtoFloats decides whether two responses are equivalent within
// tolerance. It is intended to be called only when the raw response bytes
// already differ: if the difference cannot be attributed to comparable float
// fields — no floats on either side, or a differing number of floats — the
// responses are treated as a MISMATCH rather than silently passing.
func compareProtoFloats(a, b []byte, tolerance float64) (match bool, maxDiff float64, totalFloats, mismatchCount int) {
	floatsA := extractTopLevelFloats(a)
	floatsB := extractTopLevelFloats(b)

	if len(floatsA) == 0 || len(floatsB) == 0 || len(floatsA) != len(floatsB) {
		totalFloats = len(floatsA)
		if len(floatsB) < totalFloats {
			totalFloats = len(floatsB)
		}
		for i := 0; i < totalFloats; i++ {
			diff := math.Abs(floatsA[i] - floatsB[i])
			if diff > maxDiff {
				maxDiff = diff
			}
			if diff > tolerance {
				mismatchCount++
			}
		}
		mismatchCount += abs(len(floatsA) - len(floatsB))
		if mismatchCount == 0 {
			// Bytes differ but no comparable floats explain it.
			mismatchCount = 1
		}
		return false, maxDiff, totalFloats, mismatchCount
	}

	totalFloats = len(floatsA)
	for i := 0; i < len(floatsA); i++ {
		diff := math.Abs(floatsA[i] - floatsB[i])
		if diff > maxDiff {
			maxDiff = diff
		}
		if diff > tolerance {
			mismatchCount++
		}
	}

	match = mismatchCount == 0
	return
}
