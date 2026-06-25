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

func compareProtoFloats(a, b []byte, tolerance float64) (match bool, maxDiff float64, totalFloats, mismatchCount int) {
	floatsA := extractTopLevelFloats(a)
	floatsB := extractTopLevelFloats(b)

	if len(floatsA) != len(floatsB) {
		// Different number of float values - try comparing the shorter set
		minLen := len(floatsA)
		if len(floatsB) < minLen {
			minLen = len(floatsB)
		}
		if minLen == 0 {
			return len(floatsA) == len(floatsB), -1, 0, 0
		}
		totalFloats = minLen
		for i := 0; i < minLen; i++ {
			diff := math.Abs(floatsA[i] - floatsB[i])
			if diff > maxDiff {
				maxDiff = diff
			}
			if diff > tolerance {
				mismatchCount++
			}
		}
		// Count the length difference as mismatches too
		mismatchCount += abs(len(floatsA) - len(floatsB))
		match = mismatchCount == 0
		return
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
