package main

import "testing"

func TestRawCodecDummy(t *testing.T) {
	var c rawCodec
	b, err := c.Marshal([]byte("hi"))
	if err != nil || string(b) != "hi" {
		t.Fatalf("marshal: %v %q", err, b)
	}
	var out []byte
	if err := c.Unmarshal([]byte("yo"), &out); err != nil || string(out) != "yo" {
		t.Fatalf("unmarshal: %v %q", err, out)
	}
	if c.Name() != "proto" {
		t.Errorf("name = %q", c.Name())
	}
}
