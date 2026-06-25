package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func fakeExists(paths ...string) func(string) bool {
	m := make(map[string]bool, len(paths))
	for _, path := range paths {
		m[path] = true
	}
	return func(path string) bool {
		return m[path]
	}
}

func TestResolveTakeoverInstallRootExplicit(t *testing.T) {
	root, err := resolveTakeoverInstallRoot("/custom/install", fakeExists("/custom/install"))
	if err != nil {
		t.Fatalf("expected explicit root to resolve, got error: %v", err)
	}
	if root != "/custom/install" {
		t.Fatalf("unexpected root: %s", root)
	}
}

func TestResolveTakeoverInstallRootFallsBackToKnownCandidates(t *testing.T) {
	known := takeoverInstallRootCandidates[1]
	root, err := resolveTakeoverInstallRoot("", fakeExists(known))
	if err != nil {
		t.Fatalf("expected fallback root, got error: %v", err)
	}
	if root != known {
		t.Fatalf("expected %s, got %s", known, root)
	}
}

func TestResolveTakeoverArtifactsSelectsDefaults(t *testing.T) {
	installRoot := "/install"
	exists := fakeExists(
		"/install/replay_2gb.bin",
		"/install/var/identity.cert",
		"/install/var/identity.key",
	)

	artifacts, err := resolveTakeoverArtifacts(installRoot, "", "", "", "", exists)
	if err != nil {
		t.Fatalf("expected defaults to resolve, got error: %v", err)
	}
	if artifacts.replayFile != "/install/replay_2gb.bin" {
		t.Fatalf("unexpected replay file: %s", artifacts.replayFile)
	}
	if artifacts.certFile != "/install/var/identity.cert" {
		t.Fatalf("unexpected cert file: %s", artifacts.certFile)
	}
	if artifacts.keyFile != "/install/var/identity.key" {
		t.Fatalf("unexpected key file: %s", artifacts.keyFile)
	}
}

func TestResolveTakeoverArtifactsHonorsOverrides(t *testing.T) {
	installRoot := "/install"
	exists := fakeExists(
		"/custom/replay.bin",
		"/custom/client.cert",
		"/custom/client.key",
	)

	artifacts, err := resolveTakeoverArtifacts(
		installRoot,
		"/custom/replay.bin",
		"/custom/client.cert",
		"/custom/client.key",
		"",
		exists,
	)
	if err != nil {
		t.Fatalf("expected overrides to resolve, got error: %v", err)
	}
	if artifacts.replayFile != "/custom/replay.bin" {
		t.Fatalf("unexpected replay file: %s", artifacts.replayFile)
	}
	if artifacts.certFile != "/custom/client.cert" {
		t.Fatalf("unexpected cert file: %s", artifacts.certFile)
	}
	if artifacts.keyFile != "/custom/client.key" {
		t.Fatalf("unexpected key file: %s", artifacts.keyFile)
	}
}

func TestResolveTakeoverArtifactsFailsWithoutReplayFile(t *testing.T) {
	installRoot := "/install"
	exists := fakeExists(
		"/install/var/identity.cert",
		"/install/var/identity.key",
	)

	_, err := resolveTakeoverArtifacts(installRoot, "", "", "", "", exists)
	if err == nil {
		t.Fatalf("expected missing replay file error")
	}
}

func TestNormalizeTargetArg(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		want      string
		shouldErr bool
	}{
		{name: "plain host port", in: "host01.example.com:28826", want: "host01.example.com:28826"},
		{name: "tuple paren", in: "(host01.example.com,28826)", want: "host01.example.com:28826"},
		{name: "tuple bracket quoted", in: "[\"host01.example.com\", \"28826\"]", want: "host01.example.com:28826"},
		{name: "wrapped hostport", in: "(host01.example.com:28826)", want: "host01.example.com:28826"},
		{name: "invalid tuple port", in: "(host01.example.com,abc)", shouldErr: true},
		{name: "missing port", in: "host01.example.com", shouldErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeTargetArg(tc.in)
			if tc.shouldErr {
				if err == nil {
					t.Fatalf("expected error, got success (%s)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected target: got=%q want=%q", got, tc.want)
			}
		})
	}
}

func writeReplayRecord(t *testing.T, f *os.File, method string, payload []byte) {
	t.Helper()
	if err := binary.Write(f, binary.BigEndian, uint32(len(method))); err != nil {
		t.Fatalf("failed writing method length: %v", err)
	}
	if _, err := f.Write([]byte(method)); err != nil {
		t.Fatalf("failed writing method bytes: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint32(len(payload))); err != nil {
		t.Fatalf("failed writing payload length: %v", err)
	}
	if _, err := f.Write(payload); err != nil {
		t.Fatalf("failed writing payload bytes: %v", err)
	}
}

func TestLoadRequestsSkipsZeroLengthPayloads(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "requests.bin")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create temp replay file: %v", err)
	}
	writeReplayRecord(t, f, "/m1", []byte{1, 2, 3})
	writeReplayRecord(t, f, "/m2", []byte{})
	writeReplayRecord(t, f, "/m3", []byte{4, 5})
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close temp replay file: %v", err)
	}

	reqs, err := loadRequests(path)
	if err != nil {
		t.Fatalf("loadRequests returned error: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("expected 2 valid requests, got %d", len(reqs))
	}
	if reqs[0].method != "/m1" || reqs[1].method != "/m3" {
		t.Fatalf("unexpected methods loaded: %#v", []string{reqs[0].method, reqs[1].method})
	}
	if len(reqs[0].payload) == 0 || len(reqs[1].payload) == 0 {
		t.Fatalf("zero-length payload was not filtered")
	}
}

func TestMakeDispatchJobCopiesPayload(t *testing.T) {
	req := request{
		method:  "",
		payload: []byte{1, 2, 3},
	}

	job := makeDispatchJob(req, "/fallback")
	if job.method != "/fallback" {
		t.Fatalf("expected fallback method, got %q", job.method)
	}
	if len(job.payload) != 3 {
		t.Fatalf("unexpected payload length: %d", len(job.payload))
	}

	req.payload[0] = 9
	if job.payload[0] != 1 {
		t.Fatalf("job payload changed when source payload mutated; got %d", job.payload[0])
	}

	job.payload[1] = 7
	if req.payload[1] == 7 {
		t.Fatalf("source payload changed when job payload mutated")
	}
}

func TestRoundRobinRequestWraps(t *testing.T) {
	reqs := []request{
		{method: "/m1", payload: []byte{1}},
		{method: "/m2", payload: []byte{2}},
		{method: "/m3", payload: []byte{3}},
	}
	want := []string{"/m1", "/m2", "/m3", "/m1", "/m2", "/m3", "/m1"}
	for i, expectedMethod := range want {
		got := roundRobinRequest(reqs, i)
		if got.method != expectedMethod {
			t.Fatalf("idx=%d expected method %q got %q", i, expectedMethod, got.method)
		}
	}
}

func TestRawCodecMarshalWrongTypeReturnsError(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Marshal panicked unexpectedly: %v", r)
		}
	}()

	if _, err := (rawCodec{}).Marshal("not-bytes"); err == nil {
		t.Fatalf("expected Marshal to return error for non-[]byte input")
	}
}
