package httpclient

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCurlBodyFailuresCarryResponseMetadata(t *testing.T) {
	for _, code := range []int{18, 28, 56, 97} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			bin := filepath.Join(t.TempDir(), "curl-body-fail")
			script := fmt.Sprintf("#!/bin/sh\nprintf 'HTTP/1.1 201 Created\\r\\nContent-Type: application/sdp\\r\\n\\r\\nv=0\\r\\npartial'\nexit %d\n", code)
			if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			req, _ := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
			resp, err := (&curlRoundTripper{bin: bin}).RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(resp.Body)
			var failure *CurlRequestError
			if !errors.As(fmt.Errorf("caller: %w", err), &failure) || !failure.ResponseBegan || failure.ExitCode != code || len(data) == 0 {
				t.Fatalf("body=%q failure=%+v err=%v", data, failure, err)
			}
			closeErr := resp.Body.Close()
			if !errors.As(closeErr, &failure) || !failure.ResponseBegan || failure.ExitCode != code {
				t.Fatalf("close=%v", closeErr)
			}
		})
	}
}

func TestCurlPartialHeaderEOFMarksResponseBegan(t *testing.T) {
	for _, input := range []string{"HTTP/1.1 201 Created\r\n", "HTTP/1.1 201 Created\r\nContent-Type: application/sdp", "HTTP/1.1 100 Continue\r\n\r\n", "HTTP/1"} {
		_, _, _, err := readCurlResponseHeader(bufio.NewReader(strings.NewReader(input)))
		var began responseBeganError
		if !errors.Is(err, io.EOF) || !errors.As(err, &began) {
			t.Fatalf("input=%q err=%v", input, err)
		}
	}
}

func TestCurlFailureRetainsExitAndResponseState(t *testing.T) {
	for _, partial := range []bool{false, true} {
		bin := filepath.Join(t.TempDir(), "curl-fake")
		script := "#!/bin/sh\n"
		if partial {
			script += "printf 'HTTP/1.1 201 Created\\r\\n'\n"
		}
		script += "exit 97\n"
		if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
		_, err := (&curlRoundTripper{bin: bin}).RoundTrip(req)
		var failure *CurlRequestError
		if !errors.As(err, &failure) || failure.ExitCode != 97 || failure.ResponseBegan != partial {
			t.Fatalf("partial=%v err=%+v", partial, err)
		}
	}
}

func TestMalformedResponseDoesNotExposeContent(t *testing.T) {
	for _, line := range []string{"SECRET-CONTENT-MARKER-1234\n", "HTTP/1.1 SECRET-CONTENT-MARKER-1234\n", "HTTP/SECRET-CONTENT-MARKER-1234\n"} {
		_, _, _, err := readCurlResponseHeader(bufio.NewReader(strings.NewReader(line)))
		if err == nil || strings.Contains(err.Error(), "SECRET-CONTENT-MARKER-1234") {
			t.Fatalf("unsafe error: %v", err)
		}
		var out bytes.Buffer
		slog.New(slog.NewTextHandler(&out, nil)).Error("upstream_realtime_request_failed", "error", err)
		if strings.Contains(out.String(), "SECRET-CONTENT-MARKER-1234") {
			t.Fatal(out.String())
		}
	}
}
