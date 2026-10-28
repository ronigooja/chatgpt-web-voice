package httpclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Use a fresh process because net/http caches proxy environment on first use.
func TestDirectTransportsIgnoreProxyEnvironment(t *testing.T) {
	if os.Getenv("VOICE_DIRECT_TEST_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestDirectTransportsIgnoreProxyEnvironment$")
		cmd.Env = append(os.Environ(), "VOICE_DIRECT_TEST_CHILD=1", "HTTP_PROXY=http://127.0.0.1:1", "HTTPS_PROXY=http://127.0.0.1:1", "ALL_PROXY=http://127.0.0.1:1", "http_proxy=http://127.0.0.1:1", "https_proxy=http://127.0.0.1:1", "all_proxy=http://127.0.0.1:1", "NO_PROXY=", "no_proxy=")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v\n%s", err, output)
		}
		return
	}
	if processProxyURL() != "http://127.0.0.1:1" {
		t.Fatal("test must configure environment proxy")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "local-only") }))
	defer server.Close()
	for _, transport := range []string{"go", "tls-client", "curl-impersonate"} {
		t.Run(transport, func(t *testing.T) {
			opts := Options{Transport: transport}
			if transport == "curl-impersonate" {
				binary, err := exec.LookPath("curl")
				if err != nil {
					t.Skip("curl unavailable")
				}
				opts.CurlImpersonateBin = binary
			}
			client := New(opts, ProxyDirect)
			if tr, ok := client.Transport.(*http.Transport); ok && tr.Proxy != nil {
				t.Fatal("direct stdlib transport retained a proxy callback")
			}
			if tr, ok := client.Transport.(*tlsClientRoundTripper); ok {
				c, err := tr.httpClient()
				if err != nil {
					t.Fatal(err)
				}
				if c.GetProxy() != "" {
					t.Fatal("direct tls-client retained a proxy")
				}
			}
			resp, err := client.Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			data, err := io.ReadAll(resp.Body)
			if err != nil || string(data) != "local-only" {
				t.Fatalf("response=%q error=%v", data, err)
			}
		})
	}
	// An empty native account must retain the established environment policy.
	inherited := New(Options{Transport: "tls-client"}, "").Transport.(*tlsClientRoundTripper)
	c, err := inherited.httpClient()
	if err != nil || c.GetProxy() != "http://127.0.0.1:1" {
		t.Fatalf("legacy proxy inheritance lost: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
	if got := resolveCurlProxy("", req); !strings.Contains(got, "127.0.0.1:1") {
		t.Fatalf("curl inheritance lost: %q", got)
	}
}

func TestCurlAccountProxyOverridesNoProxyEnvironment(t *testing.T) {
	binary, err := exec.LookPath("curl")
	if err != nil {
		t.Skip("curl unavailable")
	}
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "*")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "bypassed-account-proxy") }))
	defer origin.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "account-proxy") }))
	defer proxy.Close()
	client := New(Options{Transport: "curl-impersonate", CurlImpersonateBin: binary}, proxy.URL)
	resp, err := client.Get(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "account-proxy" {
		t.Fatalf("body=%q error=%v", body, err)
	}
}

func TestCurlEmptyAccountRetainsAllProxyInheritance(t *testing.T) {
	binary, err := exec.LookPath("curl")
	if err != nil {
		t.Skip("curl unavailable")
	}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "origin") }))
	defer origin.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "legacy-all-proxy") }))
	defer proxy.Close()
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy"} {
		t.Setenv(key, "")
	}
	t.Setenv("ALL_PROXY", proxy.URL)
	t.Setenv("all_proxy", proxy.URL)
	client := New(Options{Transport: "curl-impersonate", CurlImpersonateBin: binary}, "")
	resp, err := client.Get(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "legacy-all-proxy" {
		t.Fatalf("body=%q error=%v", body, err)
	}
}
