package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// the fm-provider-test-harness submodule, relative to this module
	harnessDir = "../fm-provider-test-harness"
	// mock server image repository; the tag comes from the pinned harness version.txt
	imageRepo = "ghcr.io/dynatrace/fm-provider-mock-server"
	// port the mock server listens on inside the container
	containerPort = "8080/tcp"
	// covers a first-run image pull on a cold CI runner
	containerStartupTimeout = 60 * time.Second
	// bound on one control-plane call, so a wedged mock fails the scenario instead of hanging it
	controlRequestTimeout = 10 * time.Second
)

// mockServer is a client for the containerized mock CDN, metrics and SSE backend, driven via its control API.
type mockServer struct {
	container testcontainers.Container
	// scheme://host:port the provider is pointed at for config and metrics
	origin string
	http   *http.Client
}

// startMockServer starts the mock server container; the image comes from the pinned harness version.txt, overridable via MOCKSERVER_IMAGE.
func startMockServer(ctx context.Context) (*mockServer, error) {
	image := os.Getenv("MOCKSERVER_IMAGE")
	if image == "" {
		version, err := os.ReadFile(harnessDir + "/version.txt")
		if err != nil {
			return nil, fmt.Errorf("read harness version (run `git submodule update --init fm-provider-test-harness`): %w", err)
		}
		image = imageRepo + ":v" + strings.TrimSpace(string(version))
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{containerPort},
			WaitingFor:   wait.ForHTTP("/__control__/health").WithPort(containerPort).WithStartupTimeout(containerStartupTimeout),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start mock server %s: %w", image, err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := c.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, err
	}
	return &mockServer{
		container: c,
		origin:    fmt.Sprintf("http://%s:%s", host, port.Port()),
		http:      &http.Client{Timeout: controlRequestTimeout},
	}, nil
}

// reset clears the CDN responses, recorded requests and rate-limit state.
func (m *mockServer) reset() error {
	_, err := m.sendControlRequest(http.MethodPost, "/__control__/reset", nil)
	return err
}

// setCDNResponses sets the ordered CDN responses; each fetch takes the next one and the last repeats once exhausted.
func (m *mockServer) setCDNResponses(responses []cdnResponse) error {
	_, err := m.sendControlRequest(http.MethodPut, "/__control__/cdn/responses", map[string]any{
		"responses":  responses,
		"repeatLast": true,
	})
	return err
}

// fetchCDNRequests returns the CDN requests received since the last reset, in arrival order.
func (m *mockServer) fetchCDNRequests() ([]recordedRequest, error) {
	raw, err := m.sendControlRequest(http.MethodGet, "/__control__/cdn/requests", nil)
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Requests []struct {
			Method       string            `json:"method"`
			Path         string            `json:"path"`
			Headers      map[string]string `json:"headers"`
			ReceivedAtMs int64             `json:"receivedAtMs"`
		} `json:"requests"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse recorded cdn requests %q: %w", raw, err)
	}
	out := make([]recordedRequest, 0, len(parsed.Requests))
	for _, r := range parsed.Requests {
		headers := make(map[string]string, len(r.Headers))
		for k, v := range r.Headers {
			headers[strings.ToLower(k)] = v
		}
		out = append(out, recordedRequest{Method: r.Method, Path: r.Path, Headers: headers, ReceivedAtMs: r.ReceivedAtMs})
	}
	return out, nil
}

// countCDNRequests returns the number of CDN requests since the last reset.
func (m *mockServer) countCDNRequests() (int, error) {
	reqs, err := m.fetchCDNRequests()
	return len(reqs), err
}

// sendControlRequest calls the mock's control API, JSON-encoding body when non-nil, and returns the response body.
func (m *mockServer) sendControlRequest(method, path string, body any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequest(method, m.origin+path, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := m.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("control call %s %s: %w", method, path, err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("control call %s %s failed: HTTP %d %s", method, path, res.StatusCode, raw)
	}
	return raw, nil
}

// stop terminates the mock server container.
func (m *mockServer) stop(ctx context.Context) error {
	return m.container.Terminate(ctx)
}

// cdnResponse is one scripted CDN response; a nil body means no body at all.
type cdnResponse struct {
	Status  int               `json:"status"`
	Body    *string           `json:"body,omitempty"`
	DelayMs int64             `json:"delayMs,omitempty"`
	Headers map[string]string `json:"headers"`
}

// buildCDNResponse builds a CDN response with the given status; omit body for no body at all, pass "" for an explicitly empty one.
func buildCDNResponse(code int, body ...string) cdnResponse {
	r := cdnResponse{Status: code, Headers: map[string]string{}}
	if len(body) > 0 {
		r.Body = &body[0]
	}
	return r
}

// withHeader returns the response with an extra header.
func (r cdnResponse) withHeader(name, value string) cdnResponse {
	r.Headers[name] = value
	return r
}

// withDelay returns the response held back by delay before the mock answers; the mock records the request at arrival regardless.
func (r cdnResponse) withDelay(delay time.Duration) cdnResponse {
	r.DelayMs = delay.Milliseconds()
	return r
}

// recordedRequest is a CDN request the backend recorded; header names are lower-cased.
type recordedRequest struct {
	Method       string
	Path         string
	Headers      map[string]string
	ReceivedAtMs int64
}

// getHeader returns the named header case-insensitively, and whether it was present.
func (r recordedRequest) getHeader(name string) (string, bool) {
	v, ok := r.Headers[strings.ToLower(name)]
	return v, ok
}
