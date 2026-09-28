package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/query"
	"github.com/jon-jc/fluxgate/internal/store"
)

func TestStreamSurvivesServerWriteTimeoutAndCapsConnections(t *testing.T) {
	h := newQueryHarness(t, func(c *api2Config) {
		c.stream = StreamOptions{MaxConcurrent: 1, PollInterval: time.Millisecond, HeartbeatInterval: 60 * time.Millisecond, MaxDuration: 250 * time.Millisecond, WriteTimeout: 200 * time.Millisecond}
	})
	srv := httptest.NewUnstartedServer(h.router)
	srv.Config.WriteTimeout = 10 * time.Millisecond
	srv.Start()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := func() *http.Response {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/stream", http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	first := request()
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != 200 {
		t.Fatalf("first: %d", first.StatusCode)
	}
	second := request()
	_ = second.Body.Close()
	if second.StatusCode != 429 || second.Header.Get("Retry-After") == "" {
		t.Fatalf("excess stream status: %d", second.StatusCode)
	}
	body, err := io.ReadAll(first.Body)
	if err != nil {
		t.Fatalf("stream closed prematurely: %v, %s", err, body)
	}
	if strings.Count(string(body), "keep-alive") < 2 {
		t.Fatalf("stream did not survive the server timeout: %s", body)
	}
	third := request()
	_ = third.Body.Close()
	if third.StatusCode != 200 {
		t.Fatalf("stream slot was not released: %d", third.StatusCode)
	}
}

func TestQueryReportsDatabaseFetchTruncation(t *testing.T) {
	limits := query.DefaultLimits()
	limits.MaxPoints = 1
	h := newQueryHarness(t, withQueryLimits(limits))
	h.reader.rollups = []store.StoredRollup{
		storedRollup("cpu.util", 0, nil, 2),
		storedRollup("cpu.util", -time.Minute, nil, 1),
	}
	response := h.get(t, "/v1/query?metric=cpu.util")
	var result query.Result
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Truncated || len(result.Series) != 1 || len(result.Series[0].Points) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if h.reader.filter().Limit != 2 {
		t.Fatal("query did not fetch the overflow sentinel")
	}
}

func TestQueryBudgetGivesActionableClientError(t *testing.T) {
	h := newQueryHarness(t)
	h.reader.err = store.ErrReadBudget
	response := h.get(t, "/v1/query?metric=cpu.util")
	if response.Code != 422 || !strings.Contains(response.Body.String(), "Narrow the time range") {
		t.Fatalf("budget response: %d %s", response.Code, response.Body.String())
	}
}
