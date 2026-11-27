package api

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jon-jc/fluxgate/internal/store"
)

// changeLog models the rollups table as the live tail reads it. Each row
// carries the revision that last wrote it, an update moves a row to the new
// revision, and reads return rows after a cursor in (revision, metric, kind,
// label_hash, window_start) order: the semantics of store.ChangedPage,
// including More.
type changeLog struct {
	mu       sync.Mutex
	rows     []store.StoredRollup
	revision int64
	reads    int
}

func (l *changeLog) commit(rows ...store.StoredRollup) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.revision++
	for n := range rows {
		r := &rows[n]
		r.Revision = l.revision
		i := slices.IndexFunc(l.rows, func(e store.StoredRollup) bool {
			return e.Metric == r.Metric && e.Kind == r.Kind && e.LabelHash == r.LabelHash &&
				e.WindowStart.Equal(r.WindowStart)
		})
		if i >= 0 {
			l.rows[i] = *r
		} else {
			l.rows = append(l.rows, *r)
		}
	}
	slices.SortFunc(l.rows, compareRows)
}

func compareRows(a, b store.StoredRollup) int {
	return cmp.Or(cmp.Compare(a.Revision, b.Revision), cmp.Compare(a.Metric, b.Metric),
		cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.LabelHash, b.LabelHash),
		a.WindowStart.Compare(b.WindowStart))
}

func afterCursor(r store.StoredRollup, c store.Cursor) bool {
	if c.Metric == "" { // a fresh cursor: strictly after its revision
		return r.Revision > c.Revision
	}
	return compareRows(r, store.StoredRollup{Revision: c.Revision, Metric: c.Metric, Kind: c.Kind,
		LabelHash: c.LabelHash, WindowStart: c.WindowStart}) > 0
}

func (l *changeLog) ChangedPage(
	_ context.Context, _, metric string, cursor store.Cursor, limit int,
) (store.ChangePage, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reads++
	page := store.ChangePage{Next: cursor}
	for i := range l.rows {
		r := &l.rows[i]
		if (metric != "" && r.Metric != metric) || !afterCursor(*r, cursor) {
			continue
		}
		if len(page.Rows) == limit {
			page.More = true
			break
		}
		page.Rows = append(page.Rows, *r)
		page.Next = page.Next.After(*r, r.Revision)
	}
	return page, nil
}

func (l *changeLog) NewestRevision(context.Context, string) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.revision, nil
}

func (l *changeLog) readCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reads
}

func (*changeLog) Query(context.Context, store.QueryFilter) ([]store.StoredRollup, error) {
	return nil, nil
}

func (*changeLog) Metrics(context.Context, string, int) ([]store.MetricSummary, error) {
	return nil, nil
}

func (*changeLog) LabelKeys(context.Context, string, string, int) ([]string, error) { return nil, nil }

func (*changeLog) LabelValues(context.Context, string, string, string, int) ([]string, error) {
	return nil, nil
}

func changedRow(metric string, sum float64) store.StoredRollup {
	return store.StoredRollup{Metric: metric, Kind: "gauge", LabelHash: "h", WindowStart: queryNow,
		WindowEnd: queryNow.Add(time.Minute), Count: 1, Sum: sum, Min: sum, Max: sum, Last: sum}
}

func changedRows(metrics ...string) []store.StoredRollup {
	out := make([]store.StoredRollup, len(metrics))
	for i, m := range metrics {
		out[i] = changedRow(m, 1)
	}
	return out
}

type sseFrame struct{ Event, ID, Data string }

type sseClient struct {
	t      *testing.T
	body   io.Closer
	scan   *bufio.Scanner
	cancel context.CancelFunc
}

func newResumeServer(t *testing.T, log *changeLog, stream StreamOptions) *httptest.Server {
	t.Helper()
	stream.PollInterval = cmp.Or(stream.PollInterval, 5*time.Millisecond)
	stream.HeartbeatInterval = cmp.Or(stream.HeartbeatInterval, 50*time.Millisecond)
	stream.MaxDuration = cmp.Or(stream.MaxDuration, 10*time.Second)
	h := newQueryHarness(t, func(c *api2Config) { c.reader = log; c.stream = stream })
	srv := httptest.NewServer(h.router)
	t.Cleanup(srv.Close)
	return srv
}

func streamRequest(t *testing.T, ctx context.Context, srv *httptest.Server, lastEventID string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/stream", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	if lastEventID != "" {
		req.Header.Set(HeaderLastEventID, lastEventID)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func openStream(t *testing.T, srv *httptest.Server, lastEventID string) *sseClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	resp := streamRequest(t, ctx, srv, lastEventID) //nolint:bodyclose // closed by sseClient.close via t.Cleanup
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		cancel()
		t.Fatalf("status = %d: %s", resp.StatusCode, body)
	}
	c := &sseClient{t: t, body: resp.Body, scan: bufio.NewScanner(resp.Body), cancel: cancel}
	t.Cleanup(c.close)
	return c
}

func (c *sseClient) close() {
	c.cancel()
	_ = c.body.Close()
}

// next returns the next frame that names an event or an id. Comments and
// retry-only frames are skipped, as a browser would.
func (c *sseClient) next() sseFrame {
	c.t.Helper()
	var f sseFrame
	for c.scan.Scan() {
		line := c.scan.Text()
		switch {
		case line == "":
			if f.Event != "" || f.ID != "" {
				return f
			}
			f = sseFrame{}
		case strings.HasPrefix(line, ":"):
		default:
			field, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ")
			switch field {
			case "event":
				f.Event = value
			case "id":
				f.ID = value
			case "data":
				f.Data = value
			}
		}
	}
	c.t.Fatalf("stream ended before the next frame: %v", c.scan.Err())
	return f
}

// rollups reads n rollup events and returns their metrics and sums, and the
// client's Last-Event-ID afterwards -- which, as in a browser, is the id of
// the last frame that carried one.
func (c *sseClient) rollups(n int) (metrics []string, sums []float64, lastID string) {
	c.t.Helper()
	for len(metrics) < n {
		f := c.next()
		if f.ID == "" && f.Event == "rollup" {
			c.t.Fatalf("rollup event without an id: %+v", f)
		}
		lastID = cmp.Or(f.ID, lastID)
		switch f.Event {
		case "rollup":
			var e streamEvent
			if err := json.Unmarshal([]byte(f.Data), &e); err != nil {
				c.t.Fatalf("decode %q: %v", f.Data, err)
			}
			metrics, sums = append(metrics, e.Metric), append(sums, e.Sum)
		case "":
		default:
			c.t.Fatalf("unexpected %q event while reading rollups: %s", f.Event, f.Data)
		}
	}
	return metrics, sums, lastID
}

// A client that drops its connection resumes with the next change after the
// last one it received: nothing skipped, nothing repeated, and a later
// correction to a window it already holds still arrives.
func TestStreamResumesExactlyWhereTheClientStopped(t *testing.T) {
	log := &changeLog{}
	log.commit(changedRows("a1", "a2")...)
	log.commit(changedRows("b1", "b2")...)
	log.commit(changedRows("c1", "c2")...)
	srv := newResumeServer(t, log, StreamOptions{})

	first := openStream(t, srv, store.Cursor{}.Token(testTenant))
	got, _, last := first.rollups(3)
	first.close()
	if want := []string{"a1", "a2", "b1"}; !slices.Equal(got, want) {
		t.Fatalf("first connection read %v, want %v", got, want)
	}

	second := openStream(t, srv, last)
	got, _, _ = second.rollups(3)
	if want := []string{"b2", "c1", "c2"}; !slices.Equal(got, want) {
		t.Fatalf("resumed connection read %v, want %v", got, want)
	}

	// A late arrival corrects a1, which this client already holds.
	log.commit(changedRow("a1", 99))
	got, sums, _ := second.rollups(1)
	if got[0] != "a1" || sums[0] != 99 {
		t.Fatalf("correction arrived as %v %v, want a1 99", got, sums)
	}
}

// A stream that ends before anything changes still hands the client a position.
// Without it the client has no Last-Event-ID, reconnects at whatever the latest
// revision is by then, and silently skips what committed while it was away.
func TestQuietStreamResumesFromItsStartingPosition(t *testing.T) {
	log := &changeLog{}
	log.commit(changedRows("before1", "before2")...)
	srv := newResumeServer(t, log, StreamOptions{})

	quiet := openStream(t, srv, "")
	preamble := quiet.next()
	if preamble.Event != "" || preamble.ID == "" {
		t.Fatalf("stream did not open with an id-only position frame: %+v", preamble)
	}
	quiet.close()

	log.commit(changedRows("while.away1", "while.away2")...)

	resumed := openStream(t, srv, preamble.ID)
	got, _, _ := resumed.rollups(2)
	if want := []string{"while.away1", "while.away2"}; !slices.Equal(got, want) {
		t.Fatalf("resumed stream read %v, want %v", got, want)
	}
}

// Some positions cannot be honoured. Each gets an explicit reset -- the client
// should refetch what it displays -- after which the stream carries on live.
func TestStreamResetsWhenAResumeCannotBeHonoured(t *testing.T) {
	for _, tc := range []struct {
		name, token, reason string
	}{
		{"another tenant's cursor", store.Cursor{Revision: 1}.Token("globex"), resetForeign},
		{"a cursor from the future", store.Cursor{Revision: 50}.Token(testTenant), resetFuture},
		{"a cursor further behind than allowed", store.Cursor{Revision: 1}.Token(testTenant), resetStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &changeLog{}
			for i := range 6 {
				log.commit(changedRow(fmt.Sprintf("r%d", i), 1))
			}
			srv := newResumeServer(t, log, StreamOptions{MaxResumeRevisions: 2})

			stream := openStream(t, srv, tc.token)
			reset := stream.next()
			var payload struct {
				Reason   string `json:"reason"`
				Revision int64  `json:"revision"`
			}
			if err := json.Unmarshal([]byte(reset.Data), &payload); err != nil || reset.Event != "reset" {
				t.Fatalf("first frame %+v is not a reset (%v)", reset, err)
			}
			if payload.Reason != tc.reason || payload.Revision != 6 {
				t.Fatalf("reset = %+v, want reason %s at revision 6", payload, tc.reason)
			}
			// The reset's id moves the client to the latest revision, so a
			// reconnect after it does not reset again.
			if c, err := store.ParseCursorToken(testTenant, reset.ID); err != nil || c.Revision != 6 {
				t.Fatalf("reset id = %+v, %v; want revision 6", c, err)
			}

			log.commit(changedRow("after.reset", 1))
			got, _, _ := stream.rollups(1)
			if got[0] != "after.reset" {
				t.Fatalf("stream did not continue live after the reset: %v", got)
			}
		})
	}

	t.Run("a cursor exactly at the limit resumes", func(t *testing.T) {
		log := &changeLog{}
		for i := range 6 {
			log.commit(changedRow(fmt.Sprintf("r%d", i), 1))
		}
		srv := newResumeServer(t, log, StreamOptions{MaxResumeRevisions: 2})
		stream := openStream(t, srv, store.Cursor{Revision: 4}.Token(testTenant))
		got, _, _ := stream.rollups(2)
		if want := []string{"r4", "r5"}; !slices.Equal(got, want) {
			t.Fatalf("read %v, want %v", got, want)
		}
	})
}

// A malformed Last-Event-ID is a client bug. It is reported before admission,
// so it cannot consume the stream slot it was never going to use.
func TestMalformedLastEventIDIsRejectedBeforeAdmission(t *testing.T) {
	log := &changeLog{}
	srv := newResumeServer(t, log, StreamOptions{MaxConcurrent: 1, MaxPerTenant: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for range 3 {
		resp := streamRequest(t, ctx, srv, "not-a-cursor")
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest ||
			!strings.HasPrefix(resp.Header.Get("Content-Type"), "application/problem+json") ||
			!strings.Contains(string(body), "Last-Event-ID") {
			t.Fatalf("status %d %s: %s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
		}
	}
	openStream(t, srv, "") // the only slot is still free
}

// backlog commits revisions of rows, rows per revision, and returns the log.
func backlog(revisions, rows int) *changeLog {
	log := &changeLog{}
	for r := range revisions {
		batch := make([]store.StoredRollup, rows)
		for i := range batch {
			batch[i] = changedRow(fmt.Sprintf("m%03d.%04d", r, i), 1)
		}
		log.commit(batch...)
	}
	return log
}

func countFrames(body, event string) int { return strings.Count(body, "event: "+event+"\n") }

// One page per poll interval capped a stream at 250 rows a second, so a busy
// tenant's stream fell further behind on every tick. A backlog now drains in
// a single poll.
func TestCatchUpDrainsABacklogInOnePoll(t *testing.T) {
	log := backlog(17, 100) // 1,700 rows: three full pages and a partial one
	opts := StreamOptions{}
	opts.applyDefaults()
	rec := httptest.NewRecorder()

	cursor, open := catchUp(context.Background(), rec, rec, QueryDeps{Reader: log}, opts,
		testTenant, "", store.Cursor{})

	body := rec.Body.String()
	if got := countFrames(body, "rollup"); got != 1700 || !open {
		t.Fatalf("one poll delivered %d of 1700 rows (open=%v)", got, open)
	}
	if reads := log.readCount(); reads != 4 {
		t.Errorf("reads = %d, want 4 (500+500+500+200)", reads)
	}
	if countFrames(body, "reset") != 0 || cursor.Revision != 17 || cursor.Metric == "" {
		t.Errorf("unexpected reset or cursor %+v", cursor)
	}
}

// A stream that is still too far behind after a full burst says so rather than
// lagging silently; one within reach carries on from where it stopped.
func TestCatchUpResetsAStreamThatCannotKeepUp(t *testing.T) {
	t.Run("beyond reach", func(t *testing.T) {
		log := backlog(17, 100)
		opts := StreamOptions{MaxCatchUpPages: 2, MaxResumeRevisions: 5}
		opts.applyDefaults()
		rec := httptest.NewRecorder()

		cursor, open := catchUp(context.Background(), rec, rec, QueryDeps{Reader: log}, opts,
			testTenant, "", store.Cursor{})

		body := rec.Body.String()
		if countFrames(body, "rollup") != 1000 || countFrames(body, "reset") != 1 || !open {
			t.Fatalf("rollups=%d resets=%d open=%v", countFrames(body, "rollup"), countFrames(body, "reset"), open)
		}
		if !strings.Contains(body, `"reason":"lagging"`) {
			t.Errorf("reset does not say why:\n%s", body[strings.LastIndex(body, "event: reset"):])
		}
		if cursor.Revision != 17 || cursor.Metric != "" {
			t.Errorf("cursor after reset = %+v, want the latest revision", cursor)
		}
	})

	t.Run("within reach", func(t *testing.T) {
		log := backlog(17, 100)
		opts := StreamOptions{MaxCatchUpPages: 2, MaxResumeRevisions: 100}
		opts.applyDefaults()
		rec := httptest.NewRecorder()
		deps := QueryDeps{Reader: log}

		cursor, _ := catchUp(context.Background(), rec, rec, deps, opts, testTenant, "", store.Cursor{})
		cursor, _ = catchUp(context.Background(), rec, rec, deps, opts, testTenant, "", cursor)

		body := rec.Body.String()
		if countFrames(body, "rollup") != 1700 || countFrames(body, "reset") != 0 {
			t.Fatalf("two polls: rollups=%d resets=%d", countFrames(body, "rollup"), countFrames(body, "reset"))
		}
		if cursor.Revision != 17 {
			t.Errorf("cursor = %+v", cursor)
		}
	})
}
