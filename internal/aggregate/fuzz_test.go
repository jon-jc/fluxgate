package aggregate

import (
	"testing"
	"time"
)

// FuzzHashLabels asserts the two properties the whole aggregation scheme rests
// on, against label content chosen adversarially rather than by hand.
//
// The hash is the series identity. It decides which points accumulate together
// in memory, it is the primary key rollups are written under, and it is what a
// query groups by. So:
//
//   - Equal label sets must hash equally regardless of map iteration order,
//     or one logical series splits into several and every count is wrong.
//   - Different label sets must not collide, or two tenants' distinct series
//     merge into one and a query returns a number that was never observed.
//
// The interesting collisions are the ones a delimiter mistake creates: the
// classic is {"ab":"c"} against {"a":"bc"}, which naive concatenation maps to
// the same string. Fuzzing explores that family far past what a fixed table of
// cases covers.
func FuzzHashLabels(f *testing.F) {
	f.Add("a", "b", "c", "d")
	f.Add("ab", "c", "a", "bc")
	f.Add("", "", "", "")
	f.Add("k", "v", "k", "v")
	f.Add("k\x00v", "", "k", "\x00v")
	f.Add("a=b", "c", "a", "b=c")

	f.Fuzz(func(t *testing.T, k1, v1, k2, v2 string) {
		one := map[string]string{k1: v1, k2: v2}

		// Determinism: the same content must hash the same way every time,
		// across separately allocated maps.
		//
		// The copy is deliberate. Writing the second map as a literal with the
		// keys swapped looks like it tests insertion order, but when k1 == k2
		// the two literals resolve the duplicate key differently and the maps
		// genuinely differ -- the test would then be blaming the hash for its
		// own mistake. Go randomises map iteration order anyway, so copying
		// the content and hashing it again exercises the ordering path across
		// runs without fabricating a difference.
		two := make(map[string]string, len(one))
		for k, v := range one {
			two[k] = v
		}
		if HashLabels(one) != HashLabels(two) {
			t.Fatalf("insertion order changed the identity of a series:\n"+
				" %v -> %s\n %v -> %s", one, HashLabels(one), two, HashLabels(two))
		}

		// Separation: two label sets that differ must not share an identity.
		// Built by swapping the values between the two keys, which is only a
		// meaningful second set when the keys differ.
		other := map[string]string{k1: v2, k2: v1}
		if k1 == k2 {
			other = map[string]string{k1: v1 + "~sep~" + v2}
		}
		if !sameLabels(one, other) && HashLabels(one) == HashLabels(other) {
			t.Fatalf("distinct label sets collide, so two series would merge "+
				"and report a value neither observed:\n %v\n %v\n both -> %s",
				one, other, HashLabels(one))
		}

		if HashLabels(one) == "" {
			t.Fatalf("empty identity for %v", one)
		}
	})
}

func sameLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// FuzzWindowFor asserts that windowing is a total, gap-free, non-overlapping
// partition of the timeline for any instant a client can send.
//
// Every point lands in exactly one window. A timestamp that falls outside the
// window it was assigned would be accumulated into a bucket it does not belong
// to, and the resulting rollup would be wrong in a way no error ever surfaces.
func FuzzWindowFor(f *testing.F) {
	f.Add(int64(0), int64(time.Minute))
	f.Add(int64(-1), int64(time.Minute))
	f.Add(int64(1), int64(250*time.Millisecond))
	f.Add(int64(-6213559680000000000), int64(time.Hour))
	f.Add(int64(1<<62), int64(time.Second))

	f.Fuzz(func(t *testing.T, nanos, sizeNanos int64) {
		size := time.Duration(sizeNanos)
		// The config layer refuses anything under a second; above a day the
		// arithmetic is the same, so bound the search where it is useful.
		if size < time.Second || size > 24*time.Hour {
			t.Skip()
		}
		// Keep away from the extremes where time.Time itself overflows; a
		// timestamp that far out is rejected by the backfill limit long
		// before it reaches the engine.
		if nanos > 1<<61 || nanos < -(1<<61) {
			t.Skip()
		}

		ts := time.Unix(0, nanos).UTC()
		w := WindowFor(ts, size)

		if !w.Start.Before(w.End) {
			t.Fatalf("degenerate window %s for %s", w, ts)
		}
		if got := w.End.Sub(w.Start); got != size {
			t.Fatalf("window is %s wide, want %s", got, size)
		}
		// Half-open containment: [Start, End).
		if ts.Before(w.Start) || !ts.Before(w.End) {
			t.Fatalf("%s was assigned window %s, which does not contain it", ts, w)
		}
		// Idempotence: any instant inside the window maps back to it, which is
		// what makes a window key stable across the ingest and flush paths.
		if again := WindowFor(w.Start, size); again != w {
			t.Fatalf("window start %s remapped to %s", w, again)
		}
		// Alignment: boundaries are anchored to the epoch, so every replica
		// and every replay agrees on where a window begins.
		if w.Start.UnixNano()%size.Nanoseconds() != 0 {
			t.Fatalf("window %s is not aligned to a %s boundary", w, size)
		}
		// Adjacency: the next window starts exactly where this one ends, so
		// the partition has no gaps and no overlaps.
		if next := WindowFor(w.End, size); next.Start != w.End {
			t.Fatalf("gap between %s and the following window %s", w, next)
		}
	})
}
