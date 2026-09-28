package httpx

import "net/http"

// AdmitConcurrent bounds handlers before they decode bodies or allocate query
// results. It does not queue: overload must not retain unbounded waiting work.
// Mount on data routes so health probes remain available at capacity.
func AdmitConcurrent(limit int) Middleware {
	if limit <= 0 {
		limit = 4
	}
	slots := make(chan struct{}, limit)
	return func(next http.Handler) http.Handler {
		return Handler(func(w http.ResponseWriter, r *http.Request) error {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				w.Header().Set("Retry-After", "1")
				return Unavailable("The service is at capacity. Retry shortly.")
			}
			next.ServeHTTP(w, r)
			return nil
		})
	}
}
