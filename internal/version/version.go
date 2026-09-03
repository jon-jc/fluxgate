// Package version exposes build provenance for the running binary.
//
// Values are injected at link time by the build (see the Makefile) and fall
// back to the metadata the Go toolchain embeds in every module build, so a
// plain "go build" still reports a usable revision.
package version

import (
	"runtime"
	"runtime/debug"
	"sync"
)

// Injected via -ldflags "-X github.com/jon-jc/fluxgate/internal/version.commit=...".
var (
	version = ""
	commit  = ""
	date    = ""
)

// Info describes the provenance of the running binary.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
}

var (
	once     sync.Once
	resolved Info
)

// Get returns the build provenance, resolving it from link-time flags first
// and from the embedded module metadata second. The result is computed once
// and is safe for concurrent use.
func Get() Info {
	once.Do(func() {
		resolved = Info{
			Version:   version,
			Commit:    commit,
			BuildDate: date,
			GoVersion: runtime.Version(),
			Platform:  runtime.GOOS + "/" + runtime.GOARCH,
		}

		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision":
					if resolved.Commit == "" {
						resolved.Commit = s.Value
					}
				case "vcs.time":
					if resolved.BuildDate == "" {
						resolved.BuildDate = s.Value
					}
				case "vcs.modified":
					// A binary built from a dirty tree is not reproducible;
					// say so rather than reporting a commit that does not
					// describe the code actually running.
					if s.Value == "true" && resolved.Commit != "" {
						resolved.Commit += "-dirty"
					}
				}
			}
		}

		if resolved.Version == "" {
			resolved.Version = "dev"
		}
		if resolved.Commit == "" {
			resolved.Commit = "unknown"
		}
	})
	return resolved
}

// Short returns a compact "version (commit)" string for log preambles.
func Short() string {
	i := Get()
	c := i.Commit
	if len(c) > 12 {
		c = c[:12]
	}
	return i.Version + " (" + c + ")"
}
