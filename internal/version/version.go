package version

import (
	"fmt"
	"runtime"
)

var (
	// Version is the semantic version of the application
	Version = "dev"
	// GitCommit is the git commit hash
	GitCommit = "unknown"
	// BuildTime is the build timestamp
	BuildTime = "unknown"
)

// Info returns version information as a map
func Info() map[string]string {
	return map[string]string{
		"name":      "um-ingest-server",
		"version":   Version,
		"gitCommit": GitCommit,
		"buildTime": BuildTime,
		"goVersion": runtime.Version(),
	}
}

// String returns a formatted version string
func String() string {
	return fmt.Sprintf("um-ingest-server %s (commit %s, built %s)", Version, GitCommit, BuildTime)
}

