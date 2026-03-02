package util

import (
	"fmt"
	"time"
)

// SparkTimeLayout is the timestamp format used by the Spark History Server REST API.
// The "GMT" suffix is a hardcoded literal and the calendar is always UTC.
// See: core/src/main/scala/org/apache/spark/status/api/v1/JacksonMessageWriter.scala
const SparkTimeLayout = "2006-01-02T15:04:05.000GMT"

func ParseSparkTime(s string) (time.Time, error) {
	return time.Parse(SparkTimeLayout, s)
}

// FormatBytes formats a byte count as a human-readable string.
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func Deref[T any](p *T) T {
	if p != nil {
		return *p
	}
	var zero T
	return zero
}

func Ptr[T any](v T) *T { return &v }
