package dbf

import (
	"strings"
	"time"
)

// This file contains some helper casting functions for the interface values returned from the field methods.

// ToString always returns a string
func ToString(in interface{}) string {
	if str, ok := in.(string); ok {
		return str
	}
	return ""
}

// ToTrimmedString always returns a string with spaces trimmed
func ToTrimmedString(in interface{}) string {
	if str, ok := in.(string); ok {
		return strings.TrimSpace(str)
	}
	return ""
}

// ToInt64 always returns an int64
func ToInt64(in interface{}) int64 {
	if i, ok := in.(int64); ok {
		return i
	}
	return 0
}

// ToFloat64 always returns a float64
func ToFloat64(in interface{}) float64 {
	if f, ok := in.(float64); ok {
		return f
	}
	return 0.0
}

// ToTime always returns a time.Time
func ToTime(in interface{}) time.Time {
	if t, ok := in.(time.Time); ok {
		return t
	}
	return time.Time{}
}

// ToBool always returns a boolean
func ToBool(in interface{}) bool {
	if b, ok := in.(bool); ok {
		return b
	}
	return false
}
