package dbf

import (
	"testing"
	"time"
)

func TestToFloat64(t *testing.T) {
	if ToFloat64(123.456) != float64(123.456) {
		t.Errorf("Want %f, have %f", float64(123.456), ToFloat64(123.456))
	}
	if ToFloat64("123.456") != float64(0) {
		t.Errorf("Want %f, have %f", 0.0, ToFloat64(123.456))
	}
}

func TestToInt64(t *testing.T) {
	if ToInt64(int64(123456)) != int64(123456) {
		t.Errorf("Want %d, have %d", int64(123456), ToInt64(123456))
	}
	if ToInt64("123.456") != int64(0) {
		t.Errorf("Want %d, have %d", 0, ToInt64(123456))
	}
}

func TestToString(t *testing.T) {
	if ToString("Hêllo!") != "Hêllo!" {
		t.Errorf("Want %q, have %q", "Hêllo!", ToString("Hêllo!"))
	}
	if ToString(123.456) != "" {
		t.Errorf("Want %q, have %q", "", ToString(123.456))
	}
}

func TestToTrimmedString(t *testing.T) {
	if ToTrimmedString("Hêllo!      ") != "Hêllo!" {
		t.Errorf("Want %q, have %q", "Hêllo!", ToTrimmedString("Hêllo!    "))
	}
	if ToTrimmedString(123.456) != "" {
		t.Errorf("Want %q, have %q", "", ToTrimmedString(123.456))
	}
}

func TestToTime(t *testing.T) {
	now := time.Now()
	if ToTime(now).Equal(now) == false {
		t.Errorf("Want %v, have %v", now, ToTime(now))
	}
	if ToTime("123.456").IsZero() == false {
		t.Errorf("Want %v, have %v", time.Time{}, ToTime("123.456"))
	}
}

func TestToBool(t *testing.T) {
	if ToBool(true) == false {
		t.Error("Want true")
	}
	if ToBool(33) != false {
		t.Error("Want false")
	}
}
