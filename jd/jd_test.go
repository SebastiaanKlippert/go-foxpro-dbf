package jd

import (
	"fmt"
	"testing"
)

func ymd(y, m, d int) string {
	return fmt.Sprintf("%04d-%02d-%02d", y, m, d)
}

func TestJ2YMD(t *testing.T) {
	cases := []struct {
		jdate int
		want  string
	}{
		{2453738, "2006-01-02"},
		{2460131, "2023-07-05"},
		{2440588, "1970-01-01"},
		{2451544, "1999-12-31"},
		{2487763, "2099-02-28"},
	}
	for _, c := range cases {
		y, m, d := J2YMD(c.jdate)
		if ymd(y, m, d) != c.want {
			t.Errorf("Julian date %d: want %s, have %s", c.jdate, c.want, ymd(y, m, d))
		}
	}
}
