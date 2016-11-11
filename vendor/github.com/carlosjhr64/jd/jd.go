// http://aa.usno.navy.mil/faq/docs/JD_Formula.php
package jd

import (
  "errors"
  "strings"
  "unicode"
  "strconv"
  "time"
)

const VERSION = "0.0.1"

// jd.YMD2J(2006, 1, 2) == 2453738 //=> true
func YMD2J(i, j, k int) int {
  return k - 32075 +
  1461 * (i + 4800 + (j - 14) / 12) / 4 +
  367 * (j - 2 - (j - 14) / 12 * 12) / 12 -
  3 * ((i + 4900 + (j - 14) / 12) / 100) / 4
}

// y, m, d := jd.J2YMD(2453738);
// y==2006 && m==1 && d==2 //=> true
func J2YMD(d int) (int, int, int) {
  l := d + 68569
  n := 4 * l / 146097
  l = l - (146097 * n + 3) / 4
  i := 4000 * (l + 1) / 1461001
  l = l - 1461 * i / 4 + 31
  j := 80 * l / 2447
  k := l - 2447 * j / 80
  l = j / 11
  j = j + 2 - 12 * l
  i = 100 * (n - 49) + i + l
  return i, j, k
}

// number, err = jd.ToNumber("2006-01-02");
// number == 2453738 //=> true
func ToNumber(date string) (int, error) {
  d := strings.FieldsFunc(date, unicode.IsPunct)
  if len(d) != 3 {
    return 0, errors.New("Expected 3 punctuation delimited fields.")
  }
  i, ei := strconv.Atoi(d[0])
  j, ej := strconv.Atoi(d[1])
  k, ek := strconv.Atoi(d[2])
  if ei != nil || ej != nil || ek != nil {
    return 0, errors.New("Expected 3 integers.")
  }
  return YMD2J(i, j, k), nil
}

// "2006-01-02" == jd.ToDate(2453738) //=> true
func ToDate(number int) string {
  i, j, k := J2YMD(number)
  is := strconv.Itoa(i)
  js := strconv.Itoa(j)
  ks := strconv.Itoa(k)
  if j < 10 { js = "0" + js }
  if k < 10 { ks = "0" + ks }
  return is + "-" + js + "-" + ks
}

// number = jd.Number(time)
func Number(t time.Time) int {
  n, e := ToNumber(t.Format("2006-01-02"))
  if e != nil { panic(e) }
  return n
}
