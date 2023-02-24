package jd

// J2YMD converts a Julian day number to a year, month and day
// y, m, d := jd.J2YMD(2453738);
// y==2006 && m==1 && d==2 //=> true
func J2YMD(d int) (int, int, int) {
	l := d + 68569
	n := 4 * l / 146097
	l = l - (146097*n+3)/4
	i := 4000 * (l + 1) / 1461001
	l = l - 1461*i/4 + 31
	j := 80 * l / 2447
	k := l - 2447*j/80
	l = j / 11
	j = j + 2 - 12*l
	i = 100*(n-49) + i + l
	return i, j, k
}
