package dbf

import "testing"

//TODO: Create Test DBF files and commit them for more detailed tests
//For now tests are a bit basic
const (
	DBF_PATH = "./testdbf/TEST.DBF"
)

var test_dbf *DBF

func TestOpenFile(t *testing.T) {
	var err error
	test_dbf, err = OpenFile(DBF_PATH)
	if err != nil {
		t.Fatal(err)
	}

}

//Test if the modified date of Stat() matches the header
//This is therefore also a header test, these dates should be equal, but not sure if this is always true on every OS
func TestStat(t *testing.T) {
	stat, err := test_dbf.Stat()
	if err != nil {
		t.Fatal(err)
	}
	stat_mod := stat.ModTime()
	hdr_mod := test_dbf.Header.Modified()
	format := "20060102"
	if stat_mod.Format(format) != hdr_mod.Format(format) {
		t.Errorf("Modified date in header (%s) not equal to modified date in OS (%s)", hdr_mod.Format(format), stat_mod.Format(format))
	}
}

//Tests if field headers have been parsed, fails if there are no fields
func TestFieldNames(t *testing.T) {
	fieldnames := test_dbf.FieldNames()
	if len(fieldnames) == 0 {
		t.Error("No fieldnames found")
	}
	t.Log(fieldnames)
}

func TestFieldPos(t *testing.T) {

	cases := []struct {
		name string
		pos  int
	}{
		{"NIVEAU", 0},
		{"BLABLA", -1},
		{"MELDING", 8},
	}
	for _, test := range cases {
		pos := test_dbf.FieldPos(test.name)
		if pos != test.pos {
			t.Errorf("Expected field %s at pos %d, found pos %d", test.name, test.pos, pos)
		}
	}
}

func TestClose(t *testing.T) {
	err := test_dbf.Close()
	if err != nil {
		t.Fatal(err)
	}
}
