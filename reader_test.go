package dbf

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testDbf *DBF
var usingFile bool

// use testmain to run all the tests twice
// one time with a file opened from disk and one time with a stream
func TestMain(m *testing.M) {

	fmt.Println("Running tests with file from disk...")
	usingFile = true
	testOpenFile()

	result := m.Run()
	testDbf.Close()

	if result != 0 {
		os.Exit(result)
	}

	fmt.Println("Running tests with byte stream...")
	usingFile = false
	testOpenStream()

	result = m.Run()

	os.Exit(result)
}

func testOpenFile() {
	var err error

	testDbf, err = OpenFile(filepath.Join("testdata", "TEST.DBF"), new(Win1250Decoder))
	if err != nil {
		log.Fatal(err)
	}
}

func testOpenStream() {

	dbfbytes, err := ioutil.ReadFile(filepath.Join("testdata", "TEST.DBF"))
	if err != nil {
		log.Fatal(err)
	}
	dbfreader := bytes.NewReader(dbfbytes)

	fptbytes, err := ioutil.ReadFile(filepath.Join("testdata", "TEST.FPT"))
	if err != nil {
		log.Fatal(err)
	}
	fptreader := bytes.NewReader(fptbytes)

	testDbf, err = OpenStream(dbfreader, fptreader, new(Win1250Decoder))
	if err != nil {
		log.Fatal(err)
	}
}

// Quick check if the first field matches
func TestFieldHeader(t *testing.T) {
	want := "{Name:[73 68 0 0 0 0 0 0 0 0 0] Type:73 Pos:1 Len:4 Decimals:0 Flags:0 Next:5 Step:1 Reserved:[0 0 0 0 0 0 0 78]}"
	have := fmt.Sprintf("%+v", testDbf.fields[0])
	if have != want {
		t.Errorf("First field from header does not match signature: Want %s, have %s", want, have)
	}
}

// Test if file stat size matches header file size, only run when using file mode
func TestStatAndFileSize(t *testing.T) {
	if !usingFile {
		t.Skip("Stat and FileSize not testing when using stream")
	}
	stat, err := testDbf.Stat()
	if err != nil {
		t.Fatal(err)
	}
	statSize := stat.Size()
	hdrSize := testDbf.header.FileSize()
	if statSize != hdrSize {
		t.Errorf("Calculated header size: %d, stat size: %d", hdrSize, statSize)
	}
	stat, err = testDbf.StatFPT()
	if err != nil {
		t.Fatal(err)
	}
	fptbytes, err := ioutil.ReadFile(filepath.Join("testdata", "TEST.FPT"))
	if err != nil {
		log.Fatal(err)
	}
	if stat.Size() != int64(len(fptbytes)) {
		t.Errorf("Real FPT size: %d, stat size: %d", len(fptbytes), stat.Size())
	}
	if testDbf.NumRecords() != uint32(4) {
		t.Errorf("Want 4 records, have %d", testDbf.NumRecords())
	}
	if len(testDbf.Fields()) != 13 {
		t.Errorf("Want 13 fields, have %d", len(testDbf.Fields()))
	}
	// Test modified date, because we use time.Local to represent the modified date it can change depending on the system we run
	modified := testDbf.Header().Modified().UTC()
	if modified.Format("2006-01") != "2015-09" || modified.Day() < 14 || modified.Day() > 16 {
		t.Errorf("Want modified date between 2015-09-14 and 2015-09-16, have %s", modified.Format("2006-01-02"))
	}
}

// Tests if field headers have been parsed, fails if there are no fields
func TestFieldNames(t *testing.T) {
	fieldnames := testDbf.FieldNames()
	want := 13
	if len(fieldnames) != want {
		t.Errorf("Expected %d fields, have %d", want, len(fieldnames))
	}
	// t.Log(fieldnames)
}

func TestNumFields(t *testing.T) {
	header := testDbf.NumFields()
	headerCalc := testDbf.Header().NumFields()
	if header != headerCalc {
		t.Errorf("NumFields not equal. DBF NumFields: %d, DBF Header NumField: %d", header, headerCalc)
	}
}

func TestGoTo(t *testing.T) {
	err := testDbf.GoTo(0)
	if err != nil {
		t.Error(err)
	}
	if !testDbf.BOF() {
		t.Error("Expected to be at BOF")
	}
	err = testDbf.GoTo(1)
	if err != nil {
		t.Error(err)
	}
	if testDbf.EOF() {
		t.Error("Did not expect to be at EOF")
	}
	err = testDbf.GoTo(4)
	if err != nil {
		if err != ErrEOF {
			t.Error(err)
		}
	}
	if !testDbf.EOF() {
		t.Error("Expected to be at EOF")
	}
}

func TestSkip(t *testing.T) {
	testDbf.GoTo(0)

	err := testDbf.Skip(1)
	if err != nil {
		t.Error(err)
	}
	if testDbf.EOF() {
		t.Error("Did not expect to be at EOF")
	}
	err = testDbf.Skip(3)
	if err != nil {
		if err != ErrEOF {
			t.Error(err)
		}
	}
	if !testDbf.EOF() {
		t.Error("Expected to be at EOF")
	}
	err = testDbf.Skip(-20)
	if err != nil {
		if err != ErrBOF {
			t.Error(err)
		}
	}
	if !testDbf.BOF() {
		t.Error("Expected to be at BOF")
	}
}

var wantValues = []struct {
	pos                   int
	name, strval, strtype string
}{
	{0, "ID", "2", "int32"},
	{10, "NUMBER", "1.2345678999e+08", "float64"},
	{12, "BOOL", "true", "bool"},
	{2, "DATUM", "2015-02-03 00:00:00 +0000 UTC", "time.Time"},
	{7, "COMP_NAME", "TEST2", "string"},
}

func TestFieldPos(t *testing.T) {

	for _, want := range wantValues {
		pos := testDbf.FieldPos(want.name)
		if pos != want.pos {
			t.Errorf("Wanted fieldpos %d for field %s, have pos %d", want.pos, want.name, pos)
		}
	}
}

// Tests a complete record read, reads the second record which is also deleted,
// also tests getting field values from record object
func TestRecord(t *testing.T) {
	err := testDbf.GoTo(1)
	if err != nil {
		t.Fatal(err)
	}

	// test if the record is deleted
	deleted, err := testDbf.Deleted()
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("Record should be deleted")
	}

	// read the same record using Record() and RecordAt()
	recs := [2]*Record{}
	recs[0], err = testDbf.Record()
	if err != nil {
		t.Fatal(err)
	}

	recs[1], err = testDbf.RecordAt(1)
	if err != nil {
		t.Fatal(err)
	}

	for irec, rec := range recs {
		for _, want := range wantValues {
			val, err := rec.Field(want.pos)
			if err != nil {
				t.Error(err)
			}
			strval := strings.TrimSpace(fmt.Sprintf("%v", val))
			strtype := fmt.Sprintf("%T", val)

			if want.strval != strval || want.strtype != strtype {
				t.Errorf("Record %d: Wanted value %s with type %s, have value %s with type %s", irec, want.strval, want.strtype, strval, strtype)
			}
		}
	}
}

// Test reading fields field by field
func TestField(t *testing.T) {
	for _, want := range wantValues {
		val, err := testDbf.Field(want.pos)
		if err != nil {
			t.Error(err)
		}
		strval := strings.TrimSpace(fmt.Sprintf("%v", val))
		strtype := fmt.Sprintf("%T", val)

		if want.strval != strval || want.strtype != strtype {
			t.Errorf("Wanted value %s with type %s, have value %s with type %s", want.strval, want.strtype, strval, strtype)
		}
	}
}

func TestRecordToJson(t *testing.T) {
	// below go 1.8 we want want1, for 1.8 and up we want want12
	want1 := `{"BOOL":true,"COMP_NAME":"TEST2","COMP_OS":"Windows XP","DATUM":"2015-02-03T00:00:00Z","FLOAT":1.23456789e+08,"ID":2,"ID_NR":6425886,"MELDING":"Tësting wíth éncôdings!","NIVEAU":1,"NUMBER":1.2345678999e+08,"SOORT":12345678,"TIJD":"12:00","USERNR":-600}`
	want12 := `{"BOOL":true,"COMP_NAME":"TEST2","COMP_OS":"Windows XP","DATUM":"2015-02-03T00:00:00Z","FLOAT":123456789,"ID":2,"ID_NR":6425886,"MELDING":"Tësting wíth éncôdings!","NIVEAU":1,"NUMBER":123456789.99,"SOORT":12345678,"TIJD":"12:00","USERNR":-600}`

	want2 := `{"BOOL":true,"COMP_NAME":"                                        ","COMP_OS":"                    ","DATUM":"0001-01-01T00:00:00Z","FLOAT":0,"ID":4,"ID_NR":0,"MELDING":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==","NIVEAU":0,"NUMBER":0,"SOORT":0,"TIJD":"        ","USERNR":0}`

	err := testDbf.GoTo(3)
	if err != nil {
		t.Error(err)
	}

	data, err := testDbf.RecordToJSON(1, true)
	if err != nil {
		t.Error(err)
	}
	if string(data) != want1 && string(data) != want12 {
		t.Errorf("\nWanted json\n%s\nor%s\nhave json\n%s\n", want1, want2, string(data))
	}

	err = testDbf.GoTo(3)
	if err != nil {
		t.Error(err)
	}

	data, err = testDbf.RecordToJSON(0, false)
	if err != nil {
		t.Error(err)
	}
	if string(data) != want2 {
		t.Errorf("\nWanted json\n%s\nhave json\n%s\n", want2, string(data))
	}

}

// Close file handles
func TestClose(t *testing.T) {
	err := testDbf.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// TestDbase30 runs some test against dbase_30.dbf which has more complex column types
func TestDbase30(t *testing.T) {

	if !usingFile {
		t.Skip("TestDbase30 is only tested from disk")
	}

	dbf, err := OpenFile(filepath.Join("testdata", "dbase_30.dbf"), new(Win1250Decoder))
	if err != nil {
		t.Fatal(err)
	}
	defer dbf.Close()

	err = dbf.GoTo(13)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := dbf.Record()
	if err != nil {
		t.Fatal(err)
	}

	fields := rec.FieldSlice()
	if err != nil {
		t.Fatal(err)
	}

	// test value and type of caption field
	caption, ok := fields[5].(string)
	if !ok {
		t.Error("caption field should be of type string")
	}
	wc := "Joh L. McWilliams & son"
	if strings.TrimSpace(caption) != wc {
		t.Errorf("Want caption value %q, have %q", wc, strings.TrimSpace(caption))
	}

	// test value and type of classes field
	classes, ok := fields[10].(string)
	if !ok {
		t.Error("classes field should be of type string")
	}
	wcl := "People\r\nMcWilliams"
	if strings.TrimSpace(classes) != wcl {
		t.Errorf("Want classes value %q, have %q", wc, strings.TrimSpace(classes))
	}

	// test value and type of catdate field
	catdate, ok := fields[8].(time.Time)
	if !ok {
		t.Error("catdate field should be of type time.Time")
	}
	wcd := time.Date(2000, 6, 29, 0, 0, 0, 0, time.UTC)
	if catdate.Equal(wcd) == false {
		t.Errorf("Want catdate value %v, have %v", wcd, catdate)
	}

	// test value and type of flagdate field
	flagdate, ok := fields[38].(time.Time)
	if !ok {
		t.Error("flagdate field should be of type time.Time")
	}
	wfd := time.Date(1982, 7, 5, 15, 34, 0, 0, time.UTC)
	if flagdate.Equal(wfd) == false {
		t.Errorf("Want flagdate value %v, have %v", wfd, flagdate)
	}

}

// TestDbase31 runs some test against dbase_31.dbf which has more complex column types
func TestDbase31(t *testing.T) {

	if !usingFile {
		t.Skip("TestDbase31 is only tested from disk")
	}

	dbf, err := OpenFile(filepath.Join("testdata", "dbase_31.dbf"), new(Win1250Decoder))
	if err != nil {
		t.Fatal(err)
	}
	defer dbf.Close()

	err = dbf.GoTo(28)
	if err != nil {
		t.Fatal(err)
	}

	// test value and type of PRODUCTNAM field
	val, err := dbf.Field(dbf.FieldPos("PRODUCTNAM"))
	if err != nil {
		t.Fatal(err)
	}
	name, ok := val.(string)
	if !ok {
		t.Error("PRODUCTNAM field should be of type string")
	}
	wn := "Thüringer Rostbratwurst"
	if strings.TrimSpace(name) != wn {
		t.Errorf("Want PRODUCTNAM value %q, have %q", wn, strings.TrimSpace(name))
	}

	// test value and type of CATEGORYID field
	val, err = dbf.Field(dbf.FieldPos("CATEGORYID"))
	if err != nil {
		t.Fatal(err)
	}
	cat, ok := val.(int32)
	if !ok {
		t.Error("CATEGORYID field should be of type int32")
	}
	wcat := int32(6)
	if cat != wcat {
		t.Errorf("Want CATEGORYID value %d, have %d", wcat, cat)
	}

	// test value and type of UNITPRICE field
	val, err = dbf.Field(dbf.FieldPos("UNITPRICE"))
	if err != nil {
		t.Fatal(err)
	}
	price, ok := val.(float64)
	if !ok {
		t.Error("UNITPRICE field should be of type float64")
	}
	wprice := float64(123.79)
	if price != wprice {
		t.Errorf("Want UNITPRICE value %f, have %f", wprice, price)
	}

	// Test no FPT errors
	_, err = dbf.StatFPT()
	if err == nil {
		t.Errorf("Want error %s, have no error", ErrNoFPTFile)
	} else if err != ErrNoFPTFile {
		t.Errorf("Want error %s, have error %s", ErrNoFPTFile, err)
	}
}

// TestDkeza runs some test against dkeza.dbf, added in issue #2
func TestDkeza(t *testing.T) {

	if !usingFile {
		t.Skip("TestDkeza is only tested from disk")
	}

	dbf, err := OpenFile(filepath.Join("testdata", "dkeza.dbf"), new(UTF8Decoder))
	if err != nil {
		t.Fatal(err)
	}
	defer dbf.Close()

	err = dbf.GoTo(0)
	if err != nil {
		t.Fatal(err)
	}

	// test value and type of NUMBER field
	val, err := dbf.Field(dbf.FieldPos("NUMBER"))
	if err != nil {
		t.Fatal(err)
	}
	number, ok := val.(int64)
	if !ok {
		t.Error("NUMBER field should be of type int64")
	}
	wn := int64(8027846523)
	if number != wn {
		t.Errorf("Want NUMBER value %d, have %d", wn, number)
	}

	// test value and type of CURR field
	val, err = dbf.Field(dbf.FieldPos("CURR"))
	if err != nil {
		t.Fatal(err)
	}
	curr, ok := val.(float64)
	if !ok {
		t.Error("CURR field should be of type float64")
	}
	wc := 1234567890.1234
	if number != wn {
		t.Errorf("Want CURR value %f, have %f", wc, curr)
	}

	// test value and type of DTIME field
	val, err = dbf.Field(dbf.FieldPos("DTIME"))
	if err != nil {
		t.Fatal(err)
	}
	dtime, ok := val.(time.Time)
	if !ok {
		t.Error("DTIME field should be of type time.Time")
	}
	wdt := time.Date(2016, 11, 14, 9, 10, 44, 0, time.UTC)
	if dtime.Equal(wdt) == false {
		t.Errorf("Want DTIME value %s, have %s", wdt, dtime)
	}
	t.Logf("DTIME: %s", dtime)
}

func TestSetValidFileVersionFunc(t *testing.T) {

	// open the file without overriding the validation function
	_, err := OpenFile(filepath.Join("testdata", "dbase_03.dbf"), new(Win1250Decoder))
	if err == nil || strings.HasPrefix(err.Error(), "untested") == false {
		t.Fatal("expected to have an error when opening untested file dbase_03.dbf")
	}

	// override function
	SetValidFileVersionFunc(func(version byte) error {
		if version == 0x03 {
			return nil
		}
		return errors.New("not 0x03")
	})
	defer SetValidFileVersionFunc(validFileVersion)

	dbf, err := OpenFile(filepath.Join("testdata", "dbase_03.dbf"), new(Win1250Decoder))
	if err != nil {
		t.Fatalf("expected no error, have %s:", err)
	}
	defer dbf.Close()
}

func ExampleSetValidFileVersionFunc() {
	// create function which checks that only file flag 0x03 is valid
	validFileVersionFunc := func(version byte) error {
		if version == 0x03 {
			return nil
		}
		return errors.New("not 0x03")
	}

	// set the new function as verifier
	SetValidFileVersionFunc(validFileVersionFunc)

	// open DBF as usual
	dbf, err := OpenFile("/var/test.dbf", new(Win1250Decoder))
	if err != nil {
		log.Fatal(err)
	}
	defer dbf.Close()
}

// Benchmark for reading all records sequentially
// Use a large DBF/FPT combo for more realistic results
func BenchmarkReadRecords(b *testing.B) {
	for n := 0; n < b.N; n++ {
		err := func() error {
			dbf, err := OpenFile(filepath.Join("testdata", "dbase_30.dbf"), new(Win1250Decoder))
			if err != nil {
				return err
			}
			defer dbf.Close()
			for i := uint32(0); i < dbf.NumRecords(); i++ {
				_, err := dbf.Record()
				if err != nil {
					return err
				}
				dbf.Skip(1)
			}
			return nil
		}()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRecordToJSONWithTrim(b *testing.B) {

	dbf, err := OpenFile(filepath.Join("testdata", "dbase_30.dbf"), new(Win1250Decoder))
	if err != nil {
		b.Fatal(err)
	}
	defer dbf.Close()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err = dbf.RecordToJSON(1, true)
		if err != nil {
			b.Fatal(err)
		}
	}
}
