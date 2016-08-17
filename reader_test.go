package dbf

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
)

const (
	TEST_DBF_PATH  = "./testdbf/TEST.DBF"
	BENCH_DBF_PATH = "./testdbf/TEST.DBF" //For real benchmarks replace this with the path to a large DBF/FPT combo
)

var test_dbf *DBF
var using_file bool

//use testmain to run all the tests twice
//one time with a file opened from disk and one time with a stream
func TestMain(m *testing.M) {

	fmt.Println("Running tests with file from disk...")
	using_file = true
	testOpenFile()

	result := m.Run()
	test_dbf.Close()

	if result != 0 {
		os.Exit(result)
	}

	fmt.Println("Running tests with byte stream...")
	using_file = false
	testOpenStream()

	result = m.Run()

	os.Exit(result)
}

func testOpenFile() {
	var err error
	test_dbf, err = OpenFile(TEST_DBF_PATH, new(Win1250Decoder))
	if err != nil {
		log.Fatal(err)
	}
}

func testOpenStream() {

	dbfbytes, err := ioutil.ReadFile(TEST_DBF_PATH)
	if err != nil {
		log.Fatal(err)
	}
	dbfreader := bytes.NewReader(dbfbytes)

	fptbytes, err := ioutil.ReadFile(strings.Replace(TEST_DBF_PATH, ".DBF", ".FPT", 1))
	if err != nil {
		log.Fatal(err)
	}
	fptreader := bytes.NewReader(fptbytes)

	test_dbf, err = OpenStream(dbfreader, fptreader, new(Win1250Decoder))
	if err != nil {
		log.Fatal(err)
	}
}

//Quick check if the first field matches
func TestFieldHeader(t *testing.T) {
	want := "{Name:[73 68 0 0 0 0 0 0 0 0 0] Type:73 Pos:1 Len:4 Decimals:0 Flags:0 Next:5 Step:1 Reserved:[0 0 0 0 0 0 0 78]}"
	have := fmt.Sprintf("%+v", test_dbf.fields[0])
	if have != want {
		t.Errorf("First field from header does not match signature: Want %s, have %s", want, have)
	}
}

//Test if file stat size matches header file size, only run when using file mode
func TestStatAndFileSize(t *testing.T) {
	if !using_file {
		return
	}
	stat, err := test_dbf.Stat()
	if err != nil {
		t.Fatal(err)
	}
	stat_size := stat.Size()
	hdr_size := test_dbf.header.FileSize()
	if stat_size != hdr_size {
		t.Errorf("Calculated header size: %d, stat size: %d", hdr_size, stat_size)
	}
}

//Tests if field headers have been parsed, fails if there are no fields
func TestFieldNames(t *testing.T) {
	fieldnames := test_dbf.FieldNames()
	want := 13
	if len(fieldnames) != want {
		t.Errorf("Expected %d fields, have %d", want, len(fieldnames))
	}
	//t.Log(fieldnames)
}

func TestNumFields(t *testing.T) {
	header := test_dbf.NumFields()
	header_calc := test_dbf.Header().NumFields()
	if header != header_calc {
		t.Errorf("NumFields not equal. DBF NumFields: %d, DBF Header NumField: %d", header, header_calc)
	}
}

func TestGoTo(t *testing.T) {
	err := test_dbf.GoTo(0)
	if err != nil {
		t.Error(err)
	}
	if !test_dbf.BOF() {
		t.Error("Expected to be at BOF")
	}
	err = test_dbf.GoTo(1)
	if err != nil {
		t.Error(err)
	}
	if test_dbf.EOF() {
		t.Error("Did not expect to be at EOF")
	}
	err = test_dbf.GoTo(4)
	if err != nil {
		if err != ErrEOF {
			t.Error(err)
		}
	}
	if !test_dbf.EOF() {
		t.Error("Expected to be at EOF")
	}
}

func TestSkip(t *testing.T) {
	test_dbf.GoTo(0)

	err := test_dbf.Skip(1)
	if err != nil {
		t.Error(err)
	}
	if test_dbf.EOF() {
		t.Error("Did not expect to be at EOF")
	}
	err = test_dbf.Skip(3)
	if err != nil {
		if err != ErrEOF {
			t.Error(err)
		}
	}
	if !test_dbf.EOF() {
		t.Error("Expected to be at EOF")
	}
	err = test_dbf.Skip(-20)
	if err != nil {
		if err != ErrBOF {
			t.Error(err)
		}
	}
	if !test_dbf.BOF() {
		t.Error("Expected to be at BOF")
	}
}

var want_values = []struct {
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

	for _, want := range want_values {
		pos := test_dbf.FieldPos(want.name)
		if pos != want.pos {
			t.Errorf("Wanted fieldpos %d for field %s, have pos %d", want.pos, want.name, pos)
		}
	}
}

//Tests a complete record read, reads the second record which is also deleted,
//also tests getting field values from record object
func TestRecord(t *testing.T) {
	err := test_dbf.GoTo(1)
	if err != nil {
		t.Fatal(err)
	}

	//test if the record is deleted
	deleted, err := test_dbf.Deleted()
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Errorf("Record should be deleted")
	}

	//read the same record using Record() and RecordAt()
	recs := [2]*Record{}
	recs[0], err = test_dbf.Record()
	if err != nil {
		t.Fatal(err)
	}

	recs[1], err = test_dbf.RecordAt(1)
	if err != nil {
		t.Fatal(err)
	}

	for irec, rec := range recs {
		for _, want := range want_values {
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

//Test reading fields field by field
func TestField(t *testing.T) {
	for _, want := range want_values {
		val, err := test_dbf.Field(want.pos)
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

	want1 := `{"BOOL":true,"COMP_NAME":"TEST2","COMP_OS":"Windows XP","DATUM":"2015-02-03T00:00:00Z","FLOAT":1.23456789e+08,"ID":2,"ID_NR":6425886,"MELDING":"Tësting wíth éncôdings!","NIVEAU":1,"NUMBER":1.2345678999e+08,"SOORT":12345678,"TIJD":"12:00","USERNR":-600}`
	want2 := `{"BOOL":true,"COMP_NAME":"                                        ","COMP_OS":"                    ","DATUM":"0001-01-01T00:00:00Z","FLOAT":0,"ID":4,"ID_NR":0,"MELDING":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==","NIVEAU":0,"NUMBER":0,"SOORT":0,"TIJD":"        ","USERNR":0}`

	data, err := test_dbf.RecordToJSON(1, true)
	if err != nil {
		t.Error(err)
	}
	if string(data) != want1 {
		t.Errorf("\nWanted json\n%s\nhave json\n%s\n", want1, string(data))
	}

	err = test_dbf.GoTo(3)
	if err != nil {
		t.Error(err)
	}

	data, err = test_dbf.RecordToJSON(0, false)
	if err != nil {
		t.Error(err)
	}
	if string(data) != want2 {
		t.Errorf("\nWanted json\n%s\nhave json\n%s\n", want2, string(data))
	}

}

//Close file handles
func TestClose(t *testing.T) {
	err := test_dbf.Close()
	if err != nil {
		t.Fatal(err)
	}
}

//Benchmark for reading all records sequentially
//Use a large DBF/FPT combo for more realistic results
func BenchmarkReadRecords(b *testing.B) {
	for n := 0; n < b.N; n++ {
		err := func() error {
			dbf, err := OpenFile(BENCH_DBF_PATH, new(Win1250Decoder))
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

	dbf, err := OpenFile(BENCH_DBF_PATH, new(Win1250Decoder))
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
