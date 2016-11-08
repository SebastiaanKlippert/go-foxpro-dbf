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

var testDbf *DBF
var usingFile bool

//use testmain to run all the tests twice
//one time with a file opened from disk and one time with a stream
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
	testDbf, err = OpenFile(TEST_DBF_PATH, new(Win1250Decoder))
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

	testDbf, err = OpenStream(dbfreader, fptreader, new(Win1250Decoder))
	if err != nil {
		log.Fatal(err)
	}
}

//Quick check if the first field matches
func TestFieldHeader(t *testing.T) {
	want := "{Name:[73 68 0 0 0 0 0 0 0 0 0] Type:73 Pos:1 Len:4 Decimals:0 Flags:0 Next:5 Step:1 Reserved:[0 0 0 0 0 0 0 78]}"
	have := fmt.Sprintf("%+v", testDbf.fields[0])
	if have != want {
		t.Errorf("First field from header does not match signature: Want %s, have %s", want, have)
	}
}

//Test if file stat size matches header file size, only run when using file mode
func TestStatAndFileSize(t *testing.T) {
	if !usingFile {
		return
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
}

//Tests if field headers have been parsed, fails if there are no fields
func TestFieldNames(t *testing.T) {
	fieldnames := testDbf.FieldNames()
	want := 13
	if len(fieldnames) != want {
		t.Errorf("Expected %d fields, have %d", want, len(fieldnames))
	}
	//t.Log(fieldnames)
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

//Tests a complete record read, reads the second record which is also deleted,
//also tests getting field values from record object
func TestRecord(t *testing.T) {
	err := testDbf.GoTo(1)
	if err != nil {
		t.Fatal(err)
	}

	//test if the record is deleted
	deleted, err := testDbf.Deleted()
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("Record should be deleted")
	}

	//read the same record using Record() and RecordAt()
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

//Test reading fields field by field
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

	want1 := `{"BOOL":true,"COMP_NAME":"TEST2","COMP_OS":"Windows XP","DATUM":"2015-02-03T00:00:00Z","FLOAT":1.23456789e+08,"ID":2,"ID_NR":6425886,"MELDING":"Tësting wíth éncôdings!","NIVEAU":1,"NUMBER":1.2345678999e+08,"SOORT":12345678,"TIJD":"12:00","USERNR":-600}`
	want2 := `{"BOOL":true,"COMP_NAME":"                                        ","COMP_OS":"                    ","DATUM":"0001-01-01T00:00:00Z","FLOAT":0,"ID":4,"ID_NR":0,"MELDING":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==","NIVEAU":0,"NUMBER":0,"SOORT":0,"TIJD":"        ","USERNR":0}`

	data, err := testDbf.RecordToJSON(1, true)
	if err != nil {
		t.Error(err)
	}
	if string(data) != want1 {
		t.Errorf("\nWanted json\n%s\nhave json\n%s\n", want1, string(data))
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

//Close file handles
func TestClose(t *testing.T) {
	err := testDbf.Close()
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
