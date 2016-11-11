//Package dbf provides code for reading data from FoxPro DBF/FPT files
package dbf

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/carlosjhr64/jd"
)

var (
	//ErrEOF is returned when on end of DBF file (after the last record)
	ErrEOF = errors.New("EOF")

	//ErrBOF is returned when the record pointer is attempted to be moved before the first record
	ErrBOF = errors.New("BOF")

	//ErrIncomplete is returned when the read of a record or field did not complete
	ErrIncomplete = errors.New("Incomplete read")

	//ErrInvalidField is returned when an invalid fieldpos is used (<1 or >NumFields)
	ErrInvalidField = errors.New("Invalid field pos")

	//ErrNoFPTFile is returned when there should be an FPT file but it is not found on disc
	ErrNoFPTFile = errors.New("No FPT file")

	//ErrNoDBFFile is returned when a file operation is attempted on a DBF but a reader is open
	ErrNoDBFFile = errors.New("No DBF file")
)

//ReaderAtSeeker is used when opening files from memory
type ReaderAtSeeker interface {
	io.ReadSeeker
	io.ReaderAt
}

//DBF is the main DBF struct which provides all methods for reading files and embeds the file readers and handlers.
type DBF struct {
	header    *DBFHeader
	fptheader *FPTHeader

	r    ReaderAtSeeker
	fptr ReaderAtSeeker

	//os.File handlers are only used with disk files
	f    *os.File
	fptf *os.File

	dec Decoder

	fields []FieldHeader

	recpointer uint32 //internal record pointer, can be moved using Skip() and GoTo()
}

//Close closes the file handlers to the disk files.
//The caller is responsible for calling Close to close the file handle(s)!
func (dbf *DBF) Close() error {
	var dbferr, fpterr error
	if dbf.f != nil {
		dbferr = dbf.f.Close()
	}
	if dbf.fptf != nil {
		fpterr = dbf.fptf.Close()
	}
	switch {
	case dbferr != nil:
		return fmt.Errorf("Error closing DBF: %s", dbferr)
	case fpterr != nil:
		return fmt.Errorf("Error closing FPT: %s", fpterr)
	default:
		return nil
	}
}

func (dbf *DBF) prepareFPT(fptfile ReaderAtSeeker) error {

	fptheader, err := readFPTHeader(fptfile)
	if err != nil {
		return err
	}

	dbf.fptr = fptfile
	dbf.fptheader = fptheader
	return nil
}

//Header returns the DBF Header struct for inspecting
func (dbf *DBF) Header() *DBFHeader {
	return dbf.header
}

//Stat returns the os.FileInfo for the DBF file
func (dbf *DBF) Stat() (os.FileInfo, error) {
	if dbf.f == nil {
		return nil, ErrNoDBFFile
	}
	return dbf.f.Stat()
}

//StatFPT returns the os.FileInfo for the FPT file
func (dbf *DBF) StatFPT() (os.FileInfo, error) {
	if dbf.fptf == nil {
		return nil, ErrNoFPTFile
	}
	return dbf.fptf.Stat()
}

//NumRecords returns the number of records
func (dbf *DBF) NumRecords() uint32 {
	return dbf.header.NumRec
}

//Fields returns all the FieldHeaders
func (dbf *DBF) Fields() []FieldHeader {
	return dbf.fields
}

//NumFields returns the number of fields
func (dbf *DBF) NumFields() uint16 {
	return uint16(len(dbf.fields))
}

//FieldNames returnes a slice of all the fieldnames
func (dbf *DBF) FieldNames() []string {
	num := len(dbf.fields)
	names := make([]string, num)
	for i := 0; i < num; i++ {
		names[i] = dbf.fields[i].FieldName()
	}
	return names
}

//FieldPos returns the zero-based field position of a fieldname
//or -1 if not found.
func (dbf *DBF) FieldPos(fieldname string) int {
	for i := 0; i < len(dbf.fields); i++ {
		if dbf.fields[i].FieldName() == fieldname {
			return i
		}
	}
	return -1
}

//GoTo sets the internal record pointer to record recno (zero based).
//Returns ErrEOF if at EOF and positions the pointer at lastRec+1.
func (dbf *DBF) GoTo(recno uint32) error {
	if recno >= dbf.header.NumRec {
		dbf.recpointer = dbf.header.NumRec
		return ErrEOF
	}
	dbf.recpointer = recno
	return nil
}

//Skip adds offset to the internal record pointer.
//Returns ErrEOF if at EOF and positions the pointer at lastRec+1.
//Returns ErrBOF is recpointer would be become negative and positions the pointer at 0.
//Does not skip deleted records.
func (dbf *DBF) Skip(offset int64) error {
	newval := int64(dbf.recpointer) + offset
	if newval >= int64(dbf.header.NumRec) {
		dbf.recpointer = dbf.header.NumRec
		return ErrEOF
	}
	if newval < 0 {
		dbf.recpointer = 0
		return ErrBOF
	}
	dbf.recpointer = uint32(newval)
	return nil
}

//Record reads the complete record the internal record pointer is pointing to
func (dbf *DBF) Record() (*Record, error) {
	data, err := dbf.readRecord(dbf.recpointer)
	if err != nil {
		return nil, err
	}
	return dbf.bytesToRecord(data)
}

//RecordAt reads the complete record number nrec
func (dbf *DBF) RecordAt(nrec uint32) (*Record, error) {
	data, err := dbf.readRecord(nrec)
	if err != nil {
		return nil, err
	}
	return dbf.bytesToRecord(data)
}

//RecordToMap returns a complete record as a map.
//If nrec > 0 it returns the record at nrec, if nrec <= 0 it returns the record at dbf.recpointer
func (dbf *DBF) RecordToMap(nrec uint32) (map[string]interface{}, error) {
	if nrec <= 0 {
		nrec = dbf.recpointer
	} else if nrec != dbf.recpointer {
		if err := dbf.GoTo(nrec); err != nil {
			return nil, err
		}
	}
	out := make(map[string]interface{})
	for i, fn := range dbf.FieldNames() {
		val, err := dbf.Field(i)
		if err != nil {
			return out, fmt.Errorf("error on field %s (column %d): %s", fn, i, err)
		}
		out[fn] = val
	}
	return out, nil
}

//RecordToJSON returns a complete record as a JSON object.
//If nrec > 0 it returns the record at nrec, if nrec <= 0 it returns the record at dbf.recpointer.
//If trimspaces is true we trim spaces from string values (this is slower because of an extra reflect operation and all strings in the record map are re-assigned)
func (dbf *DBF) RecordToJSON(nrec uint32, trimspaces bool) ([]byte, error) {
	m, err := dbf.RecordToMap(nrec)
	if err != nil {
		return nil, err
	}
	if trimspaces {
		for k, v := range m {
			if str, ok := v.(string); ok {
				m[k] = strings.TrimSpace(str)
			}
		}
	}
	return json.Marshal(m)
}

//Field reads field number fieldpos at the record number the internal pointer is pointing to and returns its Go value
func (dbf *DBF) Field(fieldpos int) (interface{}, error) {
	data, err := dbf.readField(dbf.recpointer, fieldpos)
	if err != nil {
		return nil, err
	}
	//fieldpos is valid or readField would have returned an error
	return dbf.fieldDataToValue(data, fieldpos)
}

//EOF returns if the internal recordpointer is at EoF
func (dbf *DBF) EOF() bool {
	return dbf.recpointer >= dbf.header.NumRec
}

//BOF returns if the internal recordpointer is at BoF (first record)
func (dbf *DBF) BOF() bool {
	return dbf.recpointer == 0
}

//Reads raw field data of one field at fieldpos at recordpos
func (dbf *DBF) readField(recordpos uint32, fieldpos int) ([]byte, error) {
	if recordpos >= dbf.header.NumRec {
		return nil, ErrEOF
	}
	if fieldpos < 0 || fieldpos > int(dbf.NumFields()) {
		return nil, ErrInvalidField
	}
	buf := make([]byte, dbf.fields[fieldpos].Len)
	pos := int64(dbf.header.FirstRec) + (int64(recordpos) * int64(dbf.header.RecLen)) + int64(dbf.fields[fieldpos].Pos)
	read, err := dbf.r.ReadAt(buf, pos)
	if err != nil {
		return buf, err
	}
	if read != int(dbf.fields[fieldpos].Len) {
		return buf, ErrIncomplete
	}
	return buf, nil
}

//Reads raw record data of one record at recordpos
func (dbf *DBF) readRecord(recordpos uint32) ([]byte, error) {
	if recordpos >= dbf.header.NumRec {
		return nil, ErrEOF
	}
	buf := make([]byte, dbf.header.RecLen)
	read, err := dbf.r.ReadAt(buf, int64(dbf.header.FirstRec)+(int64(recordpos)*int64(dbf.header.RecLen)))
	if err != nil {
		return buf, err
	}
	if read != int(dbf.header.RecLen) {
		return buf, ErrIncomplete
	}
	return buf, nil
}

//DeletedAt returns if the record at recordpos is deleted
func (dbf *DBF) DeletedAt(recordpos uint32) (bool, error) {
	if recordpos >= dbf.header.NumRec {
		return false, ErrEOF
	}
	buf := make([]byte, 1)
	read, err := dbf.r.ReadAt(buf, int64(dbf.header.FirstRec)+(int64(recordpos)*int64(dbf.header.RecLen)))
	if err != nil {
		return false, err
	}
	if read != 1 {
		return false, ErrIncomplete
	}
	return buf[0] == 0x2A, nil
}

//Deleted returns if the record at the internal record pos is deleted
func (dbf *DBF) Deleted() (bool, error) {
	return dbf.DeletedAt(dbf.recpointer)
}

//Converts raw recorddata to a Record struct.
//If the data points to a memo (FPT) file this file is also read.
func (dbf *DBF) bytesToRecord(data []byte) (*Record, error) {

	rec := new(Record)

	//a record should start with te delete flag, a space (0x20) or * (0x2A)
	rec.Deleted = data[0] == 0x2A
	if !rec.Deleted && data[0] != 0x20 {
		return nil, errors.New("Invalid record data, no delete flag found at beginning of record")
	}

	rec.data = make([]interface{}, dbf.NumFields())

	offset := uint16(1) //deleted flag already read
	for i := 0; i < len(rec.data); i++ {
		fieldinfo := dbf.fields[i]
		val, err := dbf.fieldDataToValue(data[offset:offset+uint16(fieldinfo.Len)], i)
		if err != nil {
			return rec, err
		}
		rec.data[i] = val

		offset += uint16(fieldinfo.Len)
	}

	return rec, nil
}

//Convert raw field data to the correct type for field fieldpos.
//For C and M fields a charset conversion is done
//For M fields the data is read from the FPT file
func (dbf *DBF) fieldDataToValue(raw []byte, fieldpos int) (interface{}, error) {
	//Not all field types have been implemented because we don't use them in our DBFs
	//Extend this function if needed
	if fieldpos < 0 || len(dbf.fields) < fieldpos {
		return nil, ErrInvalidField
	}

	switch dbf.fields[fieldpos].FieldType() {
	default:
		return nil, fmt.Errorf("Unsupported fieldtype: %s", dbf.fields[fieldpos].FieldType())
	case "M":
		//M values contain the address in the FPT file from where to read data
		memo, isText, err := dbf.parseMemo(raw)
		if isText {
			return string(memo), err
		}
		return memo, err
	case "C":
		//C values are stored as strings, the returned string is not trimmed
		return dbf.toUTF8String(raw)
	case "I":
		//I values are stored as numeric values
		return int32(binary.LittleEndian.Uint32(raw)), nil
	case "B":
		//B (double) values are stored as numeric values
		return math.Float64frombits(binary.LittleEndian.Uint64(raw)), nil
	case "D":
		//D values are stored as string in format YYYYMMDD, convert to time.Time
		return dbf.parseDate(raw)
	case "T":
		//T values are stores as two 4 byte integers
		// integer one is the date in julian format
		// integer two is the number of milliseconds since midnight
		//Above info from http://fox.wikis.com/wc.dll?Wiki~DateTime
		return dbf.parseDateTime(raw)
	case "L":
		//L values are stored as strings T or F, we only check for T, the rest is false...
		return string(raw) == "T", nil
	case "V":
		//V values just return the raw value
		return raw, nil
	case "Y":
		//Y values are currency values stored as ints with 4 decimal places
		return float64(float64(binary.LittleEndian.Uint64(raw)) / 10000), nil
	case "N":
		//N values are stored as string values, if no decimals return as int64, if decimals treat as float64
		if dbf.fields[fieldpos].Decimals == 0 {
			return dbf.parseNumericInt(raw)
		}
		fallthrough //same as "F"
	case "F":
		//F values are stored as string values
		return dbf.parseFloat(raw)
	}
}

//toUTF8String converts a byte slice to a UTF8 string using the decoder in dbf
func (dbf *DBF) toUTF8String(raw []byte) (string, error) {
	utf8, err := dbf.dec.Decode(raw)
	if err != nil {
		return string(raw), err
	}
	return string(utf8), nil
}

func (dbf *DBF) parseMemo(raw []byte) ([]byte, bool, error) {
	memo, isText, err := dbf.readFPT(raw)
	if err != nil {
		return []byte{}, false, err
	}
	if isText {
		memo, err = dbf.dec.Decode(memo)
		if err != nil {
			return []byte{}, false, err
		}
	}
	return memo, isText, nil
}

func (dbf *DBF) parseDate(raw []byte) (time.Time, error) {
	if string(raw) == strings.Repeat(" ", 8) {
		return time.Time{}, nil
	}
	return time.Parse("20060102", string(raw))
}

func (dbf *DBF) parseDateTime(raw []byte) (time.Time, error) {
	if len(raw) != 8 {
		return time.Time{}, ErrInvalidField
	}
	julDat := int(binary.LittleEndian.Uint32(raw[:4]))
	mSec := int(binary.LittleEndian.Uint32(raw[4:]))
	//determine year, month, day
	y, m, d := jd.J2YMD(julDat)
	if y < 0 || y > 9999 {
		//TODO some dbf files seem to contain invalid dates, not sure if we want treat this an error until I know what is going on
		return time.Time{}, nil
	}
	//create time using ymd and nanosecond timestamp
	return time.Date(y, time.Month(m), d, 0, 0, 0, mSec*int(time.Millisecond), time.UTC), nil
}

func (dbf *DBF) parseNumericInt(raw []byte) (int64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return int64(0), nil
	}
	return strconv.ParseInt(trimmed, 10, 64)
}

func (dbf *DBF) parseFloat(raw []byte) (float64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) == 0 {
		return float64(0.0), nil
	}
	return strconv.ParseFloat(strings.TrimSpace(string(trimmed)), 64)
}

//Reads one or more blocks from the FPT file, called for each memo field.
//The return value is the raw data and true if the data read is text (false is RAW binary data).
func (dbf *DBF) readFPT(blockdata []byte) ([]byte, bool, error) {

	if dbf.fptr == nil {
		return nil, false, ErrNoFPTFile
	}

	//Determine the block number
	block := binary.LittleEndian.Uint32(blockdata)
	//The position in the file is blocknumber*blocksize
	if _, err := dbf.fptr.Seek(int64(dbf.fptheader.BlockSize)*int64(block), 0); err != nil {
		return nil, false, err
	}

	//Read the memo block header, instead of reading into a struct using binary.Read we just read the two
	//uints in one buffer and then convert, this saves seconds for large DBF files with many memo fields
	//as it avoids using the reflection in binary.Read
	hbuf := make([]byte, 8)
	_, err := dbf.fptr.Read(hbuf)
	if err != nil {
		return nil, false, err
	}
	sign := binary.BigEndian.Uint32(hbuf[:4])
	leng := binary.BigEndian.Uint32(hbuf[4:])

	if leng == 0 {
		//No data according to block header? Not sure if this should be an error instead
		return []byte{}, sign == 1, nil
	}
	//Now read the actual data
	buf := make([]byte, leng)
	read, err := dbf.fptr.Read(buf)
	if err != nil {
		return buf, false, err
	}
	if read != int(leng) {
		return buf, sign == 1, ErrIncomplete
	}
	return buf, sign == 1, nil
}

//DBFHeader is the struct containing all raw DBF header fields.
//Header info from https://msdn.microsoft.com/en-us/library/st4a0s68%28VS.80%29.aspx
type DBFHeader struct {
	FileVersion byte     //File type flag
	ModYear     uint8    //Last update year (0-99)
	ModMonth    uint8    //Last update month
	ModDay      uint8    //Last update day
	NumRec      uint32   //Number of records in file
	FirstRec    uint16   //Position of first data record
	RecLen      uint16   //Length of one data record, including delete flag
	Reserved    [16]byte //Reserved
	TableFlags  byte     //Table flags
	CodePage    byte     //Code page mark
}

//Modified parses the ModYear, ModMonth and ModDay to time.Time.
//Note: The year is stored in 2 digits, 15 is 2015, we assume here that all files
//were modified after the year 2000 and always add 2000.
func (h *DBFHeader) Modified() time.Time {
	return time.Date(2000+int(h.ModYear), time.Month(h.ModMonth), int(h.ModDay), 0, 0, 0, 0, time.Local)
}

//NumFields returns the calculated number of fields from the header info alone (without the need to read the fieldinfo from the header).
//This is the fastest way to determine the number of records in the file.
//Note: when OpenFile is used the fields have already been parsed so it is better to call DBF.NumFields in that case.
func (h *DBFHeader) NumFields() uint16 {
	return uint16((h.FirstRec - 296) / 32)
}

//FileSize eturns the calculated file size based on the header info
func (h *DBFHeader) FileSize() int64 {
	return 296 + int64(h.NumFields()*32) + int64(h.NumRec*uint32(h.RecLen))
}

//FieldHeader contains the raw field info structure from the DBF header.
//Header info from https://msdn.microsoft.com/en-us/library/st4a0s68%28VS.80%29.aspx
type FieldHeader struct {
	Name     [11]byte //Field name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00).
	Type     byte     //Field type
	Pos      uint32   //Displacement of field in record
	Len      uint8    //Length of field (in bytes)
	Decimals uint8    //Number of decimal places
	Flags    byte     //Field flags
	Next     uint32   //Value of autoincrement Next value
	Step     uint16   //Value of autoincrement Step value
	Reserved [8]byte  //Reserved
}

//FieldName returns the name of the field as a trimmed string (max length 10)
func (f *FieldHeader) FieldName() string {
	return string(bytes.TrimRight(f.Name[:], "\x00"))
}

//FieldType returns the type of the field as string (length 1)
func (f *FieldHeader) FieldType() string {
	return string(f.Type)
}

//Record contains the raw record data and a deleted flag
type Record struct {
	Deleted bool
	data    []interface{}
}

//Field gets a fields value by field pos (index)
func (r *Record) Field(pos int) (interface{}, error) {
	if pos < 0 || len(r.data) < pos {
		return 0, ErrInvalidField
	}
	return r.data[pos], nil
}

//FieldSlice gets all fields as a slice
func (r *Record) FieldSlice() []interface{} {
	return r.data
}

//OpenFile opens a DBF file (and FPT if needed) from disk.
//After a successful call to this method (no error is returned), the caller
//should call DBF.Close() to close the embedded file handle(s).
//The Decoder is used for charset translation to UTF8, see decoder.go
func OpenFile(filename string, dec Decoder) (*DBF, error) {

	filename = filepath.Clean(filename)

	dbffile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	dbf, err := prepareDBF(dbffile, dec)
	if err != nil {
		return nil, err
	}

	dbf.f = dbffile

	//Check if there is an FPT according to the header
	//If there is we will try to open it in the same dir (using the same filename and case)
	//If the FPT file does not exist an error is returned
	if (dbf.header.TableFlags & 0x02) != 0 {
		ext := filepath.Ext(filename)
		fptext := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptext = ".FPT"
		}
		fptfile, err := os.Open(strings.TrimSuffix(filename, ext) + fptext)
		if err != nil {
			return nil, err
		}

		err = dbf.prepareFPT(fptfile)
		if err != nil {
			return nil, err
		}

		dbf.fptf = fptfile
	}

	return dbf, nil
}

//OpenStream creates a new DBF struct from a bytes stream, for example a bytes.Reader
//The fptfile parameter is optional, but if the DBF header has the FPT flag set, the fptfile must be provided.
//The Decoder is used for charset translation to UTF8, see decoder.go
func OpenStream(dbffile, fptfile ReaderAtSeeker, dec Decoder) (*DBF, error) {

	dbf, err := prepareDBF(dbffile, dec)
	if err != nil {
		return nil, err
	}

	if (dbf.header.TableFlags & 0x02) != 0 {
		if fptfile == nil {
			return nil, ErrNoFPTFile
		}
		err = dbf.prepareFPT(fptfile)
		if err != nil {
			return nil, err
		}
	}

	return dbf, nil
}

func prepareDBF(dbffile ReaderAtSeeker, dec Decoder) (*DBF, error) {

	header, err := readDBFHeader(dbffile)
	if err != nil {
		return nil, err
	}

	//Check if the fileversion flag is expected, expand validFileVersion if needed
	if err := validFileVersion(header.FileVersion); err != nil {
		return nil, err
	}

	//Read fieldinfo
	fields, err := readHeaderFields(dbffile)
	if err != nil {
		return nil, err
	}

	dbf := &DBF{
		header: header,
		r:      dbffile,
		fields: fields,
		dec:    dec,
	}

	return dbf, nil
}

func readDBFHeader(r io.ReadSeeker) (*DBFHeader, error) {
	h := new(DBFHeader)
	if _, err := r.Seek(0, 0); err != nil {
		return nil, err
	}
	//Integers in table files are stored with the least significant byte first.
	err := binary.Read(r, binary.LittleEndian, h)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func validFileVersion(version byte) error {
	switch version {
	default:
		return fmt.Errorf("Untested DBF file version: %d (%x hex)", version, version)
	case 0x30, 0x31:
		return nil
	}
}

//Reads fieldinfo from DBF header, starting at pos 32.
//Reads fields until it finds the Header record terminator (0x0D).
func readHeaderFields(r io.ReadSeeker) ([]FieldHeader, error) {
	fields := []FieldHeader{}

	offset := int64(32)
	b := make([]byte, 1)
	for {
		//Check if we are at 0x0D by reading one byte ahead
		if _, err := r.Seek(offset, 0); err != nil {
			return nil, err
		}
		if _, err := r.Read(b); err != nil {
			return nil, err
		}
		if b[0] == 0x0D {
			break
		}
		//Position back one byte and read the field
		if _, err := r.Seek(-1, 1); err != nil {
			return nil, err
		}
		field := FieldHeader{}
		err := binary.Read(r, binary.LittleEndian, &field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)

		offset += 32
	}
	return fields, nil
}

//FPTHeader is the raw header of the Memo file.
//Header info from https://msdn.microsoft.com/en-US/library/8599s21w%28v=vs.80%29.aspx
type FPTHeader struct {
	NextFree  uint32  //Location of next free block
	Unused    [2]byte //Unused
	BlockSize uint16  //Block size (bytes per block)
}

func readFPTHeader(r io.ReadSeeker) (*FPTHeader, error) {
	h := new(FPTHeader)
	if _, err := r.Seek(0, 0); err != nil {
		return nil, err
	}
	//Integers in memo files are stored with the most significant byte first
	err := binary.Read(r, binary.BigEndian, h)
	if err != nil {
		return nil, err
	}
	return h, nil
}
