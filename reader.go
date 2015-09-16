package dbf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	ErrEOF        = fmt.Errorf("EOF")
	ErrBOF        = fmt.Errorf("BOF")
	ErrIncomplete = fmt.Errorf("Incomplete record read")
)

//The main DBF struct provides all methods for reading files and embeds the file handlers
//Only files are supported at this time
type DBF struct {
	header    *DBFHeader
	f         *os.File
	fptheader *FPTHeader
	fptf      *os.File //if there is an FPT file handler is used for it

	fields []FieldHeader

	recpointer uint32 //internal record pointer, can be moved using Skip() and GoTo()
}

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

//Returns the DBF Header struct for inspecting
func (dbf *DBF) Header() *DBFHeader {
	return dbf.header
}

//os FileInfo for DBF file
func (dbf *DBF) Stat() (os.FileInfo, error) {
	return dbf.f.Stat()
}

//os FileInfo for FPT file
func (dbf *DBF) StatFPT() (os.FileInfo, error) {
	if dbf.fptf == nil {
		return nil, fmt.Errorf("No FPT file")
	}
	return dbf.fptf.Stat()
}

//Returns the number of records
func (dbf *DBF) NumRecords() uint32 {
	return dbf.header.NumRec
}

//Returns all the FieldHeaders
func (dbf *DBF) Fields() []FieldHeader {
	return dbf.fields
}

//Returns the number of fields
func (dbf *DBF) NumFields() uint16 {
	return uint16(len(dbf.fields))
}

//Returnes a slice of all the fieldnames
func (dbf *DBF) FieldNames() []string {
	num := len(dbf.fields)
	names := make([]string, num)
	for i := 0; i < num; i++ {
		names[i] = dbf.fields[i].FieldName()
	}
	return names
}

//Returns the zero-based field position of a fieldname
//or -1 if not found
func (dbf *DBF) FieldPos(fieldname string) int {
	for i := 0; i < len(dbf.fields); i++ {
		if dbf.fields[i].FieldName() == fieldname {
			return i
		}
	}
	return -1
}

//Sets internal record pointer to record recno (zero based)
//Returns ErrEOF if at EOF and positions the pointer at lastRec+1
func (dbf *DBF) GoTo(recno uint32) error {
	if recno > dbf.header.NumRec-1 {
		dbf.recpointer = dbf.header.NumRec
		return ErrEOF
	}
	dbf.recpointer = recno
	return nil
}

//Adds offset to the internal record pointer
//Returns ErrEOF if at EOF and positions the pointer at lastRec+1
//Returns ErrBOF is recpointer would be become negative and positions the pointer at 0
//Does not skip deleted records
func (dbf *DBF) Skip(offset int32) error {
	if int64(dbf.recpointer)+int64(offset) > int64(dbf.header.NumRec-1) {
		dbf.recpointer = dbf.header.NumRec
		return ErrEOF
	}
	if int64(dbf.recpointer)+int64(offset) < 0 {
		dbf.recpointer = 0
		return ErrBOF
	}
	neg := offset < 0
	var uintoffset uint32
	if neg {
		uintoffset = uint32(-1 * offset)
	} else {
		uintoffset = uint32(offset)
	}
	dbf.recpointer += uintoffset
	return nil
}

//Reads the complete record the internal record pointer is pointing to
func (dbf *DBF) Record() (*Record, error) {
	data, err := dbf.readRecord(dbf.recpointer)
	if err != nil {
		return nil, err
	}
	return dbf.bytesToRecord(data)
}

//Reads  the complete record number nrec
func (dbf *DBF) RecordAt(nrec uint32) (*Record, error) {
	data, err := dbf.readRecord(nrec)
	if err != nil {
		return nil, err
	}
	return dbf.bytesToRecord(data)
}

//Reads field nfield at the record number the internal pointer is pointing to
func (dbf *DBF) Field(nfield uint16) (interface{}, error) {
	return nil, nil
}

//Returns if the internal recordpointer is at EoF
func (dbf *DBF) EOF() bool {
	return dbf.recpointer > dbf.header.NumRec-1
}

//Returns if the internal recordpointer is at BoF (first record)
func (dbf *DBF) BOF() bool {
	return dbf.recpointer == 0
}

//Reads raw field data of one field at fieldpos at recordpos
func (dbf *DBF) readField(recordpos uint32, fieldpos uint16) ([]byte, error) {
	return nil, nil
}

//Reads raw record data of one record at recordpos
func (dbf *DBF) readRecord(recordpos uint32) ([]byte, error) {
	if recordpos > dbf.header.NumRec-1 {
		return nil, ErrEOF
	}
	buf := make([]byte, dbf.header.RecLen)
	read, err := dbf.f.ReadAt(buf, int64(dbf.header.FirstRec)+(int64(recordpos)*int64(dbf.header.RecLen)))
	if err != nil {
		return buf, err
	}
	if read != int(dbf.header.RecLen) {
		return buf, ErrIncomplete
	}
	return buf, nil
}

//Converts raw recorddata to a Record struct
//If the data points to a memo (FPT) file this file is also read
func (dbf *DBF) bytesToRecord(data []byte) (*Record, error) {
	rec := new(Record)
	rec.Deleted = data[0] == 0x20
	rec.data = make([]interface{}, dbf.NumFields())

	offset := uint8(1) //deleted flag already read
	for i := 0; i < len(rec.data); i++ {
		fieldinfo := dbf.fields[i]
		//fmt.Println(fieldinfo.FieldName())
		val, err := fieldDataToValue(data[offset:offset+fieldinfo.Len], fieldinfo.FieldType(), fieldinfo.Decimals)
		if err != nil {
			fmt.Println(err)
			//return rec, err
		}
		rec.data[i] = val

		offset += fieldinfo.Len
	}
	/*for _, val := range rec.data {
		fmt.Println(val)
	}*/
	return rec, nil
}

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

//Parses the ModYear, ModMonth and ModDay to time.Time
//Note: The Date is stored in 2 digits, 15 is 2015, we assume here that all files
//were modified after the year 2000 and always add 2000
func (h *DBFHeader) Modified() time.Time {
	return time.Date(2000+int(h.ModYear), time.Month(h.ModMonth), int(h.ModDay), 0, 0, 0, 0, time.Local)
}

//Returns the calculated number of fields from the header info alone (without the need to read the fieldinfo from the header)
//This is the fastest way to determine the number of records in the file
//Note: when OpenFile is used the fields have already been parsed so it is better to call DBF.NumFields in that case
func (h *DBFHeader) NumFields() uint16 {
	return uint16((h.FirstRec - 296) / 32)
}

//Returns the calculated filesize based on the header info
func (h *DBFHeader) FileSize() int64 {
	return 296 + int64(h.NumFields()*32) + int64(h.NumRec*uint32(h.RecLen)) + 1 //why +1?
}

//Header info from https://msdn.microsoft.com/en-US/library/8599s21w%28v=vs.80%29.aspx
type FPTHeader struct {
	NextFree  uint32  //Location of next free block
	Unused    [2]byte //Unused
	BlockSize uint16  //Block size (bytes per block)
}

//Field subrecord structure from header
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

const FieldHeaderSize = 32 //sum of all FieldHeader fieldsizes so we don't have to use reflect or unsafe

//Fieldname as a trimmed string (max length 10)
func (f *FieldHeader) FieldName() string {
	return string(bytes.TrimRight(f.Name[:], "\x00"))
}

//Fieldtype as string (length 1)
func (f *FieldHeader) FieldType() string {
	return string(f.Type)
}

//Record data
type Record struct {
	Deleted bool
	data    []interface{}
}

//Opens a DBF file (and FPT if needed) from disk.
//After a successful call to this method (no error is returned) the caller
//should call DBF.Close() to close the embedded file handle(s)
func OpenFile(filename string) (*DBF, error) {

	filename = filepath.Clean(filename)

	dbffile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

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
		f:      dbffile,
		fields: fields,
	}

	//Check if there is an FPT according to the header
	//If there is we will try to open it in the same dir (using the same filename and case)
	//If the FPT file does not exist an error is returned
	if (header.TableFlags & 0x02) != 0 {
		ext := filepath.Ext(filename)
		fptext := ".fpt"
		if strings.ToUpper(ext) == ext {
			fptext = ".FPT"
		}
		fptfile, err := os.Open(strings.TrimSuffix(filename, ext) + fptext)
		if err != nil {
			return nil, err
		}

		fptheader, err := readFPTHeader(fptfile)
		if err != nil {
			return nil, err
		}

		dbf.fptf = fptfile
		dbf.fptheader = fptheader
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

func validFileVersion(version byte) error {
	switch version {
	default:
		return fmt.Errorf("Untested DBF file version: %d (%x hex)", version, version)
	case 0x30, 0x31:
		return nil
	}
}

//Reads fieldinfo from DBF header, starting at pos 32
//Reads fields until it finds the Header record terminator (0x0D)
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

		offset += FieldHeaderSize
	}
	return fields, nil
}

//Convert raw field data to the correct type
func fieldDataToValue(raw []byte, fieldtype string, decimals uint8) (interface{}, error) {
	//Not all fieldtypes have been implemented beacause we don't use them in our DBFs
	//Extend this function if needed
	switch fieldtype {
	default:
		return nil, fmt.Errorf("Unsupported fieldtype: %s", fieldtype)
	case "C":
		//TODO: Encoding
		return string(raw), nil
	case "M":
		//TODO: Encoding
		return "Memo: TODO", nil
	case "I":
		//I values are stored as numeric values
		return rawToInt(raw)
	case "N":
		//N values are stored as string values
		if decimals == 0 {
			return strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 32)
		}
		fallthrough //same as "F"
	case "F":
		//N values are stored as string values
		return strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
	case "D":
		//D values are stored as string in format YYYYMMDD, convert to time.Time
		if string(raw) == strings.Repeat(" ", 8) {
			return time.Time{}, nil
		}
		return time.Parse("20060102", string(raw))
	case "L":
		return len(raw) > 0 && raw[0] == 0, nil
	}
}

//Convert numeric binary byte value to int
func rawToInt(raw []byte) (int32, error) {
	var val int32 //The DBF size is 4 bytes so an int32 is big enough
	buf := bytes.NewBuffer(raw)
	err := binary.Read(buf, binary.LittleEndian, &val)
	return val, err
}