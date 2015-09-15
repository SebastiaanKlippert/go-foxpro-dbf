package dbf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//The main DBF struct provides all methods for reading files and embeds the file handlers
//Only files are supported at this time
type DBF struct {
	Header    *DBFHeader
	f         *os.File
	FPTHeader *FPTHeader
	fptf      *os.File //if there is an FPT file handler is used for it

	fields []FieldHeader
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

//Returns all the FieldHeaders
func (dbf *DBF) Fields() []FieldHeader {
	return dbf.fields
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

//Header info from https://msdn.microsoft.com/en-us/library/st4a0s68%28VS.80%29.aspx
type DBFHeader struct {
	FileVersion byte     //File type, only tested for 0x30 and 0x31!
	ModYear     uint8    //Last update year (0-99)
	ModMonth    uint8    //Last update month
	ModDay      uint8    //Last update day
	NumRec      uint32   //Number of records in file
	HeaderLen   uint16   //Position of first data record
	RecordLen   uint16   //Length of one data record, including delete flag
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

//Header info from https://msdn.microsoft.com/en-US/library/8599s21w%28v=vs.80%29.aspx
type FPTHeader struct {
	NextFree  uint32  //Location of next free block
	Unused    [2]byte //Unused
	BlockSize uint16  //Block size (bytes per block)
}

//Field subrecord structure from header
//Header info from https://msdn.microsoft.com/en-us/library/st4a0s68%28VS.80%29.aspx
type FieldHeader struct {
	Name     [10]byte //Field name with a maximum of 10 characters. If less than 10, it is padded with null characters (0x00).
	Type     byte     //Field type
	Pos      uint32   //Displacement of field in record
	Len      uint8    //Length of field (in bytes)
	Decimals uint8    //Number of decimal places
	Flags    byte     //Field flags
	Next     uint32   //Value of autoincrement Next value
	Step     uint8    //Value of autoincrement Step value
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

	//Read fieldinfo (dbffile is at correct offset because of readDBFHeader)
	fields, err := readHeaderFields(dbffile)
	if err != nil {
		return nil, err
	}

	dbf := &DBF{
		Header: header,
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
		dbf.FPTHeader = fptheader
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
	//Integers stored with the most significant byte first
	err := binary.Read(r, binary.BigEndian, h)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func validFileVersion(version byte) error {
	switch version {
	default:
		return fmt.Errorf("Untested file version: %d (%x hex)", version, version)
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
