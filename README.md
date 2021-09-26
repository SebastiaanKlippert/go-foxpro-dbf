# go-foxpro-dbf

[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/SebastiaanKlippert/go-foxpro-dbf)
[![Build & test](https://github.com/SebastiaanKlippert/go-foxpro-dbf/actions/workflows/ubuntu.yml/badge.svg)](https://github.com/SebastiaanKlippert/go-foxpro-dbf/actions/workflows/ubuntu.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/SebastiaanKlippert/go-foxpro-dbf)](https://goreportcard.com/report/github.com/SebastiaanKlippert/go-foxpro-dbf)
[![codecov](https://codecov.io/gh/SebastiaanKlippert/go-foxpro-dbf/branch/master/graph/badge.svg)](https://codecov.io/gh/SebastiaanKlippert/go-foxpro-dbf)

Golang package for reading FoxPro DBF/FPT files.

This package provides a reader for reading FoxPro database files.
At this moment it is only tested for Alaska XBase++ DBF/FPT files in FoxPro format and some
older FoxPro files, see the included `testdbf` folder for these files.
These files have file flag 0x30 (or 0x31 if autoincrement fields are present).

Since these files are almost always used on Windows platforms the default encoding is
from Windows-1250 to UTF8 but a universal encoder will be provided for other code pages.

# Features 

There are several similar packages but they are not suited for our use case, this package will try to implement:
* Support for FPT (memo) files
* Full support for Windows-1250 encoding to UTF8
* File readers for scanning files (instead of reading the entire file to memory)

The focus is on performance while also trying to keep the code readable and easy to use.

# Supported field types

At this moment not all FoxPro field types are supported.
When reading field values, the value returned by this package is always `interface{}`. 
If you need to cast this to the correct value helper functions are provided.

The supported field types with their return Go types are: 

| Field Type | Field Type Name | Golang type |
|------------|-----------------|-------------|
| B | Double | float64 |
| C | Character | string |
| D | Date | time.Time |
| F | Float | float64 |
| I | Integer | int32 |
| L | Logical | bool |
| M | Memo  | string |
| M | Memo (Binary) | []byte |
| N | Numeric (0 decimals) | int64 |
| N | Numeric (with decimals) | float64 |
| T | DateTime | time.Time |
| Y | Currency | float64 |

# Example

```go
func Test() error {
	// Open file
	testdbf, err := dbf.OpenFile("TEST.DBF", new(dbf.Win1250Decoder))
	if err != nil {
		return err
	}
	defer testdbf.Close()

	// Print all the fieldnames
	for _, name := range testdbf.FieldNames() {
		fmt.Println(name)
	}

	// Get fieldinfo for all fields
	for _, field := range testdbf.Fields() {
		fmt.Println(field.FieldName(), field.FieldType(), field.Decimals /*etc*/)
	}

	// Read the complete second record
	record, err := testdbf.RecordAt(1)
	if err != nil {
		return err
	}
	// Print all the fields in their Go values
	fmt.Println(record.FieldSlice())

	// Loop through all records using recordpointer in DBF struct
	// Reads the complete record
	for !testdbf.EOF() { // or for i := uint32(0); i < testdbf.NumRecords(); i++ {

		// This reads the complete record
		record, err := testdbf.Record()
		if err != nil {
			return err
		}
		testdbf.Skip(1)

		// skip deleted records
		if record.Deleted {
			continue
		}
		// get field by position
		field1, err := record.Field(0)
		if err != nil {
			return err
		}
		// get field by name
		field2, err := record.Field(testdbf.FieldPos("NAAM"))
		if err != nil {
			return err
		}

		fmt.Println(field1, field2)
	}

	// Read only the third field of records 2, 50 and 300
	recnumbers := []uint32{2, 50, 300}
	for _, rec := range recnumbers {
		err := testdbf.GoTo(rec)
		if err != nil {
			return err
		}
		deleted, err := testdbf.Deleted()
		if err != nil {
			return err
		}
		if !deleted {
			field3, err := testdbf.Field(3)
			if err != nil {
				return err
			}
			fmt.Println(field3)
		}
	}

	return nil
}
```

# Example using a byte reader

You can use OpenStream with any ReaderAt and ReadSeeker combo, for example a bytes.Reader.
The FPT stream is only required when the DBF header indicates there must be an FPT file.

```go
func TestBytes() error {
	
	dbfbytes, err := ioutil.ReadFile("TEST.DBF")
	if err != nil {
		return err
	}
	dbfreader := bytes.NewReader(dbfbytes)

	fptbytes, err := ioutil.ReadFile("TEST.FPT")
	if err != nil {
		return err
	}
	fptreader := bytes.NewReader(fptbytes)

	test_dbf, err = OpenStream(dbfreader, fptreader, new(Win1250Decoder))
	if err != nil {
		return err
	}
	defer testdbf.Close()

	// Print all the fieldnames
	for _, name := range testdbf.FieldNames() {
		fmt.Println(name)
	}
	
	// ETC...
	
	return nil	
}
```

# Thanks

* To [carlosjhr64](https://github.com/carlosjhr64) for the Julian date conversion package <https://github.com/carlosjhr64/jd>
* To [dkeza](https://github.com/dkeza) for submitting some issues and testing FoxPro files on 32 bit Go
