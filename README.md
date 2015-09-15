# go-foxpro-dbf

[![GoDoc](https://godoc.org/github.com/golang/gddo?status.svg)](http://godoc.org/github.com/SebastiaanKlippert/go-foxpro-dbf)
[![Build Status](https://travis-ci.org/SebastiaanKlippert/go-foxpro-dbf.svg?branch=master)](https://travis-ci.org/SebastiaanKlippert/go-foxpro-dbf)


Golang package for reading FoxPro DBF/FPT files

This is a work in progress and is only tested for Alaska XBase++ DBF/FPT files in FoxPro format.
These files have file flag 0x30 (or 0x31 if autoincrement fields are present).

Since these files are almsot always used on Windows platforms the default encoding is
from Windows-1250 to UTF8 but a universal encoder will be provided for other codepages.

# Features 

There are several similar packages but they are not suited for our use case, this package will try to implement:
* Support for FPT (memo) files
* Full support for Windows-1250 encoding to UTF8
* Filereaders for scanning files (instead of reading the entire file to memory)

The focus is on performance while also trying to keep the code readable.
