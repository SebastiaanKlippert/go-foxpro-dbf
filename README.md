# go-foxpro-dbf
Golang package for reading FoxPro DBF/FPT files

This is a work in progress and is only tested for Alaska XBase++ DBF/FPT files in FoxPro format.
These files have file flag 0x30 or 0x31 for autoincrement values

Since these files are almsot always used on Windows platforms the default encoding is
from Windows-1250 to UTF8 but a universal encoder will be provided for other codepages.

Tests are incomplete at this point