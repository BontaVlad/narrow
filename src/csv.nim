import ./[ffi, filesystem, gtables, error]

# proc garrow_csv_read_options_get_type*(): GType {.
#   cdecl, importc: "garrow_csv_read_options_get_type"
# .}

# proc garrow_csv_read_options_new*(): ptr GArrowCSVReadOptions {.
#   cdecl, importc: "garrow_csv_read_options_new"
# .}

# proc garrow_csv_reader_new*(
#   input: ptr GArrowInputStream, options: ptr GArrowCSVReadOptions, error: ptr ptr GError
# ): ptr GArrowCSVReader {.cdecl, importc: "garrow_csv_reader_new".}

# proc garrow_csv_reader_read*(
#   reader: ptr GArrowCSVReader, error: ptr ptr GError
# ): ptr GArrowTable {.cdecl, importc: "garrow_csv_reader_read".}

proc readCSV*(path: string): ArrowTable =
  let options = garrow_csv_read_options_new()

  if options.isNil:
    raise newException(IOError, "Cannot read csv file " & path)

  let fs = newLocalFileSystem()
  with fs.openInputStream(path), stream:
    let reader = check garrow_csv_reader_new(stream.handle, options)
    let tablePtr = check garrow_csv_reader_read(reader)
    result = ArrowTable(tablePtr)

