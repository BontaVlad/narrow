import std/[strformat, options, tables, sets, sequtils, strutils]
import
  ./[
    ffi, filesystem, gtables, error, gtypes, gschema, garray, grecordbatch,
    gchunkedarray,
  ]

type
  CsvReadOptions* = object
    handle*: ptr GArrowCSVReadOptions
    schema*: Option[Schema]

  WriteOptions* = object
    includeHeader*: bool
    batchSize*: int
    delimiter*: char
    nullString*: string
    eol*: string

  Writable* =
    concept w
        # w.columnNames is seq[string]
        w.nRows is int64
        # for row in w.rows:
        #   row is seq[string]

  # RecordBatchWriter* = object
  #   handle*: ptr GArrowRecordBatchWriter
  # RecordBatchStreamWriter* = object
  #   handle*: ptr GArrowRecordBatchStreamWriter

proc newCsvReadOptions*(
    allowNewlinesInValues: Option[bool] = none(bool),
    allowNullStrings: Option[bool] = none(bool),
    blockSize: Option[int] = none(int),
    checkUtf8: Option[bool] = none(bool),
    delimiter: Option[char] = none(char),
    escapeCharacter: Option[char] = none(char),
    generateColumnNames: Option[bool] = none(bool),
    ignoreEmptyLines: Option[bool] = none(bool),
    isDoubleQuoted: Option[bool] = none(bool),
    isEscaped: Option[bool] = none(bool),
    isQuoted: Option[bool] = none(bool),
    nSkipRows: Option[int] = none(int),
    quoteCharacter: Option[char] = none(char),
    useThreads: Option[bool] = none(bool),
): CsvReadOptions =
  let handle = garrow_csv_read_options_new()
  if handle.isNil:
    raise newException(IOError, "Failed to create CsvReadOptions")
  result.handle = handle

  # Set optional properties
  if allowNewlinesInValues.isSome:
    g_object_set(
      result.handle,
      "allow-newlines-in-values",
      gboolean(allowNewlinesInValues.get),
      nil,
    )

  if allowNullStrings.isSome:
    g_object_set(
      result.handle, "allow-null-strings", gboolean(allowNullStrings.get), nil
    )

  if blockSize.isSome:
    g_object_set(result.handle, "block-size", gint(blockSize.get), nil)

  if checkUtf8.isSome:
    g_object_set(result.handle, "check-utf8", gboolean(checkUtf8.get), nil)

  if delimiter.isSome:
    g_object_set(result.handle, "delimiter", gchar(delimiter.get), nil)

  if escapeCharacter.isSome:
    g_object_set(result.handle, "escape-character", gchar(escapeCharacter.get), nil)

  if generateColumnNames.isSome:
    g_object_set(
      result.handle, "generate-column-names", gboolean(generateColumnNames.get), nil
    )

  if ignoreEmptyLines.isSome:
    g_object_set(
      result.handle, "ignore-empty-lines", gboolean(ignoreEmptyLines.get), nil
    )

  if isDoubleQuoted.isSome:
    g_object_set(result.handle, "is-double-quoted", gboolean(isDoubleQuoted.get), nil)

  if isEscaped.isSome:
    g_object_set(result.handle, "is-escaped", gboolean(isEscaped.get), nil)

  if isQuoted.isSome:
    g_object_set(result.handle, "is-quoted", gboolean(isQuoted.get), nil)

  if nSkipRows.isSome:
    g_object_set(result.handle, "n-skip-rows", gint(nSkipRows.get), nil)

  if quoteCharacter.isSome:
    g_object_set(result.handle, "quote-character", gchar(quoteCharacter.get), nil)

  if useThreads.isSome:
    g_object_set(result.handle, "use-threads", gboolean(useThreads.get), nil)

proc `=destroy`*(o: CsvReadOptions) =
  if o.handle != nil:
    if o.schema.isSome:
      g_object_unref(o.schema.get().toPtr)
    g_object_unref(o.handle)

proc `=sink`*(dest: var CsvReadOptions, src: CsvReadOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var CsvReadOptions, src: CsvReadOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

# Property getters
proc getAllowNewlinesInValues*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "allow-newlines-in-values", addr value, nil)
  result = value.bool

proc getAllowNullStrings*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "allow-null-strings", addr value, nil)
  result = value.bool

proc getBlockSize*(options: CsvReadOptions): int =
  var value: gint
  g_object_get(options.handle, "block-size", addr value, nil)
  result = value.int

proc getCheckUtf8*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "check-utf8", addr value, nil)
  result = value.bool

proc getDelimiter*(options: CsvReadOptions): char =
  var value: gchar
  g_object_get(options.handle, "delimiter", addr value, nil)
  result = value.char

proc getEscapeCharacter*(options: CsvReadOptions): char =
  var value: gchar
  g_object_get(options.handle, "escape-character", addr value, nil)
  result = value.char

proc getGenerateColumnNames*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "generate-column-names", addr value, nil)
  result = value.bool

proc getIgnoreEmptyLines*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "ignore-empty-lines", addr value, nil)
  result = value.bool

proc getIsDoubleQuoted*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "is-double-quoted", addr value, nil)
  result = value.bool

proc getIsEscaped*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "is-escaped", addr value, nil)
  result = value.bool

proc getIsQuoted*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "is-quoted", addr value, nil)
  result = value.bool

proc getNSkipRows*(options: CsvReadOptions): int =
  var value: gint
  g_object_get(options.handle, "n-skip-rows", addr value, nil)
  result = value.int

proc getQuoteCharacter*(options: CsvReadOptions): char =
  var value: gchar
  g_object_get(options.handle, "quote-character", addr value, nil)
  result = value.char

proc getUseThreads*(options: CsvReadOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "use-threads", addr value, nil)
  result = value.bool

# Property setters
proc setAllowNewlinesInValues*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "allow-newlines-in-values", gboolean(value), nil)

proc setAllowNullStrings*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "allow-null-strings", gboolean(value), nil)

proc setBlockSize*(options: CsvReadOptions, value: int) =
  g_object_set(options.handle, "block-size", gint(value), nil)

proc setCheckUtf8*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "check-utf8", gboolean(value), nil)

proc setDelimiter*(options: CsvReadOptions, value: char) =
  g_object_set(options.handle, "delimiter", gchar(value), nil)

proc setEscapeCharacter*(options: CsvReadOptions, value: char) =
  g_object_set(options.handle, "escape-character", gchar(value), nil)

proc setGenerateColumnNames*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "generate-column-names", gboolean(value), nil)

proc setIgnoreEmptyLines*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "ignore-empty-lines", gboolean(value), nil)

proc setIsDoubleQuoted*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "is-double-quoted", gboolean(value), nil)

proc setIsEscaped*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "is-escaped", gboolean(value), nil)

proc setIsQuoted*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "is-quoted", gboolean(value), nil)

proc setNSkipRows*(options: CsvReadOptions, value: int) =
  g_object_set(options.handle, "n-skip-rows", gint(value), nil)

proc setQuoteCharacter*(options: CsvReadOptions, value: char) =
  g_object_set(options.handle, "quote-character", gchar(value), nil)

proc setUseThreads*(options: CsvReadOptions, value: bool) =
  g_object_set(options.handle, "use-threads", gboolean(value), nil)

# Column names methods
proc addColumnName*(options: CsvReadOptions, name: string) =
  garrow_csv_read_options_add_column_name(options.handle, name.cstring)

proc setColumnNames*(options: CsvReadOptions, names: openArray[string]) =
  var cnames = newSeq[cstring](names.len)
  for i, name in names:
    cnames[i] = name.cstring
  garrow_csv_read_options_set_column_names(
    options.handle, cast[ptr cstring](cnames[0].addr), gsize(names.len)
  )

proc getColumnNames*(options: CsvReadOptions): seq[string] =
  let cnames = cast[ptr UncheckedArray[cstring]](garrow_csv_read_options_get_column_names(
    options.handle
  ))
  result = newSeq[string]()
  if cnames.isNil:
    return result

  var i = 0
  while not cnames[i].isNil:
    result.add($cnames[i])
    inc i

# Column types methods (requires DataType)
proc addColumnType*(options: CsvReadOptions, name: string, dataType: GADType) =
  garrow_csv_read_options_add_column_type(options.handle, name.cstring, dataType.handle)

proc getColumnTypes*(options: CsvReadOptions): Table[string, GADType] =
  let hashTable = garrow_csv_read_options_get_column_types(options.handle)
  result = initTable[string, GADType]()
  # Note: You'll need to implement GHashTable iteration
  # This is a placeholder showing the structure

# Schema methods
proc addSchema*(options: var CsvReadOptions, schema: Schema) =
  garrow_csv_read_options_add_schema(options.handle, schema.handle)
  options.schema = some(schema)

# Null values methods
proc addNullValue*(options: CsvReadOptions, value: string) =
  garrow_csv_read_options_add_null_value(options.handle, value.cstring)

proc setNullValues*(options: CsvReadOptions, values: openArray[string]) =
  var cvalues = newSeq[cstring](values.len)
  for i, value in values:
    cvalues[i] = value.cstring
  garrow_csv_read_options_set_null_values(
    options.handle, cast[ptr cstring](cvalues[0].addr), gsize(values.len)
  )

# proc getNullValues*(options: CsvReadOptions): seq[string] =
#   var nValues: gsize
#   let cvalues = garrow_csv_read_options_get_null_values(options.handle, addr nValues)
#   result = newSeq[string](nValues)
#   for i in 0 ..< nValues:
#     result[i] = $cvalues[i]

# True values methods
proc addTrueValue*(options: CsvReadOptions, value: string) =
  garrow_csv_read_options_add_true_value(options.handle, value.cstring)

proc setTrueValues*(options: CsvReadOptions, values: openArray[string]) =
  var cvalues = newSeq[cstring](values.len)
  for i, value in values:
    cvalues[i] = value.cstring
  garrow_csv_read_options_set_true_values(
    options.handle, cast[ptr cstring](cvalues[0].addr), gsize(values.len)
  )

# proc getTrueValues*(options: CsvReadOptions): seq[string] =
#   var nValues: gsize
#   let cvalues = garrow_csv_read_options_get_true_values(options.handle, addr nValues)
#   result = newSeq[string](nValues)
#   for i in 0 ..< nValues:
#     result[i] = $cvalues[i]

# # False values methods
# proc addFalseValue*(options: CsvReadOptions, value: string) =
#   discard garrow_csv_read_options_add_false_value(options.handle, value.cstring)

proc setFalseValues*(options: CsvReadOptions, values: openArray[string]) =
  var cvalues = newSeq[cstring](values.len)
  for i, value in values:
    cvalues[i] = value.cstring
  garrow_csv_read_options_set_false_values(
    options.handle, cast[ptr cstring](cvalues[0].addr), gsize(values.len)
  )

# proc getFalseValues*(options: CsvReadOptions): seq[string] =
#   var nValues: gsize
#   let cvalues = garrow_csv_read_options_get_false_values(options.handle, addr nValues)
#   result = newSeq[string](nValues)
#   for i in 0 ..< nValues:
#     result[i] = $cvalues[i]

# # Timestamp parsers methods
# proc addTimestampParser*(options: CsvReadOptions, parser: string) =
#   discard garrow_csv_read_options_add_timestamp_parser(options.handle, parser.cstring)

# proc setTimestampParsers*(options: CsvReadOptions, parsers: openArray[string]) =
#   var cparsers = newSeq[cstring](parsers.len)
#   for i, parser in parsers:
#     cparsers[i] = parser.cstring
#   garrow_csv_read_options_set_timestamp_parsers(options.handle, 
#                                                 cast[ptr cstring](cparsers[0].addr), 
#                                                 gsize(parsers.len))

# proc getTimestampParsers*(options: CsvReadOptions): seq[string] =
#   var nParsers: gsize
#   let cparsers = garrow_csv_read_options_get_timestamp_parsers(options.handle, addr nParsers)
#   result = newSeq[string](nParsers)
#   for i in 0 ..< nParsers:
#     result[i] = $cparsers[i]

proc readCSV*(uri: string, options: CsvReadOptions): ArrowTable =
  let scheme = g_uri_peek_scheme(uri)
  var fullUri = uri
  if $scheme == "":
    fullUri = fmt"file://{uri}"

  let guri = check g_uri_parse(fullUri.cstring, G_URI_FLAGS_NONE)
  defer:
    g_uri_unref(guri)
  let path = $g_uri_get_path(guri)
  let fs = newFileSystem(fullUri)

  with fs.openInputStream(path), stream:
    var err: ptr GError
    let reader = garrow_csv_reader_new(stream.handle, options.handle, err.addr)
    defer:
      g_object_unref(reader)
    let tablePtr = check garrow_csv_reader_read(reader)
    result = newArrowTable(tablePtr)

  if options.schema.isSome:
    var keep = initHashSet[string]()
    for f in options.schema.get().ffields:
      keep.incl(f.name)

    for k in result.keys:
      if k notin keep:
        result = result.removeColumn(k)

proc readCSV*(uri: string): ArrowTable =
  let options = newCsvReadOptions()
  return readCSV(uri, options)

# proc garrow_record_batch_writer_write_table*(
#   writer: ptr GArrowRecordBatchWriter, table: ptr GArrowTable, error: ptr ptr GError
# ): gboolean {.cdecl, importc: "garrow_record_batch_writer_write_table".}

# proc newRecordBatchStreamWriter(sinkStream: OutputStream, schema: Schema): RecordBatchStreamWriter =
#   result.handle = check garrow_record_batch_stream_writer_new(sinkStream.handle, schema.toPtr)

# proc asRecordBatchWriter*(writer: RecordBatchStreamWriter): ptr GArrowRecordBatchWriter =
#   ## Safely upcast to parent type using GObject type system
#   result = cast[ptr GArrowRecordBatchWriter](writer.handle)

proc newWriteOptions*(
    includeHeader = true, batchSize = 1024, delimiter = ',', nullString = "", eol = "\n"
): WriteOptions =
  WriteOptions(
    includeHeader: includeHeader,
    batchSize: batchSize,
    delimiter: delimiter,
    nullString: nullString,
    eol: eol,
  )

proc needsQuoting(value: string, delimiter: char): bool =
  ## Check if a value needs to be quoted in CSV output
  value.contains(delimiter) or value.contains('"') or value.contains('\n') or
    value.contains('\r')

proc escapeField(value: string, delimiter: char): string =
  ## Escape a CSV field, quoting and escaping as necessary
  if value.needsQuoting(delimiter):
    '"' & value.replace("\"", "\"\"") & '"'
  else:
    value

proc formatRow(fields: openArray[string], options: WriteOptions): string =
  ## Format a sequence of fields as a CSV row

  fields.mapIt(it.escapeField(options.delimiter)).join($options.delimiter) & options.eol

proc columnToStrings*[T](arr: ChunkedArray[T], options: WriteOptions): seq[string] =
  result = newSeq[string](arr.len)
  var idx = 0
  for chunk in arr.chunks:
    for i in 0 ..< chunk.len:
      if chunk.isNull(i):
        result[idx] = options.nullString
      else:
        # result[idx] = toCSVString(chunk[i], options)
        result[idx] = $chunk[i]
      inc idx

proc writeCsv*[T: Writable](writable: T, options: WriteOptions, output: OutputStream) =
  ## Write any Writable type to CSV format

  # Write header row if requested
  if options.includeHeader:
    output.write(writable.keys.toSeq.formatRow(options))

  let nRows = writable.nRows
  let nCols = writable.nColumns

  # Process in batches for memory efficiency
  for offset in countup(0, nRows - 1, options.batchSize):
    let batchEnd = min(offset + options.batchSize, nRows)
    let batchLen = batchEnd - offset
    let tbl = writable.slice(offset, batchLen)

    # Pre-allocate string columns for this batch
    var columns = newSeq[seq[string]](nCols)

    # Convert each column to strings based on its type
    for colIdx in 0 ..< nCols:
      let dataType = tbl.schema.getField(colIdx).dataType.nimTypeName
      case dataType
      of "bool":
        columns[colIdx] = columnToStrings(tbl[colIdx, bool], options)
      of "int8":
        columns[colIdx] = columnToStrings(tbl[colIdx, int8], options)
      of "uint8":
        columns[colIdx] = columnToStrings(tbl[colIdx, uint8], options)
      of "int16":
        columns[colIdx] = columnToStrings(tbl[colIdx, int16], options)
      of "uint16":
        columns[colIdx] = columnToStrings(tbl[colIdx, uint16], options)
      of "int32":
        columns[colIdx] = columnToStrings(tbl[colIdx, int32], options)
      of "uint32":
        columns[colIdx] = columnToStrings(tbl[colIdx, uint32], options)
      of "int64":
        columns[colIdx] = columnToStrings(tbl[colIdx, int64], options)
      of "uint64":
        columns[colIdx] = columnToStrings(tbl[colIdx, uint64], options)
      of "float32":
        columns[colIdx] = columnToStrings(tbl[colIdx, float32], options)
      of "float64":
        columns[colIdx] = columnToStrings(tbl[colIdx, float64], options)
      of "string":
        columns[colIdx] = columnToStrings(tbl[colIdx, string], options)

    # Write rows from the columnar data
    var rowBuffer = newSeq[string](nCols)
    for rowIdx in 0 ..< batchLen:
      for colIdx in 0 ..< nCols:
        rowBuffer[colIdx] = columns[colIdx][rowIdx]
      output.write(rowBuffer.formatRow(options))

proc writeCsv*[T: Writable](
    uri: string, writable: T, options: WriteOptions = newWriteOptions()
) =
  let scheme = g_uri_peek_scheme(uri)
  var fullUri = uri
  if $scheme == "":
    fullUri = fmt"file://{uri}"

  let guri = check g_uri_parse(fullUri.cstring, G_URI_FLAGS_NONE)
  defer:
    g_uri_unref(guri)
  let path = $g_uri_get_path(guri)
  let fs = newFileSystem(fullUri)

  with fs.openOutputStream(path), stream:
    writeCsv(writable, options, stream)
