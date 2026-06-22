## CSV file reading and writing.
##
## Use `readCSV` to read a CSV file into an `ArrowTable` with automatic type
## inference, or `writeCsv` to write a table to CSV. `CsvReadOptions` and
## `WriteOptions` control parsing and formatting behavior.
import std/[strformat, options, tables, sets, sequtils, strutils]
import ../core/[ffi, error]
import ../types/gtypes
import ../column/[metadata, primitive]
import ../tabular/[table, batch]
import ./filesystem

type
  CsvReadOptions* = object
    ## Options for CSV reading: delimiter, header row, column types, etc.
    handle*: ptr GArrowCSVReadOptions
    schema*: Option[Schema]

  WriteOptions* = object
    ## Options for CSV writing: header inclusion, batch size, delimiter, etc.
    includeHeader*: bool
    batchSize*: int
    delimiter*: char
    nullString*: string
    eol*: string

  Writable* = concept w
    w.nRows is int64
    w.nColumns is int
    slice(w, int64, int64) is typed
    for col in w.columns:
      col is Field

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
  ## Creates `CsvReadOptions` with optional property overrides.
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
    g_object_unref(o.handle)

proc `=wasMoved`*(o: var CsvReadOptions) =
  o.handle = nil
  o.schema = none(Schema)

proc `=dup`*(o: CsvReadOptions): CsvReadOptions =
  result.handle = o.handle
  result.schema = o.schema
  if o.handle != nil:
    discard g_object_ref(o.handle)

proc `=copy`*(dest: var CsvReadOptions, src: CsvReadOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.schema = src.schema
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
  if cnames.isNil:
    return @[]

  var i = 0
  while not cnames[i].isNil:
    inc i
  result = newSeq[string](i)
  for j in 0 ..< i:
    result[j] = $cnames[j]

# Column types methods (requires DataType)
proc addColumnType*(options: CsvReadOptions, name: string, dataType: GADType) =
  garrow_csv_read_options_add_column_type(options.handle, name.cstring, dataType.handle)

proc addSchema*(options: var CsvReadOptions, schema: Schema) =
  garrow_csv_read_options_add_schema(options.handle, schema.handle)
  options.schema = some(schema)

proc addNullValue*(options: CsvReadOptions, value: string) =
  garrow_csv_read_options_add_null_value(options.handle, value.cstring)

proc setNullValues*(options: CsvReadOptions, values: openArray[string]) =
  var cvalues = newSeq[cstring](values.len)
  for i, value in values:
    cvalues[i] = value.cstring
  garrow_csv_read_options_set_null_values(
    options.handle, cast[ptr cstring](cvalues[0].addr), gsize(values.len)
  )

proc addTrueValue*(options: CsvReadOptions, value: string) =
  garrow_csv_read_options_add_true_value(options.handle, value.cstring)

proc setTrueValues*(options: CsvReadOptions, values: openArray[string]) =
  var cvalues = newSeq[cstring](values.len)
  for i, value in values:
    cvalues[i] = value.cstring
  garrow_csv_read_options_set_true_values(
    options.handle, cast[ptr cstring](cvalues[0].addr), gsize(values.len)
  )

proc setFalseValues*(options: CsvReadOptions, values: openArray[string]) =
  var cvalues = newSeq[cstring](values.len)
  for i, value in values:
    cvalues[i] = value.cstring
  garrow_csv_read_options_set_false_values(
    options.handle, cast[ptr cstring](cvalues[0].addr), gsize(values.len)
  )

proc readCSV*(uri: string, options: CsvReadOptions): ArrowTable =
  ## Reads a CSV file into an `ArrowTable` using the given options.
  let scheme = g_uri_peek_scheme(uri)
  var fullUri = uri
  if $scheme == "":
    fullUri = fmt"file://{uri}"

  let guri = verify g_uri_parse(fullUri.cstring, G_URI_FLAGS_NONE)
  defer:
    g_uri_unref(guri)
  let path = $g_uri_get_path(guri)
  let fs = newFileSystem(fullUri)

  with fs.openInputStream(path), stream:
    var err: ptr GError
    let reader = garrow_csv_reader_new(stream.handle, options.handle, err.addr)
    defer:
      g_object_unref(reader)
    let tablePtr = verify garrow_csv_reader_read(reader)
    result = newArrowTable(tablePtr)

  if options.schema.isSome:
    var keep = initHashSet[string]()
    for f in options.schema.get().ffields:
      keep.incl(f.name)

    for k in result.keys:
      if k notin keep:
        result = result.removeColumn(k)

proc readCSV*(uri: string): ArrowTable =
  ## Reads a CSV file into an `ArrowTable` with default options.
  let options = newCsvReadOptions()
  return readCSV(uri, options)

proc newWriteOptions*(
    includeHeader = true, batchSize = 1024, delimiter = ',', nullString = "", eol = "\n"
): WriteOptions =
  ## Creates `WriteOptions` with the given formatting parameters.
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

proc formatRow(columns: openArray[string], options: WriteOptions): string =
  ## Format a sequence of fields as a CSV row

  columns.mapIt(it.escapeField(options.delimiter)).join($options.delimiter) & options.eol

# Helper to format a cell value without closures to avoid ARC issues
template formatCell(col: typed, rowIdx: int): string =
  if col.isValid(rowIdx):
    $col[rowIdx]
  else:
    ""

proc writeCsv*[T: Writable](writable: T, options: WriteOptions, output: OutputStream) =
  ## Writes a table to an output stream as CSV.
  let columns = writable.columns.toSeq
  if options.includeHeader:
    var columnNames = newSeq[string](columns.len)
    for i, c in columns:
      columnNames[i] = c.name
    output.write(columnNames.formatRow(options))

  let nRows = writable.nRows
  let nCols = writable.nColumns

  for offset in countup(0, nRows - 1, options.batchSize):
    let
      batchEnd = min(offset + options.batchSize, nRows)
      batchLen = batchEnd - offset
      tbl = writable.slice(offset, batchLen)

    # Extract each column once per batch, then iterate rows.
    # This avoids calling tbl[colIdx, T] inside the per-cell loop,
    # which created a new ChunkedArray with GObject refcount churn
    # on every single cell access.
    var colStrings = newSeq[seq[string]](nCols)
    for c in 0 ..< nCols:
      let colMeta = columns[c]
      colStrings[c] = newSeq[string](batchLen.int)
      case colMeta.dataType.nimTypeName
      of "int", "int64":
        let col = tbl[c, int64]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "int32":
        let col = tbl[c, int32]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "int16":
        let col = tbl[c, int16]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "int8":
        let col = tbl[c, int8]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "uint64":
        let col = tbl[c, uint64]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "uint32":
        let col = tbl[c, uint32]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "float64", "float":
        let col = tbl[c, float64]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "float32":
        let col = tbl[c, float32]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "bool":
        let col = tbl[c, bool]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      of "string", "utf8":
        let col = tbl[c, string]
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = formatCell(col, r)
      else:
        for r in 0 ..< batchLen.int:
          colStrings[c][r] = ""

    var rowBuffer = newSeq[string](nCols)
    for r in 0 ..< batchLen.int:
      for c in 0 ..< nCols:
        rowBuffer[c] = colStrings[c][r]
      output.write(rowBuffer.formatRow(options))

proc writeCsv*[T: Writable](
    uri: string, writable: T, options: WriteOptions = newWriteOptions()
) =
  ## Writes a table to a CSV file at the given URI.
  let scheme = g_uri_peek_scheme(uri)
  var fullUri = uri
  if $scheme == "":
    fullUri = fmt"file://{uri}"

  let guri = verify g_uri_parse(fullUri.cstring, G_URI_FLAGS_NONE)
  defer:
    g_uri_unref(guri)
  let path = $g_uri_get_path(guri)
  let fs = newFileSystem(fullUri)

  with fs.openOutputStream(path), stream:
    writeCsv(writable, options, stream)
