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

  ColFormatter = proc(rowIdx: int): string

  Writable* =
    concept w
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

proc formatRow(columns: openArray[string], options: WriteOptions): string =
  ## Format a sequence of fields as a CSV row

  columns.mapIt(it.escapeField(options.delimiter)).join($options.delimiter) & options.eol

template createFormatter(tbl, idx, T): ColFormatter {.inject.} =
  let col = tbl[idx, T]
  proc(r: int): string =
    if col.isValid(r): $col[r] else: ""

proc writeCsv*[T: Writable](writable: T, options: WriteOptions, output: OutputStream) =
  let columns = writable.columns.toSeq
  if options.includeHeader:
    var columnNames = newSeq[string]()
    for c in columns:
      columnNames.add(c.name)
    output.write(columnNames.formatRow(options))

  let nRows = writable.nRows
  let nCols = writable.nColumns

  for offset in countup(0, nRows - 1, options.batchSize):
    let
      batchEnd = min(offset + options.batchSize, nRows)
      batchLen = batchEnd - offset
      tbl = writable.slice(offset, batchLen)

    var formatters = newSeq[ColFormatter](nCols)
    
    for i in 0 ..< nCols:
      let colMeta = columns[i]
      # Mapping Arrow types to Nim types
      case colMeta.dataType.nimTypeName
      of "int", "int64":   formatters[i] = createFormatter(tbl, i, int64)
      of "int32":          formatters[i] = createFormatter(tbl, i, int32)
      of "int16":          formatters[i] = createFormatter(tbl, i, int16)
      of "int8":           formatters[i] = createFormatter(tbl, i, int8)
      of "uint64":         formatters[i] = createFormatter(tbl, i, uint64)
      of "uint32":         formatters[i] = createFormatter(tbl, i, uint32)
      of "float64", "float": formatters[i] = createFormatter(tbl, i, float64)
      of "float32":        formatters[i] = createFormatter(tbl, i, float32)
      of "bool":           formatters[i] = createFormatter(tbl, i, bool)
      of "string", "utf8": formatters[i] = createFormatter(tbl, i, string)
      
      else:
        # Generic fallback for unsupported types
        formatters[i] = proc(r: int): string = ""

    # Iterating over the batch rows
    var rowBuffer = newSeq[string](nCols)
    for r in 0 ..< batchLen: # Important: Iterate to batchLen, not nRows
      for c in 0 ..< nCols:
        rowBuffer[c] = formatters[c](r)
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
