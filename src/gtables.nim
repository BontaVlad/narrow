import std/[macros, sequtils]
import ./[ffi, gchunkedarray, garray, glist, gtypes, gschema, grecordbatch, error]

type ArrowTable* = object
  handle: ptr GArrowTable

proc `=destroy`*(tbl: ArrowTable) =
  if tbl.handle != nil:
    g_object_unref(tbl.handle)

proc `=sink`*(dest: var ArrowTable, src: ArrowTable) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var ArrowTable, src: ArrowTable) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(tbl: ArrowTable): ptr GArrowTable {.inline.} =
  tbl.handle

proc newArrowTableFromRecordBatches*(
    schema: Schema, recordBatches: openArray[ptr GArrowRecordBatch]
): ArrowTable =
  if recordBatches.len == 0:
    raise newException(ValueError, "Cannot create table from empty record batches")

  let handle = check garrow_table_new_record_batches(
    schema.toPtr, addr recordBatches[0], recordBatches.len.gsize
  )

  result.handle = handle

proc newArrowTableFromArrays*(
    schema: Schema, arrays: openArray[ptr GArrowArray]
): ArrowTable =
  if arrays.len == 0:
    raise newException(ValueError, "Cannot create table from empty arrays")

  let handle =
    check garrow_table_new_arrays(schema.toPtr, addr arrays[0], arrays.len.gsize)

  result.handle = handle

proc newArrowTableFromChunkedArrays*(
    schema: Schema, chunkedArrays: openArray[ptr GArrowChunkedArray]
): ArrowTable =
  if chunkedArrays.len == 0:
    raise newException(ValueError, "Cannot create table from empty chunked arrays")

  let handle = check garrow_table_new_chunked_arrays(
    schema.toPtr, addr chunkedArrays[0], chunkedArrays.len.gsize
  )

  result.handle = handle

# FIXME: THIS SHOULD USE CONCEPTS BECAUSE THERE IS AN EXPECTATION THAT ALL TYPES HAVE toPtr
# FIXME: this relies on name checks, no bueno
macro newArrowTable*(schema: Schema, args: varargs[typed]): ArrowTable =
  ## Creates a new ArrowTable from a schema and either:
  ## - RecordBatch objects
  ## - Array[T] objects (can be mixed types)
  ## - ChunkedArray[T] objects (can be mixed types)
  if args.len == 0:
    error("newArrowTable requires at least one argument after schema")

  let firstArg = args[0]
  let arrType = firstArg.getTypeInst()

  var typeName: string
  if arrType.kind == nnkBracketExpr:
    typeName = arrType[0].strVal
  elif arrType.kind == nnkSym:
    typeName = arrType.strVal
  else:
    error("Unexpected type node kind: " & $arrType.kind)

  # Check if it's a RecordBatch
  if typeName == "RecordBatch":
    var bracket = newNimNode(nnkBracket)
    for arg in args:
      bracket.add quote do:
        `arg`.toPtr
    result = quote:
      newArrowTableFromRecordBatches(`schema`, `bracket`)

  # Check if it's an Array[T]
  elif typeName == "Array":
    var bracket = newNimNode(nnkBracket)
    for arg in args:
      bracket.add quote do:
        `arg`.toPtr
    result = quote:
      newArrowTableFromArrays(`schema`, `bracket`)

  # Check if it's a ChunkedArray[T]
  elif typeName == "ChunkedArray":
    var bracket = newNimNode(nnkBracket)
    for arg in args:
      bracket.add quote do:
        `arg`.toPtr
    result = quote:
      newArrowTableFromChunkedArrays(`schema`, `bracket`)
  else:
    error(
      "newArrowTable expects RecordBatch, Array[T], or ChunkedArray[T], got: " & typeName
    )

proc newArrowTable*(handle: ptr GArrowTable): ArrowTable =
  result.handle = handle

proc `$`*(tbl: ArrowTable): string =
  var err: ptr GError
  let cstr = garrow_table_to_string(tbl.handle, addr err)

  if not isNil(err):
    g_error_free(err)
    return "<Table: error>"

  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc isValid*(tbl: ArrowTable): bool {.inline.} =
  tbl.handle != nil

proc schema*(tbl: ArrowTable): Schema =
  let handle = garrow_table_get_schema(tbl.handle)
  result = newSchema(handle)

proc nColumns*(tbl: ArrowTable): int =
  garrow_table_get_n_columns(tbl.handle).int

proc nRows*(tbl: ArrowTable): int64 =
  garrow_table_get_n_rows(tbl.handle).int64

proc addColumn*(
    tbl: ArrowTable, idx: int, field: Field, column: ChunkedArray
): ArrowTable =
  var err: ptr GError
  let handle =
    garrow_table_add_column(tbl.handle, idx.guint, field.toPtr, column.toPtr, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Add column failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  result = newArrowTable(handle)

proc removeColumn*(tbl: ArrowTable, idx: int): ArrowTable =
  var err: ptr GError
  let handle = garrow_table_remove_column(tbl.handle, idx.guint, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Remove column failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  result = newArrowTable(handle)

proc removeColumn*(tbl: ArrowTable, key: string): ArrowTable =
  try:
    let idx = tbl.schema.getFieldIndex(key)
    result = tbl.removeColumn(idx)
  except IndexError:
    raise newException(KeyError, "Column not found: " & key)

proc replaceColumn*(
    tbl: ArrowTable, idx: int, field: Field, column: ChunkedArray
): ArrowTable =
  let handle =
    check garrow_table_replace_column(tbl.handle, idx.guint, field.toPtr, column.toPtr)
  result = newArrowTable(handle)

proc equal*(a, b: ArrowTable): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_table_equal(a.handle, b.handle).bool

proc `==`*(a, b: ArrowTable): bool =
  a.equal(b)

proc equalMetadata*(a, b: ArrowTable, checkMetadata: bool): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_table_equal_metadata(a.handle, b.handle, checkMetadata.gboolean).bool

proc slice*(tbl: ArrowTable, offset, length: int64): ArrowTable =
  let handle = garrow_table_slice(tbl.handle, offset.gint64, length.gint64)
  result = newArrowTable(handle)

proc combineChunks*(tbl: ArrowTable): ArrowTable =
  var err: ptr GError
  let handle = garrow_table_combine_chunks(tbl.handle, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Combine chunks failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  result = newArrowTable(handle)

proc validate*(tbl: ArrowTable): bool =
  var err: ptr GError
  result = garrow_table_validate(tbl.handle, addr err).bool
  if not isNil(err):
    g_error_free(err)

proc validateFull*(tbl: ArrowTable): bool =
  var err: ptr GError
  result = garrow_table_validate_full(tbl.handle, addr err).bool
  if not isNil(err):
    g_error_free(err)

proc concatenate*(tbl: ArrowTable, others: openArray[ArrowTable]): ArrowTable =
  var tableList = newGList[ptr GArrowTable]()
  # var tableList: ptr GList = nil
  for other in others:
    tableList.append(other.toPtr)

  let options = garrow_table_concatenate_options_new()
  defer:
    g_object_unref(options)

  let handle = check garrow_table_concatenate(tbl.handle, tableList.toPtr, options)
  result = newArrowTable(handle)

proc getColumnData*[T: ArrowPrimitive](tbl: ArrowTable, idx: int): ChunkedArray[T] =
  ## Get column data with compile-time type and runtime type validation

  when defined(debug):
    let schema = tbl.schema
    let field = schema.getField(idx)
    let dataType = field.dataType
    # Runtime type check
    dataType.checkType(T)

  let handle = garrow_table_get_column_data(tbl.handle, idx.gint)
  result = newChunkedArray[T](handle)

proc `[]`*(tbl: ArrowTable, idx: int, T: typedesc): ChunkedArray[T] =
  result = getColumnData[T](tbl, idx)

proc `[]`*(tbl: ArrowTable, key: string, T: typedesc): ChunkedArray[T] =
  let schema = tbl.schema
  try:
    let idx = schema.getFieldIndex(key)
    result = getColumnData[T](tbl, idx)
  except IndexError:
    raise newException(KeyError, "Column not found: " & key)

iterator keys*(tbl: ArrowTable): string =
  for field in tbl.schema:
    yield field.name
