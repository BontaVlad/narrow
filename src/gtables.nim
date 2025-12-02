import macros
import ./[ffi, gchunkedarray, garray, glist, gtypes, gschema, grecordbatch, error]

type ArrowTable* = object
  handle: ptr GArrowTable

# =============================================================================
# ArrowTable Implementation
# =============================================================================

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

proc newArrowTable*(schema: Schema, recordBatches: openArray[RecordBatch]): ArrowTable =
  if recordBatches.len == 0:
    raise newException(ValueError, "Cannot create table from empty record batches")

  var rbHandles = newSeq[ptr GArrowRecordBatch](recordBatches.len)
  for i, rb in recordBatches:
    rbHandles[i] = rb.toPtr

  let handle = check garrow_table_new_record_batches(
    schema.handle, addr rbHandles[0], recordBatches.len.gsize
  )

  result.handle = handle

proc newArrowTable*(handle: ptr GArrowTable): ArrowTable =
  result.handle = handle

proc newArrowTable*(schema: Schema, values: openArray[seq[auto]]): ArrowTable =
  ## Create a table from schema and values using GList
  if values.len == 0:
    raise newException(ValueError, "Cannot create table from empty values")

  var valueList = newGList[pointer]()
  for val in values:
    valueList.append(cast[pointer](val))

  let handle = check garrow_table_new_values(schema.handle, valueList.list)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc newArrowTable*(
    schema: Schema, chunkedArrays: openArray[ChunkedArray]
): ArrowTable =
  ## Create a table from schema and chunked arrays
  if chunkedArrays.len == 0:
    raise newException(ValueError, "Cannot create table from empty chunked arrays")

  var caHandles = newSeq[ptr GArrowChunkedArray](chunkedArrays.len)
  for i, ca in chunkedArrays:
    echo repr cast[pointer](ca.toPtr)
    caHandles[i] = ca.toPtr

  var err: ptr GError = nil
  let handle = garrow_table_new_chunked_arrays(
    schema.handle, addr caHandles[0], chunkedArrays.len.gsize, err.addr
  )

proc newArrowTable*(schema: Schema, arrays: openArray[Array]): ArrowTable =
  ## Create a table from schema and arrays
  if arrays.len == 0:
    raise newException(ValueError, "Cannot create table from empty arrays")

  var arrHandles = newSeq[ptr GArrowArray](arrays.len)
  for i, arr in arrays:
    arrHandles[i] = arr.toPtr

  let handle =
    check garrow_table_new_arrays(schema.handle, addr arrHandles[0], arrays.len.gsize)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

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
    garrow_table_add_column(tbl.handle, idx.guint, field.handle, column.toPtr, addr err)

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
  let idx = tbl.schema.getFieldIndex(key)
  if idx < 0:
    raise newException(KeyError, "Column not found: " & key)
  result = tbl.removeColumn(idx)

proc replaceColumn*(
    tbl: ArrowTable, idx: int, field: Field, column: ChunkedArray
): ArrowTable =
  let handle =
    check garrow_table_replace_column(tbl.handle, idx.guint, field.handle, column.toPtr)
  result = newArrowTable(handle)

proc equal*(a, b: ArrowTable): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_table_equal(a.handle, b.handle).bool

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
  var tableList: ptr GList = nil
  for other in others:
    tableList = g_list_append(tableList, other.handle)

  let options = garrow_table_concatenate_options_new()

  var err: ptr GError
  let handle = garrow_table_concatenate(tbl.handle, tableList, options, addr err)

  g_list_free(tableList)
  g_object_unref(options)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Concatenate failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  result = newArrowTable(handle)

proc getColumnData*(tbl: ArrowTable, idx: int): ChunkedArray =
  let handle = garrow_table_get_column_data(tbl.handle, idx.gint)
  result = newChunkedArray(handle)

proc `[]`*(tbl: ArrowTable, idx: int): ChunkedArray =
  tbl.getColumnData(idx)

proc `[]`*(tbl: ArrowTable, key: string): ChunkedArray =
  let idx = tbl.schema.getFieldIndex(key)
  if idx < 0:
    raise newException(KeyError, "Column not found: " & key)
  tbl.getColumnData(idx)

iterator keys*(tbl: ArrowTable): string =
  for field in tbl.schema:
    yield field.name

iterator columns*(tbl: ArrowTable): (string, ChunkedArray) =
  for i in 0 ..< tbl.nColumns:
    let field = tbl.schema.getField(i)
    let column = tbl.getColumnData(i)
    yield (field.name, column)
