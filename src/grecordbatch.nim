import std/[macros, options, strformat]
import ./[ffi, gchunkedarray, garray, glist, gtypes, gschema, error]

type
  RecordBatchBuilder* = object
    handle: ptr GArrowRecordBatchBuilder

  RecordBatch* = object
    handle: ptr GArrowRecordBatch

  RecordBatchIterator* = object
    handle: ptr GArrowRecordBatchIterator

  RecordBatchReader* = object
    handle: ptr GArrowRecordBatchReader

  WriteOptions* = object
    handle: ptr GArrowWriteOptions

  GBuffer* = object
    handle: ptr GArrowBuffer

proc newBuffer*(data: pointer, size: int64): GBuffer =
  return GBuffer(handle: garrow_buffer_new(cast[ptr uint8](data), size.gint64))

proc toPtr*(opt: WriteOptions): ptr GArrowWriteOptions =
  opt.handle

proc newWriteOptions(): WriteOptions =
  discard

proc `=destroy`*(rb: RecordBatch) =
  # echo "DESTROY ______________________---------------"
  # echo repr cast[pointer](rb.handle)
  if rb.handle != nil:
    g_object_unref(rb.handle)

proc `=sink`*(dest: var RecordBatch, src: RecordBatch) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var RecordBatch, src: RecordBatch) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(it: RecordBatchIterator) =
  if it.handle != nil:
    g_object_unref(it.handle)

proc `=sink`*(dest: var RecordBatchIterator, src: RecordBatchIterator) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var RecordBatchIterator, src: RecordBatchIterator) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(rb: RecordBatchBuilder) =
  if rb.handle != nil:
    g_object_unref(rb.handle)

proc `=sink`*(dest: var RecordBatchBuilder, src: RecordBatchBuilder) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var RecordBatchBuilder, src: RecordBatchBuilder) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(rb: RecordBatch): ptr GArrowRecordBatch {.inline.} =
  rb.handle

proc toPtr*(it: RecordBatchIterator): ptr GArrowRecordBatchIterator {.inline.} =
  it.handle

proc newRecordBatch*(arr: pointer, schema: Schema): RecordBatch =
  let handle = check garrow_record_batch_import(arr, schema.handle)
  result.handle = handle

proc newRecordBatch*(handle: ptr GArrowRecordBatch): RecordBatch =
  result.handle = handle

macro newRecordBatch*(schema: Schema, arrays: varargs[typed]): RecordBatch =
  let builderSym = genSym(nskLet, "builder")

  var bodyStmts = newStmtList()

  # For each array, add columnBuilder().appendValues() call
  for i, arr in arrays:
    let idx = newLit(i)
    let arrType = arr.getTypeInst()

    var elementType: NimNode
    if arrType.kind == nnkBracketExpr:
      elementType = arrType[1]
    else:
      error("Expected iterable type, got: " & arrType.repr, arr)

    let typeDesc = nnkBracketExpr.newTree(ident"typedesc", elementType)

    bodyStmts.add quote do:
      columnBuilder[`typeDesc`](`builderSym`, `idx`).appendValues(`arr`)

  result = quote:
    let `builderSym` = newRecordBatchBuilder(`schema`)
    `bodyStmts`
    `builderSym`.flush()

proc `$`*(rb: RecordBatch): string =
  var err: ptr GError
  let cstr = garrow_record_batch_to_string(rb.handle, addr err)

  if not isNil(err):
    g_error_free(err)
    return "<RecordBatch: error>"

  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc validate*(rb: RecordBatch): bool =
  ## Validate the record batch
  var err: ptr GError
  result = garrow_record_batch_validate(rb.toPtr, addr err).bool
  if not err.isNil:
    raise
      newException(ValueError, fmt"RecordBatch validation failed, got {err[].message}")

proc validateFull*(rb: RecordBatch): bool =
  ## Perform full validation of the record batch
  var err: ptr GError
  result = garrow_record_batch_validate_full(rb.toPtr, addr err).bool
  if not err.isNil:
    raise newException(
      ValueError, fmt"RecordBatch full validation failed, got {err[].message}"
    )

proc schema*(rb: RecordBatch): Schema =
  let handle = garrow_record_batch_get_schema(rb.handle)
  result = newSchema(handle)

proc nColumns*(rb: RecordBatch): int =
  garrow_record_batch_get_n_columns(rb.handle).int

proc nRows*(rb: RecordBatch): int64 =
  garrow_record_batch_get_n_rows(rb.handle).int64

proc getColumnName*(rb: RecordBatch, idx: int): string =
  result = $garrow_record_batch_get_column_name(rb.toPtr, idx.gint)

proc getColumnData*[T](rb: RecordBatch, idx: int): Array[T] =
  let handle = garrow_record_batch_get_column_data(rb.toPtr, idx.gint)
  result = newArray[T](handle)

proc `[]`*[T](rb: RecordBatch, idx: int, _: typedesc[T]): Array[T] =
  result = getColumnData[T](rb, idx)

proc `[]`*[T](rb: RecordBatch, key: string, _: typedesc[T]): Array[T] =
  # try:
  let schema = rb.schema
  let idx = schema.getFieldIndex(key)
  result = getColumnData[T](rb, idx)

proc `==`*(rb1, rb2: RecordBatch): bool =
  ## Check equality without metadata
  if rb1.handle == rb2.handle:
    return true
  if rb1.handle == nil or rb2.handle == nil:
    return false
  garrow_record_batch_equal(rb1.toPtr, rb2.toPtr).bool

proc equalMetadata*(rb1, rb2: RecordBatch, checkMetadata: bool = true): bool =
  ## Check equality with optional metadata checking
  if rb1.handle == rb2.handle:
    return true
  if rb1.handle == nil or rb2.handle == nil:
    return false
  garrow_record_batch_equal_metadata(rb1.toPtr, rb2.toPtr, checkMetadata.gboolean).bool

proc slice*(rb: RecordBatch, offset: int64, length: int64): RecordBatch =
  ## Create a zero-copy slice of the record batch
  let handle = garrow_record_batch_slice(rb.toPtr, offset.gint64, length.gint64)
  result = newRecordBatch(handle)

proc addColumn*(rb: RecordBatch, idx: uint, field: Field, column: Array): RecordBatch =
  ## Add a column at the specified index
  let handle =
    check garrow_record_batch_add_column(rb.toPtr, idx.guint, field.toPtr, column.toPtr)
  result = newRecordBatch(handle)

proc removeColumn*(rb: RecordBatch, idx: uint): RecordBatch =
  ## Remove a column at the specified index
  let handle = check garrow_record_batch_remove_column(rb.toPtr, idx.guint)
  result = newRecordBatch(handle)

proc toPtr*(b: RecordBatchBuilder): ptr GArrowRecordBatchBuilder {.inline.} =
  b.handle

proc schema*(builder: RecordBatchBuilder): Schema =
  result = newSchema(garrow_record_batch_builder_get_schema(builder.toPtr))

proc newRecordBatchBuilder*(schema: Schema): RecordBatchBuilder =
  let handle = check garrow_record_batch_builder_new(schema.toPtr)
  result = RecordBatchBuilder(handle: handle)

proc newRecordBatchBuilder*(schema: Schema, capacity: int): RecordBatchBuilder =
  result = newRecordBatchBuilder(schema)
  garrow_record_batch_builder_set_initial_capacity(result.toPtr, capacity.gint64)

proc capacity*(builder: RecordBatchBuilder): int64 =
  result = garrow_record_batch_builder_get_initial_capacity(builder.toPtr).int64

proc columnBuilder*[T](
    builder: RecordBatchBuilder, idx: int
): ArrayBuilder[T] {.inline.} =
  let cBuilder = garrow_record_batch_builder_get_column_builder(builder.toPtr, idx.gint)
  result = newArrayBuilder[T](cBuilder)

proc flush*(builder: RecordBatchBuilder): RecordBatch =
  let handle = check garrow_record_batch_builder_flush(builder.toPtr)
  result = RecordBatch(handle: handle)

proc newRecordBatchIterator*(recordBatches: seq[RecordBatch]): RecordBatchIterator =
  ## Create an iterator from a sequence of record batches
  var glist = newGList[ptr GArrowRecordBatch]()
  for rb in recordBatches:
    glist.append(rb.toPtr)

  let handle = garrow_record_batch_iterator_new(glist.toPtr)
  result = RecordBatchIterator(handle: handle)

proc next*(it: RecordBatchIterator): Option[RecordBatch] =
  ## Get the next record batch, or none if exhausted
  var err: ptr GError
  let handle = garrow_record_batch_iterator_next(it.toPtr, addr err)

  if not isNil(err):
    g_error_free(err)
    return none(RecordBatch)

  if handle == nil:
    return none(RecordBatch)

  result = some(newRecordBatch(handle))

proc `==`*(it1, it2: RecordBatchIterator): bool =
  ## Check if two iterators are equal
  if it1.handle == it2.handle:
    return true
  if it1.handle == nil or it2.handle == nil:
    return false
  garrow_record_batch_iterator_equal(it1.toPtr, it2.toPtr).bool

proc toList*(it: RecordBatchIterator): seq[RecordBatch] =
  ## Convert iterator to a list of record batches
  var err: ptr GError
  let glist = garrow_record_batch_iterator_to_list(it.toPtr, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Failed to convert iterator to list"
    g_error_free(err)
    raise newException(OperationError, msg)

  if glist == nil:
    return @[]

  let list = newGList[ptr GArrowRecordBatch](glist)
  result = @[]
  for i in 0 ..< list.len:
    let handle = list[i]
    result.add(newRecordBatch(handle))

iterator items*(it: RecordBatchIterator): RecordBatch =
  ## Iterate over record batches
  var maybeRb = it.next()
  while maybeRb.isSome:
    yield maybeRb.get()
    maybeRb = it.next()

iterator columns*(rb: RecordBatch): Field =
  for field in rb.schema:
    yield field

# Row-level access methods
proc isNull*(rb: RecordBatch, rowIdx: int, colIdx: int): bool =
  ## Check if cell at (rowIdx, colIdx) is null
  if rowIdx < 0 or rowIdx >= rb.nRows:
    raise newException(IndexDefect, "Row index out of bounds: " & $rowIdx)
  if colIdx < 0 or colIdx >= rb.nColumns:
    raise newException(IndexDefect, "Column index out of bounds: " & $colIdx)

  let handle = garrow_record_batch_get_column_data(rb.toPtr, colIdx.gint)
  let colArray = newArray[void](handle)
  result = colArray.isNull(rowIdx)

proc isNull*(rb: RecordBatch, rowIdx: int, colName: string): bool =
  ## Check if cell at (rowIdx, colName) is null
  let schema = rb.schema
  let colIdx = schema.getFieldIndex(colName)
  result = rb.isNull(rowIdx, colIdx)

proc isValid*(rb: RecordBatch, rowIdx: int, colIdx: int): bool {.inline.} =
  ## Check if cell at (rowIdx, colIdx) is valid (not null)
  result = not rb.isNull(rowIdx, colIdx)

proc isValid*(rb: RecordBatch, rowIdx: int, colName: string): bool {.inline.} =
  ## Check if cell at (rowIdx, colName) is valid (not null)
  result = not rb.isNull(rowIdx, colName)

proc tryGet*[T](rb: RecordBatch, rowIdx: int, colIdx: int): Option[T] =
  ## Safely get value at (rowIdx, colIdx), returns none if out of bounds or null
  if rowIdx < 0 or rowIdx >= rb.nRows or colIdx < 0 or colIdx >= rb.nColumns:
    return none(T)

  let colData = rb.getColumnData[T](colIdx)
  if colData.isNull(rowIdx):
    return none(T)

  result = some(colData[rowIdx])

proc tryGet*[T](rb: RecordBatch, rowIdx: int, colName: string): Option[T] =
  ## Safely get value at (rowIdx, colName), returns none if out of bounds or null
  if rowIdx < 0 or rowIdx >= rb.nRows:
    return none(T)

  let schema = rb.schema
  let colIdx = schema.getFieldIndex(colName)
  if colIdx < 0:
    return none(T)

  result = rb.tryGet[T](rowIdx, colIdx)

type RecordBatchRow* = object ## Represents a single row in a RecordBatch for iteration
  batch*: RecordBatch
  index*: int

proc len*(row: RecordBatchRow): int {.inline.} =
  ## Number of columns in the row
  result = row.batch.nColumns

proc isNull*(row: RecordBatchRow, idx: int): bool =
  ## Check if column idx in this row is null
  result = row.batch.isNull(row.index, idx)

proc isValid*(row: RecordBatchRow, idx: int): bool {.inline.} =
  ## Check if column idx in this row is valid
  result = not row.isNull(idx)

proc `$`*(row: RecordBatchRow): string =
  ## String representation of a row
  result = "Row " & $row.index & ": ["
  for i in 0 ..< row.batch.nColumns:
    if i > 0:
      result &= ", "
    if row.isNull(i):
      result &= "null"
    else:
      result &= "?" # Cannot easily convert without type info
  result &= "]"

iterator items*(rb: RecordBatch): RecordBatchRow =
  ## Iterate over rows in the record batch
  for i in 0 ..< rb.nRows:
    yield RecordBatchRow(batch: rb, index: i.int)

proc nNulls*(rb: RecordBatch): int64 =
  ## Total number of null values across all columns
  result = 0
  for i in 0 ..< rb.nColumns:
    let handle = garrow_record_batch_get_column_data(rb.toPtr, i.gint)
    let colArray = newArray[int8](handle) # Type doesn't matter for null checking
    # We need to count nulls manually since Array doesn't expose this directly
    for j in 0 ..< rb.nRows:
      if colArray.isNull(j):
        result += 1
