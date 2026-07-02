## Arrow record batches, readers, iterators, and builders.
##
## A `RecordBatch` is a collection of equal-length arrays sharing a schema —
## the unit of streaming data in Arrow. `RecordBatchReader` provides streaming
## access, and `RecordBatchBuilder` constructs batches incrementally.

import std/[macros, options, strformat]
import ../core/[ffi, error, utils]
import ../types/[gtypes, glist]
import ../column/[primitive, metadata, buffer]

# ============================================================================
# RecordBatch and Related Types
# ============================================================================

arcGObject:
  type
    RecordBatchBuilder* = object
      ## Builder for constructing `RecordBatch`es incrementally.
      ## Access column builders via `columnBuilder`, then call `flush`.
      handle: ptr GArrowRecordBatchBuilder

    RecordBatch* = object
      ## A collection of equal-length arrays sharing a schema.
      ## The unit of streaming data in Arrow.
      handle*: ptr GArrowRecordBatch

    RecordBatchIterator* = object ## Iterator over a sequence of record batches.
      handle: ptr GArrowRecordBatchIterator

    WriteOptions* = object
      handle: ptr GArrowWriteOptions

# Manual type — multiple handles, not eligible for arcGObject
type RecordBatchReader* = object
  ## Streaming reader that produces `RecordBatch`es one at a time.
  ## `nil` from `readNext` signals end-of-stream.
  handle*: ptr GArrowRecordBatchReader
  streamHandle*: ptr GArrowInputStream # Keep stream alive as long as reader exists

proc `=destroy`*(reader: RecordBatchReader) =
  if reader.handle != nil:
    g_object_unref(reader.handle)
  if reader.streamHandle != nil:
    g_object_unref(reader.streamHandle)

proc `=wasMoved`*(reader: var RecordBatchReader) =
  reader.handle = nil
  reader.streamHandle = nil

proc `=dup`*(reader: RecordBatchReader): RecordBatchReader =
  result.handle = reader.handle
  result.streamHandle = reader.streamHandle
  if reader.handle != nil:
    discard g_object_ref(result.handle)
  if reader.streamHandle != nil:
    discard g_object_ref(result.streamHandle)

proc `=copy`*(dest: var RecordBatchReader, src: RecordBatchReader) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)
  if dest.streamHandle != src.streamHandle:
    if dest.streamHandle != nil:
      g_object_unref(dest.streamHandle)
    dest.streamHandle = src.streamHandle
    if src.streamHandle != nil:
      discard g_object_ref(dest.streamHandle)

proc toPtr*(reader: RecordBatchReader): ptr GArrowRecordBatchReader {.inline.} =
  reader.handle

proc schema*(reader: RecordBatchReader): Schema =
  ## Returns the schema of the record batch reader.
  let handle = garrow_record_batch_reader_get_schema(reader.toPtr)
  result = newSchema(handle)

proc newRecordBatch*(arr: pointer, schema: Schema): RecordBatch =
  let handle = verify garrow_record_batch_import(arr, schema.handle)
  result.handle = handle

proc newRecordBatch*(handle: ptr GArrowRecordBatch): RecordBatch =
  result.handle = handle

macro newRecordBatch*(schema: Schema, arrays: varargs[typed]): RecordBatch =
  ## Create a record batch from a schema and arrays (one per field).
  let builderSym = genSym(nskLet, "builder")

  var bodyStmts = newStmtList()

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
  let cstr = verify garrow_record_batch_to_string(rb.handle)
  result = $newGString(cstr, owned = true)

proc validate*(rb: RecordBatch): bool =
  var err: ptr GError
  result = garrow_record_batch_validate(rb.toPtr, addr err).bool
  if not err.isNil:
    raise
      newException(ValueError, fmt"RecordBatch validation failed, got {err[].message}")

proc validateFull*(rb: RecordBatch): bool =
  var err: ptr GError
  result = garrow_record_batch_validate_full(rb.toPtr, addr err).bool
  if not err.isNil:
    raise newException(
      ValueError, fmt"RecordBatch full validation failed, got {err[].message}"
    )

proc schema*(rb: RecordBatch): Schema =
  ## Returns the schema of the record batch.
  let handle = garrow_record_batch_get_schema(rb.handle)
  result = newSchema(handle)

proc nColumns*(rb: RecordBatch): int =
  ## Returns the number of columns in the record batch.
  garrow_record_batch_get_n_columns(rb.handle).int

proc nRows*(rb: RecordBatch): int64 =
  ## Returns the number of rows in the record batch.
  garrow_record_batch_get_n_rows(rb.handle).int64

proc getColumnName*(rb: RecordBatch, idx: int): string =
  result = $garrow_record_batch_get_column_name(rb.toPtr, idx.gint)

proc getColumnData*[T](rb: RecordBatch, idx: int): Array[T] =
  ## Returns the column data at index `idx` as a typed array.
  ## The column's runtime GArrowType tag is checked against `T`; a mismatch
  ## raises `TypeError`.
  let handle = garrow_record_batch_get_column_data(rb.toPtr, idx.gint)
  result = newArray[T](handle)
  result.dataType.checkType(T)

proc `[]`*[T](rb: RecordBatch, idx: int, _: typedesc[T]): Array[T] =
  ## Returns the column at `idx` as `Array[T]`.
  result = getColumnData[T](rb, idx)

proc `[]`*[T](rb: RecordBatch, key: string, _: typedesc[T]): Array[T] =
  ## Returns the column named `key` as `Array[T]`. Raises `KeyError` if not found.
  let schema = rb.schema
  let idx = schema.getFieldIndex(key)
  result = getColumnData[T](rb, idx)

proc `==`*(rb1, rb2: RecordBatch): bool {.inline.} =
  garrow_record_batch_equal(rb1.toPtr, rb2.toPtr).bool

proc equalMetadata*(
    rb1, rb2: RecordBatch, checkMetadata: bool = true
): bool {.inline.} =
  garrow_record_batch_equal_metadata(rb1.toPtr, rb2.toPtr, checkMetadata.gboolean).bool

proc slice*(rb: RecordBatch, offset: int64, length: int64): RecordBatch =
  ## Returns a sub-batch covering `offset` to `offset + length`. Shares data with the original.
  let handle = garrow_record_batch_slice(rb.toPtr, offset.gint64, length.gint64)
  result = newRecordBatch(handle)

proc addColumn*(rb: RecordBatch, idx: uint, field: Field, column: Array): RecordBatch =
  ## Returns a new batch with a column inserted at `idx`.
  let handle = verify garrow_record_batch_add_column(
    rb.toPtr, idx.guint, field.toPtr, column.toPtr
  )
  result = newRecordBatch(handle)

proc removeColumn*(rb: RecordBatch, idx: uint): RecordBatch =
  ## Returns a new batch without the column at `idx`.
  let handle = verify garrow_record_batch_remove_column(rb.toPtr, idx.guint)
  result = newRecordBatch(handle)

proc schema*(builder: RecordBatchBuilder): Schema =
  result = newSchema(garrow_record_batch_builder_get_schema(builder.toPtr))

proc newRecordBatchBuilder*(schema: Schema): RecordBatchBuilder =
  ## Create a new builder for the given schema.
  let handle = verify garrow_record_batch_builder_new(schema.toPtr)
  result.handle = handle

proc newRecordBatchBuilder*(schema: Schema, capacity: int): RecordBatchBuilder =
  result = newRecordBatchBuilder(schema)
  garrow_record_batch_builder_set_initial_capacity(result.toPtr, capacity.gint64)

proc capacity*(builder: RecordBatchBuilder): int64 =
  result = garrow_record_batch_builder_get_initial_capacity(builder.toPtr).int64

proc columnBuilder*[T](
    builder: RecordBatchBuilder, idx: int
): ArrayBuilder[T] {.inline.} =
  ## Returns the array builder for column `idx`. Append values to it.
  let cBuilder = garrow_record_batch_builder_get_column_builder(builder.toPtr, idx.gint)
  # get_column_builder returns transfer-none (cached internal reference),
  # so we must ref before wrapping in an owning ArrayBuilder
  discard g_object_ref(cBuilder)
  result = newArrayBuilder[T](cBuilder)

proc flush*(builder: RecordBatchBuilder): RecordBatch =
  ## Builds the record batch and resets the column builders.
  let handle = verify garrow_record_batch_builder_flush(builder.toPtr)
  result.handle = handle

proc newRecordBatchIterator*(recordBatches: seq[RecordBatch]): RecordBatchIterator =
  ## Create an iterator over the given batches.
  var glist = newGList[ptr GArrowRecordBatch]()
  for rb in recordBatches:
    glist.append(rb.toPtr)

  let handle = garrow_record_batch_iterator_new(glist.toPtr)
  result.handle = handle

proc next*(it: RecordBatchIterator): Option[RecordBatch] =
  ## Returns the next batch, or `none` at end-of-stream.
  var err: ptr GError
  let handle = garrow_record_batch_iterator_next(it.toPtr, addr err)

  if not isNil(err):
    g_error_free(err)
    return none(RecordBatch)

  if handle == nil:
    return none(RecordBatch)

  result = some(newRecordBatch(handle))

proc `==`*(it1, it2: RecordBatchIterator): bool {.inline.} =
  garrow_record_batch_iterator_equal(it1.toPtr, it2.toPtr).bool

proc toList*(it: RecordBatchIterator): seq[RecordBatch] =
  ## Consumes the iterator and returns all remaining batches.
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
  var maybeRb = it.next()
  while maybeRb.isSome:
    yield maybeRb.get()
    maybeRb = it.next()

iterator columns*(rb: RecordBatch): Field =
  for field in rb.schema:
    yield field

proc isNull*(rb: RecordBatch, rowIdx: int, colIdx: int): bool =
  if rowIdx < 0 or rowIdx >= rb.nRows:
    raise newException(IndexDefect, "Row index out of bounds: " & $rowIdx)
  if colIdx < 0 or colIdx >= rb.nColumns:
    raise newException(IndexDefect, "Column index out of bounds: " & $colIdx)

  let handle = garrow_record_batch_get_column_data(rb.toPtr, colIdx.gint)
  let colArray = newArray[Untyped](handle)
  result = colArray.isNull(rowIdx)

proc isNull*(rb: RecordBatch, rowIdx: int, colName: string): bool =
  let schema = rb.schema
  let colIdx = schema.getFieldIndex(colName)
  result = rb.isNull(rowIdx, colIdx)

proc isValid*(rb: RecordBatch, rowIdx: int, colIdx: int): bool {.inline.} =
  result = not rb.isNull(rowIdx, colIdx)

proc isValid*(rb: RecordBatch, rowIdx: int, colName: string): bool {.inline.} =
  result = not rb.isNull(rowIdx, colName)

proc tryGet*[T](rb: RecordBatch, rowIdx: int, colIdx: int): Option[T] =
  if rowIdx < 0 or rowIdx >= rb.nRows or colIdx < 0 or colIdx >= rb.nColumns:
    return none(T)

  let colData = rb.getColumnData[T](colIdx)
  if colData.isNull(rowIdx):
    return none(T)

  result = some(colData[rowIdx])

proc tryGet*[T](rb: RecordBatch, rowIdx: int, colName: string): Option[T] =
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
  result = row.batch.nColumns

proc isNull*(row: RecordBatchRow, idx: int): bool =
  result = row.batch.isNull(row.index, idx)

proc isValid*(row: RecordBatchRow, idx: int): bool {.inline.} =
  result = not row.isNull(idx)

proc `$`*(row: RecordBatchRow): string =
  result = "Row " & $row.index & ": ["
  for i in 0 ..< row.batch.nColumns:
    if i > 0:
      result &= ", "
    if row.isNull(i):
      result &= "null"
    else:
      result &= "?"
  result &= "]"

iterator items*(rb: RecordBatch): RecordBatchRow =
  for i in 0 ..< rb.nRows:
    yield RecordBatchRow(batch: rb, index: i.int)

proc nNulls*(rb: RecordBatch): int64 =
  result = 0
  for i in 0 ..< rb.nColumns:
    result += rb[i, int8].nNulls
