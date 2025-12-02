import macros
import ./[ffi, gchunkedarray, garray, glist, gtypes, gschema, error]

type
  RecordBatchBuilder* = object
    handle: ptr GArrowRecordBatchBuilder

  RecordBatch* = object
    handle: ptr GArrowRecordBatch

proc `=destroy`*(rb: RecordBatch) =
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

proc toPtr*(rb: RecordBatch): ptr GArrowRecordBatch {.inline.} =
  rb.handle

proc newRecordBatch*(arr: pointer, schema: Schema): RecordBatch =
  var err: ptr GError
  let handle = garrow_record_batch_import(arr, schema.handle, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "RecordBatch import failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc newRecordBatch*(handle: ptr GArrowRecordBatch): RecordBatch =
  if handle != nil:
    if g_object_is_floating(handle) != 0:
      discard g_object_ref_sink(handle)
    else:
      discard g_object_ref(handle)
  result.handle = handle

macro newRecordBatch*(schema: Schema, arrays: varargs[typed]): RecordBatch =
  ## Create a RecordBatch from a schema and typed arrays
  ## This macro builds a RecordBatch using RecordBatchBuilder

  # TODO: add check if array is the same type as declared in the schema
  var stmts = newStmtList()
  let builderSym = genSym(nskLet, "builder")
  let resultSym = genSym(nskLet, "recordBatch")

  # Create the builder
  stmts.add quote do:
    let `builderSym` = newRecordBatchBuilder(`schema`)

  # For each array, add columnBuilder().appendValues() call
  for i, arr in arrays:
    let idx = newLit(i)

    # Extract the type from Array[T]
    let arrType = arr.getTypeInst()

    # Get the element type T from Array[T]
    var elementType: NimNode
    if arrType.kind == nnkBracketExpr:
      elementType = arrType[1]
    else:
      error("Expected iterable type, got: " & arrType.repr, arr)

    let typeDesc = nnkBracketExpr.newTree(ident"typedesc", elementType)

    # Build: builder.columnBuilder(T, i).appendValues(array)
    stmts.add quote do:
      `builderSym`.columnBuilder(`typeDesc`, `idx`).appendValues(`arr`)

  # Add the flush call
  stmts.add quote do:
    let `resultSym` = `builderSym`.flush()

  # Return the record batch
  stmts.add(resultSym)

  result = newBlockStmt(stmts)

proc `$`*(rb: RecordBatch): string =
  var err: ptr GError
  let cstr = garrow_record_batch_to_string(rb.handle, addr err)

  if not isNil(err):
    g_error_free(err)
    return "<RecordBatch: error>"

  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc isValid*(rb: RecordBatch): bool {.inline.} =
  rb.handle != nil

proc schema*(rb: RecordBatch): Schema =
  let handle = garrow_record_batch_get_schema(rb.handle)
  result = newSchema(handle)

proc nColumns*(rb: RecordBatch): int =
  garrow_record_batch_get_n_columns(rb.handle).int

proc nRows*(rb: RecordBatch): int64 =
  garrow_record_batch_get_n_rows(rb.handle).int64

proc getColumnName*(rb: RecordBatch, idx: int): string =
  result = $newGString(garrow_record_batch_get_column_name(rb.toPtr, idx.gint))

proc getColumnData*[T](rb: RecordBatch, _: typedesc[T], idx: int): Array[T] =
  let handle = garrow_record_batch_get_column_data(rb.toPtr, idx.gint)
  result = newArray[T](handle)

template `[]`*(rb: RecordBatch, idx: int, T: typedesc): Array[T] =
  rb.getColumnData(idx, T)

template `[]`*(rb: RecordBatch, key: string, T: typedesc): Array[T] =
  let idx = rb.schema.getFieldIndex(key)
  if idx < 0:
    raise newException(KeyError, "Column not found: " & key)
  rb.getColumnData(idx, T)

proc toPtr*(b: RecordBatchBuilder): ptr GArrowRecordBatchBuilder {.inline.} =
  b.handle

proc schema*(builder: RecordBatchBuilder): Schema =
  result = newSchema(garrow_record_batch_builder_get_schema(builder.toPtr))

# proc `=destroy`*(builder: RecordBatchBuilder) =
#   if not isNil(builder.handle):
#     g_object_unref(builder.handle)

# proc `=sink`*(dest: var RecordBatchBuilder, src: RecordBatchBuilder) =
#   if not isNil(dest.handle) and dest.handle != src.handle:
#     g_object_unref(dest.handle)
#   dest.handle = src.handle

# proc `=copy`*(dest: var RecordBatchBuilder, src: RecordBatchBuilder) =
#   if dest.handle != src.handle:
#     if not isNil(dest.handle):
#       g_object_unref(dest.handle)
#     dest.handle = src.handle
#     if not isNil(dest.handle):
#       discard g_object_ref(dest.handle)

proc newRecordBatchBuilder*(schema: Schema): RecordBatchBuilder =
  let handle = check garrow_record_batch_builder_new(schema.toPtr)
  result = RecordBatchBuilder(handle: handle)

proc newRecordBatchBuilder*(schema: Schema, capacity: int): RecordBatchBuilder =
  result = newRecordBatchBuilder(schema)
  garrow_record_batch_builder_set_initial_capacity(result.toPtr, capacity.gint64)

proc capacity*(builder: RecordBatchBuilder): int64 =
  result = garrow_record_batch_builder_get_initial_capacity(builder.toPtr).int64

proc columnBuilder*[T](
    builder: RecordBatchBuilder, _: typedesc[T], idx: int
): ArrayBuilder[T] =
  let cBuilder = garrow_record_batch_builder_get_column_builder(builder.toPtr, idx.gint)
  result = newArrayBuilder[T](cBuilder)

proc flush*(builder: RecordBatchBuilder): RecordBatch =
  let handle = check garrow_record_batch_builder_flush(builder.toPtr)
  result = RecordBatch(handle: handle)
