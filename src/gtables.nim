import macros
import ./[ffi, gchunkedarray, garray, glist, gtypes, error]


# # Signal handler for crashes
# proc crashHandler(sig: cint) {.noconv.} =
#   stderr.write("\n" & "=".repeat(60) & "\n")
#   stderr.write("CRASH DETECTED - Signal: " & $sig & "\n")
#   stderr.write("=".repeat(60) & "\n")
#   writeStackTrace()
#   stderr.write("=".repeat(60) & "\n")
#   quit(1)

# # Setup handlers
# signal(SIGSEGV, crashHandler)
# signal(SIGABRT, crashHandler)
# signal(SIGILL, crashHandler)
# signal(SIGFPE, crashHandler)


type
  Field* = object
    handle: ptr GArrowField

  Schema* = object
    handle*: ptr GArrowSchema

  ArrowTable* = object
    handle: ptr GArrowTable

  RecordBatchBuilder* = object
    handle: ptr GArrowRecordBatchBuilder

  RecordBatch* = object
    handle: ptr GArrowRecordBatch

# =============================================================================
# Field Implementation
# =============================================================================

proc `=destroy`*(field: Field) =
  if field.handle != nil:
    g_object_unref(field.handle)

proc `=sink`*(dest: var Field, src: Field) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Field, src: Field) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(f: Field): ptr GArrowField {.inline.} =
  f.handle

proc newField*[T](name: string): Field =
  let gType = newGType(T)
  let handle = garrow_field_new(name.cstring, gType)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc newField*(handle: ptr GArrowField): Field =
  result.handle = handle

proc name*(field: Field): string =
  let cstr = garrow_field_get_name(field.handle)
  if cstr != nil:
    result = $cstr

proc dataType*(field: Field): ptr GArrowDataType =
  garrow_field_get_data_type(field.handle)

proc `$`*(field: Field): string =
  let cstr = garrow_field_to_string(field.handle)
  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc `==`*(a, b: Field): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_field_equal(a.handle, b.handle).bool

proc isValid*(field: Field): bool {.inline.} =
  field.handle != nil

# =============================================================================
# Schema Implementation
# =============================================================================

proc `=destroy`*(schema: Schema) =
  if schema.handle != nil:
    g_object_unref(schema.handle)

proc `=sink`*(dest: var Schema, src: Schema) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Schema, src: Schema) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(s: Schema): ptr GArrowSchema {.inline.} =
  s.handle

proc newSchema*(fields: openArray[Field]): Schema =
  var fieldList = newGList[ptr GArrowField]()
  for field in fields:
    fieldList.append(field.handle)
  result.handle = garrow_schema_new(fieldList.list)

proc newSchema*(gptr: pointer): Schema =
  var err: ptr GError
  let handle = garrow_schema_import(gptr, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Schema import failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc newSchema*(handle: ptr GArrowSchema): Schema =
  result.handle = handle

proc `$`*(schema: Schema): string =
  let cstr = garrow_schema_to_string(schema.handle)
  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc nFields*(schema: Schema): int =
  garrow_schema_n_fields(schema.handle).int

proc getField*(schema: Schema, idx: int): Field =
  let handle = garrow_schema_get_field(schema.handle, idx.guint)
  result = newField(handle)

proc getFieldByName*(schema: Schema, name: string): Field =
  let handle = garrow_schema_get_field_by_name(schema.handle, name.cstring)
  result = newField(handle)

proc getFieldIndex*(schema: Schema, name: string): int =
  garrow_schema_get_field_index(schema.handle, name.cstring).int

proc ffields*(schema: Schema): seq[Field] =
  let glistPtr = garrow_schema_get_fields(schema.handle)

  if glistPtr == nil:
    return @[]

  result = newSeq[Field]()
  var current = glistPtr
  while current != nil:
    let item = current.data
    if item != nil:
      let fieldPtr = cast[ptr GArrowField](item)
      result.add(newField(fieldPtr))
    current = current.next

  g_list_free(glistPtr)

iterator items*(schema: Schema): Field =
  for field in schema.ffields:
    yield field

proc isValid*(schema: Schema): bool {.inline.} =
  schema.handle != nil

proc `==`*(a, b: Schema): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_schema_equal(a.handle, b.handle).bool

# =============================================================================
# RecordBatch Implementation
# =============================================================================

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
  echo repr result

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

# =============================================================================
# RecordBatchBuilder Implementation
# =============================================================================

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

proc columnBuilder*[T](builder: RecordBatchBuilder, _: typedesc[T], idx: int): ArrayBuilder[T] =
  let cBuilder = garrow_record_batch_builder_get_column_builder(builder.toPtr, idx.gint)
  result = newArrayBuilder[T](cBuilder)
  
proc flush*(builder: RecordBatchBuilder): RecordBatch =
  let handle = check garrow_record_batch_builder_flush(builder.toPtr)
  result = RecordBatch(handle: handle)

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

  let handle = check garrow_table_new_record_batches(schema.handle, addr rbHandles[0], recordBatches.len.gsize)

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

proc newArrowTable*(schema: Schema, chunkedArrays: openArray[ChunkedArray]): ArrowTable =
  ## Create a table from schema and chunked arrays
  if chunkedArrays.len == 0:
    raise newException(ValueError, "Cannot create table from empty chunked arrays")
  
  var caHandles = newSeq[ptr GArrowChunkedArray](chunkedArrays.len)
  for i, ca in chunkedArrays:
    echo repr cast[pointer](ca.toPtr)
    caHandles[i] = ca.toPtr
  
  var err: ptr GError = nil
  let handle = garrow_table_new_chunked_arrays(
    schema.handle, 
    addr caHandles[0], 
    chunkedArrays.len.gsize,
    err.addr
  )
  # if not err.isNil:
  #   echo err[].message
  
  # result.handle = handle

proc newArrowTable*(schema: Schema, arrays: openArray[Array]): ArrowTable =
  ## Create a table from schema and arrays
  if arrays.len == 0:
    raise newException(ValueError, "Cannot create table from empty arrays")
  
  var arrHandles = newSeq[ptr GArrowArray](arrays.len)
  for i, arr in arrays:
    arrHandles[i] = arr.toPtr
  
  let handle = check garrow_table_new_arrays(
    schema.handle, 
    addr arrHandles[0], 
    arrays.len.gsize
  )
  
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
