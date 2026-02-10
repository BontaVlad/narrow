import std/[macros, options]
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

template dispatchNewTable(
    schema: Schema, ptrs: openArray[ptr GArrowRecordBatch]
): ArrowTable =
  newArrowTableFromRecordBatches(schema, ptrs)

template dispatchNewTable(
    schema: Schema, ptrs: openArray[ptr GArrowArray]
): ArrowTable =
  newArrowTableFromArrays(schema, ptrs)

template dispatchNewTable(
    schema: Schema, ptrs: openArray[ptr GArrowChunkedArray]
): ArrowTable =
  newArrowTableFromChunkedArrays(schema, ptrs)

macro newArrowTable*(schema: Schema, args: varargs[typed]): ArrowTable =
  ## Creates a new ArrowTable from a schema and either:
  ## - RecordBatch objects
  ## - Array[T] objects (can be mixed types)
  ## - ChunkedArray[T] objects (can be mixed types)
  if args.len == 0:
    error("newArrowTable requires at least one argument after schema")

  var bracket = newNimNode(nnkBracket)
  for arg in args:
    bracket.add quote do:
      `arg`.toPtr

  result = quote:
    dispatchNewTable(`schema`, `bracket`)

macro newArrowTable*(rows: typed): ArrowTable =
  ## Creates a new ArrowTable from a sequence of named tuples.
  ##
  ## The tuple field names become column names and types are inferred.
  ## Only named tuples like `(col1: 1, col2: "a")` are supported.
  ##
  ## Example:
  ##   let data = @[
  ##     (col1: 1, col2: "a"),
  ##     (col1: 2, col2: "b")
  ##   ]
  ##   let table = newArrowTable(data)

  let rowsType = rows.getTypeInst()

  # Extract element type from seq[T]
  if rowsType.kind != nnkBracketExpr or rowsType.len < 2:
    error("Expected seq of tuples, got: " & rowsType.repr, rows)

  let elemType = rowsType[1]

  # Extract field names and types from TupleTy
  var fieldInfo: seq[tuple[name: string, typ: NimNode]] = @[]

  if elemType.kind == nnkTupleTy:
    for identDef in elemType:
      if identDef.kind == nnkIdentDefs and identDef.len >= 2:
        let nameNode = identDef[0]
        let typeNode = identDef[1]

        if nameNode.kind == nnkSym:
          fieldInfo.add(($nameNode, typeNode))
        else:
          error("Expected named tuple field, got: " & nameNode.repr, rows)
  else:
    error(
      "Expected named tuple type (tuple[field: Type, ...]), got: " & elemType.repr, rows
    )

  if fieldInfo.len == 0:
    error("No fields found in tuple type. Anonymous tuples are not supported.", rows)

  # Build all statements in the result directly
  result = newStmtList()

  # let data = rows
  let dataSym = genSym(nskLet, "data")
  result.add newLetStmt(dataSym, rows)

  # if data.len == 0: raise ValueError
  let checkEmpty = quote:
    if `dataSym`.len == 0:
      raise newException(ValueError, "Cannot create ArrowTable from empty data")
  result.add checkEmpty

  # var schemaFields = newSeq[Field]()
  let schemaFieldsSym = genSym(nskVar, "schemaFields")
  result.add newVarStmt(
    schemaFieldsSym,
    quote do:
      newSeq[Field](),
  )

  # Add field creation for each field
  for (name, typ) in fieldInfo:
    let nameLit = newLit(name)
    let fieldStmt = quote:
      `schemaFieldsSym`.add(newField[`typ`](`nameLit`))
    result.add fieldStmt

  # let schema = newSchema(schemaFields)
  let schemaSym = genSym(nskLet, "schema")
  result.add newLetStmt(
    schemaSym,
    quote do:
      newSchema(`schemaFieldsSym`),
  )

  # Create a ChunkedArray for each column and collect their pointers
  let chunkedArraysSym = genSym(nskVar, "chunkedArrays")
  result.add newVarStmt(
    chunkedArraysSym,
    quote do:
      newSeq[ptr GArrowChunkedArray](),
  )

  # Add column extraction for each field
  for (name, typ) in fieldInfo:
    let fieldName = ident(name)
    let colSym = genSym(nskVar, "colData")
    let arrSym = genSym(nskLet, "arr")
    let chunkedSym = genSym(nskLet, "chunkedArray")

    # var colData = newSeq[Typ](data.len)
    let newSeqCall = newNimNode(nnkCall).add(
        newNimNode(nnkBracketExpr).add(bindSym"newSeq", typ),
        newDotExpr(dataSym, ident"len"),
      )
    result.add newVarStmt(colSym, newSeqCall)

    # for rowIdx in 0 ..< data.len: colData[rowIdx] = data[rowIdx].fieldName
    let rowIdxSym = genSym(nskForVar, "rowIdx")
    let rowLoop = newNimNode(nnkForStmt)
    rowLoop.add rowIdxSym
    rowLoop.add newNimNode(nnkInfix).add(
      ident"..<", newLit(0), newDotExpr(dataSym, ident"len")
    )
    let rowLoopBody = newStmtList()
    let assignStmt = newAssignment(
      newNimNode(nnkBracketExpr).add(colSym, rowIdxSym),
      newDotExpr(newNimNode(nnkBracketExpr).add(dataSym, rowIdxSym), fieldName),
    )
    rowLoopBody.add assignStmt
    rowLoop.add rowLoopBody
    result.add rowLoop

    # let arr = newArray(colData)
    result.add newLetStmt(arrSym, newCall(bindSym"newArray", colSym))

    # let chunkedArray = newChunkedArray[Typ]([arr])
    let chunkedCall = newNimNode(nnkCall).add(
        newNimNode(nnkBracketExpr).add(bindSym"newChunkedArray", typ),
        newNimNode(nnkBracket).add(arrSym),
      )
    result.add newLetStmt(chunkedSym, chunkedCall)

    # chunkedArrays.add(chunkedArray.toPtr)
    result.add newCall(
      newDotExpr(chunkedArraysSym, ident"add"), newDotExpr(chunkedSym, ident"toPtr")
    )

  # newArrowTableFromChunkedArrays(schema, chunkedArrays)
  result.add quote do:
    newArrowTableFromChunkedArrays(`schemaSym`, `chunkedArraysSym`)

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
  let idx = tbl.schema.getFieldIndex(key)
  result = tbl.removeColumn(idx)

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

proc getColumnData*[T](tbl: ArrowTable, idx: int): ChunkedArray[T] =
  ## Get column data with compile-time type and runtime type validation

  when defined(debug):
    let schema = tbl.schema
    let field = schema.getField(idx)
    let dataType = field.dataType
    # Runtime type check
    dataType.checkType(T)

  let handle = garrow_table_get_column_data(tbl.handle, idx.gint)
  result = newChunkedArray[T](handle)

proc `[]`*(tbl: ArrowTable, idx: int): ChunkedArray[byte] =
  ## Get column by index without specifying type (returns ChunkedArray[byte])
  ## This avoids ARC destructor issues with ChunkedArray[void]
  let handle = garrow_table_get_column_data(tbl.handle, idx.gint)
  result = newChunkedArray[byte](handle)

proc `[]`*(tbl: ArrowTable, idx: int, T: typedesc): ChunkedArray[T] =
  result = getColumnData[T](tbl, idx)

proc `[]`*(tbl: ArrowTable, key: string): ChunkedArray[byte] =
  ## Get column by name without specifying type (returns ChunkedArray[byte])
  ## This avoids ARC destructor issues with ChunkedArray[void]
  let schema = tbl.schema
  let idx = schema.getFieldIndex(key)
  let handle = garrow_table_get_column_data(tbl.handle, idx.gint)
  result = newChunkedArray[byte](handle)

proc `[]`*(tbl: ArrowTable, key: string, T: typedesc): ChunkedArray[T] =
  let schema = tbl.schema
  let idx = schema.getFieldIndex(key)
  result = getColumnData[T](tbl, idx)

iterator keys*(tbl: ArrowTable): string =
  for field in tbl.schema:
    yield field.name

iterator columns*(tbl: ArrowTable): Field =
  for field in tbl.schema:
    yield field

# Row-level access methods
proc isNull*(tbl: ArrowTable, rowIdx: int, colIdx: int): bool =
  ## Check if cell at (rowIdx, colIdx) is null
  if rowIdx < 0 or rowIdx >= tbl.nRows:
    raise newException(IndexDefect, "Row index out of bounds: " & $rowIdx)
  if colIdx < 0 or colIdx >= tbl.nColumns:
    raise newException(IndexDefect, "Column index out of bounds: " & $colIdx)

  let handle = garrow_table_get_column_data(tbl.handle, colIdx.gint)
  let colArray = newChunkedArray[int8](handle) # Type doesn't matter for null checking
  result = colArray.isNull(rowIdx)

proc isNull*(tbl: ArrowTable, rowIdx: int, colName: string): bool =
  ## Check if cell at (rowIdx, colName) is null
  let schema = tbl.schema
  let colIdx = schema.getFieldIndex(colName)
  result = tbl.isNull(rowIdx, colIdx)

proc isValid*(tbl: ArrowTable, rowIdx: int, colIdx: int): bool {.inline.} =
  ## Check if cell at (rowIdx, colIdx) is valid (not null)
  result = not tbl.isNull(rowIdx, colIdx)

proc isValid*(tbl: ArrowTable, rowIdx: int, colName: string): bool {.inline.} =
  ## Check if cell at (rowIdx, colName) is valid (not null)
  result = not tbl.isNull(rowIdx, colName)

proc tryGet*[T](tbl: ArrowTable, rowIdx: int, colIdx: int): Option[T] =
  ## Safely get value at (rowIdx, colIdx), returns none if out of bounds or null
  if rowIdx < 0 or rowIdx >= tbl.nRows or colIdx < 0 or colIdx >= tbl.nColumns:
    return none(T)

  let colData = tbl.getColumnData[T](colIdx)
  if colData.isNull(rowIdx):
    return none(T)

  result = some(colData[rowIdx])

proc tryGet*[T](tbl: ArrowTable, rowIdx: int, colName: string): Option[T] =
  ## Safely get value at (rowIdx, colName), returns none if out of bounds or null
  if rowIdx < 0 or rowIdx >= tbl.nRows:
    return none(T)

  let schema = tbl.schema
  let colIdx = schema.getFieldIndex(colName)
  if colIdx < 0:
    return none(T)

  result = tbl.tryGet[T](rowIdx, colIdx)

type TableRow* = object ## Represents a single row in an ArrowTable for iteration
  table*: ArrowTable
  index*: int

proc len*(row: TableRow): int {.inline.} =
  ## Number of columns in the row
  result = row.table.nColumns

proc isNull*(row: TableRow, idx: int): bool =
  ## Check if column idx in this row is null
  result = row.table.isNull(row.index, idx)

proc isValid*(row: TableRow, idx: int): bool {.inline.} =
  ## Check if column idx in this row is valid
  result = not row.isNull(idx)

proc `$`*(row: TableRow): string =
  ## String representation of a row
  result = "Row " & $row.index & ": ["
  for i in 0 ..< row.table.nColumns:
    if i > 0:
      result &= ", "
    if row.isNull(i):
      result &= "null"
    else:
      result &= "?" # Cannot easily convert without type info
  result &= "]"

iterator items*(tbl: ArrowTable): TableRow =
  ## Iterate over rows in the table
  for i in 0 ..< tbl.nRows:
    yield TableRow(table: tbl, index: i.int)

proc nNulls*(tbl: ArrowTable): int64 =
  ## Total number of null values across all columns
  result = 0
  for i in 0 ..< tbl.nColumns:
    let handle = garrow_table_get_column_data(tbl.handle, i.gint)
    let colArray = newChunkedArray[int8](handle) # Type doesn't matter for null checking
    # Need to count nulls across all chunks
    for j in 0 ..< tbl.nRows.int:
      if colArray.isNull(j):
        result += 1
