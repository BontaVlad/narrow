import std/tables
import ../core/[ffi, error, utils]
import ../types/[gtypes, glist]
import ../column/primitive
import ../tabular/table
import ../column/metadata
import ./functions

# ============================================================================
# Type Definitions
# ============================================================================

arcGObject:
  type CastOptions* = object
    handle*: ptr GArrowCastOptions

# ============================================================================
# Constructors
# ============================================================================

proc newCastOptions*(): CastOptions =
  let handle = garrow_cast_options_new()
  if isNil(handle):
    raise newException(IOError, "Failed to create CastOptions")
  result.handle = handle

# ============================================================================
# Conversion to FunctionOptions (for compute kernel use)
# ============================================================================

proc toFunctionOptions*(options: CastOptions): FunctionOptions =
  ## Cast CastOptions to the base FunctionOptions type for use with
  ## the compute function registry (e.g., ``call("cast", ...)``).
  result.handle = cast[ptr GArrowFunctionOptions](options.handle)
  if not isNil(options.handle):
    discard g_object_ref(options.handle)

# ============================================================================
# Properties
# ============================================================================

proc allowIntOverflow*(options: CastOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "allow-int-overflow", addr value, nil)
  result = value.bool

proc `allowIntOverflow=`*(options: CastOptions, value: bool) =
  g_object_set(options.handle, "allow-int-overflow", gboolean(value), nil)

proc allowTimeTruncate*(options: CastOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "allow-time-truncate", addr value, nil)
  result = value.bool

proc `allowTimeTruncate=`*(options: CastOptions, value: bool) =
  g_object_set(options.handle, "allow-time-truncate", gboolean(value), nil)

proc allowTimeOverflow*(options: CastOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "allow-time-overflow", addr value, nil)
  result = value.bool

proc `allowTimeOverflow=`*(options: CastOptions, value: bool) =
  g_object_set(options.handle, "allow-time-overflow", gboolean(value), nil)

proc allowDecimalTruncate*(options: CastOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "allow-decimal-truncate", addr value, nil)
  result = value.bool

proc `allowDecimalTruncate=`*(options: CastOptions, value: bool) =
  g_object_set(options.handle, "allow-decimal-truncate", gboolean(value), nil)

proc allowFloatTruncate*(options: CastOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "allow-float-truncate", addr value, nil)
  result = value.bool

proc `allowFloatTruncate=`*(options: CastOptions, value: bool) =
  g_object_set(options.handle, "allow-float-truncate", gboolean(value), nil)

proc allowInvalidUtf8*(options: CastOptions): bool =
  var value: gboolean
  g_object_get(options.handle, "allow-invalid-utf8", addr value, nil)
  result = value.bool

proc `allowInvalidUtf8=`*(options: CastOptions, value: bool) =
  g_object_set(options.handle, "allow-invalid-utf8", gboolean(value), nil)

proc toDataType*(options: CastOptions): GADType =
  ## Get the target data type for casting.
  var value: ptr GArrowDataType
  g_object_get(options.handle, "to-data-type", addr value, nil)
  if isNil(value):
    raise newException(ValueError, "to-data-type not set on CastOptions")
  result = GADType(handle: value)

proc `toDataType=`*(options: CastOptions, value: GADType) =
  ## Set the target data type for casting.
  g_object_set(options.handle, "to-data-type", value.toPtr, nil)

# ============================================================================
# Array cast
# ============================================================================

proc castTo*[T](arr: Array, options: CastOptions = newCastOptions()): Array[T] =
  ## Cast an array to a new data type.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let arr = newArray(@[1'i32, 2, 3])
  ##     let asFloat = castTo[float64](arr)   # [1.0, 2.0, 3.0]
  let targetType = newGType(T)
  options.toDataType = targetType
  let handle = verify garrow_array_cast(arr.toPtr, targetType.toPtr, options.toPtr)
  result = newArray[T](handle)

# ============================================================================
# Table cast helpers (chunk-by-chunk, preserves chunking)
# ============================================================================

proc castChunks(
    colHandle: ptr GArrowChunkedArray, targetGType: GADType, options: CastOptions
): ptr GArrowChunkedArray =
  let nChunks = garrow_chunked_array_get_n_chunks(colHandle)
  var chunkList = newGList[ptr GArrowArray]()
  g_object_set(options.handle, "to-data-type", targetGType.toPtr, nil)

  for chunkIdx in 0.uint ..< nChunks:
    let chunk = garrow_chunked_array_get_chunk(colHandle, chunkIdx.guint)
    let casted = verify garrow_array_cast(chunk, targetGType.toPtr, options.toPtr)
    chunkList.append(casted)
    g_object_unref(chunk)

  result = verify garrow_chunked_array_new(chunkList.toPtr)

  # Unref our array refs — the new ChunkedArray holds shared_ptrs to the data
  for casted in chunkList.items:
    g_object_unref(casted)

# ============================================================================
# Hashmap-driven castTable
# ============================================================================

proc castTable*(
    table: ArrowTable,
    castMap: openArray[(string, GADType)],
    options: CastOptions = newCastOptions(),
): ArrowTable =
  ## Cast specific columns in a table to new types.
  ## Columns not mentioned in the map are passed through unchanged.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let result = castTable(table, [
  ##       ("id", newGType(int64)),
  ##       ("score", newGType(float32)),
  ##     ])
  var targetMap = initTable[string, GADType]()
  for (name, gtype) in castMap:
    targetMap[name] = gtype

  let schema = table.schema

  # Fast path: if no types actually change, return the original table
  var anyCastNeeded = false
  for i in 0 ..< schema.nFields:
    let field = schema.getField(i)
    if targetMap.hasKey(field.name) and
        garrow_data_type_equal(field.dataType.toPtr, targetMap[field.name].toPtr) == 0:
      anyCastNeeded = true
      break
  if not anyCastNeeded:
    result = table
    return

  var columns = newSeq[ptr GArrowChunkedArray](schema.nFields)
  var fields = newSeq[Field](schema.nFields)

  for i in 0 ..< schema.nFields:
    let field = schema.getField(i)
    let name = field.name
    let colHandle = garrow_table_get_column_data(table.toPtr, i.gint)

    if targetMap.hasKey(name):
      let targetGType = targetMap[name]
      columns[i] = castChunks(colHandle, targetGType, options)
      fields[i] = newField(name, targetGType)
      g_object_unref(colHandle)
    else:
      columns[i] = colHandle
      fields[i] = field

  let newSchema = newSchema(fields)
  result = newArrowTableFromChunkedArrays(newSchema, columns)

  # Unref our column handles — the table owns them now
  for col in columns:
    g_object_unref(col)

# ============================================================================
# Schema-driven castTable
# ============================================================================

proc castTable*(
    table: ArrowTable, schema: Schema, options: CastOptions = newCastOptions()
): ArrowTable =
  ## Cast table columns to match the given schema.
  ## Only columns whose type differs are cast; identical columns pass through.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let newSchema = newSchema([
  ##       newField[int64]("id"),
  ##       newField[string]("name"),
  ##       newField[float32]("score"),
  ##     ])
  ##     let result = castTable(table, newSchema)
  let oldSchema = table.schema

  # Fast path: if schemas are identical, return the original table
  var anyCastNeeded = false
  for i in 0 ..< schema.nFields:
    let newField = schema.getField(i)
    let oldIdx = oldSchema.getFieldIndex(newField.name)
    let oldField = oldSchema.getField(oldIdx)
    if garrow_data_type_equal(oldField.dataType.toPtr, newField.dataType.toPtr) == 0:
      anyCastNeeded = true
      break
  if not anyCastNeeded:
    result = table
    return

  var columns = newSeq[ptr GArrowChunkedArray](schema.nFields)
  var fields = newSeq[Field](schema.nFields)

  for i in 0 ..< schema.nFields:
    let newField = schema.getField(i)
    let name = newField.name
    let oldIdx = oldSchema.getFieldIndex(name)
    let colHandle = garrow_table_get_column_data(table.toPtr, oldIdx.gint)
    let oldField = oldSchema.getField(oldIdx)

    if garrow_data_type_equal(oldField.dataType.toPtr, newField.dataType.toPtr) == 0:
      columns[i] = castChunks(colHandle, newField.dataType, options)
      g_object_unref(colHandle)
    else:
      columns[i] = colHandle

    fields[i] = newField

  result = newArrowTableFromChunkedArrays(schema, columns)

  # Unref our column handles — the table owns them now
  for col in columns:
    g_object_unref(col)
