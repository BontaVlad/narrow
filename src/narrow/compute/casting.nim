import std/[options, tables]
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

proc castTo*(
    arr: Array, gtype: GADType, options: CastOptions = newCastOptions()
): Array[void] =
  let handle = verify garrow_array_cast(arr.toPtr, gtype.toPtr, options.toPtr)
  result = newArray[void](handle)

proc castTo*[T: ArrowValue](
    arr: Array, options: CastOptions = newCastOptions()
): Array[T] =
  let gtype = newGType(T)
  let handle = verify garrow_array_cast(arr.toPtr, gtype.toPtr, options.toPtr)
  result = newArray[T](handle)

proc castChunks*(
    chunkedArray: ChunkedArray, gtype: GADType, options: CastOptions = newCastOptions()
): ChunkedArray[void] =
  var casted = newSeq[Array[void]]()
  for chunk in chunkedArray.chunks:
    casted.add castTo(chunk, gtype, options)
  if casted.len == 0:
    let handle = verify garrow_chunked_array_new_empty(gtype.toPtr)
    result = newChunkedArray[void](handle)
  else:
    result = newChunkedArray[void](casted)

proc castTable*(
    table: ArrowTable,
    castMap: openArray[(string, GADType)],
    options: CastOptions = newCastOptions(),
): ArrowTable =
  ## Cast specific columns in a table to new types.
  ## Columns not mentioned in the map are passed through unchanged.

  if castMap.len == 0:
    return table

  let schema = table.schema
  let nCols = table.nColumns

  var allIdentity = true
  for (name, gtype) in castMap:
    let fieldOpt = schema.tryGetField(name)
    if fieldOpt.isNone:
      raise newException(ValueError, "Column not found in table: " & name)
    if fieldOpt.get.dataType != gtype:
      allIdentity = false
      break

  if allIdentity:
    return table

  var fields = newSeq[Field](nCols)
  var castedChunks = newSeq[ChunkedArray[void]](nCols)
  var touched = newSeq[bool](nCols)

  # Process castMap first — build new fields/chunks for changed columns
  for (name, gtype) in castMap:
    let fieldOpt = schema.tryGetField(name)
    if fieldOpt.isNone:
      raise newException(ValueError, "Column not found in table: " & name)

    let field = fieldOpt.get
    let idx = schema.getFieldIndex(name)
    touched[idx] = true
    if field.dataType != gtype:
      fields[idx] = newField(name, gtype)
      castedChunks[idx] = castChunks(table[idx], gtype, options)
    else:
      fields[idx] = field
      castedChunks[idx] = table[idx]

  # Copy pass-through columns (not in castMap at all)
  for i in 0 ..< nCols:
    if not touched[i]:
      fields[i] = schema.getField(i)
      castedChunks[i] = table[i]

  result = newArrowTable(newSchema(fields), castedChunks)
