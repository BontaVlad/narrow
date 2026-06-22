## Type casting for arrays, chunked arrays, and tables.
##
## `castTo` produces a new array with a different data type. `castTable`
## casts specific columns in a table, passing through unchanged columns.
import std/[options, tables]
import ../core/[ffi, error, utils]
import ../types/[gtypes, glist]
import ../column/primitive
import ../tabular/table
import ../column/metadata
import ../compute/expressions
import ./functions

# ============================================================================
# Type Definitions
# ============================================================================

arcGObject:
  type CastOptions* = object
    ## Options for cast operations: overflow, truncation, and error behavior.
    handle*: ptr GArrowCastOptions

# ============================================================================
# Constructors
# ============================================================================

proc newCastOptions*(): CastOptions =
  ## Create default cast options.
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
  if not isNil(result.handle):
    discard g_object_ref(result.handle)

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
  ## Cast an array to a different data type (untyped result).
  let handle = verify garrow_array_cast(arr.toPtr, gtype.toPtr, options.toPtr)
  result = newArray[void](handle)

proc castTo*[T: ArrowValue](
    arr: Array, options: CastOptions = newCastOptions()
): Array[T] =
  ## Cast an array to a typed `Array[T]` using `T`'s Arrow data type.
  let gtype = newGType(T)
  let handle = verify garrow_array_cast(arr.toPtr, gtype.toPtr, options.toPtr)
  result = newArray[T](handle)

proc castChunks*(
    chunkedArray: ChunkedArray, gtype: GADType, options: CastOptions = newCastOptions()
): ChunkedArray[void] =
  ## Cast each chunk of a chunked array to a new data type.
  var opts = options
  opts.toDataType = gtype
  let arg = newDatum(chunkedArray)
  let datum = call("cast", arg, options = toFunctionOptions(opts))
  result = datum.toChunkedArray

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
