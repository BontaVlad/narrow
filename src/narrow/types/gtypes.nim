import std/[macros]
import ../core/[ffi, error]

type
  GADType* = object
    handle*: ptr GArrowDataType

  GString* = object
    handle*: cstring

  # Distinct scalar types for temporal values (to distinguish from primitives)
  Date32* = distinct int32 ## Days since epoch
  Date64* = distinct int64 ## Milliseconds since epoch
  MonthInterval* = distinct int32 ## Number of months

  ArrowPrimitive* =
    void | bool | int8 | uint8 | int16 | uint16 | int32 | uint32 | int | int64 | uint64 |
    float32 | float64 | string | seq[byte] | cstring | Date32 | Date64 | MonthInterval

  # Integer type groupings (compile-time)
  ArrowSignedInt* = int8 | int16 | int32 | int | int64
  ArrowUnsignedInt* = uint8 | uint16 | uint32 | uint64
  ArrowInteger* = ArrowSignedInt | ArrowUnsignedInt

  # Floating point types
  ArrowFloating* = float32 | float64

  # Numeric types
  ArrowNumeric* = ArrowInteger | ArrowFloating

  # Temporal types
  ArrowTemporal* = object # Placeholder - actual types are in gtemporal.nim
  ArrowDate* = object # Placeholder - Date32Array | Date64Array
  ArrowTime* = object # Placeholder - Time32Array | Time64Array

  # Nested types
  ArrowNested* = StructArray | MapArray | ListArray | ListArray[void]

  # Union types - will be added when union types are implemented

  # Forward declarations for complex types (defined in respective modules)
  StructArray* = object # Defined in gstruct.nim
  MapArray*[K, V] = object # Defined in gmaparray.nim
  TimestampArray* = object # Defined in gtemporal.nim
  Date32Array* = object # Defined in gtemporal.nim
  Date64Array* = object # Defined in gtemporal.nim
  Time32Array* = object # Defined in gtemporal.nim
  Time64Array* = object # Defined in gtemporal.nim
  ListArray*[T] = object # Defined in glistarray.nim

  # Union of primitive and complex types that can be stored in Arrow arrays
  ArrowComplex* =
    StructArray | MapArray | TimestampArray | Date32Array | Date64Array | Time32Array |
    Time64Array | ListArray

  # All valid Arrow value types
  ArrowValue* = ArrowPrimitive | ArrowComplex

  TypeError* = object of CatchableError

# Runtime type category sets (mirroring Python's pyarrow.type
const
  SignedIntegerTypes* =
    {GARROW_TYPE_INT8, GARROW_TYPE_INT16, GARROW_TYPE_INT32, GARROW_TYPE_INT64}
  UnsignedIntegerTypes* =
    {GARROW_TYPE_UINT8, GARROW_TYPE_UINT16, GARROW_TYPE_UINT32, GARROW_TYPE_UINT64}
  IntegerTypes* = SignedIntegerTypes + UnsignedIntegerTypes
  FloatingTypes* = {GARROW_TYPE_HALF_FLOAT, GARROW_TYPE_FLOAT, GARROW_TYPE_DOUBLE}
  DecimalTypes* = {
    GARROW_TYPE_DECIMAL32, GARROW_TYPE_DECIMAL64, GARROW_TYPE_DECIMAL128,
    GARROW_TYPE_DECIMAL256,
  }
  DateTypes* = {GARROW_TYPE_DATE32, GARROW_TYPE_DATE64}
  TimeTypes* = {GARROW_TYPE_TIME32, GARROW_TYPE_TIME64}
  IntervalTypes* = {
    GARROW_TYPE_MONTH_INTERVAL, GARROW_TYPE_DAY_TIME_INTERVAL,
    GARROW_TYPE_MONTH_DAY_NANO_INTERVAL,
  }
  TemporalTypes* =
    {GARROW_TYPE_TIMESTAMP, GARROW_TYPE_DURATION} + TimeTypes + DateTypes + IntervalTypes
  UnionTypes* = {GARROW_TYPE_SPARSE_UNION, GARROW_TYPE_DENSE_UNION}
  NestedTypes* =
    {
      GARROW_TYPE_LIST, GARROW_TYPE_FIXED_SIZE_LIST, GARROW_TYPE_LARGE_LIST,
      GARROW_TYPE_STRUCT, GARROW_TYPE_MAP,
    } + UnionTypes

# Type checking procs for runtime type categories
proc isSignedInteger*(arrowType: GArrowType): bool {.inline.} =
  arrowType in SignedIntegerTypes

proc isUnsignedInteger*(arrowType: GArrowType): bool {.inline.} =
  arrowType in UnsignedIntegerTypes

proc isInteger*(arrowType: GArrowType): bool {.inline.} =
  arrowType in IntegerTypes

proc isFloating*(arrowType: GArrowType): bool {.inline.} =
  arrowType in FloatingTypes

proc isDecimal*(arrowType: GArrowType): bool {.inline.} =
  arrowType in DecimalTypes

proc isNumeric*(arrowType: GArrowType): bool {.inline.} =
  arrowType in IntegerTypes + FloatingTypes + DecimalTypes

proc isDate*(arrowType: GArrowType): bool {.inline.} =
  arrowType in DateTypes

proc isTime*(arrowType: GArrowType): bool {.inline.} =
  arrowType in TimeTypes

proc isInterval*(arrowType: GArrowType): bool {.inline.} =
  arrowType in IntervalTypes

proc isTemporal*(arrowType: GArrowType): bool {.inline.} =
  arrowType in TemporalTypes

proc isUnion*(arrowType: GArrowType): bool {.inline.} =
  arrowType in UnionTypes

proc isNested*(arrowType: GArrowType): bool {.inline.} =
  arrowType in NestedTypes

proc isPrimitive*(arrowType: GArrowType): bool {.inline.} =
  not arrowType.isNested

proc toPtr*(g: GADType): ptr GArrowDataType {.inline.} =
  g.handle

proc `=destroy`*(tp: GADType) =
  if not isNil(tp.toPtr):
    g_object_unref(tp.toPtr)

proc `=destroy`*(s: GString) =
  if not isNil(s.handle):
    gFree(s.handle)

proc `=sink`*(dest: var GADType, src: GADType) =
  if not isNil(dest.toPtr) and dest.toPtr != src.toPtr:
    g_object_unref(dest.toPtr)
  # Transfer ownership (move semantics)
  dest.handle = src.handle

proc `=copy`*(dest: var GADType, src: GADType) =
  if dest.toPtr != src.toPtr:
    if not isNil(dest.toPtr):
      g_object_unref(dest.toPtr)
    dest.handle = src.handle
    if not isNil(dest.toPtr):
      discard g_object_ref(dest.toPtr) # bump ref count

proc `=sink`*(dest: var GString, src: GString) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    gFree(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var GString, src: GString) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      gFree(dest.handle)
    dest.handle =
      if not isNil(src.handle):
        g_strdup(src.handle)
      else:
        nil

proc newGString*(str: cstring): GString =
  result.handle = str

proc `$`*(str: GString): string =
  $str.handle

proc `$`*(tp: GADType): string =
  let namePtr = garrow_data_type_get_name(tp.toPtr)
  if isNil(namePtr):
    return "unknown"
  result = $newGString(namePtr)

proc `id`*(tp: GADType): GArrowType =
  garrow_data_type_get_id(tp.toPtr)

# Convenience type checking procs on GADType
proc isSignedInteger*(tp: GADType): bool {.inline.} =
  tp.id.isSignedInteger

proc isUnsignedInteger*(tp: GADType): bool {.inline.} =
  tp.id.isUnsignedInteger

proc isInteger*(tp: GADType): bool {.inline.} =
  tp.id.isInteger

proc isFloating*(tp: GADType): bool {.inline.} =
  tp.id.isFloating

proc isDecimal*(tp: GADType): bool {.inline.} =
  tp.id.isDecimal

proc isNumeric*(tp: GADType): bool {.inline.} =
  tp.id.isNumeric

proc isDate*(tp: GADType): bool {.inline.} =
  tp.id.isDate

proc isTime*(tp: GADType): bool {.inline.} =
  tp.id.isTime

proc isInterval*(tp: GADType): bool {.inline.} =
  tp.id.isInterval

proc isTemporal*(tp: GADType): bool {.inline.} =
  tp.id.isTemporal

proc isUnion*(tp: GADType): bool {.inline.} =
  tp.id.isUnion

proc isNested*(tp: GADType): bool {.inline.} =
  tp.id.isNested

proc isPrimitive*(tp: GADType): bool {.inline.} =
  tp.id.isPrimitive

proc nimTypeName*(tp: GADType): string =
  ## Returns the Nim type name corresponding to an Arrow data type
  case tp.id
  of GARROW_TYPE_BOOLEAN:
    "bool"
  of GARROW_TYPE_INT8:
    "int8"
  of GARROW_TYPE_UINT8:
    "uint8"
  of GARROW_TYPE_INT16:
    "int16"
  of GARROW_TYPE_UINT16:
    "uint16"
  of GARROW_TYPE_INT32:
    "int32"
  of GARROW_TYPE_UINT32:
    "uint32"
  of GARROW_TYPE_INT64:
    "int64"
  of GARROW_TYPE_UINT64:
    "uint64"
  of GARROW_TYPE_FLOAT, GARROW_TYPE_HALF_FLOAT:
    "float32"
  of GARROW_TYPE_DOUBLE:
    "float64"
  of GARROW_TYPE_STRING, GARROW_TYPE_LARGE_STRING, GARROW_TYPE_STRING_VIEW:
    "string"
  of GARROW_TYPE_BINARY, GARROW_TYPE_LARGE_BINARY, GARROW_TYPE_FIXED_SIZE_BINARY,
      GARROW_TYPE_BINARY_VIEW:
    "seq[byte]"
  of GARROW_TYPE_DATE32:
    "Date32Array"
  of GARROW_TYPE_DATE64:
    "Date64Array"
  of GARROW_TYPE_TIMESTAMP:
    "TimestampArray"
  of GARROW_TYPE_TIME32:
    "Time32Array"
  of GARROW_TYPE_TIME64:
    "Time64Array"
  of GARROW_TYPE_DURATION:
    "DurationArray"
  of GARROW_TYPE_LIST, GARROW_TYPE_LARGE_LIST:
    "ListArray"
  of GARROW_TYPE_FIXED_SIZE_LIST:
    "FixedSizeListArray"
  of GARROW_TYPE_STRUCT:
    "StructArray"
  of GARROW_TYPE_MAP:
    "MapArray"
  of GARROW_TYPE_SPARSE_UNION, GARROW_TYPE_DENSE_UNION:
    "UnionArray"
  of GARROW_TYPE_DECIMAL32, GARROW_TYPE_DECIMAL64, GARROW_TYPE_DECIMAL128,
      GARROW_TYPE_DECIMAL256:
    "DecimalArray"
  of GARROW_TYPE_MONTH_INTERVAL, GARROW_TYPE_DAY_TIME_INTERVAL,
      GARROW_TYPE_MONTH_DAY_NANO_INTERVAL:
    "IntervalArray"
  of GARROW_TYPE_NA:
    "null"
  else:
    "unsupported"

proc isCompatible*(tp: GADType, T: typedesc): bool =
  ## Check if a GADType is compatible with the given Nim type
  let arrowId = tp.id
  when T is bool:
    arrowId == GARROW_TYPE_BOOLEAN
  elif T is int8:
    arrowId == GARROW_TYPE_INT8
  elif T is uint8:
    arrowId == GARROW_TYPE_UINT8
  elif T is int16:
    arrowId == GARROW_TYPE_INT16
  elif T is uint16:
    arrowId == GARROW_TYPE_UINT16
  elif T is int32:
    arrowId in {
      GARROW_TYPE_INT32, GARROW_TYPE_DATE32, GARROW_TYPE_TIME32,
      GARROW_TYPE_MONTH_INTERVAL, GARROW_TYPE_DECIMAL32,
    }
  elif T is uint32:
    arrowId == GARROW_TYPE_UINT32
  elif T is int64 or T is int:
    arrowId in {
      GARROW_TYPE_INT64, GARROW_TYPE_DATE64, GARROW_TYPE_TIMESTAMP, GARROW_TYPE_TIME64,
      GARROW_TYPE_DURATION, GARROW_TYPE_DAY_TIME_INTERVAL, GARROW_TYPE_DECIMAL64,
    }
  elif T is uint64:
    arrowId == GARROW_TYPE_UINT64
  elif T is float32:
    arrowId in {GARROW_TYPE_FLOAT, GARROW_TYPE_HALF_FLOAT}
  elif T is float64:
    arrowId == GARROW_TYPE_DOUBLE
  elif T is string:
    arrowId in {GARROW_TYPE_STRING, GARROW_TYPE_LARGE_STRING, GARROW_TYPE_STRING_VIEW}
  elif T is seq[byte]:
    arrowId in {
      GARROW_TYPE_BINARY, GARROW_TYPE_LARGE_BINARY, GARROW_TYPE_FIXED_SIZE_BINARY,
      GARROW_TYPE_BINARY_VIEW,
    }
  else:
    false

proc checkType*(tp: GADType, T: typedesc) =
  ## Raises an error if the type is not compatible
  if not tp.isCompatible(T):
    raise newException(
      TypeError,
      "Type mismatch: Arrow type '" & $tp & "' (" & tp.nimTypeName &
        ") is not compatible with Nim type '" & $T & "'",
    )

proc newGType*(T: typedesc[ArrowPrimitive]): GADType =
  when T is bool:
    result.handle = cast[ptr GArrowDataType](garrow_boolean_data_type_new())
  elif T is int8:
    result.handle = cast[ptr GArrowDataType](garrow_int8_data_type_new())
  elif T is uint8:
    result.handle = cast[ptr GArrowDataType](garrow_uint8_data_type_new())
  elif T is int16:
    result.handle = cast[ptr GArrowDataType](garrow_int16_data_type_new())
  elif T is uint16:
    result.handle = cast[ptr GArrowDataType](garrow_uint16_data_type_new())
  elif T is int32:
    result.handle = cast[ptr GArrowDataType](garrow_int32_data_type_new())
  elif T is uint32:
    result.handle = cast[ptr GArrowDataType](garrow_uint32_data_type_new())
  elif T is int64 or T is int:
    result.handle = cast[ptr GArrowDataType](garrow_int64_data_type_new())
  elif T is uint64:
    result.handle = cast[ptr GArrowDataType](garrow_uint64_data_type_new())
  elif T is float32:
    result.handle = cast[ptr GArrowDataType](garrow_float_data_type_new())
  elif T is float64:
    result.handle = cast[ptr GArrowDataType](garrow_double_data_type_new())
  elif T is string:
    result.handle = cast[ptr GArrowDataType](garrow_string_data_type_new())
  elif T is seq[byte]:
    result.handle = cast[ptr GArrowDataType](garrow_binary_data_type_new())
  elif T is cstring:
    result.handle = cast[ptr GArrowDataType](garrow_large_string_data_type_new())
  else:
    static:
      doAssert false,
        "newGType: unsupported type for automatic Arrow GType construction."

proc newGType*(pt: ptr GArrowDataType): GADType =
  if pt.isNil:
    raise newException(OperationError, "Failed to create GADType, got nil")
  let handle = cast[ptr GArrowDataType](g_object_ref(pt))
  result = GADType(handle: handle)
