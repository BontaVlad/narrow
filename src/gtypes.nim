import std/[macros]
import ./[ffi]

type
  GADType* = object
    handle*: ptr GArrowDataType

  GString* = object
    handle*: cstring

  ArrowPrimitive* = bool | int8 | uint8 | int16 | uint16 | 
                    int32 | uint32 | int64 | uint64 | 
                    float32 | float64 | string | seq[byte] | cstring

  TypeError* = object of CatchableError


proc toPtr*(g: GADType): ptr GArrowDataType {.inline.} =
  g.handle

proc `=destroy`*(tp: GADType) =
  if not isNil(tp.toPtr):
    g_object_unref(tp.toPtr)

proc `=destroy`*(s: GString) =
  if not isNil(s.handle):
    gFree(s.handle)

proc `=sink`*(dest: var GADType, src: GADType) =
  # Clean up destination if different
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
  result = garrow_data_type_get_id(tp.toPtr)

proc nimTypeName*(tp: GADType): string =
  ## Returns the Nim type name corresponding to an Arrow data type
  case tp.id
  of GArrowType.GARROW_TYPE_BOOLEAN:
    "bool"
  of GArrowType.GARROW_TYPE_INT8:
    "int8"
  of GArrowType.GARROW_TYPE_UINT8:
    "uint8"
  of GArrowType.GARROW_TYPE_INT16:
    "int16"
  of GArrowType.GARROW_TYPE_UINT16:
    "uint16"
  of GArrowType.GARROW_TYPE_INT32:
    "int32"
  of GArrowType.GARROW_TYPE_UINT32:
    "uint32"
  of GArrowType.GARROW_TYPE_INT64:
    "int64"
  of GArrowType.GARROW_TYPE_UINT64:
    "uint64"
  of GArrowType.GARROW_TYPE_FLOAT, GArrowType.GARROW_TYPE_HALF_FLOAT:
    "float32"
  of GArrowType.GARROW_TYPE_DOUBLE:
    "float64"
  of GArrowType.GARROW_TYPE_STRING:
    "string"
  of GArrowType.GARROW_TYPE_LARGE_STRING:
    "string"
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
    arrowId in {GARROW_TYPE_INT32, GARROW_TYPE_DATE32, 
                GARROW_TYPE_TIME32, GARROW_TYPE_MONTH_INTERVAL,
                GARROW_TYPE_DECIMAL32}
  elif T is uint32:
    arrowId == GARROW_TYPE_UINT32
  elif T is int64 or T is int:
    arrowId in {GARROW_TYPE_INT64, GARROW_TYPE_DATE64, 
                GARROW_TYPE_TIMESTAMP, GARROW_TYPE_TIME64, 
                GARROW_TYPE_DURATION, GARROW_TYPE_DAY_TIME_INTERVAL,
                GARROW_TYPE_DECIMAL64}
  elif T is uint64:
    arrowId == GARROW_TYPE_UINT64
  elif T is float32:
    arrowId in {GARROW_TYPE_FLOAT, GARROW_TYPE_HALF_FLOAT}
  elif T is float64:
    arrowId == GARROW_TYPE_DOUBLE
  elif T is string:
    arrowId in {GARROW_TYPE_STRING, GARROW_TYPE_LARGE_STRING, 
                GARROW_TYPE_STRING_VIEW}
  elif T is seq[byte]:
    arrowId in {GARROW_TYPE_BINARY, GARROW_TYPE_LARGE_BINARY, 
                GARROW_TYPE_FIXED_SIZE_BINARY, GARROW_TYPE_BINARY_VIEW}
  else:
    false

proc checkType*(tp: GADType, T: typedesc) =
  ## Raises an error if the type is not compatible
  if not tp.isCompatible(T):
    raise newException(TypeError, 
      "Type mismatch: Arrow type '" & $tp & "' (" & tp.nimTypeName & 
      ") is not compatible with Nim type '" & $T & "'")
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
  result = GADType(handle: pt)
