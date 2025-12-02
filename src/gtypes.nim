# import std/[strutils]
# import posix

import ./[ffi]

type
  GADType* = object
    handle*: ptr GArrowDataType

  GString* = object
    handle*: cstring

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

proc newGType*(T: typedesc): GADType =
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
