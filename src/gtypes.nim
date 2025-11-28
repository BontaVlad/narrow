# import std/[strutils]
# import posix

import ./[ffi]

type
  GADType*[T] = object
    handle*: ptr GArrowDataType

  GString* = object
    handle*: cstring

converter toArrowType*(g: GADType): ptr GArrowDataType =
  g.handle

proc `=destroy`*[T](tp: GADType[T]) =
  if not isNil(tp.handle):
    g_object_unref(tp.handle)

proc `=destroy`*(str: GString) =
  if not isNil(str.handle):
    gFree(str.handle)

proc `=sink`*[T](dest: var GADType[T], src: GADType[T]) =
  # Clean up destination if different
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  # Transfer ownership (move semantics)
  dest.handle = src.handle

proc `=copy`*[T](dest: var GADType[T], src: GADType[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle) # bump ref count

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

proc `$`*[T](tp: GADType[T]): string =
  let namePtr = garrow_data_type_get_name(tp.handle)
  if isNil(namePtr):
    return "unknown"
  result = $newGString(namePtr)

proc newGType*(T: typedesc): GADType[T] =
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
