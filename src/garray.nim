import std/[options, strformat]
import ./[ffi, gtypes, error]

type
  ArrayBuilder*[T] = object
    handle: ptr GArrowArrayBuilder

  Array*[T] = object
    handle: ptr GArrowArray

proc toPtr*[T](b: ArrayBuilder[T]): ptr GArrowArrayBuilder {.inline.} =
  b.handle

proc toPtr*[T](a: Array[T]): ptr GArrowArray {.inline.} =
  a.handle

proc `=destroy`*[T](builder: ArrayBuilder[T]) =
  if not isNil(builder.handle):
    g_object_unref(builder.handle)

proc `=sink`*[T](dest: var ArrayBuilder[T], src: ArrayBuilder[T]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[T](dest: var ArrayBuilder[T], src: ArrayBuilder[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc `=destroy`*[T](ar: Array[T]) =
  if not isNil(ar.handle):
    g_object_unref(ar.handle)

proc `=sink`*[T](dest: var Array[T], src: Array[T]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[T](dest: var Array[T], src: Array[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newArrayBuilder*[T](builderPtr: ptr GArrowArrayBuilder): ArrayBuilder[T] =
  let handle = cast[ptr GArrowArrayBuilder](g_object_ref(builderPtr))
  result = ArrayBuilder[T](handle: handle)

proc newArrayBuilder*[T](): ArrayBuilder[T] =
  var handle: gpointer

  when T is bool:
    handle = garrow_boolean_array_builder_new()
  elif T is int8:
    handle = garrow_int8_array_builder_new()
  elif T is uint8:
    handle = garrow_uint8_array_builder_new()
  elif T is int16:
    handle = garrow_int16_array_builder_new()
  elif T is uint16:
    handle = garrow_uint16_array_builder_new()
  elif T is int32:
    handle = garrow_int32_array_builder_new()
  elif T is uint32:
    handle = garrow_uint32_array_builder_new()
  elif T is int64 or T is int:
    handle = garrow_int64_array_builder_new()
  elif T is uint64 or T is uint:
    handle = garrow_uint64_array_builder_new()
  elif T is float32:
    handle = garrow_float_array_builder_new()
  elif T is float64:
    handle = garrow_double_array_builder_new()
  elif T is string:
    handle = garrow_string_array_builder_new()
  else:
    static:
      doAssert false, "Unsupported type for ArrayBuilder"

  if isNil(handle):
    raise newException(OperationError, "Error creating the builder")

  # Sink floating reference if present
  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result = ArrayBuilder[T](handle: cast[ptr GArrowArrayBuilder](handle))

proc append*[T](builder: ArrayBuilder[T], val: sink T) =
  when T is bool:
    check(
      garrow_boolean_array_builder_append_value(
        cast[ptr GArrowBooleanArrayBuilder](builder.handle),
        if val: 1.gboolean else: 0.gboolean,
      )
    )
  elif T is int8:
    check(
      garrow_int8_array_builder_append_value(
        cast[ptr GArrowInt8ArrayBuilder](builder.handle), val
      )
    )
  elif T is uint8:
    check(
      garrow_uint8_array_builder_append_value(
        cast[ptr GArrowUInt8ArrayBuilder](builder.handle), val
      )
    )
  elif T is int16:
    check(
      garrow_int16_array_builder_append_value(
        cast[ptr GArrowInt16ArrayBuilder](builder.handle), val
      )
    )
  elif T is uint16:
    check(
      garrow_uint16_array_builder_append_value(
        cast[ptr GArrowUInt16ArrayBuilder](builder.handle), val
      )
    )
  elif T is int32:
    check(
      garrow_int32_array_builder_append_value(
        cast[ptr GArrowInt32ArrayBuilder](builder.handle), val
      )
    )
  elif T is uint32:
    check(
      garrow_uint32_array_builder_append_value(
        cast[ptr GArrowUInt32ArrayBuilder](builder.handle), val
      )
    )
  elif T is int64:
    check(
      garrow_int64_array_builder_append_value(
        cast[ptr GArrowInt64ArrayBuilder](builder.handle), val
      )
    )
  elif T is uint64:
    check(
      garrow_uint64_array_builder_append_value(
        cast[ptr GArrowUInt64ArrayBuilder](builder.handle), val
      )
    )
  elif T is float32:
    check(
      garrow_float_array_builder_append_value(
        cast[ptr GArrowFloatArrayBuilder](builder.handle), val
      )
    )
  elif T is float64:
    check garrow_double_array_builder_append_value(
      cast[ptr GArrowDoubleArrayBuilder](builder.handle), val
    )
  elif T is string:
    check garrow_string_array_builder_append_value(
      cast[ptr GArrowStringArrayBuilder](builder.handle), val.cstring
    )

proc appendNull*[T](builder: ArrayBuilder[T]) =
  when T is bool:
    check garrow_boolean_array_builder_append_null(
      cast[ptr GArrowBooleanArrayBuilder](builder.handle)
    )
  elif T is int8:
    check garrow_int8_array_builder_append_null(
      cast[ptr GArrowInt8ArrayBuilder](builder.handle)
    )
  elif T is uint8:
    check garrow_uint8_array_builder_append_null(
      cast[ptr GArrowUInt8ArrayBuilder](builder.handle)
    )
  elif T is int16:
    check garrow_int16_array_builder_append_null(
      cast[ptr GArrowInt16ArrayBuilder](builder.handle)
    )
  elif T is uint16:
    check garrow_uint16_array_builder_append_null(
      cast[ptr GArrowUInt16ArrayBuilder](builder.handle)
    )
  elif T is int32:
    check garrow_int32_array_builder_append_null(
      cast[ptr GArrowInt32ArrayBuilder](builder.handle)
    )
  elif T is uint32:
    check garrow_uint32_array_builder_append_null(
      cast[ptr GArrowUInt32ArrayBuilder](builder.handle)
    )
  elif T is int64:
    check garrow_int64_array_builder_append_null(
      cast[ptr GArrowInt64ArrayBuilder](builder.handle)
    )
  elif T is uint64:
    check garrow_uint64_array_builder_append_null(
      cast[ptr GArrowUInt64ArrayBuilder](builder.handle)
    )
  elif T is float32:
    check garrow_float_array_builder_append_null(
      cast[ptr GArrowFloatArrayBuilder](builder.handle)
    )
  elif T is float64:
    check garrow_double_array_builder_append_null(
      cast[ptr GArrowDoubleArrayBuilder](builder.handle)
    )
  else:
    check garrow_array_builder_append_null(builder.handle)

proc append*[T](builder: ArrayBuilder[T], val: sink Option[T]) =
  if val.isSome():
    builder.append(val.get())
  else:
    builder.appendNull()

proc appendValues*[T](builder: ArrayBuilder[T], values: openArray[T]) =
  if len(values) == 0:
    return

  when T is int32:
    check garrow_int32_array_builder_append_values(
      cast[ptr GArrowInt32ArrayBuilder](builder.handle),
      cast[ptr gint32](values[0].addr),
      values.len.gint64,
      nil,
      0,
    )
  elif T is int64:
    check garrow_int64_array_builder_append_values(
      cast[ptr GArrowInt64ArrayBuilder](builder.handle),
      cast[ptr gint64](values[0].addr),
      values.len.gint64,
      nil,
      0,
    )
  elif T is float32:
    check garrow_float_array_builder_append_values(
      cast[ptr GArrowFloatArrayBuilder](builder.handle),
      cast[ptr gfloat](values[0].addr),
      values.len.gint64,
      nil,
      0,
    )
  elif T is float64:
    check garrow_double_array_builder_append_values(
      cast[ptr GArrowDoubleArrayBuilder](builder.handle),
      cast[ptr gdouble](values[0].addr),
      values.len.gint64,
      nil,
      0,
    )
  else:
    # Fallback to individual appends for unsupported batch operations
    for val in values:
      builder.append(val)

proc appendValues*[T](builder: ArrayBuilder[T], values: Array[T]) =
  if len(values) == 0:
    return

  when T is int32:
    var length: gint64
    let data = garrow_int32_array_get_values(
      cast[ptr GArrowInt32Array](values.handle), addr length
    )
    check garrow_int32_array_builder_append_values(
      cast[ptr GArrowInt32ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is int64:
    var length: gint64
    let data = garrow_int64_array_get_values(
      cast[ptr GArrowInt64Array](values.handle), addr length
    )
    check garrow_int64_array_builder_append_values(
      cast[ptr GArrowInt64ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is uint32:
    var length: gint64
    let data = garrow_uint32_array_get_values(
      cast[ptr GArrowUInt32Array](values.handle), addr length
    )
    check garrow_uint32_array_builder_append_values(
      cast[ptr GArrowUInt32ArrayBuilder](builder.handle),
      data,
      values.len.gint64,
      nil,
      0,
    )
  elif T is uint64:
    var length: gint64
    let data = garrow_uint64_array_get_values(
      cast[ptr GArrowUInt64Array](values.handle), addr length
    )
    check garrow_uint64_array_builder_append_values(
      cast[ptr GArrowUInt64ArrayBuilder](builder.handle),
      data,
      values.len.gint64,
      nil,
      0,
    )
  elif T is int16:
    var length: gint64
    let data = garrow_int16_array_get_values(
      cast[ptr GArrowInt16Array](values.handle), addr length
    )
    check garrow_int16_array_builder_append_values(
      cast[ptr GArrowInt16ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is uint16:
    var length: gint64
    let data = garrow_uint16_array_get_values(
      cast[ptr GArrowUInt16Array](values.handle), addr length
    )
    check garrow_uint16_array_builder_append_values(
      cast[ptr GArrowUInt16ArrayBuilder](builder.handle),
      data,
      values.len.gint64,
      nil,
      0,
    )
  elif T is int8:
    var length: gint64
    let data = garrow_int8_array_get_values(
      cast[ptr GArrowInt8Array](values.handle), addr length
    )
    check garrow_int8_array_builder_append_values(
      cast[ptr GArrowInt8ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is uint8:
    var length: gint64
    let data = garrow_uint8_array_get_values(
      cast[ptr GArrowUInt8Array](values.handle), addr length
    )
    check garrow_uint8_array_builder_append_values(
      cast[ptr GArrowUInt8ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is float32:
    var length: gint64
    let data = garrow_float_array_get_values(
      cast[ptr GArrowFloatArray](values.handle), addr length
    )
    check garrow_float_array_builder_append_values(
      cast[ptr GArrowFloatArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is float64:
    var length: gint64
    let data = garrow_double_array_get_values(
      cast[ptr GArrowDoubleArray](values.handle), addr length
    )
    check garrow_double_array_builder_append_values(
      cast[ptr GArrowDoubleArrayBuilder](builder.handle),
      data,
      values.len.gint64,
      nil,
      0,
    )
  else:
    # Fallback to individual appends for unsupported batch operations
    for val in values:
      builder.append(val)

proc finish*[T](builder: ArrayBuilder[T]): Array[T] =
  let handle = check garrow_array_builder_finish(builder.handle)
  result = Array[T](handle: handle)

# ARRAY
proc newArray*[T](values: sink seq[T]): Array[T] =
  let builder = newArrayBuilder[T]()
  if len(values) != 0:
    builder.appendValues(values)
  result = builder.finish()

proc newArray*[T](gptr: pointer): Array[T] =
  let gTp = newGType(T)
  let rawPtr = check garrow_array_import(gptr, gTp.toPtr)
  result = cast[Array[T]](rawPtr)

proc newArray*[T](handle: ptr GArrowArray): Array[T] =
  # Increment reference count since we're taking ownership
  result = Array[T](handle: handle)

proc `==`*(a, b: Array): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_array_equal(a.handle, b.handle).bool

proc len*(ar: Array): int =
  return garrow_array_get_length(ar.handle)

proc `[]`*[T](arr: Array[T], i: int): T =
  if len(arr) == 0:
    raise newException(IndexDefect, "Empty array")
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i > len(arr):
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {len(arr)}")

  when T is bool:
    let val =
      garrow_boolean_array_get_value(cast[ptr GArrowBooleanArray](arr.handle), i)
    return val != 0
  elif T is int8:
    return garrow_int8_array_get_value(cast[ptr GArrowInt8Array](arr.handle), i)
  elif T is uint8:
    return garrow_uint8_array_get_value(cast[ptr GArrowUInt8Array](arr.handle), i)
  elif T is int16:
    return garrow_int16_array_get_value(cast[ptr GArrowInt16Array](arr.handle), i)
  elif T is uint16:
    return garrow_uint16_array_get_value(cast[ptr GArrowUInt16Array](arr.handle), i)
  elif T is int32:
    return garrow_int32_array_get_value(cast[ptr GArrowInt32Array](arr.handle), i)
  elif T is uint32:
    return garrow_uint32_array_get_value(cast[ptr GArrowUInt32Array](arr.handle), i)
  elif T is int64:
    return garrow_int64_array_get_value(cast[ptr GArrowInt64Array](arr.handle), i)
  elif T is uint64:
    return garrow_uint64_array_get_value(cast[ptr GArrowUInt64Array](arr.handle), i)
  elif T is float32:
    return garrow_float_array_get_value(cast[ptr GArrowFloatArray](arr.handle), i)
  elif T is float64:
    return garrow_double_array_get_value(cast[ptr GArrowDoubleArray](arr.handle), i)
  elif T is string:
    let cstr =
      garrow_string_array_get_string(cast[ptr GArrowStringArray](arr.handle), i)
    return $newGstring(cstr)
  else:
    {.error: "Unsupported array type for indexing".}

proc `[]`*[T](arr: Array[T], slice: HSlice[int, int]): Array[T] =
  if slice.a < 0 or slice.b < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if slice.a > slice.b:
    raise newException(IndexDefect, fmt"Start: {slice.a} is greather than {slice.b}")
  if slice.b > len(arr):
    raise newException(IndexDefect, fmt"index {slice.b} not in 0 .. {len(arr)}")

  let offset = slice.a
  let length = slice.b - slice.a + 1
  result.handle = garrow_array_slice(arr.handle, offset.gint64, length.gint64)

iterator items*[T](arr: Array[T]): T =
  for i in 0 ..< arr.len:
    yield arr[i]

proc isNull*(arr: Array, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i > len(arr):
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {len(arr)}")
  return garrow_array_is_null(arr.handle, i) != 0

proc isValid*(arr: Array, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i > len(arr):
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {len(arr)}")
  return garrow_array_is_valid(arr.handle, i) != 0

proc tryGet*[T](arr: Array[T], i: int): Option[T] =
  if i < 0 or i >= arr.len:
    return none(T)
  if arr.isNull(i):
    return none(T)
  return some(arr[i])

proc toSeq*[T](arr: Array[T]): seq[T] =
  result = newSeq[T](arr.len)
  for i in 0 ..< arr.len:
    result[i] = arr[i]

proc `@`*[T](arr: Array[T]): seq[T] =
  arr.toSeq

proc `$`*(arr: Array): string =
  let cStr = check garrow_array_to_string(arr.handle)
  result = $newGString(cStr)
