## Typed Arrow arrays, array builders, and chunked arrays.
##
## This module provides `Array[T]`, `ArrayBuilder[T]`, and `ChunkedArray[T]` —
## the core columnar data structures used throughout narrow. All arrays are
## immutable; use `ArrayBuilder` to construct them.
import std/[options, strformat]
import ../core/[ffi, error, utils]
import ../types/[gtypes, glist]
import ./buffer

# ============================================================================
# Array Types
# ============================================================================

type
  ArrayBuilder*[T: ArrowValue] = object
    ## Builder for constructing typed Arrow arrays. Append values one at a time or in bulk, then call `finish()` to produce an immutable `Array`.
    handle: ptr GArrowArrayBuilder

  Array*[T: ArrowValue = Untyped] = object
    ## An immutable, typed Arrow array. Element type `T` is inferred at construction. Use `[]` for element access, `len` for length.
    handle: ptr GArrowArray

func toPtr*[T](b: ArrayBuilder[T]): ptr GArrowArrayBuilder {.inline.} =
  b.handle

func toPtr*[T](a: Array[T]): ptr GArrowArray {.inline.} =
  a.handle

proc `=destroy`*[T](builder: ArrayBuilder[T]) =
  if not isNil(builder.handle):
    g_object_unref(builder.handle)

proc `=wasMoved`*[T](builder: var ArrayBuilder[T]) =
  builder.handle = nil

proc `=dup`*[T](builder: ArrayBuilder[T]): ArrayBuilder[T] =
  result.handle = builder.handle
  if not isNil(builder.handle):
    discard g_object_ref(builder.handle)

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

proc `=wasMoved`*[T](ar: var Array[T]) =
  ar.handle = nil

proc `=dup`*[T](ar: Array[T]): Array[T] =
  result.handle = ar.handle
  if not isNil(ar.handle):
    discard g_object_ref(ar.handle)

proc `=copy`*[T](dest: var Array[T], src: Array[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc toUntyped*[T: ArrowValue](arr: Array[T]): Array =
  ## Convert a typed Array to untyped.
  result.handle = arr.handle

proc dataType*(arr: Array): GADType =
  ## Returns the logical data type of the array.
  result = newGType(garrow_array_get_value_data_type(arr.handle))

# ============================================================================
# Array Builders
# ============================================================================

proc newArrayBuilder*[T](builderPtr: ptr GArrowArrayBuilder): ArrayBuilder[T] =
  ## Wrap an existing C builder handle.
  result.handle = cast[ptr GArrowArrayBuilder](builderPtr)

proc newArrayBuilder*[T](): ArrayBuilder[T] =
  ## Create a new empty array builder for type `T`.
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
  elif T is seq[byte]:
    handle = garrow_binary_array_builder_new()
  else:
    static:
      doAssert false, "Unsupported type for ArrayBuilder"

  if isNil(handle):
    raise newException(OperationError, "Error creating the builder")

  result.handle = cast[ptr GArrowArrayBuilder](handle)

proc append*[T](builder: ArrayBuilder[T], val: sink T) =
  when T is bool:
    verify(
      garrow_boolean_array_builder_append_value(
        cast[ptr GArrowBooleanArrayBuilder](builder.handle),
        if val: 1.gboolean else: 0.gboolean,
      )
    )
  elif T is int8:
    verify(
      garrow_int8_array_builder_append_value(
        cast[ptr GArrowInt8ArrayBuilder](builder.handle), val
      )
    )
  elif T is uint8:
    verify(
      garrow_uint8_array_builder_append_value(
        cast[ptr GArrowUInt8ArrayBuilder](builder.handle), val
      )
    )
  elif T is int16:
    verify(
      garrow_int16_array_builder_append_value(
        cast[ptr GArrowInt16ArrayBuilder](builder.handle), val
      )
    )
  elif T is uint16:
    verify(
      garrow_uint16_array_builder_append_value(
        cast[ptr GArrowUInt16ArrayBuilder](builder.handle), val
      )
    )
  elif T is int32:
    verify(
      garrow_int32_array_builder_append_value(
        cast[ptr GArrowInt32ArrayBuilder](builder.handle), val
      )
    )
  elif T is uint32:
    verify(
      garrow_uint32_array_builder_append_value(
        cast[ptr GArrowUInt32ArrayBuilder](builder.handle), val
      )
    )
  elif T is int64 or T is int:
    verify(
      garrow_int64_array_builder_append_value(
        cast[ptr GArrowInt64ArrayBuilder](builder.handle), val
      )
    )
  elif T is uint64:
    verify(
      garrow_uint64_array_builder_append_value(
        cast[ptr GArrowUInt64ArrayBuilder](builder.handle), val
      )
    )
  elif T is float32:
    verify(
      garrow_float_array_builder_append_value(
        cast[ptr GArrowFloatArrayBuilder](builder.handle), val
      )
    )
  elif T is float64:
    verify garrow_double_array_builder_append_value(
      cast[ptr GArrowDoubleArrayBuilder](builder.handle), val
    )
  elif T is string:
    verify garrow_string_array_builder_append_value(
      cast[ptr GArrowStringArrayBuilder](builder.handle), val.cstring
    )
  elif T is seq[byte]:
    let gb = g_bytes_new(
      if val.len > 0:
        cast[pointer](val[0].unsafeAddr)
      else:
        nil,
      val.len.csize_t,
    )
    verify garrow_binary_array_builder_append_value_bytes(
      cast[ptr GArrowBinaryArrayBuilder](builder.handle), gb
    )
    g_bytes_unref(gb)

proc appendNull*[T](builder: ArrayBuilder[T]) =
  ## Append a null value to the builder.
  when T is bool:
    verify garrow_boolean_array_builder_append_null(
      cast[ptr GArrowBooleanArrayBuilder](builder.handle)
    )
  elif T is int8:
    verify garrow_int8_array_builder_append_null(
      cast[ptr GArrowInt8ArrayBuilder](builder.handle)
    )
  elif T is uint8:
    verify garrow_uint8_array_builder_append_null(
      cast[ptr GArrowUInt8ArrayBuilder](builder.handle)
    )
  elif T is int16:
    verify garrow_int16_array_builder_append_null(
      cast[ptr GArrowInt16ArrayBuilder](builder.handle)
    )
  elif T is uint16:
    verify garrow_uint16_array_builder_append_null(
      cast[ptr GArrowUInt16ArrayBuilder](builder.handle)
    )
  elif T is int32:
    verify garrow_int32_array_builder_append_null(
      cast[ptr GArrowInt32ArrayBuilder](builder.handle)
    )
  elif T is uint32:
    verify garrow_uint32_array_builder_append_null(
      cast[ptr GArrowUInt32ArrayBuilder](builder.handle)
    )
  elif T is int64:
    verify garrow_int64_array_builder_append_null(
      cast[ptr GArrowInt64ArrayBuilder](builder.handle)
    )
  elif T is uint64:
    verify garrow_uint64_array_builder_append_null(
      cast[ptr GArrowUInt64ArrayBuilder](builder.handle)
    )
  elif T is float32:
    verify garrow_float_array_builder_append_null(
      cast[ptr GArrowFloatArrayBuilder](builder.handle)
    )
  elif T is float64:
    verify garrow_double_array_builder_append_null(
      cast[ptr GArrowDoubleArrayBuilder](builder.handle)
    )
  elif T is seq[byte]:
    verify garrow_binary_array_builder_append_null(
      cast[ptr GArrowBinaryArrayBuilder](builder.handle)
    )
  else:
    verify garrow_array_builder_append_null(builder.handle)

proc append*[T](builder: ArrayBuilder[T], val: sink Option[T]) =
  if val.isSome():
    builder.append(val.get())
  else:
    builder.appendNull()

proc appendValues*[T](builder: ArrayBuilder[T], values: openArray[T]) =
  ## Append multiple values at once. More efficient than repeated `append` calls.
  if len(values) == 0:
    return

  when T is int32:
    verify garrow_int32_array_builder_append_values(
      cast[ptr GArrowInt32ArrayBuilder](builder.handle),
      cast[ptr gint32](values[0].addr),
      values.len.gint64,
      nil,
      0,
    )
  elif T is int64 or T is int:
    verify garrow_int64_array_builder_append_values(
      cast[ptr GArrowInt64ArrayBuilder](builder.handle),
      cast[ptr gint64](values[0].addr),
      values.len.gint64,
      nil,
      0,
    )
  elif T is float32:
    verify garrow_float_array_builder_append_values(
      cast[ptr GArrowFloatArrayBuilder](builder.handle),
      cast[ptr gfloat](values[0].addr),
      values.len.gint64,
      nil,
      0,
    )
  elif T is float64:
    verify garrow_double_array_builder_append_values(
      cast[ptr GArrowDoubleArrayBuilder](builder.handle),
      cast[ptr gdouble](values[0].addr),
      values.len.gint64,
      nil,
      0,
    )
  elif T is bool:
    var bools = newSeq[gboolean](values.len)
    for i, v in values:
      bools[i] = if v: 1.gboolean else: 0.gboolean
    verify garrow_boolean_array_builder_append_values(
      cast[ptr GArrowBooleanArrayBuilder](builder.handle),
      bools[0].addr,
      values.len.gint64,
      nil,
      0,
    )
  elif T is string:
    var cstrs = newSeq[cstring](values.len)
    for i, v in values:
      cstrs[i] = v.cstring
    verify garrow_string_array_builder_append_strings(
      cast[ptr GArrowStringArrayBuilder](builder.handle),
      cstrs[0].addr,
      values.len.gint64,
      nil,
      0,
    )
  else:
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
    verify garrow_int32_array_builder_append_values(
      cast[ptr GArrowInt32ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is int64 or T is int:
    var length: gint64
    let data = garrow_int64_array_get_values(
      cast[ptr GArrowInt64Array](values.handle), addr length
    )
    verify garrow_int64_array_builder_append_values(
      cast[ptr GArrowInt64ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is uint32:
    var length: gint64
    let data = garrow_uint32_array_get_values(
      cast[ptr GArrowUInt32Array](values.handle), addr length
    )
    verify garrow_uint32_array_builder_append_values(
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
    verify garrow_uint64_array_builder_append_values(
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
    verify garrow_int16_array_builder_append_values(
      cast[ptr GArrowInt16ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is uint16:
    var length: gint64
    let data = garrow_uint16_array_get_values(
      cast[ptr GArrowUInt16Array](values.handle), addr length
    )
    verify garrow_uint16_array_builder_append_values(
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
    verify garrow_int8_array_builder_append_values(
      cast[ptr GArrowInt8ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is uint8:
    var length: gint64
    let data = garrow_uint8_array_get_values(
      cast[ptr GArrowUInt8Array](values.handle), addr length
    )
    verify garrow_uint8_array_builder_append_values(
      cast[ptr GArrowUInt8ArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is float32:
    var length: gint64
    let data = garrow_float_array_get_values(
      cast[ptr GArrowFloatArray](values.handle), addr length
    )
    verify garrow_float_array_builder_append_values(
      cast[ptr GArrowFloatArrayBuilder](builder.handle), data, values.len.gint64, nil, 0
    )
  elif T is float64:
    var length: gint64
    let data = garrow_double_array_get_values(
      cast[ptr GArrowDoubleArray](values.handle), addr length
    )
    verify garrow_double_array_builder_append_values(
      cast[ptr GArrowDoubleArrayBuilder](builder.handle),
      data,
      values.len.gint64,
      nil,
      0,
    )
  elif T is bool:
    var length: gint64
    let data = garrow_boolean_array_get_values(
      cast[ptr GArrowBooleanArray](values.handle), addr length
    )
    verify garrow_boolean_array_builder_append_values(
      cast[ptr GArrowBooleanArrayBuilder](builder.handle),
      data,
      values.len.gint64,
      nil,
      0,
    )
    g_free(data)
  else:
    for val in values:
      builder.append(val)

proc finish*[T](builder: ArrayBuilder[T]): Array[T] =
  ## Build and return the finished `Array`. The builder is reset.
  let handle = verify garrow_array_builder_finish(builder.handle)
  result.handle = handle

# ============================================================================
# Array Operations
# ============================================================================

proc newArray*[T](values: sink seq[T]): Array[T] =
  ## Create an array from a Nim sequence.
  let builder = newArrayBuilder[T]()
  if len(values) != 0:
    builder.appendValues(values)
  result = builder.finish()

proc newArray*[T](values: sink seq[T], mask: openArray[bool]): Array[T] =
  ## Create an array with a validity mask. `true` in the mask marks a null.
  let builder = newArrayBuilder[T]()
  for i in 0 ..< values.len:
    if mask[i]:
      builder.appendNull()
    else:
      builder.append(values[i])
  result = builder.finish()

proc newArray*[T](gptr: pointer): Array[T] =
  let gTp = newGType(T)
  let rawPtr = verify garrow_array_import(gptr, gTp.toPtr)
  result.handle = rawPtr

proc newArray*[T](handle: ptr GArrowArray): Array[T] =
  ## Wrap an existing C array handle.
  result.handle = handle

proc toTyped*[T: ArrowValue](arr: Array): Array[T] =
  ## Convert an untyped (or differently typed) Array to a typed Array.
  ## This is the key runtime -> compile-time bridge. The runtime GArrowType
  ## tag of the handle is checked against `T` before returning; a mismatch
  ## raises `TypeError`.
  if not isNil(arr.handle):
    arr.dataType.checkType(T)
    result.handle = cast[ptr GArrowArray](g_object_ref(arr.handle))

proc viewAs*[T: ArrowValue](arr: Array): Array[T] =
  ## Returns a zero-copy view of the array reinterpreted as type `T`.
  ## The underlying buffers are shared; only the type descriptor changes.
  let targetType = newGType(T)
  let handle = verify garrow_array_view(arr.handle, targetType.toPtr)
  result = toTyped[T](Array(handle: handle))

proc nullBitmap*(arr: Array): GBuffer =
  ## Returns the validity bitmap buffer of the array.
  let handle = garrow_array_get_null_bitmap(arr.handle)
  result = GBuffer(handle: handle)

proc valuesBuffer*(arr: Array): GBuffer =
  ## Returns the raw values data buffer of the array.
  ## Only valid for primitive arrays.
  let handle =
    garrow_primitive_array_get_data_buffer(cast[ptr GArrowPrimitiveArray](arr.handle))
  result = GBuffer(handle: handle)

proc `==`*[T, U](a: Array[T], b: Array[U]): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_array_equal(a.handle, b.handle).bool

# useful for testing or debugging
proc `==`*[T](a: Array[T], b: openArray[T]): bool =
  if a.len != b.len:
    return false

  for i in a:
    if not i in b:
      return false
  return true

proc len*(ar: Array): int =
  ## Returns the number of rows in the array.
  return garrow_array_get_length(ar.handle)

proc `[]`*[T](arr: Array[T], i: int): T =
  ## Returns the element at index `i`. Raises `IndexDefect` on out-of-bounds.
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
  elif T is int64 or T is int:
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
    return $newGString(cstr, owned = true)
  elif T is seq[byte]:
    let gb =
      garrow_binary_array_get_value(cast[ptr GArrowBinaryArray](arr.handle), i.gint64)
    var size: gsize
    let data = g_bytes_get_data(gb, addr size)
    let sz = int(size)
    result = newSeq[byte](sz)
    if sz > 0:
      copyMem(addr result[0], data, sz)
    g_bytes_unref(gb)
    return result
  else:
    {.error: "Unsupported array type for indexing".}

proc `[]`*[T](arr: Array[T], slice: HSlice[int, int]): Array[T] =
  ## Returns a zero-copy slice covering `slice.a` to `slice.b`. The slice shares data with the base array.
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
  ## Iterates over all elements of the array.
  for i in 0 ..< arr.len:
    yield arr[i]

proc isNull*(arr: Array, i: int): bool =
  ## Returns whether the `i`-th value is null.
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i > len(arr):
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {len(arr)}")
  return garrow_array_is_null(arr.handle, i) != 0

proc isValid*(arr: Array, i: int): bool =
  ## Returns whether the `i`-th value is valid (not null).
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i > len(arr):
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {len(arr)}")
  return garrow_array_is_valid(arr.handle, i) != 0

proc nNulls*(arr: Array): int64 =
  ## Number of null values in the array
  result = garrow_array_get_n_nulls(arr.handle)

proc tryGet*[T](arr: Array[T], i: int): Option[T] =
  ## Returns `some(value)` if the `i`-th element is valid, `none` if null or out of bounds.
  if i < 0 or i >= arr.len:
    return none(T)
  if arr.isNull(i):
    return none(T)
  return some(arr[i])

proc toSeq*[T](arr: Array[T]): seq[T] =
  result = newSeq[T](arr.len)
  if arr.len == 0:
    return

  when T is bool:
    var length: gint64
    let data = garrow_boolean_array_get_values(
      cast[ptr GArrowBooleanArray](arr.handle), addr length
    )
    let bools = cast[ptr UncheckedArray[gboolean]](data)
    for i in 0 ..< arr.len:
      result[i] = bools[i] != 0
    g_free(data)
  elif T is int32:
    var length: gint64
    let data =
      garrow_int32_array_get_values(cast[ptr GArrowInt32Array](arr.handle), addr length)
    copyMem(addr result[0], data, arr.len * sizeof(int32))
  elif T is int64 or T is int:
    var length: gint64
    let data =
      garrow_int64_array_get_values(cast[ptr GArrowInt64Array](arr.handle), addr length)
    copyMem(addr result[0], data, arr.len * sizeof(int64))
  elif T is uint32:
    var length: gint64
    let data = garrow_uint32_array_get_values(
      cast[ptr GArrowUInt32Array](arr.handle), addr length
    )
    copyMem(addr result[0], data, arr.len * sizeof(uint32))
  elif T is uint64:
    var length: gint64
    let data = garrow_uint64_array_get_values(
      cast[ptr GArrowUInt64Array](arr.handle), addr length
    )
    copyMem(addr result[0], data, arr.len * sizeof(uint64))
  elif T is int16:
    var length: gint64
    let data =
      garrow_int16_array_get_values(cast[ptr GArrowInt16Array](arr.handle), addr length)
    copyMem(addr result[0], data, arr.len * sizeof(int16))
  elif T is uint16:
    var length: gint64
    let data = garrow_uint16_array_get_values(
      cast[ptr GArrowUInt16Array](arr.handle), addr length
    )
    copyMem(addr result[0], data, arr.len * sizeof(uint16))
  elif T is int8:
    var length: gint64
    let data =
      garrow_int8_array_get_values(cast[ptr GArrowInt8Array](arr.handle), addr length)
    copyMem(addr result[0], data, arr.len * sizeof(int8))
  elif T is uint8:
    var length: gint64
    let data =
      garrow_uint8_array_get_values(cast[ptr GArrowUInt8Array](arr.handle), addr length)
    copyMem(addr result[0], data, arr.len * sizeof(uint8))
  elif T is float32:
    var length: gint64
    let data =
      garrow_float_array_get_values(cast[ptr GArrowFloatArray](arr.handle), addr length)
    copyMem(addr result[0], data, arr.len * sizeof(float32))
  elif T is float64:
    var length: gint64
    let data = garrow_double_array_get_values(
      cast[ptr GArrowDoubleArray](arr.handle), addr length
    )
    copyMem(addr result[0], data, arr.len * sizeof(float64))
  else:
    for i in 0 ..< arr.len:
      result[i] = arr[i]

proc `@`*[T](arr: Array[T]): seq[T] =
  arr.toSeq

proc `$`*(arr: Array): string =
  let cStr = verify garrow_array_to_string(arr.handle)
  result = $newGString(cStr, owned = true)

# ============================================================================
# ChunkedArray Types
# ============================================================================

type ChunkedArray*[T = Untyped] = object
  ## A columnar data structure composed of one or more `Array` chunks. Useful for data that doesn't fit in a single contiguous buffer.
  handle*: ptr GArrowChunkedArray

proc toPtr*[T](c: ChunkedArray[T] | ChunkedArray): ptr GArrowChunkedArray {.inline.} =
  c.handle

proc `=destroy`*[T](c: ChunkedArray[T]) =
  if not isNil(c.toPtr):
    g_object_unref(cast[gpointer](c.toPtr))

proc `=wasMoved`*[T](c: var ChunkedArray[T]) =
  c.handle = nil

proc `=dup`*[T](c: ChunkedArray[T]): ChunkedArray[T] =
  result.handle = c.handle
  if not isNil(c.handle):
    discard g_object_ref(c.handle)

proc `=copy`*[T](dest: var ChunkedArray[T], src: ChunkedArray[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

proc newChunkedArray*[T](chunks: openArray[Array[T]]): ChunkedArray[T] =
  ## Create a chunked array from one or more typed array chunks.
  let cList = newGList(chunks)
  var handle: ptr GArrowChunkedArray
  if len(cList) == 0:
    handle = verify garrow_chunked_array_new_empty(newGType(T).toPtr)
  else:
    handle = verify garrow_chunked_array_new(cList.list)
  result.handle = handle

proc newChunkedArray*[T](): ChunkedArray[T] {.inline.} =
  let dataType = newGType(T)
  let handle = verify garrow_chunked_array_new_empty(dataType.toPtr)
  result.handle = handle

proc newChunkedArray*[T](cAbiArrayStream: pointer): ChunkedArray[T] =
  let handle = verify garrow_chunked_array_import(cAbiArrayStream)
  result.handle = handle

proc newChunkedArray*[T](rawPtr: ptr GArrowChunkedArray): ChunkedArray[T] =
  result.handle = rawPtr

proc toTyped*[T: ArrowValue](ca: ChunkedArray): ChunkedArray[T] =
  ## Convert an untyped (or differently typed) ChunkedArray to a typed one.
  ## The runtime GArrowType tag is checked against `T` before returning; a
  ## mismatch raises `TypeError`.
  if not isNil(ca.handle):
    ca.getValueDataType.checkType(T)
    result.handle = cast[ptr GArrowChunkedArray](g_object_ref(ca.handle))

proc `==`*[T, U](a: ChunkedArray[T], b: ChunkedArray[U]): bool {.inline.} =
  ## Returns whether two chunked arrays are equal.
  result = garrow_chunked_array_equal(a.toPtr, b.toPtr) != 0

proc getValueDataType*(chunkedArray: ChunkedArray): GADType =
  result = newGType(garrow_chunked_array_get_value_data_type(chunkedArray.toPtr))

proc getValueType*(chunkedArray: ChunkedArray): GArrowType =
  result = garrow_chunked_array_get_value_type(chunkedArray.toPtr)

proc len*(chunkedArray: ChunkedArray): int =
  ## Returns the number of rows in the chunked array.
  result = int(garrow_chunked_array_get_length(chunkedArray.toPtr))

proc nRows*(chunkedArray: ChunkedArray): int64 {.inline.} =
  ## Number of rows in the chunked array
  result = garrow_chunked_array_get_n_rows(chunkedArray.toPtr).int64

proc nNulls*(chunkedArray: ChunkedArray): int64 {.inline.} =
  ## Number of null values in the chunked array
  result = garrow_chunked_array_get_n_nulls(chunkedArray.toPtr).int64

proc nChunks*(chunkedArray: ChunkedArray): uint =
  ## Returns the number of chunks in the chunked array.
  result = garrow_chunked_array_get_n_chunks(chunkedArray.toPtr)

proc getChunk*[T](chunkedArray: ChunkedArray[T], i: uint): Array[T] =
  ## Returns the chunk at index `i`. Raises `IndexDefect` on out-of-bounds.
  if i.int >= chunkedArray.len:
    raise newException(IndexDefect, "Chunk index out of bounds")
  let handle = garrow_chunked_array_get_chunk(chunkedArray.toPtr, i.guint)
  result.handle = handle

proc getChunks*[T](chunkedArray: ChunkedArray[T]): ptr GList =
  ## Returns a GList of all chunks. The caller must not free the list.
  result = garrow_chunked_array_get_chunks(chunkedArray.toPtr)

proc slice*(chunkedArray: ChunkedArray, offset: uint64, length: uint64): ChunkedArray =
  ## Returns a zero-copy slice of the chunked array starting at `offset` with the given `length`.
  let handle =
    garrow_chunked_array_slice(chunkedArray.toPtr, offset.guint64, length.guint64)
  result = ChunkedArray(handle: handle)

proc slice*(chunkedArray: ChunkedArray, slice: HSlice[int, int]): ChunkedArray =
  let start = uint64(slice.a)
  let length = uint64(slice.b - slice.a + 1)
  result = chunkedArray.slice(start, length)

proc `$`*(chunkedArray: ChunkedArray): string =
  let cStr = verify garrow_chunked_array_to_string(chunkedArray.toPtr)
  result = $newGString(cStr, owned = true)

proc combine*[T](chunkedArray: ChunkedArray[T]): Array[T] =
  ## Concatenate all chunks into a single contiguous `Array`.
  let handle = verify garrow_chunked_array_combine(chunkedArray.toPtr)
  result.handle = handle

proc exportCArray*(chunkedArray: ChunkedArray): pointer =
  result = verify garrow_chunked_array_export(chunkedArray.toPtr)

iterator chunks*[T](chunkedArray: ChunkedArray[T]): Array[T] =
  let nChunks = chunkedArray.nChunks()
  if len(chunkedArray) > 0:
    for i in 0.uint ..< nChunks:
      yield chunkedArray.getChunk(i)

proc `[]`*[T](chunkedArray: ChunkedArray[T], i: int): T =
  if i < 0 or i >= chunkedArray.len:
    raise newException(IndexDefect, "Index out of bounds")

  var currentIndex = i
  let nChunks = chunkedArray.nChunks()

  for chunkIdx in 0.uint ..< nChunks:
    let chunk = chunkedArray.getChunk(chunkIdx)
    let chunkLen = chunk.len

    if currentIndex < chunkLen:
      return chunk[currentIndex]
    else:
      currentIndex -= chunkLen

  raise newException(IndexDefect, "Index out of bounds")

proc isNull*(chunkedArray: ChunkedArray, i: int): bool =
  if i < 0 or i >= chunkedArray.len:
    raise newException(IndexDefect, "Index out of bounds")

  var currentIndex = i
  let nChunks = chunkedArray.nChunks()

  for chunkIdx in 0.uint ..< nChunks:
    let chunk = chunkedArray.getChunk(chunkIdx)
    let chunkLen = chunk.len

    if currentIndex < chunkLen:
      return chunk.isNull(currentIndex)
    else:
      currentIndex -= chunkLen

  raise newException(IndexDefect, "Index out of bounds")

proc isValid*(chunkedArray: ChunkedArray, i: int): bool =
  result = not chunkedArray.isNull(i)

proc tryGet*[T](chunkedArray: ChunkedArray[T], i: int): Option[T] =
  if i < 0 or i >= chunkedArray.len:
    return none(T)
  if chunkedArray.isNull(i):
    return none(T)
  return some(chunkedArray[i])

proc `@`*[T](chunkedArray: ChunkedArray[T]): seq[T] =
  result = newSeq[T](chunkedArray.len)
  var idx = 0
  for chunk in chunkedArray.chunks:
    for item in chunk:
      result[idx] = item
      inc idx

iterator items*[T](chunkedArray: ChunkedArray[T]): lent T =
  if len(chunkedArray) > 0:
    for chunk in chunkedArray.chunks:
      for item in chunk:
        yield item

# ============================================================================
# Half-Float Array Types
# ============================================================================

arcGObject:
  type
    HGFloatArray* = object
      handle*: ptr GArrowHalfFloatArray

    HGFloatArrayBuilder* = object
      handle*: ptr GArrowHalfFloatArrayBuilder

    HGFloatScalar* = object
      handle*: ptr GArrowHalfFloatScalar

proc newHalfFloatArrayBuilder*(): HGFloatArrayBuilder =
  result.handle = garrow_half_float_array_builder_new()
  if isNil(result.handle):
    raise newException(OperationError, "Error creating half-float builder")

proc append*(builder: var HGFloatArrayBuilder, val: sink HalfFloat) =
  verify garrow_half_float_array_builder_append_value(builder.handle, val.uint16)

proc appendNull*(builder: var HGFloatArrayBuilder) =
  verify garrow_array_builder_append_null(cast[ptr GArrowArrayBuilder](builder.handle))

proc appendValues*(builder: var HGFloatArrayBuilder, values: openArray[HalfFloat]) =
  let len = values.len.gint64
  var vals = newSeq[uint16](values.len)
  for i, v in values:
    vals[i] = v.uint16
  verify garrow_half_float_array_builder_append_values(
    builder.handle, cast[ptr guint16](addr vals[0]), len, nil, 0
  )

proc finish*(builder: HGFloatArrayBuilder): HGFloatArray =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](builder.handle))
  result.handle = cast[ptr GArrowHalfFloatArray](handle)

proc newHalfFloatArray*(values: sink seq[HalfFloat]): HGFloatArray =
  var builder = newHalfFloatArrayBuilder()
  if values.len > 0:
    builder.appendValues(values)
  result = builder.finish()

proc newHalfFloatArray*(
    values: sink seq[HalfFloat], mask: openArray[bool]
): HGFloatArray =
  var builder = newHalfFloatArrayBuilder()
  for i in 0 ..< values.len:
    if i < mask.len and mask[i]:
      builder.appendNull()
    else:
      builder.append(values[i])
  result = builder.finish()

func len*(arr: HGFloatArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

func isNull*(arr: HGFloatArray, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i.gint64) != 0

func `[]`*(arr: HGFloatArray, i: int): HalfFloat =
  if arr.len == 0:
    raise newException(IndexDefect, "Empty array")
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  HalfFloat(garrow_half_float_array_get_value(arr.handle, i.gint64))

proc `$`*(arr: HGFloatArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

proc toSeq*(arr: HGFloatArray): seq[HalfFloat] =
  result = newSeq[HalfFloat](arr.len)
  var length: gint64
  let data = garrow_half_float_array_get_values(arr.handle, addr length)
  let src = cast[ptr UncheckedArray[guint16]](data)
  for i in 0 ..< arr.len:
    result[i] = HalfFloat(src[i])

proc `@`*(arr: HGFloatArray): seq[HalfFloat] =
  arr.toSeq

iterator items*(arr: HGFloatArray): HalfFloat =
  for i in 0 ..< arr.len:
    yield arr[i]

proc newHalfFloatScalar*(value: HalfFloat): HGFloatScalar =
  result.handle = garrow_half_float_scalar_new(value.uint16)

func getValue*(scalar: HGFloatScalar): HalfFloat =
  HalfFloat(garrow_half_float_scalar_get_value(scalar.handle))

# ============================================================================
# Null Array Types
# ============================================================================

arcGObject:
  type
    NullArray* = object
      handle*: ptr GArrowNullArray

    NullArrayBuilder* = object
      handle*: ptr GArrowNullArrayBuilder

    NullScalar* = object
      handle*: ptr GArrowNullScalar

proc newNullArrayBuilder*(): NullArrayBuilder =
  result.handle = garrow_null_array_builder_new()
  if isNil(result.handle):
    raise newException(OperationError, "Error creating null array builder")

proc appendNull*(builder: var NullArrayBuilder) =
  verify garrow_null_array_builder_append_null(builder.handle)

proc appendNulls*(builder: var NullArrayBuilder, n: int) =
  verify garrow_null_array_builder_append_nulls(builder.handle, n.gint64)

proc finish*(builder: NullArrayBuilder): NullArray =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](builder.handle))
  result.handle = cast[ptr GArrowNullArray](handle)

proc newNullArray*(length: int): NullArray =
  result.handle = garrow_null_array_new(length.gint64)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating null array")

proc newNullArray*(builder: NullArrayBuilder): NullArray =
  result = builder.finish()

func len*(arr: NullArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

func isNull*(arr: NullArray, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  true

proc `$`*(arr: NullArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

proc newNullScalar*(): NullScalar =
  result.handle = garrow_null_scalar_new()
