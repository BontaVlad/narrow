## Binary and string array types: fixed-size binary, binary view, large binary.
##
## `FixedSizeBinaryArray` stores fixed-length byte sequences. `BinaryViewArray`
## and `StringViewArray` use the Arrow string view format for efficient
## variable-length data. `LargeBinaryArray` supports > 2GB of data.
import std/[strformat]
import ../core/[ffi, error, utils]
import ../types/gtypes

# ============================================================================
# Fixed-Size Binary Data Type
# ============================================================================

arcGObject:
  type FixedSizeBinaryDataType* = object ## Data type for fixed-length byte sequences.
    handle*: ptr GArrowFixedSizeBinaryDataType

proc newFixedSizeBinaryDataType*(byteWidth: int32): FixedSizeBinaryDataType =
  result.handle = garrow_fixed_size_binary_data_type_new(byteWidth)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating fixed-size binary type")

func byteWidth*(dt: FixedSizeBinaryDataType): int32 =
  garrow_fixed_size_binary_data_type_get_byte_width(dt.handle)

# ============================================================================
# Fixed-Size Binary Array
# ============================================================================

arcGObject:
  type
    FixedSizeBinaryArray* = object ## An array of fixed-length byte sequences.
      handle*: ptr GArrowFixedSizeBinaryArray

    FixedSizeBinaryArrayBuilder* = object
      handle*: ptr GArrowFixedSizeBinaryArrayBuilder

    FixedSizeBinaryScalar* = object
      handle*: ptr GArrowFixedSizeBinaryScalar

# ============================================================================
# Array Builder
# ============================================================================

proc newFixedSizeBinaryArrayBuilder*(byteWidth: int32): FixedSizeBinaryArrayBuilder =
  let dt = garrow_fixed_size_binary_data_type_new(byteWidth)
  if isNil(dt):
    raise newException(OperationError, "Error creating fixed-size binary type")
  result.handle = garrow_fixed_size_binary_array_builder_new(dt)
  g_object_unref(dt)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating fixed-size binary builder")

proc append*(builder: var FixedSizeBinaryArrayBuilder, val: seq[byte]) =
  let gb = g_bytes_new(
    if val.len > 0:
      cast[pointer](val[0].unsafeAddr)
    else:
      nil,
    val.len.csize_t,
  )
  verify garrow_fixed_size_binary_array_builder_append_value_bytes(builder.handle, gb)
  g_bytes_unref(gb)

proc appendNull*(builder: var FixedSizeBinaryArrayBuilder) =
  verify garrow_array_builder_append_null(cast[ptr GArrowArrayBuilder](builder.handle))

proc finish*(builder: FixedSizeBinaryArrayBuilder): FixedSizeBinaryArray =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](builder.handle))
  result.handle = cast[ptr GArrowFixedSizeBinaryArray](handle)

# ============================================================================
# Array Accessors
# ============================================================================

func len*(arr: FixedSizeBinaryArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

func byteWidth*(arr: FixedSizeBinaryArray): int32 =
  garrow_fixed_size_binary_array_get_byte_width(arr.handle)

func isNull*(arr: FixedSizeBinaryArray, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i.gint64) != 0

func `[]`*(arr: FixedSizeBinaryArray, i: int): seq[byte] =
  if arr.len == 0:
    raise newException(IndexDefect, "Empty array")
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  let gb = garrow_fixed_size_binary_array_get_value(arr.handle, i.gint64)
  var size: gsize
  let data = g_bytes_get_data(gb, addr size)
  let sz = int(size)
  result = newSeq[byte](sz)
  if sz > 0:
    copyMem(addr result[0], data, sz)
  g_bytes_unref(gb)

proc `$`*(arr: FixedSizeBinaryArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

proc toSeq*(arr: FixedSizeBinaryArray): seq[seq[byte]] =
  result = newSeq[seq[byte]](arr.len)
  for i in 0 ..< arr.len:
    result[i] = arr[i]

proc `@`*(arr: FixedSizeBinaryArray): seq[seq[byte]] =
  arr.toSeq

iterator items*(arr: FixedSizeBinaryArray): seq[byte] =
  for i in 0 ..< arr.len:
    yield arr[i]

# ============================================================================
# Scalar
# ============================================================================

proc newFixedSizeBinaryScalar*(value: seq[byte]): FixedSizeBinaryScalar =
  let dt = newFixedSizeBinaryDataType(value.len.int32)
  let gb = g_bytes_new(
    if value.len > 0:
      cast[pointer](value[0].unsafeAddr)
    else:
      nil,
    value.len.csize_t,
  )
  let buf = garrow_buffer_new_bytes(gb)
  result.handle = garrow_fixed_size_binary_scalar_new(dt.handle, buf)
  g_object_unref(buf)
  g_bytes_unref(gb)

# ============================================================================
# BinaryView / StringView Types
# ============================================================================

arcGObject:
  type
    BinaryViewArray* = object
      ## An array of variable-length byte sequences using the Arrow binary view format.
      handle*: ptr GArrowBinaryViewArray

    StringViewArray* = object
      ## An array of variable-length strings using the Arrow string view format.
      handle*: ptr GArrowStringViewArray

func len*(arr: BinaryViewArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

func len*(arr: StringViewArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

func isNull*(arr: BinaryViewArray, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i.gint64) != 0

func isNull*(arr: StringViewArray, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i.gint64) != 0

func `[]`*(arr: BinaryViewArray, i: int): seq[byte] =
  if arr.len == 0:
    raise newException(IndexDefect, "Empty array")
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  let gb = garrow_binary_view_array_get_value(arr.handle, i.gint64)
  var size: gsize
  let data = g_bytes_get_data(gb, addr size)
  let sz = int(size)
  result = newSeq[byte](sz)
  if sz > 0:
    copyMem(addr result[0], data, sz)
  g_bytes_unref(gb)

func `[]`*(arr: StringViewArray, i: int): string =
  if arr.len == 0:
    raise newException(IndexDefect, "Empty array")
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  let gb = garrow_string_view_array_get_value(arr.handle, i.gint64)
  var size: gsize
  let data = g_bytes_get_data(gb, addr size)
  let sz = int(size)
  result = newString(sz)
  if sz > 0:
    copyMem(addr result[0], data, sz)
  g_bytes_unref(gb)

proc `$`*(arr: BinaryViewArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

proc `$`*(arr: StringViewArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

# ============================================================================
# LargeBinary, LargeString Types
# ============================================================================

arcGObject:
  type
    LargeBinaryArray* = object
      ## An array of variable-length byte sequences supporting > 2GB of data.
      handle*: ptr GArrowLargeBinaryArray

    LargeBinaryArrayBuilder* = object
      handle*: ptr GArrowLargeBinaryArrayBuilder

    LargeStringArray* = object
      handle*: ptr GArrowLargeStringArray

    LargeStringArrayBuilder* = object
      handle*: ptr GArrowLargeStringArrayBuilder

# ============================================================================
# LargeBinaryArrayBuilder
# ============================================================================

proc newLargeBinaryArrayBuilder*(): LargeBinaryArrayBuilder =
  result.handle = garrow_large_binary_array_builder_new()
  if isNil(result.handle):
    raise newException(OperationError, "Error creating large binary builder")

proc append*(builder: var LargeBinaryArrayBuilder, val: seq[byte]) =
  let gb = g_bytes_new(
    if val.len > 0:
      cast[pointer](val[0].unsafeAddr)
    else:
      nil,
    val.len.csize_t,
  )
  verify garrow_large_binary_array_builder_append_value_bytes(builder.handle, gb)
  g_bytes_unref(gb)

proc appendNull*(builder: var LargeBinaryArrayBuilder) =
  verify garrow_array_builder_append_null(cast[ptr GArrowArrayBuilder](builder.handle))

proc finish*(builder: LargeBinaryArrayBuilder): LargeBinaryArray =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](builder.handle))
  result.handle = cast[ptr GArrowLargeBinaryArray](handle)

# ============================================================================
# LargeBinaryArray Accessors
# ============================================================================

func len*(arr: LargeBinaryArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

func isNull*(arr: LargeBinaryArray, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i.gint64) != 0

func `[]`*(arr: LargeBinaryArray, i: int): seq[byte] =
  if arr.len == 0:
    raise newException(IndexDefect, "Empty array")
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  let gb = garrow_large_binary_array_get_value(arr.handle, i.gint64)
  var size: gsize
  let data = g_bytes_get_data(gb, addr size)
  let sz = int(size)
  result = newSeq[byte](sz)
  if sz > 0:
    copyMem(addr result[0], data, sz)
  g_bytes_unref(gb)

proc `$`*(arr: LargeBinaryArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

proc toSeq*(arr: LargeBinaryArray): seq[seq[byte]] =
  result = newSeq[seq[byte]](arr.len)
  for i in 0 ..< arr.len:
    result[i] = arr[i]

proc `@`*(arr: LargeBinaryArray): seq[seq[byte]] =
  arr.toSeq

iterator items*(arr: LargeBinaryArray): seq[byte] =
  for i in 0 ..< arr.len:
    yield arr[i]

# ============================================================================
# LargeStringArrayBuilder
# ============================================================================

proc newLargeStringArrayBuilder*(): LargeStringArrayBuilder =
  result.handle = garrow_large_string_array_builder_new()
  if isNil(result.handle):
    raise newException(OperationError, "Error creating large string builder")

proc append*(builder: var LargeStringArrayBuilder, val: string) =
  verify garrow_large_string_array_builder_append_string(builder.handle, val.cstring)

proc appendNull*(builder: var LargeStringArrayBuilder) =
  verify garrow_array_builder_append_null(cast[ptr GArrowArrayBuilder](builder.handle))

proc finish*(builder: LargeStringArrayBuilder): LargeStringArray =
  let handle =
    verify garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](builder.handle))
  result.handle = cast[ptr GArrowLargeStringArray](handle)

# ============================================================================
# LargeStringArray Accessors
# ============================================================================

func len*(arr: LargeStringArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

func isNull*(arr: LargeStringArray, i: int): bool =
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i.gint64) != 0

func `[]`*(arr: LargeStringArray, i: int): string =
  if arr.len == 0:
    raise newException(IndexDefect, "Empty array")
  if i < 0:
    raise newException(IndexDefect, "Negative indexes are not supported")
  if i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 ..< {arr.len}")
  let cstr = garrow_large_string_array_get_string(arr.handle, i.gint64)
  result = $newGString(cstr, owned = true)

proc `$`*(arr: LargeStringArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

proc toSeq*(arr: LargeStringArray): seq[string] =
  result = newSeq[string](arr.len)
  for i in 0 ..< arr.len:
    result[i] = arr[i]

proc `@`*(arr: LargeStringArray): seq[string] =
  arr.toSeq

iterator items*(arr: LargeStringArray): string =
  for i in 0 ..< arr.len:
    yield arr[i]
