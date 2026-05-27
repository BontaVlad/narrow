import ../core/[ffi, error, utils]
import ./gtypes

# ============================================================================
# Decimal128 Value Type
# ============================================================================

arcGObject:
  type Decimal128* = object
    handle*: ptr GArrowDecimal128

proc newDecimal128*(s: string): Decimal128 =
  result.handle = verify garrow_decimal128_new_string(s.cstring)

proc newDecimal128*(val: int64): Decimal128 =
  result.handle = garrow_decimal128_new_integer(val)

proc `$`*(d: Decimal128): string =
  let cstr = garrow_decimal128_to_string(d.handle)
  result = $newGString(cstr, owned = true)

proc toBytes*(d: Decimal128): seq[byte] =
  let gb = garrow_decimal128_to_bytes(d.handle)
  var size: gsize
  let data = g_bytes_get_data(gb, addr size)
  let sz = int(size)
  result = newSeq[byte](sz)
  if sz > 0:
    copyMem(addr result[0], data, sz)
  g_bytes_unref(gb)

proc rescale*(d: Decimal128, originalScale, newScale: int32): Decimal128 =
  result.handle = verify garrow_decimal128_rescale(
    d.handle, originalScale, newScale
  )

proc toInt*(d: Decimal128): int64 =
  garrow_decimal128_to_integer(d.handle)

proc `==`*(a, b: Decimal128): bool =
  garrow_decimal128_equal(a.handle, b.handle).bool

proc `<`*(a, b: Decimal128): bool =
  garrow_decimal128_less_than(a.handle, b.handle).bool

proc `<=`*(a, b: Decimal128): bool =
  garrow_decimal128_less_than_or_equal(a.handle, b.handle).bool

proc `>`*(a, b: Decimal128): bool =
  garrow_decimal128_greater_than(a.handle, b.handle).bool

proc `>=`*(a, b: Decimal128): bool =
  garrow_decimal128_greater_than_or_equal(a.handle, b.handle).bool

proc `+`*(a, b: Decimal128): Decimal128 =
  result.handle = garrow_decimal128_plus(a.handle, b.handle)

proc `-`*(a, b: Decimal128): Decimal128 =
  result.handle = garrow_decimal128_minus(a.handle, b.handle)

proc `*`*(a, b: Decimal128): Decimal128 =
  result.handle = garrow_decimal128_multiply(a.handle, b.handle)

proc `/`*(a, b: Decimal128): Decimal128 =
  result.handle = verify garrow_decimal128_divide(a.handle, b.handle, nil)

proc abs*(d: sink Decimal128): Decimal128 =
  let copied = garrow_decimal128_copy(d.handle)
  garrow_decimal128_abs(copied)
  result.handle = copied

proc negate*(d: sink Decimal128): Decimal128 =
  let copied = garrow_decimal128_copy(d.handle)
  garrow_decimal128_negate(copied)
  result.handle = copied

# ============================================================================
# Decimal256 Value Type
# ============================================================================

arcGObject:
  type Decimal256* = object
    handle*: ptr GArrowDecimal256

proc newDecimal256*(s: string): Decimal256 =
  result.handle = verify garrow_decimal256_new_string(s.cstring)

proc newDecimal256*(val: int64): Decimal256 =
  result.handle = garrow_decimal256_new_integer(val)

proc `$`*(d: Decimal256): string =
  let cstr = garrow_decimal256_to_string(d.handle)
  result = $newGString(cstr, owned = true)

proc toBytes*(d: Decimal256): seq[byte] =
  let gb = garrow_decimal256_to_bytes(d.handle)
  var size: gsize
  let data = g_bytes_get_data(gb, addr size)
  let sz = int(size)
  result = newSeq[byte](sz)
  if sz > 0:
    copyMem(addr result[0], data, sz)
  g_bytes_unref(gb)

proc rescale*(d: Decimal256, originalScale, newScale: int32): Decimal256 =
  result.handle = verify garrow_decimal256_rescale(
    d.handle, originalScale, newScale
  )

proc `==`*(a, b: Decimal256): bool =
  garrow_decimal256_equal(a.handle, b.handle).bool

proc `<`*(a, b: Decimal256): bool =
  garrow_decimal256_less_than(a.handle, b.handle).bool

proc `<=`*(a, b: Decimal256): bool =
  garrow_decimal256_less_than_or_equal(a.handle, b.handle).bool

proc `>`*(a, b: Decimal256): bool =
  garrow_decimal256_greater_than(a.handle, b.handle).bool

proc `>=`*(a, b: Decimal256): bool =
  garrow_decimal256_greater_than_or_equal(a.handle, b.handle).bool

proc `+`*(a, b: Decimal256): Decimal256 =
  result.handle = garrow_decimal256_plus(a.handle, b.handle)

proc `*`*(a, b: Decimal256): Decimal256 =
  result.handle = garrow_decimal256_multiply(a.handle, b.handle)

proc `/`*(a, b: Decimal256): Decimal256 =
  result.handle = verify garrow_decimal256_divide(a.handle, b.handle, nil)

proc abs*(d: sink Decimal256): Decimal256 =
  let copied = garrow_decimal256_copy(d.handle)
  garrow_decimal256_abs(copied)
  result.handle = copied

proc negate*(d: sink Decimal256): Decimal256 =
  let copied = garrow_decimal256_copy(d.handle)
  garrow_decimal256_negate(copied)
  result.handle = copied

# ============================================================================
# Decimal128 DataType
# ============================================================================

arcGObject:
  type Decimal128DataType* = object
    handle*: ptr GArrowDecimal128DataType

proc newDecimal128DataType*(precision, scale: int32): Decimal128DataType =
  result.handle = verify garrow_decimal128_data_type_new(precision, scale)

proc precision*(dt: Decimal128DataType): int32 =
  garrow_decimal_data_type_get_precision(cast[ptr GArrowDecimalDataType](dt.handle))

proc scale*(dt: Decimal128DataType): int32 =
  garrow_decimal_data_type_get_scale(cast[ptr GArrowDecimalDataType](dt.handle))

# ============================================================================
# Decimal256 DataType
# ============================================================================

arcGObject:
  type Decimal256DataType* = object
    handle*: ptr GArrowDecimal256DataType

proc newDecimal256DataType*(precision, scale: int32): Decimal256DataType =
  result.handle = verify garrow_decimal256_data_type_new(precision, scale)

proc precision*(dt: Decimal256DataType): int32 =
  garrow_decimal_data_type_get_precision(cast[ptr GArrowDecimalDataType](dt.handle))

proc scale*(dt: Decimal256DataType): int32 =
  garrow_decimal_data_type_get_scale(cast[ptr GArrowDecimalDataType](dt.handle))

# ============================================================================
# Decimal128 Array
# ============================================================================

type
  Decimal128Array* = object
    handle: ptr GArrowDecimal128Array
    precision*: int32
    scale*: int32

proc `=destroy`*(a: Decimal128Array) =
  if not isNil(a.handle):
    g_object_unref(a.handle)

proc `=wasMoved`*(a: var Decimal128Array) =
  a.handle = nil

proc `=dup`*(a: Decimal128Array): Decimal128Array =
  result.handle = a.handle
  result.precision = a.precision
  result.scale = a.scale
  if not isNil(a.handle):
    discard g_object_ref(a.handle)

proc `=copy`*(dest: var Decimal128Array, src: Decimal128Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.precision = src.precision
    dest.scale = src.scale
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc len*(a: Decimal128Array): int =
  garrow_array_get_length(cast[ptr GArrowArray](a.handle))

proc `[]`*(a: Decimal128Array, idx: int): Decimal128 =
  if idx < 0 or idx >= a.len:
    raise newException(IndexDefect, "Index out of bounds")
  let val = garrow_decimal128_array_get_value(a.handle, idx.gint64)
  Decimal128(handle: val)

proc isNull*(a: Decimal128Array, idx: int): bool =
  if idx < 0 or idx >= a.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_array_is_null(cast[ptr GArrowArray](a.handle), idx).bool

proc formatValue*(a: Decimal128Array, idx: int): string =
  if idx < 0 or idx >= a.len:
    raise newException(IndexDefect, "Index out of bounds")
  let cstr = garrow_decimal128_array_format_value(a.handle, idx.gint64)
  result = $newGString(cstr, owned = true)

proc `$`*(a: Decimal128Array): string =
  let cstr = verify garrow_array_to_string(cast[ptr GArrowArray](a.handle))
  result = $newGString(cstr, owned = true)

# ============================================================================
# Decimal128 Array Builder
# ============================================================================

type
  Decimal128ArrayBuilder* = object
    handle: ptr GArrowDecimal128ArrayBuilder
    precision: int32
    scale: int32

proc `=destroy`*(b: Decimal128ArrayBuilder) =
  if not isNil(b.handle):
    g_object_unref(b.handle)

proc `=wasMoved`*(b: var Decimal128ArrayBuilder) =
  b.handle = nil

proc `=dup`*(b: Decimal128ArrayBuilder): Decimal128ArrayBuilder =
  result.handle = b.handle
  result.precision = b.precision
  result.scale = b.scale
  if not isNil(b.handle):
    discard g_object_ref(b.handle)

proc `=copy`*(dest: var Decimal128ArrayBuilder, src: Decimal128ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.precision = src.precision
    dest.scale = src.scale
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newDecimal128ArrayBuilder*(
    precision, scale: int32
): Decimal128ArrayBuilder =
  let dt = newDecimal128DataType(precision, scale)
  result.handle = garrow_decimal128_array_builder_new(dt.handle)
  result.precision = precision
  result.scale = scale

proc append*(b: var Decimal128ArrayBuilder, val: Decimal128) =
  verify garrow_decimal128_array_builder_append_value(b.handle, val.handle)

proc append*(b: var Decimal128ArrayBuilder, val: string) =
  let d = newDecimal128(val)
  b.append(d)

proc append*(b: var Decimal128ArrayBuilder, val: int64) =
  let d = newDecimal128(val)
  b.append(d)

proc appendNull*(b: var Decimal128ArrayBuilder) =
  verify garrow_array_builder_append_null(cast[ptr GArrowArrayBuilder](b.handle))

proc finish*(b: var Decimal128ArrayBuilder): Decimal128Array =
  let handle = verify garrow_array_builder_finish(
    cast[ptr GArrowArrayBuilder](b.handle)
  )
  Decimal128Array(
    handle: cast[ptr GArrowDecimal128Array](handle),
    precision: b.precision,
    scale: b.scale,
  )

# ============================================================================
# Decimal256 Array
# ============================================================================

type
  Decimal256Array* = object
    handle: ptr GArrowDecimal256Array
    precision*: int32
    scale*: int32

proc `=destroy`*(a: Decimal256Array) =
  if not isNil(a.handle):
    g_object_unref(a.handle)

proc `=wasMoved`*(a: var Decimal256Array) =
  a.handle = nil

proc `=dup`*(a: Decimal256Array): Decimal256Array =
  result.handle = a.handle
  result.precision = a.precision
  result.scale = a.scale
  if not isNil(a.handle):
    discard g_object_ref(a.handle)

proc `=copy`*(dest: var Decimal256Array, src: Decimal256Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.precision = src.precision
    dest.scale = src.scale
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc len*(a: Decimal256Array): int =
  garrow_array_get_length(cast[ptr GArrowArray](a.handle))

proc `[]`*(a: Decimal256Array, idx: int): Decimal256 =
  if idx < 0 or idx >= a.len:
    raise newException(IndexDefect, "Index out of bounds")
  let val = garrow_decimal256_array_get_value(a.handle, idx.gint64)
  Decimal256(handle: val)

proc isNull*(a: Decimal256Array, idx: int): bool =
  if idx < 0 or idx >= a.len:
    raise newException(IndexDefect, "Index out of bounds")
  garrow_array_is_null(cast[ptr GArrowArray](a.handle), idx).bool

proc formatValue*(a: Decimal256Array, idx: int): string =
  if idx < 0 or idx >= a.len:
    raise newException(IndexDefect, "Index out of bounds")
  let cstr = garrow_decimal256_array_format_value(a.handle, idx.gint64)
  result = $newGString(cstr, owned = true)

proc `$`*(a: Decimal256Array): string =
  let cstr = verify garrow_array_to_string(cast[ptr GArrowArray](a.handle))
  result = $newGString(cstr, owned = true)

# ============================================================================
# Decimal256 Array Builder
# ============================================================================

type
  Decimal256ArrayBuilder* = object
    handle: ptr GArrowDecimal256ArrayBuilder
    precision: int32
    scale: int32

proc `=destroy`*(b: Decimal256ArrayBuilder) =
  if not isNil(b.handle):
    g_object_unref(b.handle)

proc `=wasMoved`*(b: var Decimal256ArrayBuilder) =
  b.handle = nil

proc `=dup`*(b: Decimal256ArrayBuilder): Decimal256ArrayBuilder =
  result.handle = b.handle
  result.precision = b.precision
  result.scale = b.scale
  if not isNil(b.handle):
    discard g_object_ref(b.handle)

proc `=copy`*(dest: var Decimal256ArrayBuilder, src: Decimal256ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.precision = src.precision
    dest.scale = src.scale
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newDecimal256ArrayBuilder*(
    precision, scale: int32
): Decimal256ArrayBuilder =
  let dt = newDecimal256DataType(precision, scale)
  result.handle = garrow_decimal256_array_builder_new(dt.handle)
  result.precision = precision
  result.scale = scale

proc append*(b: var Decimal256ArrayBuilder, val: Decimal256) =
  verify garrow_decimal256_array_builder_append_value(b.handle, val.handle)

proc append*(b: var Decimal256ArrayBuilder, val: string) =
  let d = newDecimal256(val)
  b.append(d)

proc append*(b: var Decimal256ArrayBuilder, val: int64) =
  let d = newDecimal256(val)
  b.append(d)

proc appendNull*(b: var Decimal256ArrayBuilder) =
  verify garrow_array_builder_append_null(cast[ptr GArrowArrayBuilder](b.handle))

proc finish*(b: var Decimal256ArrayBuilder): Decimal256Array =
  let handle = verify garrow_array_builder_finish(
    cast[ptr GArrowArrayBuilder](b.handle)
  )
  Decimal256Array(
    handle: cast[ptr GArrowDecimal256Array](handle),
    precision: b.precision,
    scale: b.scale,
  )
