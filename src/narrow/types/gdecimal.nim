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

# ============================================================================
# Decimal32 / Decimal64 — Compact decimal types
# ============================================================================

# --- Decimal32 Value Type ---

arcGObject:
  type Decimal32* = object
    handle*: ptr GArrowDecimal32

proc newDecimal32*(s: string): Decimal32 =
  result.handle = verify garrow_decimal32_new_string(s.cstring)

proc newDecimal32*(val: int64): Decimal32 =
  result.handle = garrow_decimal32_new_integer(val)

proc `$`*(d: Decimal32): string =
  let cstr = garrow_decimal32_to_string(d.handle)
  result = $newGString(cstr, owned = true)

proc toStringScale*(d: Decimal32, scale: int32): string =
  let cstr = garrow_decimal32_to_string_scale(d.handle, scale)
  result = $newGString(cstr, owned = true)

proc toBytes*(d: Decimal32): seq[byte] =
  let gb = garrow_decimal32_to_bytes(d.handle)
  var size: gsize
  let data = g_bytes_get_data(gb, addr size)
  let sz = int(size)
  result = newSeq[byte](sz)
  if sz > 0:
    copyMem(addr result[0], data, sz)
  g_bytes_unref(gb)

proc toInt*(d: Decimal32): int64 =
  garrow_decimal32_to_integer(d.handle)

proc rescale*(d: Decimal32, originalScale, newScale: int32): Decimal32 =
  result.handle = verify garrow_decimal32_rescale(
    d.handle, originalScale, newScale)

proc `==`*(a, b: Decimal32): bool =
  garrow_decimal32_equal(a.handle, b.handle).bool
proc `<`*(a, b: Decimal32): bool =
  garrow_decimal32_less_than(a.handle, b.handle).bool
proc `<=`*(a, b: Decimal32): bool =
  garrow_decimal32_less_than_or_equal(a.handle, b.handle).bool
proc `>`*(a, b: Decimal32): bool =
  garrow_decimal32_greater_than(a.handle, b.handle).bool
proc `>=`*(a, b: Decimal32): bool =
  garrow_decimal32_greater_than_or_equal(a.handle, b.handle).bool

proc `+`*(a, b: Decimal32): Decimal32 =
  result.handle = garrow_decimal32_plus(a.handle, b.handle)
proc `-`*(a, b: Decimal32): Decimal32 =
  result.handle = garrow_decimal32_minus(a.handle, b.handle)
proc `*`*(a, b: Decimal32): Decimal32 =
  result.handle = garrow_decimal32_multiply(a.handle, b.handle)
proc `/`*(a, b: Decimal32): Decimal32 =
  result.handle = verify garrow_decimal32_divide(a.handle, b.handle, nil)

proc abs*(d: sink Decimal32): Decimal32 =
  let copied = garrow_decimal32_copy(d.handle)
  garrow_decimal32_abs(copied)
  result.handle = copied

proc negate*(d: sink Decimal32): Decimal32 =
  let copied = garrow_decimal32_copy(d.handle)
  garrow_decimal32_negate(copied)
  result.handle = copied

# --- Decimal64 Value Type ---

arcGObject:
  type Decimal64* = object
    handle*: ptr GArrowDecimal64

proc newDecimal64*(s: string): Decimal64 =
  result.handle = verify garrow_decimal64_new_string(s.cstring)

proc newDecimal64*(val: int64): Decimal64 =
  result.handle = garrow_decimal64_new_integer(val)

proc `$`*(d: Decimal64): string =
  let cstr = garrow_decimal64_to_string(d.handle)
  result = $newGString(cstr, owned = true)

proc toStringScale*(d: Decimal64, scale: int32): string =
  let cstr = garrow_decimal64_to_string_scale(d.handle, scale)
  result = $newGString(cstr, owned = true)

proc toBytes*(d: Decimal64): seq[byte] =
  let gb = garrow_decimal64_to_bytes(d.handle)
  var size: gsize
  let data = g_bytes_get_data(gb, addr size)
  let sz = int(size)
  result = newSeq[byte](sz)
  if sz > 0:
    copyMem(addr result[0], data, sz)
  g_bytes_unref(gb)

proc toInt*(d: Decimal64): int64 =
  garrow_decimal64_to_integer(d.handle)

proc rescale*(d: Decimal64, originalScale, newScale: int32): Decimal64 =
  result.handle = verify garrow_decimal64_rescale(
    d.handle, originalScale, newScale)

proc `==`*(a, b: Decimal64): bool =
  garrow_decimal64_equal(a.handle, b.handle).bool
proc `<`*(a, b: Decimal64): bool =
  garrow_decimal64_less_than(a.handle, b.handle).bool
proc `<=`*(a, b: Decimal64): bool =
  garrow_decimal64_less_than_or_equal(a.handle, b.handle).bool
proc `>`*(a, b: Decimal64): bool =
  garrow_decimal64_greater_than(a.handle, b.handle).bool
proc `>=`*(a, b: Decimal64): bool =
  garrow_decimal64_greater_than_or_equal(a.handle, b.handle).bool

proc `+`*(a, b: Decimal64): Decimal64 =
  result.handle = garrow_decimal64_plus(a.handle, b.handle)
proc `-`*(a, b: Decimal64): Decimal64 =
  result.handle = garrow_decimal64_minus(a.handle, b.handle)
proc `*`*(a, b: Decimal64): Decimal64 =
  result.handle = garrow_decimal64_multiply(a.handle, b.handle)
proc `/`*(a, b: Decimal64): Decimal64 =
  result.handle = verify garrow_decimal64_divide(a.handle, b.handle, nil)

proc abs*(d: sink Decimal64): Decimal64 =
  let copied = garrow_decimal64_copy(d.handle)
  garrow_decimal64_abs(copied)
  result.handle = copied

proc negate*(d: sink Decimal64): Decimal64 =
  let copied = garrow_decimal64_copy(d.handle)
  garrow_decimal64_negate(copied)
  result.handle = copied

# --- Decimal32 / Decimal64 Data Types ---

arcGObject:
  type Decimal32DataType* = object
    handle*: ptr GArrowDecimal32DataType

  type Decimal64DataType* = object
    handle*: ptr GArrowDecimal64DataType

proc newDecimal32DataType*(precision, scale: int32): Decimal32DataType =
  result.handle = verify garrow_decimal32_data_type_new(precision, scale)

proc newDecimal64DataType*(precision, scale: int32): Decimal64DataType =
  result.handle = verify garrow_decimal64_data_type_new(precision, scale)

proc precision*(dt: Decimal32DataType): int32 =
  garrow_decimal_data_type_get_precision(
    cast[ptr GArrowDecimalDataType](dt.handle))

proc scale*(dt: Decimal32DataType): int32 =
  garrow_decimal_data_type_get_scale(
    cast[ptr GArrowDecimalDataType](dt.handle))

proc precision*(dt: Decimal64DataType): int32 =
  garrow_decimal_data_type_get_precision(
    cast[ptr GArrowDecimalDataType](dt.handle))

proc scale*(dt: Decimal64DataType): int32 =
  garrow_decimal_data_type_get_scale(
    cast[ptr GArrowDecimalDataType](dt.handle))

func maxPrecision*(T: typedesc[Decimal32DataType]): int32 =
  garrow_decimal32_data_type_max_precision()

func maxPrecision*(T: typedesc[Decimal64DataType]): int32 =
  garrow_decimal64_data_type_max_precision()

# --- Decimal32 Array ---

type
  Decimal32Array* = object
    handle: ptr GArrowDecimal32Array
    precision*: int32
    scale*: int32

proc `=destroy`*(a: Decimal32Array) =
  if not isNil(a.handle):
    g_object_unref(a.handle)

proc `=wasMoved`*(a: var Decimal32Array) =
  a.handle = nil

proc `=dup`*(a: Decimal32Array): Decimal32Array =
  result.handle = a.handle
  result.precision = a.precision
  result.scale = a.scale
  if not isNil(a.handle):
    discard g_object_ref(a.handle)

proc `=copy`*(dest: var Decimal32Array, src: Decimal32Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.precision = src.precision
    dest.scale = src.scale
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

func len*(a: Decimal32Array): int =
  garrow_array_get_length(cast[ptr GArrowArray](a.handle)).int

func isNull*(a: Decimal32Array, i: int): bool =
  garrow_array_is_null(cast[ptr GArrowArray](a.handle), i.gint64).bool

func `[]`*(a: Decimal32Array, i: int): Decimal32 =
  result.handle = garrow_decimal32_array_get_value(a.handle, i.gint64)

proc formatValue*(a: Decimal32Array, i: Natural): string =
  let cstr = garrow_decimal32_array_format_value(a.handle, i.gint64)
  result = $newGString(cstr, owned = true)

proc `$`*(a: Decimal32Array): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](a.handle))
  result = $newGString(cStr, owned = true)

# --- Decimal64 Array ---

type
  Decimal64Array* = object
    handle: ptr GArrowDecimal64Array
    precision*: int32
    scale*: int32

proc `=destroy`*(a: Decimal64Array) =
  if not isNil(a.handle):
    g_object_unref(a.handle)

proc `=wasMoved`*(a: var Decimal64Array) =
  a.handle = nil

proc `=dup`*(a: Decimal64Array): Decimal64Array =
  result.handle = a.handle
  result.precision = a.precision
  result.scale = a.scale
  if not isNil(a.handle):
    discard g_object_ref(a.handle)

proc `=copy`*(dest: var Decimal64Array, src: Decimal64Array) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.precision = src.precision
    dest.scale = src.scale
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

func len*(a: Decimal64Array): int =
  garrow_array_get_length(cast[ptr GArrowArray](a.handle)).int

func isNull*(a: Decimal64Array, i: int): bool =
  garrow_array_is_null(cast[ptr GArrowArray](a.handle), i.gint64).bool

func `[]`*(a: Decimal64Array, i: int): Decimal64 =
  result.handle = garrow_decimal64_array_get_value(a.handle, i.gint64)

proc formatValue*(a: Decimal64Array, i: Natural): string =
  let cstr = garrow_decimal64_array_format_value(a.handle, i.gint64)
  result = $newGString(cstr, owned = true)

proc `$`*(a: Decimal64Array): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](a.handle))
  result = $newGString(cStr, owned = true)

# --- Decimal32 / Decimal64 Array Builders ---

type
  Decimal32ArrayBuilder* = object
    handle: ptr GArrowDecimal32ArrayBuilder
    precision: int32
    scale: int32

proc `=destroy`*(b: Decimal32ArrayBuilder) =
  if not isNil(b.handle):
    g_object_unref(b.handle)

proc `=wasMoved`*(b: var Decimal32ArrayBuilder) =
  b.handle = nil

proc `=dup`*(b: Decimal32ArrayBuilder): Decimal32ArrayBuilder =
  result.handle = b.handle
  result.precision = b.precision
  result.scale = b.scale
  if not isNil(b.handle):
    discard g_object_ref(b.handle)

proc `=copy`*(dest: var Decimal32ArrayBuilder, src: Decimal32ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.precision = src.precision
    dest.scale = src.scale
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newDecimal32ArrayBuilder*(
    precision, scale: int32): Decimal32ArrayBuilder =
  let dt = newDecimal32DataType(precision, scale)
  result.handle = garrow_decimal32_array_builder_new(dt.handle)
  result.precision = precision
  result.scale = scale

proc append*(b: var Decimal32ArrayBuilder, val: Decimal32) =
  verify garrow_decimal32_array_builder_append_value(b.handle, val.handle)

proc append*(b: var Decimal32ArrayBuilder, val: string) =
  let d = newDecimal32(val)
  b.append(d)

proc append*(b: var Decimal32ArrayBuilder, val: int64) =
  let d = newDecimal32(val)
  b.append(d)

proc appendNull*(b: var Decimal32ArrayBuilder) =
  verify garrow_array_builder_append_null(
    cast[ptr GArrowArrayBuilder](b.handle))

proc finish*(b: var Decimal32ArrayBuilder): Decimal32Array =
  let handle = verify garrow_array_builder_finish(
    cast[ptr GArrowArrayBuilder](b.handle))
  Decimal32Array(
    handle: cast[ptr GArrowDecimal32Array](handle),
    precision: b.precision,
    scale: b.scale)

type
  Decimal64ArrayBuilder* = object
    handle: ptr GArrowDecimal64ArrayBuilder
    precision: int32
    scale: int32

proc `=destroy`*(b: Decimal64ArrayBuilder) =
  if not isNil(b.handle):
    g_object_unref(b.handle)

proc `=wasMoved`*(b: var Decimal64ArrayBuilder) =
  b.handle = nil

proc `=dup`*(b: Decimal64ArrayBuilder): Decimal64ArrayBuilder =
  result.handle = b.handle
  result.precision = b.precision
  result.scale = b.scale
  if not isNil(b.handle):
    discard g_object_ref(b.handle)

proc `=copy`*(dest: var Decimal64ArrayBuilder, src: Decimal64ArrayBuilder) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    dest.precision = src.precision
    dest.scale = src.scale
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newDecimal64ArrayBuilder*(
    precision, scale: int32): Decimal64ArrayBuilder =
  let dt = newDecimal64DataType(precision, scale)
  result.handle = garrow_decimal64_array_builder_new(dt.handle)
  result.precision = precision
  result.scale = scale

proc append*(b: var Decimal64ArrayBuilder, val: Decimal64) =
  verify garrow_decimal64_array_builder_append_value(b.handle, val.handle)

proc append*(b: var Decimal64ArrayBuilder, val: string) =
  let d = newDecimal64(val)
  b.append(d)

proc append*(b: var Decimal64ArrayBuilder, val: int64) =
  let d = newDecimal64(val)
  b.append(d)

proc appendNull*(b: var Decimal64ArrayBuilder) =
  verify garrow_array_builder_append_null(
    cast[ptr GArrowArrayBuilder](b.handle))

proc finish*(b: var Decimal64ArrayBuilder): Decimal64Array =
  let handle = verify garrow_array_builder_finish(
    cast[ptr GArrowArrayBuilder](b.handle))
  Decimal64Array(
    handle: cast[ptr GArrowDecimal64Array](handle),
    precision: b.precision,
    scale: b.scale)

# --- Decimal32 / Decimal64 Scalars ---

arcGObject:
  type Decimal32Scalar* = object
    handle*: ptr GArrowDecimal32Scalar

  type Decimal64Scalar* = object
    handle*: ptr GArrowDecimal64Scalar

proc newDecimal32Scalar*(
    dt: Decimal32DataType, value: Decimal32): Decimal32Scalar =
  result.handle = garrow_decimal32_scalar_new(dt.handle, value.handle)

proc newDecimal64Scalar*(
    dt: Decimal64DataType, value: Decimal64): Decimal64Scalar =
  result.handle = garrow_decimal64_scalar_new(dt.handle, value.handle)

proc getValue*(sc: Decimal32Scalar): Decimal32 =
  let raw = garrow_decimal32_scalar_get_value(sc.handle)
  result.handle = cast[ptr GArrowDecimal32](g_object_ref(raw))

proc getValue*(sc: Decimal64Scalar): Decimal64 =
  let raw = garrow_decimal64_scalar_get_value(sc.handle)
  result.handle = cast[ptr GArrowDecimal64](g_object_ref(raw))
