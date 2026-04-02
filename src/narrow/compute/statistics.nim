import ../core/[ffi]

type
  Statistics* = object
    handle*: ptr GParquetStatistics

  BooleanStatistics* = object
    handle*: ptr GParquetBooleanStatistics

  Int32Statistics* = object
    handle*: ptr GParquetInt32Statistics

  Int64Statistics* = object
    handle*: ptr GParquetInt64Statistics

  FloatStatistics* = object
    handle*: ptr GParquetFloatStatistics

  DoubleStatistics* = object
    handle*: ptr GParquetDoubleStatistics

  ByteArrayStatistics* = object
    handle*: ptr GParquetByteArrayStatistics

  FixedLengthByteArrayStatistics* = object
    handle*: ptr GParquetFixedLengthByteArrayStatistics

proc toPtr*(s: Statistics): ptr GParquetStatistics =
  s.handle

proc toPtr*(s: BooleanStatistics): ptr GParquetBooleanStatistics =
  s.handle

proc toPtr*(s: Int32Statistics): ptr GParquetInt32Statistics =  
  s.handle

proc toPtr*(s: Int64Statistics): ptr GParquetInt64Statistics =
  s.handle

proc toPtr*(s: FloatStatistics): ptr GParquetFloatStatistics =  
  s.handle

proc toPtr*(s: DoubleStatistics): ptr GParquetDoubleStatistics =        
  s.handle

proc toPtr*(s: ByteArrayStatistics): ptr GParquetByteArrayStatistics =
  s.handle

proc toPtr*(s: FixedLengthByteArrayStatistics): ptr GParquetFixedLengthByteArrayStatistics =    
  s.handle  

proc newStatistics*(handle: ptr GParquetStatistics): Statistics =
  result.handle = handle

proc newBooleanStatistics*(handle: ptr GParquetBooleanStatistics): BooleanStatistics =
  result.handle = handle

proc newInt32Statistics*(handle: ptr GParquetInt32Statistics): Int32Statistics =
  result.handle = handle

proc newInt64Statistics*(handle: ptr GParquetInt64Statistics): Int64Statistics =
  result.handle = handle

proc newFloatStatistics*(handle: ptr GParquetFloatStatistics): FloatStatistics =
  result.handle = handle

proc newDoubleStatistics*(handle: ptr GParquetDoubleStatistics): DoubleStatistics =
  result.handle = handle

proc newByteArrayStatistics*(
    handle: ptr GParquetByteArrayStatistics
): ByteArrayStatistics =
  result.handle = handle

proc newFixedLengthByteArrayStatistics*(
    handle: ptr GParquetFixedLengthByteArrayStatistics
): FixedLengthByteArrayStatistics =
  result.handle = handle

proc `==`*(a, b: Statistics): bool =
  gparquet_statistics_equal(a.toPtr, b.toPtr) == 1

proc hasNulls*(s: Statistics): bool =
  gparquet_statistics_has_n_nulls(s.toPtr) == 1

proc nullCount*(s: Statistics): int64 =
  gparquet_statistics_get_n_nulls(s.toPtr)

proc hasDistinctValues*(s: Statistics): bool =
  gparquet_statistics_has_n_distinct_values(s.toPtr) == 1

proc distinctValueCount*(s: Statistics): int64 =
  gparquet_statistics_get_n_distinct_values(s.toPtr)

proc valueCount*(s: Statistics): int64 =
  gparquet_statistics_get_n_values(s.toPtr)

proc hasMinMax*(s: Statistics): bool =
  gparquet_statistics_has_min_max(s.toPtr) == 1

proc min*(s: BooleanStatistics): bool =
  gparquet_boolean_statistics_get_min(s.toPtr) == 1

proc max*(s: BooleanStatistics): bool =
  gparquet_boolean_statistics_get_max(s.toPtr) == 1

proc min*(s: Int32Statistics): int32 =
  gparquet_int32_statistics_get_min(s.toPtr)

proc max*(s: Int32Statistics): int32 =
  gparquet_int32_statistics_get_max(s.toPtr)

proc min*(s: Int64Statistics): int64 =
  gparquet_int64_statistics_get_min(s.toPtr)

proc max*(s: Int64Statistics): int64 =
  gparquet_int64_statistics_get_max(s.toPtr)

proc min*(s: FloatStatistics): float32 =
  gparquet_float_statistics_get_min(s.toPtr)

proc max*(s: FloatStatistics): float32 =
  gparquet_float_statistics_get_max(s.toPtr)

proc min*(s: DoubleStatistics): float64 =
  gparquet_double_statistics_get_min(s.toPtr)

proc max*(s: DoubleStatistics): float64 =
  gparquet_double_statistics_get_max(s.toPtr)

proc min*(s: ByteArrayStatistics): seq[byte] =
  let bytes = gparquet_byte_array_statistics_get_min(s.toPtr)
  if not isNil(bytes):
    let size = g_bytes_get_size(bytes)
    let data = g_bytes_get_data(bytes, nil)
    result = newSeq[byte](size)
    if size > 0:
      copyMem(result[0].addr, data, size)
    g_bytes_unref(bytes)

proc max*(s: ByteArrayStatistics): seq[byte] =
  let bytes = gparquet_byte_array_statistics_get_max(s.toPtr)
  if not isNil(bytes):
    let size = g_bytes_get_size(bytes)
    let data = g_bytes_get_data(bytes, nil)
    result = newSeq[byte](size)
    if size > 0:
      copyMem(result[0].addr, data, size)
    g_bytes_unref(bytes)

proc toString*(bytes: seq[byte]): string =
  ## Converts a byte sequence to a string.
  result = newString(bytes.len)
  if bytes.len > 0:
    copyMem(addr result[0], unsafeAddr bytes[0], bytes.len)

proc min*(s: FixedLengthByteArrayStatistics): seq[byte] =
  let bytes = gparquet_fixed_length_byte_array_statistics_get_min(s.toPtr)
  if not isNil(bytes):
    let size = g_bytes_get_size(bytes)
    let data = g_bytes_get_data(bytes, nil)
    result = newSeq[byte](size)
    if size > 0:
      copyMem(result[0].addr, data, size)
    g_bytes_unref(bytes)

proc max*(s: FixedLengthByteArrayStatistics): seq[byte] =
  let bytes = gparquet_fixed_length_byte_array_statistics_get_max(s.toPtr)
  if not isNil(bytes):
    let size = g_bytes_get_size(bytes)
    let data = g_bytes_get_data(bytes, nil)
    result = newSeq[byte](size)
    if size > 0:
      copyMem(result[0].addr, data, size)
    g_bytes_unref(bytes)

# ============================================================================
# Type Checking Functions
# ============================================================================

proc isBooleanStatistics*(s: Statistics): bool =
  let inst = cast[ptr GTypeInstance](s.handle)
  g_type_check_instance_is_a(inst, garrow_null_scalar_get_type()) != 0

proc isInt32Statistics*(s: Statistics): bool =
  let inst = cast[ptr GTypeInstance](s.handle)
  g_type_check_instance_is_a(inst, gparquet_int32_statistics_get_type()) != 0

proc isInt64Statistics*(s: Statistics): bool =
  let inst = cast[ptr GTypeInstance](s.handle)
  g_type_check_instance_is_a(inst, gparquet_int64_statistics_get_type()) != 0

proc isFloatStatistics*(s: Statistics): bool =
  let inst = cast[ptr GTypeInstance](s.handle)
  g_type_check_instance_is_a(inst, gparquet_float_statistics_get_type()) != 0

proc isDoubleStatistics*(s: Statistics): bool =
  let inst = cast[ptr GTypeInstance](s.handle)
  g_type_check_instance_is_a(inst, gparquet_double_statistics_get_type()) != 0

proc isByteArrayStatistics*(s: Statistics): bool =
  let inst = cast[ptr GTypeInstance](s.handle)
  g_type_check_instance_is_a(inst, gparquet_byte_array_statistics_get_type()) != 0

proc isFixedLengthByteArrayStatistics*(s: Statistics): bool =
  let inst = cast[ptr GTypeInstance](s.handle)
  g_type_check_instance_is_a(inst, gparquet_fixed_length_byte_array_statistics_get_type()) != 0

# ============================================================================
# Type Conversion Functions
# ============================================================================

proc toBooleanStatistics*(s: Statistics): BooleanStatistics =
  let handle = cast[ptr GParquetBooleanStatistics](s.toPtr)
  discard g_object_ref(handle)
  newBooleanStatistics(handle)

proc toInt32Statistics*(s: Statistics): Int32Statistics =
  let handle = cast[ptr GParquetInt32Statistics](s.toPtr)
  discard g_object_ref(handle)
  newInt32Statistics(handle)

proc toInt64Statistics*(s: Statistics): Int64Statistics =
  let handle = cast[ptr GParquetInt64Statistics](s.toPtr)
  discard g_object_ref(handle)
  newInt64Statistics(handle)

proc toFloatStatistics*(s: Statistics): FloatStatistics =
  let handle = cast[ptr GParquetFloatStatistics](s.toPtr)
  discard g_object_ref(handle)
  newFloatStatistics(handle)

proc toDoubleStatistics*(s: Statistics): DoubleStatistics =
  let handle = cast[ptr GParquetDoubleStatistics](s.toPtr)
  discard g_object_ref(handle)
  newDoubleStatistics(handle)

proc toByteArrayStatistics*(s: Statistics): ByteArrayStatistics =
  let handle = cast[ptr GParquetByteArrayStatistics](s.toPtr)
  discard g_object_ref(handle)
  newByteArrayStatistics(handle)

proc toFixedLengthByteArrayStatistics*(s: Statistics): FixedLengthByteArrayStatistics =
  let handle = cast[ptr GParquetFixedLengthByteArrayStatistics](s.toPtr)
  discard g_object_ref(handle)
  newFixedLengthByteArrayStatistics(handle)
