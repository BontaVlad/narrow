import ./[ffi]

# Primitive type: A type that has no child types and so consists of a single array, such
# as fixed-bit-width arrays (for example, int32) or variable-size types (for example,
# string arrays).
#
# Nested type: A type that depends on one or more other child types. Nested types
# are only equal if their child types are also equal (for example, List<T> and
# List<U> are equal if T and U are equal).
#
# Logical type: A particular type of interpreting the values in an array that is
# implemented using a specific physical layout. For example, the decimal logical
# type stores values as 16 bytes per value in a fixed-size binary layout. Similarly,
# a timestamp logical type stores values using a 64-bit fixed-size layout.
#
# +-------------------+-----------+----------------+---------+-------------------------------------+
# | Layout Type       | Buffer 0  | Buffer 1       | Buffer 2| Children                            |
# +-------------------+-----------+----------------+---------+-------------------------------------+
# | Primitive         | Bitmap    | Data           |         | No                                  |
# | Variable Binary   | Bitmap    | Offsets        | Data    | No                                  |
# | List              | Bitmap    | Offsets        |         | 1                                   |
# | Fixed-Size List   | Bitmap    |                |         | 1                                   |
# | Struct            | Bitmap    |                |         | 1 per field                         |
# | Sparse Union      | Type IDs  |                |         | 1 per type                          |
# | Dense Union       | Type IDs  | Offsets        |         | 1 per type                          |
# | Null              |           |                |         | No                                  |
# | Dictionary Encoded| Bitmap    | Data (Indices) |         | Dictionary (not considered a child) |
# +-------------------+-----------+----------------+---------+-------------------------------------+

# • Null logical type: Null physical type
# • Boolean: Primitive array with data represented as a bitmap
# • Primitive integer types: Primitive, fixed-size array layout:
#     Int8, Uint8, Int16, Uint16, Int32, Uint32, Int64, and Uint64
# • Floating-point types: Primitive fixed-size array layout:
#     Float16, Float32 (float), and Float64 (double)
# • VarBinary types: Variable length binary physical layout:
#     Binary and String (UTF-8)
#     LargeBinary and LargeString (variable length binary with 64-bit offsets)
# • Decimal128 and Decimal256: 128-bit and 256-bit fixed-size primitive arrays
# with metadata to specify the precision and scale of the values
# • Fixed-size binary: Fixed-size binary physical layout
# • Temporal types: Primitive fixed-size array physical layout
#   Date types: Dates with no time information:
#   Date32: 32-bit integers representing the number of days since the Unix epoch
# (1970-01-01)
#   Date64: 64-bit integers representing milliseconds since the Unix epoch
# (1970-01-01)
#   Time types: Time information with no date attached:
#   Time32: 32-bit integers representing elapsed time since midnight as seconds or
# milliseconds. A unit specified by metadata.
#   Time64: 64-bit integers representing elapsed time since midnight as
# microseconds or nanoseconds. A unit specified by metadata.
#   Timestamp: 64-bit integer representing the time since the Unix epoch, not
# including leap seconds. Metadata defines the unit (seconds, milliseconds,
# microseconds, or nanoseconds) and, optionally, a time zone as a string.
#    Interval types: An absolute length of time in terms of calendar artifacts:
#    YearMonth: Number of elapsed whole months as a 32-bit signed integer.
#    DayTime: Number of elapsed days and milliseconds as two consecutive 4-byte
# signed integers (8-bytes total per value).
#    MonthDayNano: Elapsed months, days, and nanoseconds stored as contiguous
# 16-byte blocks. Months and days as two 32-bit integers and nanoseconds since
# midnight as a 64-bit integer.
#    Duration: An absolute length of time not related to calendars as a 64-bit
# integer and a unit specified by metadata indicating seconds, milliseconds,
# microseconds, or nanoseconds.
# • List and FixedSizeList: Their respective physical layouts:
#    LargeList: A list type with 64-bit offsets
# • Struct, DenseUnion, and SparseUnion types: Their respective physical layouts
# • Map: A logical type that is physically represented as List<entries:
# Struct<key: K, value: V>>, where K and V are the respective types of the
# keys and values in the map:
#   Metadata is included indicating whether or not the keys are sorted.

type
  GADType*[T] = object
    handle: ptr GArrowDataType

  GString* = object
    handle: cstring

converter toArrowType*(g: GADType): ptr GArrowDataType =
  g.handle

proc `=destroy`*[T](tp: GADType[T]) =
  if not isNil(tp.handle):
    g_object_unref(tp.handle)
    tp.handle = nil

proc `=destroy`*(str: GString) =
  if not isNil(str.handle):
    gFree(str.handle)
    # str.handle = nil

proc `=sink`*[T](dest: var GADType[T], src: GADType[T]) =
  dest.handle = src.handle

proc `=copy`*[T](dest: var GADType[T], src: GADType[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle) # bump ref count

proc `=sink`*(dest: var GString, src: GString) =
  dest.handle = src.handle

proc `=copy`*(dest: var GString, src: GString) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      gFree(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      # duplicate so both copies own memory
      dest.handle = g_strdup(src.handle)

proc newGString*(str: cstring): GString =
  result.handle = str

proc `$`*(str: GString): string =
  $str.handle

proc `$`*(tp: GADType): string =
  let gStr = newGString(garrow_data_type_get_name(tp.handle))
  result = $gStr

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
