import std/options
import ../core/[ffi, error, utils]
import ../types/gtypes
import ../column/primitive
import ../column/nested
import ./expressions
import ./functions

# ============================================================================
# CountMode & CountOptions
# ============================================================================

type CountMode* = enum
  OnlyValid = GARROW_COUNT_MODE_ONLY_VALID.int
  OnlyNull = GARROW_COUNT_MODE_ONLY_NULL.int
  All = GARROW_COUNT_MODE_ALL.int

arcGObject:
  type CountOptions* = object
    handle*: ptr GArrowCountOptions

proc newCountOptions*(mode: CountMode = All): CountOptions =
  result.handle = garrow_count_options_new()
  if result.handle.isNil:
    raise newException(IOError, "Failed to create CountOptions")
  g_object_set(result.handle, "mode", mode.cint, nil)

proc mode*(options: CountOptions): CountMode {.inline.} =
  var value: cint
  g_object_get(options.handle, "mode", addr value, nil)
  result = cast[CountMode](value.int)

proc `mode=`*(options: CountOptions, value: CountMode) {.inline.} =
  g_object_set(options.handle, "mode", value.cint, nil)

# ============================================================================
# Scalar Reductions (direct FFI)
# ============================================================================

proc mean*[T: ArrowNumeric](arr: Array[T]): float64 =
  ## Compute the arithmetic mean of a numeric array.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let arr = newArray(@[1'i32, 2, 3, 4, 5])
  ##     echo mean(arr)  # 3.0
  result = verify(garrow_numeric_array_mean(cast[ptr GArrowNumericArray](arr.toPtr)))

proc sum*[T: ArrowSignedInt](arr: Array[T]): int64 =
  ## Compute the sum of a signed integer array.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let arr = newArray(@[1'i32, 2, 3, 4, 5])
  ##     echo sum(arr)  # 15
  when T is int8:
    result = verify(garrow_int8_array_sum(cast[ptr GArrowInt8Array](arr.toPtr)))
  elif T is int16:
    result = verify(garrow_int16_array_sum(cast[ptr GArrowInt16Array](arr.toPtr)))
  elif T is int32:
    result = verify(garrow_int32_array_sum(cast[ptr GArrowInt32Array](arr.toPtr)))
  elif T is int64 or T is int:
    result = verify(garrow_int64_array_sum(cast[ptr GArrowInt64Array](arr.toPtr)))

proc sum*[T: ArrowUnsignedInt](arr: Array[T]): uint64 =
  ## Compute the sum of an unsigned integer array.
  when T is uint8:
    result = verify(garrow_uint8_array_sum(cast[ptr GArrowUInt8Array](arr.toPtr)))
  elif T is uint16:
    result = verify(garrow_uint16_array_sum(cast[ptr GArrowUInt16Array](arr.toPtr)))
  elif T is uint32:
    result = verify(garrow_uint32_array_sum(cast[ptr GArrowUInt32Array](arr.toPtr)))
  elif T is uint64:
    result = verify(garrow_uint64_array_sum(cast[ptr GArrowUInt64Array](arr.toPtr)))

proc sum*[T: ArrowFloating](arr: Array[T]): float64 =
  ## Compute the sum of a floating-point array.
  when T is float32:
    result = verify(garrow_float_array_sum(cast[ptr GArrowFloatArray](arr.toPtr)))
  elif T is float64:
    result = verify(garrow_double_array_sum(cast[ptr GArrowDoubleArray](arr.toPtr)))

proc count*(arr: Array, options: CountOptions = newCountOptions()): int64 =
  ## Count elements in an array according to the given mode.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let arr = newArray(@[1'i32, 2, 3])
  ##     echo count(arr)              # 3
  ##     echo count(arr, OnlyValid)   # 3
  result = verify(garrow_array_count(arr.toPtr, options.handle))

proc countValues*[T](arr: Array[T]): nested.StructArray =
  ## Count occurrences of each distinct value.
  ## Returns a StructArray with fields ``values`` and ``counts``.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let arr = newArray(@[1'i32, 2, 1, 2, 1])
  ##     let cv = countValues(arr)
  ##     # cv has two child arrays: values [1, 2] and counts [3, 2]
  let handle = verify(garrow_array_count_values(arr.toPtr))
  result = newStructArray(handle)

# ============================================================================
# Element-Wise Helpers (via function registry)
# ============================================================================

proc multiply*[T: ArrowNumeric](a, b: Array[T]): Datum =
  ## Element-wise multiplication of two arrays.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("multiply", [da, db])

proc multiply*[T: ArrowNumeric, U: ArrowNumeric](a: Array[T], b: U): Datum =
  ## Element-wise multiplication of an array by a scalar.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("multiply", [da, db])

proc subtract*[T: ArrowNumeric](a, b: Array[T]): Datum =
  ## Element-wise subtraction of two arrays.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("subtract", [da, db])

proc subtract*[T: ArrowNumeric, U: ArrowNumeric](a: Array[T], b: U): Datum =
  ## Element-wise subtraction of a scalar from an array.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("subtract", [da, db])

proc divide*[T: ArrowNumeric](a, b: Array[T]): Datum =
  ## Element-wise division of two arrays.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("divide", [da, db])

proc divide*[T: ArrowNumeric, U: ArrowNumeric](a: Array[T], b: U): Datum =
  ## Element-wise division of an array by a scalar.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("divide", [da, db])

proc equal*[T](a, b: Array[T]): Datum =
  ## Element-wise equality comparison of two arrays.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("equal", [da, db])

proc equal*[T, U: ArrowPrimitive](a: Array[T], b: U): Datum =
  ## Element-wise equality comparison of an array against a scalar.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("equal", [da, db])

proc greater*[T: ArrowNumeric](a, b: Array[T]): Datum =
  ## Element-wise ``a > b`` comparison.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("greater", [da, db])

proc greater*[T: ArrowNumeric, U: ArrowNumeric](a: Array[T], b: U): Datum =
  ## Element-wise ``a > scalar`` comparison.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("greater", [da, db])

proc less*[T: ArrowNumeric](a, b: Array[T]): Datum =
  ## Element-wise ``a < b`` comparison.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("less", [da, db])

proc less*[T: ArrowNumeric, U: ArrowNumeric](a: Array[T], b: U): Datum =
  ## Element-wise ``a < scalar`` comparison.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("less", [da, db])

proc greaterEqual*[T: ArrowNumeric](a, b: Array[T]): Datum =
  ## Element-wise ``a >= b`` comparison.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("greater_equal", [da, db])

proc greaterEqual*[T: ArrowNumeric, U: ArrowNumeric](a: Array[T], b: U): Datum =
  ## Element-wise ``a >= scalar`` comparison.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("greater_equal", [da, db])

proc lessEqual*[T: ArrowNumeric](a, b: Array[T]): Datum =
  ## Element-wise ``a <= b`` comparison.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("less_equal", [da, db])

proc lessEqual*[T: ArrowNumeric, U: ArrowNumeric](a: Array[T], b: U): Datum =
  ## Element-wise ``a <= scalar`` comparison.
  let da = newDatum(a)
  let db = newDatum(b)
  result = call("less_equal", [da, db])
