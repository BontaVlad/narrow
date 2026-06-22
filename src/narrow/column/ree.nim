## Run-end encoded arrays for efficiently storing runs of repeated values.
import ../core/[ffi, error, utils]
import ../types/gtypes
import ./primitive

# ============================================================================
# Run-End Encoded Data Type
# ============================================================================

arcGObject:
  type
    RunEndEncodedDataType* = object
      ## Data type for run-end encoded arrays: a run-end type and a value type.
      handle*: ptr GArrowRunEndEncodedDataType

    RunEndEncodedArray* = object
      ## An array storing runs of repeated values via run-end offsets.
      handle*: ptr GArrowRunEndEncodedArray

proc newRunEndEncodedDataType*(
    runEndType: GADType, valueType: GADType
): RunEndEncodedDataType =
  result.handle =
    garrow_run_end_encoded_data_type_new(runEndType.handle, valueType.handle)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating run-end encoded data type")

proc runEndDataType*(dt: RunEndEncodedDataType): ptr GArrowDataType =
  garrow_run_end_encoded_data_type_get_run_end_data_type(dt.handle)

proc valueDataType*(dt: RunEndEncodedDataType): ptr GArrowDataType =
  garrow_run_end_encoded_data_type_get_value_data_type(dt.handle)

proc newRunEndEncodedArray*[T, U](
    dataType: RunEndEncodedDataType,
    logicalLength: int64,
    runEnds: Array[T],
    values: Array[U],
    logicalOffset: int64 = 0,
): RunEndEncodedArray =
  result.handle = verify garrow_run_end_encoded_array_new(
    cast[ptr GArrowDataType](dataType.handle),
    logicalLength,
    cast[ptr GArrowArray](runEnds.toPtr),
    cast[ptr GArrowArray](values.toPtr),
    logicalOffset,
  )
  if isNil(result.handle):
    raise newException(OperationError, "Error creating run-end encoded array")

proc runEnds*(arr: RunEndEncodedArray): ptr GArrowArray =
  garrow_run_end_encoded_array_get_run_ends(arr.handle)

proc values*(arr: RunEndEncodedArray): ptr GArrowArray =
  garrow_run_end_encoded_array_get_values(arr.handle)

proc logicalRunEnds*(arr: RunEndEncodedArray): ptr GArrowArray =
  verify garrow_run_end_encoded_array_get_logical_run_ends(arr.handle)

proc logicalValues*(arr: RunEndEncodedArray): ptr GArrowArray =
  garrow_run_end_encoded_array_get_logical_values(arr.handle)

proc findPhysicalOffset*(arr: RunEndEncodedArray): int64 =
  garrow_run_end_encoded_array_find_physical_offset(arr.handle).int64

proc findPhysicalLength*(arr: RunEndEncodedArray): int64 =
  garrow_run_end_encoded_array_find_physical_length(arr.handle).int64

proc decode*(arr: RunEndEncodedArray): ptr GArrowArray =
  verify garrow_run_end_encoded_array_decode(arr.handle)

proc len*(arr: RunEndEncodedArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

proc `$`*(arr: RunEndEncodedArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)
