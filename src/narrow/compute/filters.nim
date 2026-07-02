## Boolean arrays and filtering.
##
## `BooleanArray` wraps a boolean array used as a filter mask. `filter()`
## selects elements from an array where the mask is `true`.
import std/[options, strformat]
import ../core/[ffi, error, utils]
import ../types/gtypes
import ../column/primitive
import ../compute/expressions
import ../tabular/[table, batch]

arcGObject:
  type
    FilterOptions* = object
      ## Options controlling filter behavior, including how null selections are handled.
      handle*: ptr GArrowFilterOptions

    BooleanArray* = object ## A typed boolean array used as a filter mask.
      handle*: ptr GArrowBooleanArray

  type FilterNullSelectionBehavior* = enum
    Drop = GARROW_FILTER_NULL_SELECTION_DROP.int
    EmitNull = GARROW_FILTER_NULL_SELECTION_EMIT_NULL.int

proc newFilterOptions*(): FilterOptions =
  result.handle = garrow_filter_options_new()

proc nullSelectionBehavior*(
    options: FilterOptions
): FilterNullSelectionBehavior {.inline.} =
  var behavior: cint
  g_object_get(options.handle, "null-selection-behavior", addr behavior, nil)
  result = cast[FilterNullSelectionBehavior](behavior.int)

proc `nullSelectionBehavior=`*(
    options: FilterOptions, behavior: FilterNullSelectionBehavior
) {.inline.} =
  g_object_set(options.handle, "null-selection-behavior", behavior.cint, nil)

# Constructor for BooleanArray from Array[bool]
proc newBooleanArray*(arr: Array[bool]): BooleanArray =
  ## Create a boolean array from a mask or seq.
  result.handle = cast[ptr GArrowBooleanArray](arr.toPtr)
  if not isNil(result.handle):
    discard g_object_ref(result.handle)

# Constructor for BooleanArray from sequence
proc newBooleanArray*(values: sink seq[bool]): BooleanArray =
  ## Create a boolean array from a mask or seq.
  let builder = newArrayBuilder[bool]()
  if len(values) != 0:
    builder.appendValues(values)
  let arr = builder.finish()
  result.handle = cast[ptr GArrowBooleanArray](arr.toPtr)
  if not isNil(result.handle):
    discard g_object_ref(result.handle)

# Constructor for BooleanArray from sequence with null mask
proc newBooleanArray*(values: sink seq[bool], mask: openArray[bool]): BooleanArray =
  ## Create a boolean array from a mask or seq.
  let builder = newArrayBuilder[bool]()
  for i in 0 ..< values.len:
    if mask[i]:
      builder.appendNull()
    else:
      builder.append(values[i])
  let arr = builder.finish()
  result.handle = cast[ptr GArrowBooleanArray](arr.toPtr)
  if not isNil(result.handle):
    discard g_object_ref(result.handle)

# Constructor for BooleanArray with Options
proc newBooleanArray*(values: sink seq[Option[bool]]): BooleanArray =
  ## Create a boolean array from a mask or seq.
  let builder = newArrayBuilder[bool]()
  for val in values:
    if val.isSome():
      builder.append(val.get())
    else:
      builder.appendNull()
  let arr = builder.finish()
  result.handle = cast[ptr GArrowBooleanArray](arr.toPtr)
  if not isNil(result.handle):
    discard g_object_ref(result.handle)

func len*(arr: BooleanArray): int {.inline.} =
  if not isNil(arr.handle):
    result = garrow_array_get_length(cast[ptr GArrowArray](arr.handle))
  else:
    result = 0

func `[]`*(arr: BooleanArray, i: int): bool {.inline.} =
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {arr.len}")
  let val = garrow_boolean_array_get_value(arr.handle, i)
  return val != 0

func isNull*(arr: BooleanArray, i: int): bool {.inline.} =
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {arr.len}")
  return garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i) != 0

func isValid*(arr: BooleanArray, i: int): bool {.inline.} =
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {arr.len}")
  return garrow_array_is_valid(cast[ptr GArrowArray](arr.handle), i) != 0

iterator items*(arr: BooleanArray): bool =
  for i in 0 ..< arr.len:
    yield arr[i]

func toSeq*(arr: BooleanArray): seq[bool] {.inline.} =
  result = newSeq[bool](arr.len)
  for i in 0 ..< arr.len:
    result[i] = arr[i]

func `@`*(arr: BooleanArray): seq[bool] {.inline.} =
  arr.toSeq

proc `$`*(arr: BooleanArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

# Filter implementations
proc filter*(
    table: ArrowTable, filter: BooleanArray, options: FilterOptions
): ArrowTable =
  ## Returns a new array containing only elements where the mask is `true`.
  let handle = verify garrow_table_filter(table.toPtr, filter.handle, options.handle)
  result = newArrowTable(handle)

proc filter*(
    table: ArrowTable, filter: ChunkedArray[Untyped], options: FilterOptions
): ArrowTable =
  ## Returns a new array containing only elements where the mask is `true`.
  let handle =
    verify garrow_table_filter_chunked_array(table.toPtr, filter.toPtr, options.handle)
  result = newArrowTable(handle)

proc filter*[T](
    chunkedArray: ChunkedArray[T], filter: BooleanArray, options: FilterOptions
): ChunkedArray[T] =
  ## Returns a new array containing only elements where the mask is `true`.
  let handle = verify garrow_chunked_array_filter(
    chunkedArray.toPtr, filter.handle, options.handle
  )
  result = newChunkedArray[T](handle)

proc filter*[T](
    chunkedArray: ChunkedArray[T], filter: ChunkedArray[bool], options: FilterOptions
): ChunkedArray[T] =
  ## Returns a new array containing only elements where the mask is `true`.
  let handle = verify garrow_chunked_array_filter_chunked_array(
    chunkedArray.toPtr, filter.toPtr, options.handle
  )
  result = newChunkedArray[T](handle)

# Convenience overloads with default options
proc filter*(table: ArrowTable, filter: BooleanArray): ArrowTable =
  ## Returns a new array containing only elements where the mask is `true`.
  let options = newFilterOptions()
  result = table.filter(filter, options)

proc filter*(table: ArrowTable, filter: ChunkedArray[Untyped]): ArrowTable =
  ## Returns a new array containing only elements where the mask is `true`.
  let options = newFilterOptions()
  result = table.filter(filter, options)

proc filter*[T](chunkedArray: ChunkedArray[T], filter: BooleanArray): ChunkedArray[T] =
  ## Returns a new array containing only elements where the mask is `true`.
  let options = newFilterOptions()
  result = chunkedArray.filter(filter, options)

proc filter*[T](
    chunkedArray: ChunkedArray[T], filter: ChunkedArray[bool]
): ChunkedArray[T] =
  ## Returns a new array containing only elements where the mask is `true`.
  let options = newFilterOptions()
  result = chunkedArray.filter(filter, options)

# Array filter support
proc filter*[T](arr: Array[T], filter: BooleanArray, options: FilterOptions): Array[T] =
  ## Returns a new array containing only elements where the mask is `true`.
  let handle = verify garrow_array_filter(arr.toPtr, filter.handle, options.handle)
  result = newArray[T](handle)

proc filter*[T](arr: Array[T], filter: BooleanArray): Array[T] =
  ## Returns a new array containing only elements where the mask is `true`.
  let options = newFilterOptions()
  result = arr.filter(filter, options)

# RecordBatch filter support
proc filter*(rb: RecordBatch, mask: BooleanArray, options: FilterOptions): RecordBatch =
  ## Returns a new array containing only elements where the mask is `true`.
  ensureComputeInitialized()
  let handle = verify garrow_record_batch_filter(rb.toPtr, mask.handle, options.handle)
  result = newRecordBatch(handle)

proc filter*(rb: RecordBatch, mask: BooleanArray): RecordBatch =
  ## Returns a new array containing only elements where the mask is `true`.
  let options = newFilterOptions()
  result = rb.filter(mask, options)
