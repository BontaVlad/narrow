import std/[options, strformat, strutils]
import ../core/[ffi, error]
import ../types/gtypes
import ../column/primitive
import ../compute/expressions
import ../tabular/table

type
  FilterOptions* = object
    handle*: ptr GArrowFilterOptions

  BooleanArray* = object
    handle: ptr GArrowBooleanArray

  FilterNullSelectionBehavior* = enum
    Drop = GARROW_FILTER_NULL_SELECTION_DROP.int
    EmitNull = GARROW_FILTER_NULL_SELECTION_EMIT_NULL.int

# FilterOptions ARC hooks
proc `=destroy`*(options: FilterOptions) =
  if options.handle != nil:
    g_object_unref(options.handle)

proc `=sink`*(dest: var FilterOptions, src: FilterOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FilterOptions, src: FilterOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

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

# BooleanArray ARC hooks
proc `=destroy`*(arr: BooleanArray) =
  if not isNil(arr.handle):
    g_object_unref(arr.handle)

proc `=sink`*(dest: var BooleanArray, src: BooleanArray) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var BooleanArray, src: BooleanArray) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

proc toPtr*(arr: BooleanArray): ptr GArrowBooleanArray {.inline.} =
  arr.handle

# Constructor for BooleanArray from Array[bool]
proc newBooleanArray*(arr: Array[bool]): BooleanArray =
  result.handle = cast[ptr GArrowBooleanArray](g_object_ref(arr.toPtr))

# Constructor for BooleanArray from sequence
proc newBooleanArray*(values: sink seq[bool]): BooleanArray =
  let builder = newArrayBuilder[bool]()
  if len(values) != 0:
    builder.appendValues(values)
  let arr = builder.finish()
  result.handle = cast[ptr GArrowBooleanArray](g_object_ref(arr.toPtr))

# Constructor for BooleanArray from sequence with null mask
proc newBooleanArray*(values: sink seq[bool], mask: openArray[bool]): BooleanArray =
  let builder = newArrayBuilder[bool]()
  for i in 0 ..< values.len:
    if mask[i]:
      builder.appendNull()
    else:
      builder.append(values[i])
  let arr = builder.finish()
  result.handle = cast[ptr GArrowBooleanArray](g_object_ref(arr.toPtr))

# Constructor for BooleanArray with Options
proc newBooleanArray*(values: sink seq[Option[bool]]): BooleanArray =
  let builder = newArrayBuilder[bool]()
  for val in values:
    if val.isSome():
      builder.append(val.get())
    else:
      builder.appendNull()
  let arr = builder.finish()
  result.handle = cast[ptr GArrowBooleanArray](g_object_ref(arr.toPtr))

proc len*(arr: BooleanArray): int {.inline.} =
  if not isNil(arr.handle):
    result = garrow_array_get_length(cast[ptr GArrowArray](arr.handle))
  else:
    result = 0

proc `[]`*(arr: BooleanArray, i: int): bool =
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {arr.len}")
  let val = garrow_boolean_array_get_value(arr.handle, i)
  return val != 0

proc isNull*(arr: BooleanArray, i: int): bool =
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {arr.len}")
  return garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i) != 0

proc isValid*(arr: BooleanArray, i: int): bool =
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, fmt"index {i} not in 0 .. {arr.len}")
  return garrow_array_is_valid(cast[ptr GArrowArray](arr.handle), i) != 0

iterator items*(arr: BooleanArray): bool =
  for i in 0 ..< arr.len:
    yield arr[i]

proc toSeq*(arr: BooleanArray): seq[bool] =
  result = newSeq[bool](arr.len)
  for i in 0 ..< arr.len:
    result[i] = arr[i]

proc `@`*(arr: BooleanArray): seq[bool] =
  arr.toSeq

proc `$`*(arr: BooleanArray): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr)

# Filter implementations
proc filter*(
    table: ArrowTable, filter: BooleanArray, options: FilterOptions
): ArrowTable =
  let handle = check garrow_table_filter(table.toPtr, filter.handle, options.handle)
  result = newArrowTable(handle)

proc filter*(
    table: ArrowTable, filter: ChunkedArray[void], options: FilterOptions
): ArrowTable =
  let handle =
    check garrow_table_filter_chunked_array(table.toPtr, filter.toPtr, options.handle)
  result = newArrowTable(handle)

proc filter*[T](
    chunkedArray: ChunkedArray[T], filter: BooleanArray, options: FilterOptions
): ChunkedArray[T] =
  let handle =
    check garrow_chunked_array_filter(chunkedArray.toPtr, filter.handle, options.handle)
  result = newChunkedArray[T](handle)

proc filter*[T](
    chunkedArray: ChunkedArray[T], filter: ChunkedArray[bool], options: FilterOptions
): ChunkedArray[T] =
  let handle = check garrow_chunked_array_filter_chunked_array(
    chunkedArray.toPtr, filter.toPtr, options.handle
  )
  result = newChunkedArray[T](handle)

# Convenience overloads with default options
proc filter*(table: ArrowTable, filter: BooleanArray): ArrowTable =
  let options = newFilterOptions()
  result = table.filter(filter, options)

proc filter*(table: ArrowTable, filter: ChunkedArray[void]): ArrowTable =
  let options = newFilterOptions()
  result = table.filter(filter, options)

proc filter*[T](chunkedArray: ChunkedArray[T], filter: BooleanArray): ChunkedArray[T] =
  let options = newFilterOptions()
  result = chunkedArray.filter(filter, options)

proc filter*[T](
    chunkedArray: ChunkedArray[T], filter: ChunkedArray[bool]
): ChunkedArray[T] =
  let options = newFilterOptions()
  result = chunkedArray.filter(filter, options)

# Array filter support
proc filter*[T](arr: Array[T], filter: BooleanArray, options: FilterOptions): Array[T] =
  let handle = check garrow_array_filter(arr.toPtr, filter.handle, options.handle)
  result = newArray[T](handle)

proc filter*[T](arr: Array[T], filter: BooleanArray): Array[T] =
  let options = newFilterOptions()
  result = arr.filter(filter, options)
