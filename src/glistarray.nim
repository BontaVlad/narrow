import std/[options]
import ./[ffi, gtypes, error, garray]

type ListArray*[T] = object
  handle: ptr GArrowListArray

proc toPtr*[T](arr: ListArray[T]): ptr GArrowListArray {.inline.} =
  arr.handle

proc `=destroy`*[T](arr: ListArray[T]) =
  if not isNil(arr.handle):
    g_object_unref(arr.handle)

proc `=sink`*[T](dest: var ListArray[T], src: ListArray[T]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[T](dest: var ListArray[T], src: ListArray[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newListArray*[T](handle: ptr GArrowListArray): ListArray[T] =
  result.handle = handle

proc len*[T](arr: ListArray[T]): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle))

proc valueAt*[T](arr: ListArray[T], idx: int): Array[T] =
  ## Get the array of values at the given index
  if idx < 0 or idx >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  let handle = garrow_list_array_get_value(arr.handle, idx.gint64)
  if handle.isNil:
    raise newException(OperationError, "Failed to get value at index " & $idx)
  result = newArray[T](handle)

proc valueLength*[T](arr: ListArray[T], idx: int): int32 =
  ## Get the length of the list at the given index
  if idx < 0 or idx >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_list_array_get_value_length(arr.handle, idx.gint64)

proc valueOffset*[T](arr: ListArray[T], idx: int): int32 =
  ## Get the offset of the list at the given index
  if idx < 0 or idx >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_list_array_get_value_offset(arr.handle, idx.gint64)

proc isNull*[T](arr: ListArray[T], idx: int): bool =
  ## Check if the value at the given index is null
  if idx < 0 or idx >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_array_is_null(cast[ptr GArrowArray](arr.handle), idx) != 0

proc isValid*[T](arr: ListArray[T], idx: int): bool {.inline.} =
  ## Check if the value at the given index is valid (not null)
  result = not arr.isNull(idx)

proc nNulls*[T](arr: ListArray[T]): int64 =
  ## Count of null values in the array
  result = garrow_array_get_n_nulls(cast[ptr GArrowArray](arr.handle)).int64

proc `$`*[T](arr: ListArray[T]): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr)

# Iteration support
type ListValue*[T] = object ## Represents a single list value (an array of T)
  array*: ListArray[T]
  index*: int

proc len*[T](lv: ListValue[T]): int {.inline.} =
  ## Number of elements in this list value
  lv.array.valueLength(lv.index).int

proc `[]`*[T](lv: ListValue[T], idx: int): T =
  ## Get element at idx within this list value
  let arr = lv.array.valueAt(lv.index)
  result = arr[idx]

proc tryGet*[T](arr: ListArray[T], idx: int): Option[ListValue[T]] =
  ## Safely get a list value at index
  if idx < 0 or idx >= arr.len or arr.isNull(idx):
    return none(ListValue[T])
  result = some(ListValue[T](array: arr, index: idx))

iterator items*[T](arr: ListArray[T]): ListValue[T] =
  ## Iterate over all list values
  for i in 0 ..< arr.len:
    if not arr.isNull(i):
      yield ListValue[T](array: arr, index: i)

proc toSeq*[T](arr: ListArray[T]): seq[seq[T]] =
  ## Convert to sequence of sequences
  result = newSeq[seq[T]](arr.len)
  for i in 0 ..< arr.len:
    if arr.isNull(i):
      result[i] = @[]
    else:
      let innerArr = arr.valueAt(i)
      result[i] = @innerArr

proc `@`*[T](arr: ListArray[T]): seq[seq[T]] {.inline.} =
  ## Operator alias for toSeq
  arr.toSeq

proc `==`*[T](a, b: ListArray[T]): bool =
  ## Check equality of two list arrays
  if a.handle == b.handle:
    return true
  if a.handle == nil or b.handle == nil:
    return false
  result =
    garrow_array_equal(cast[ptr GArrowArray](a.handle), cast[ptr GArrowArray](b.handle)) !=
    0
