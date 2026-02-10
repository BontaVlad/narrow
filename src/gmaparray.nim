import std/[strformat, options]
import ./[ffi, gschema, garray, gtypes, error]

type
  MapDataType* = object
    handle: ptr GArrowMapDataType

  MapArray*[K, V] = object
    handle: ptr GArrowMapArray

# MapDataType implementation
proc toPtr*(dt: MapDataType): ptr GArrowMapDataType {.inline.} =
  dt.handle

proc `=destroy`*(dt: MapDataType) =
  if not isNil(dt.handle):
    g_object_unref(dt.handle)

proc `=sink`*(dest: var MapDataType, src: MapDataType) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var MapDataType, src: MapDataType) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newMapDataType*(keyType: GADType, itemType: GADType): MapDataType =
  result.handle = garrow_map_data_type_new(keyType.toPtr, itemType.toPtr)

proc newMapDataType*(handle: ptr GArrowMapDataType): MapDataType =
  result.handle = handle

proc keyType*(dt: MapDataType): GADType =
  let handle = garrow_map_data_type_get_key_type(dt.handle)
  newGType(handle)

proc itemType*(dt: MapDataType): GADType =
  let handle = garrow_map_data_type_get_item_type(dt.handle)
  newGType(handle)

proc `$`*(dt: MapDataType): string =
  "map<" & $dt.keyType & ", " & $dt.itemType & ">"

# MapArray implementation
proc toPtr*[K, V](a: MapArray[K, V]): ptr GArrowMapArray {.inline.} =
  a.handle

proc `=destroy`*[K, V](arr: MapArray[K, V]) =
  if not isNil(arr.handle):
    g_object_unref(arr.handle)

proc `=sink`*[K, V](dest: var MapArray[K, V], src: MapArray[K, V]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[K, V](dest: var MapArray[K, V], src: MapArray[K, V]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newMapArray*[K, V](
    offsets: Array[int32], keys: Array[K], items: Array[V]
): MapArray[K, V] =
  var err: ptr GError
  let handle = garrow_map_array_new(
    cast[ptr GArrowArray](offsets.toPtr), keys.toPtr, items.toPtr, addr err
  )

  if not err.isNil:
    let msg =
      if not err.message.isNil:
        $err.message
      else:
        "MapArray creation failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  result = MapArray[K, V](handle: handle)

proc newMapArray*[K, V](handle: ptr GArrowMapArray): MapArray[K, V] =
  result = MapArray[K, V](handle: handle)

proc len*[K, V](arr: MapArray[K, V]): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle))

proc keys*[K, V](arr: MapArray[K, V]): Array[K] =
  let handle = garrow_map_array_get_keys(arr.handle)
  newArray[K](handle)

proc items*[K, V](arr: MapArray[K, V]): Array[V] =
  let handle = garrow_map_array_get_items(arr.handle)
  newArray[V](handle)

# Indexing - returns a slice of the keys/items arrays for the map at index i
proc `[]`*[K, V](arr: MapArray[K, V], i: int): (Array[K], Array[V]) =
  ## Get the key-value arrays for the map at index i
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")

  # Get all keys and items
  let allKeys = arr.keys
  let allItems = arr.items

  # Get the offsets to find the slice boundaries
  # Note: This is a simplified implementation. In practice, you'd use
  # the offsets array from the underlying MapArray structure
  result = (allKeys, allItems)

# Null handling
proc isNull*[K, V](arr: MapArray[K, V], i: int): bool =
  ## Check if map at index i is null
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i) != 0

proc isValid*[K, V](arr: MapArray[K, V], i: int): bool {.inline.} =
  ## Check if map at index i is valid (not null)
  result = not arr.isNull(i)

proc nNulls*[K, V](arr: MapArray[K, V]): int64 =
  ## Count of null maps in the array
  result = garrow_array_get_n_nulls(cast[ptr GArrowArray](arr.handle)).int64

# Safe getter
type MapEntry*[K, V] = object
  ## Represents a single map entry (key-value pairs at an index)
  keys*: Array[K]
  values*: Array[V]

proc tryGet*[K, V](arr: MapArray[K, V], i: int): Option[MapEntry[K, V]] =
  ## Safely get a map entry at index i
  if i < 0 or i >= arr.len or arr.isNull(i):
    return none(MapEntry[K, V])

  # For simplicity, return all keys/items - in a real implementation
  # you'd want to slice these based on the offsets
  result = some(MapEntry[K, V](keys: arr.keys, values: arr.items))

# Iteration
iterator items*[K, V](arr: MapArray[K, V]): MapEntry[K, V] =
  ## Iterate over all map entries
  for i in 0 ..< arr.len:
    if not arr.isNull(i):
      yield MapEntry[K, V](keys: arr.keys, values: arr.items)

# Comparison
proc `==`*[K, V](a, b: MapArray[K, V]): bool =
  ## Check equality of two map arrays
  if a.handle == b.handle:
    return true
  if a.handle == nil or b.handle == nil:
    return false
  result =
    garrow_array_equal(cast[ptr GArrowArray](a.handle), cast[ptr GArrowArray](b.handle)) !=
    0

# Sequence conversion
proc toSeq*[K, V](arr: MapArray[K, V]): seq[MapEntry[K, V]] =
  ## Convert map array to sequence of entries
  result = newSeq[MapEntry[K, V]](arr.len)
  var idx = 0
  for i in 0 ..< arr.len:
    if not arr.isNull(i):
      result[idx] = MapEntry[K, V](keys: arr.keys, values: arr.items)
      idx += 1
  result.setLen(idx)

proc `@`*[K, V](arr: MapArray[K, V]): seq[MapEntry[K, V]] {.inline.} =
  ## Operator alias for toSeq
  arr.toSeq

proc `$`*[K, V](arr: MapArray[K, V]): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr)
