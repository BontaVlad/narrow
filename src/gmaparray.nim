import std/[strformat]
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

proc `$`*[K, V](arr: MapArray[K, V]): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr)
