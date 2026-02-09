import ./[ffi, gtypes, error]

type
  MapType* = object
    handle*: ptr GArrowMapDataType

proc toPtr*(m: MapType): ptr GArrowMapDataType {.inline.} =
  m.handle

proc `=destroy`*(m: MapType) =
  if not isNil(m.handle):
    g_object_unref(m.handle)

proc `=sink`*(dest: var MapType, src: MapType) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var MapType, src: MapType) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newMapType*(keyType, itemType: GADType): MapType =
  result.handle = garrow_map_data_type_new(keyType.handle, itemType.handle)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create MapType")

proc keyType*(m: MapType): GADType =
  let handle = garrow_map_data_type_get_key_type(m.handle)
  if handle.isNil:
    raise newException(OperationError, "Failed to get key type from MapType")
  result = newGType(handle)
  g_object_unref(handle)

proc itemType*(m: MapType): GADType =
  let handle = garrow_map_data_type_get_item_type(m.handle)
  if handle.isNil:
    raise newException(OperationError, "Failed to get item type from MapType")
  result = newGType(handle)
  g_object_unref(handle)

proc toGADType*(m: MapType): GADType =
  if m.handle.isNil:
    raise newException(OperationError, "Cannot convert nil MapType to GADType")
  # Increment ref count since GADType will unref on destroy
  discard g_object_ref(m.handle)
  result = GADType(handle: cast[ptr GArrowDataType](m.handle))

proc `$`*(m: MapType): string =
  if m.handle.isNil:
    return "MapType(nil)"
  let gadType = m.toGADType()
  let namePtr = garrow_data_type_get_name(gadType.handle)
  if isNil(namePtr):
    return "map<unknown, unknown>"
  result = $newGString(namePtr)
