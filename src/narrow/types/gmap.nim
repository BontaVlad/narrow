import ../core/[ffi, error, utils]
import ./gtypes

arcGObject:
  type MapType* = object
    handle*: ptr GArrowMapDataType

proc newMapType*(keyType, itemType: GADType): MapType =
  result.handle = garrow_map_data_type_new(keyType.handle, itemType.handle)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create MapType")
  discard g_object_ref_sink(result.handle)

proc keyType*(m: MapType): GADType =
  let handle = garrow_map_data_type_get_key_type(m.handle)
  if handle.isNil:
    raise newException(OperationError, "Failed to get key type from MapType")
  result = newGType(handle)

proc itemType*(m: MapType): GADType =
  let handle = garrow_map_data_type_get_item_type(m.handle)
  if handle.isNil:
    raise newException(OperationError, "Failed to get item type from MapType")
  result = newGType(handle)

proc toGADType*(m: MapType): GADType =
  if m.handle.isNil:
    raise newException(OperationError, "Cannot convert nil MapType to GADType")
  discard g_object_ref_sink(m.handle)
  result = GADType(handle: cast[ptr GArrowDataType](m.handle))

proc `$`*(m: MapType): string =
  if m.handle.isNil:
    return "MapType(nil)"
  let gadType = m.toGADType()
  let namePtr = garrow_data_type_get_name(gadType.handle)
  if isNil(namePtr):
    return "map<unknown, unknown>"
  result = $newGString(namePtr, owned = true)
