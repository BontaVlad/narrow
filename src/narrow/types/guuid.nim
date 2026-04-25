import ../core/[ffi, error, utils]
import ./gtypes

arcGObject:
  type UUIDType* = object
    handle*: ptr GArrowUUIDDataType

proc newUUIDType*(): UUIDType =
  result.handle = verify garrow_uuid_data_type_new()

proc extensionName*(u: UUIDType): string =
  let namePtr = garrow_extension_data_type_get_extension_name(
    cast[ptr GArrowExtensionDataType](u.handle)
  )
  result = $newGstring(namePtr)

proc toGADType*(u: UUIDType): GADType =
  if u.handle.isNil:
    raise newException(OperationError, "Cannot convert nil UUIDType to GADType")
  discard g_object_ref(u.handle)
  result = GADType(handle: cast[ptr GArrowDataType](u.handle))

proc `$`*(u: UUIDType): string =
  if u.handle.isNil:
    return "UUIDType(nil)"
  let gadType = u.toGADType()
  let namePtr = garrow_data_type_get_name(gadType.handle)
  if isNil(namePtr):
    return "uuid"
  result = $newGString(namePtr)
