import ./[ffi, gtypes, error]

type UUIDType* = object
  handle*: ptr GArrowUUIDDataType

proc toPtr*(u: UUIDType): ptr GArrowUUIDDataType {.inline.} =
  u.handle

proc `=destroy`*(u: UUIDType) =
  if not isNil(u.handle):
    g_object_unref(u.handle)

proc `=sink`*(dest: var UUIDType, src: UUIDType) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var UUIDType, src: UUIDType) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newUUIDType*(): UUIDType =
  var err: ptr GError
  result.handle = garrow_uuid_data_type_new(addr err)
  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Failed to create UUIDType"
    g_error_free(err)
    raise newException(OperationError, msg)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create UUIDType")

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
