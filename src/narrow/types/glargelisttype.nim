import ../core/[ffi, error]
import ./gtypes
import ../column/metadata

type LargeListType* = object
  handle*: ptr GArrowLargeListDataType

proc toPtr*(l: LargeListType): ptr GArrowLargeListDataType {.inline.} =
  l.handle

proc `=destroy`*(l: LargeListType) =
  if not isNil(l.handle):
    g_object_unref(l.handle)

proc `=sink`*(dest: var LargeListType, src: LargeListType) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var LargeListType, src: LargeListType) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newLargeListType*(valueField: Field): LargeListType =
  result.handle = garrow_large_list_data_type_new(valueField.toPtr)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create LargeListType")

proc valueField*(l: LargeListType): Field =
  let handle = garrow_large_list_data_type_get_field(l.handle)
  if handle.isNil:
    raise newException(OperationError, "Failed to get value field from LargeListType")
  result = newField(handle)

proc toGADType*(l: LargeListType): GADType =
  if l.handle.isNil:
    raise newException(OperationError, "Cannot convert nil LargeListType to GADType")
  discard g_object_ref(l.handle)
  result = GADType(handle: cast[ptr GArrowDataType](l.handle))

proc `$`*(l: LargeListType): string =
  if l.handle.isNil:
    return "LargeListType(nil)"
  let gadType = l.toGADType()
  let namePtr = garrow_data_type_get_name(gadType.handle)
  if isNil(namePtr):
    return "large_list<unknown>"
  result = $newGString(namePtr)
