import ../core/[ffi, error, utils]
import ./gtypes
import ../column/metadata

arcGObject:
  type LargeListType* = object
    handle*: ptr GArrowLargeListDataType

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
