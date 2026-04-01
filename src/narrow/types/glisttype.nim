import ../core/[ffi, error, utils]
import ./gtypes
import ../column/metadata

arcGObject:
  type ListType* = object
    handle*: ptr GArrowListDataType

proc newListType*(valueField: Field): ListType =
  result.handle = garrow_list_data_type_new(valueField.toPtr)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create ListType")

proc valueField*(l: ListType): Field =
  let handle = garrow_list_data_type_get_field(l.handle)
  if handle.isNil:
    raise newException(OperationError, "Failed to get value field from ListType")
  result = newField(handle)

proc toGADType*(l: ListType): GADType =
  if l.handle.isNil:
    raise newException(OperationError, "Cannot convert nil ListType to GADType")
  # Increment ref count since GADType will unref on destroy
  discard g_object_ref(l.handle)
  result = GADType(handle: cast[ptr GArrowDataType](l.handle))

proc `$`*(l: ListType): string =
  if l.handle.isNil:
    return "ListType(nil)"
  let gadType = l.toGADType()
  let namePtr = garrow_data_type_get_name(gadType.handle)
  if isNil(namePtr):
    return "list<unknown>"
  result = $newGString(namePtr)
