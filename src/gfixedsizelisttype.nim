import ./[ffi, gschema, gtypes, error]

type FixedSizeListType* = object
  handle*: ptr GArrowFixedSizeListDataType

proc toPtr*(f: FixedSizeListType): ptr GArrowFixedSizeListDataType {.inline.} =
  f.handle

proc `=destroy`*(f: FixedSizeListType) =
  if not isNil(f.handle):
    g_object_unref(f.handle)

proc `=sink`*(dest: var FixedSizeListType, src: FixedSizeListType) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FixedSizeListType, src: FixedSizeListType) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newFixedSizeListType*(valueType: GADType, listSize: int32): FixedSizeListType =
  result.handle =
    garrow_fixed_size_list_data_type_new_data_type(valueType.handle, listSize)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create FixedSizeListType")

proc newFixedSizeListType*(valueField: Field, listSize: int32): FixedSizeListType =
  result.handle = garrow_fixed_size_list_data_type_new_field(valueField.toPtr, listSize)
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create FixedSizeListType")

proc valueField*(f: FixedSizeListType): Field =
  let handle =
    garrow_base_list_data_type_get_field(cast[ptr GArrowBaseListDataType](f.handle))
  if handle.isNil:
    raise
      newException(OperationError, "Failed to get value field from FixedSizeListType")
  result = newField(handle)

proc toGADType*(f: FixedSizeListType): GADType =
  if f.handle.isNil:
    raise
      newException(OperationError, "Cannot convert nil FixedSizeListType to GADType")
  discard g_object_ref(f.handle)
  result = GADType(handle: cast[ptr GArrowDataType](f.handle))

proc `$`*(f: FixedSizeListType): string =
  if f.handle.isNil:
    return "FixedSizeListType(nil)"
  let gadType = f.toGADType()
  let namePtr = garrow_data_type_get_name(gadType.handle)
  if isNil(namePtr):
    return "fixed_size_list<unknown>"
  result = $newGString(namePtr)
