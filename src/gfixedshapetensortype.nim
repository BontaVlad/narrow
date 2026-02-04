import ./[ffi, gtypes, error]

type
  FixedShapeTensorType* = object
    handle*: ptr GArrowFixedShapeTensorDataType

proc toPtr*(t: FixedShapeTensorType): ptr GArrowFixedShapeTensorDataType {.inline.} =
  t.handle

proc `=destroy`*(t: FixedShapeTensorType) =
  if not isNil(t.handle):
    g_object_unref(t.handle)

proc `=sink`*(dest: var FixedShapeTensorType, src: FixedShapeTensorType) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FixedShapeTensorType, src: FixedShapeTensorType) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newFixedShapeTensorType*(
    valueType: GADType,
    shape: openArray[int64],
    permutation: openArray[int64] = [],
    dimNames: seq[string] = @[],
): FixedShapeTensorType =
  var err: ptr GError
  
  # Convert shape array
  var shapePtr: ptr gint64 = nil
  var shapeLen: gsize = 0
  if shape.len > 0:
    shapePtr = cast[ptr gint64](addr shape[0])
    shapeLen = shape.len.gsize
  
  # Convert permutation array
  var permPtr: ptr gint64 = nil
  var permLen: gsize = 0
  if permutation.len > 0:
    permPtr = cast[ptr gint64](addr permutation[0])
    permLen = permutation.len.gsize
  
  # Convert dimension names
  var dimNamesPtr: ptr cstring = nil
  var nDimNames: gsize = 0
  if dimNames.len > 0:
    # Create array of cstring
    var cstrings = newSeq[cstring](dimNames.len)
    for i, name in dimNames:
      cstrings[i] = name.cstring
    dimNamesPtr = cast[ptr cstring](addr cstrings[0])
    nDimNames = dimNames.len.gsize
  
  result.handle = garrow_fixed_shape_tensor_data_type_new(
    valueType.handle,
    shapePtr,
    shapeLen,
    permPtr,
    permLen,
    dimNamesPtr,
    nDimNames,
    addr err
  )
  
  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Failed to create FixedShapeTensorType"
    g_error_free(err)
    raise newException(OperationError, msg)
  
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create FixedShapeTensorType")

proc shape*(t: FixedShapeTensorType): seq[int64] =
  var len: gsize
  let shapePtr = garrow_fixed_shape_tensor_data_type_get_shape(t.handle, addr len)
  if shapePtr.isNil or len == 0:
    return @[]
  result = newSeq[int64](len.int)
  let int64Ptr = cast[ptr UncheckedArray[int64]](shapePtr)
  for i in 0 ..< len.int:
    result[i] = int64Ptr[i]

proc permutation*(t: FixedShapeTensorType): seq[int64] =
  var len: gsize
  let permPtr = garrow_fixed_shape_tensor_data_type_get_permutation(t.handle, addr len)
  if permPtr.isNil or len == 0:
    return @[]
  result = newSeq[int64](len.int)
  let int64Ptr = cast[ptr UncheckedArray[int64]](permPtr)
  for i in 0 ..< len.int:
    result[i] = int64Ptr[i]

proc dimNames*(t: FixedShapeTensorType): seq[string] =
  let namesPtr = garrow_fixed_shape_tensor_data_type_get_dim_names(t.handle)
  if namesPtr.isNil:
    return @[]
  # This returns a null-terminated array of strings
  var i = 0
  while true:
    let strPtr = cast[ptr cstring](cast[int](namesPtr) + i * sizeof(cstring))
    if strPtr.isNil or strPtr[] == nil:
      break
    result.add($strPtr[])
    i += 1

proc strides*(t: FixedShapeTensorType): seq[int64] =
  var len: gsize
  let stridesPtr = garrow_fixed_shape_tensor_data_type_get_strides(t.handle, addr len)
  if stridesPtr.isNil or len == 0:
    return @[]
  result = newSeq[int64](len.int)
  let int64Ptr = cast[ptr UncheckedArray[int64]](stridesPtr)
  for i in 0 ..< len.int:
    result[i] = int64Ptr[i]

proc extensionName*(t: FixedShapeTensorType): string =
  let namePtr = garrow_extension_data_type_get_extension_name(
    cast[ptr GArrowExtensionDataType](t.handle)
  )
  if isNil(namePtr):
    return ""
  result = $namePtr

proc toGADType*(t: FixedShapeTensorType): GADType =
  if t.handle.isNil:
    raise newException(OperationError, "Cannot convert nil FixedShapeTensorType to GADType")
  discard g_object_ref(t.handle)
  result = GADType(handle: cast[ptr GArrowDataType](t.handle))

proc `$`*(t: FixedShapeTensorType): string =
  if t.handle.isNil:
    return "FixedShapeTensorType(nil)"
  let gadType = t.toGADType()
  let namePtr = garrow_data_type_get_name(gadType.handle)
  if isNil(namePtr):
    return "fixed_shape_tensor"
  result = $newGString(namePtr)
