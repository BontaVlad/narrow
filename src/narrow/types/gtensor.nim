import std/strformat
import ../core/[ffi, error, utils]
import ../types/gtypes
import ../column/buffer

# ============================================================================
# Tensor Type
# ============================================================================

arcGObject:
  type Tensor* = object
    handle*: ptr GArrowTensor

proc newTensor*(
    dataType: GADType,
    data: GBuffer,
    shape: openArray[int64],
    strides: openArray[int64] = [],
    dimNames: openArray[string] = [],
): Tensor =
  var cShape: seq[gint64]
  for s in shape:
    cShape.add(gint64(s))
  var cStrides: seq[gint64]
  for s in strides:
    cStrides.add(gint64(s))
  var cNames: seq[cstring]
  var cNameStorage: seq[string]
  for n in dimNames:
    cNameStorage.add(n)
    cNames.add(cstring(cNameStorage[^1]))

  result.handle = garrow_tensor_new(
    dataType.handle,
    data.handle,
    if cShape.len > 0:
      cast[ptr gint64](addr cShape[0])
    else:
      nil,
    cShape.len.gsize,
    if cStrides.len > 0:
      cast[ptr gint64](addr cStrides[0])
    else:
      nil,
    cStrides.len.gsize,
    if cNames.len > 0:
      cast[ptr cstring](addr cNames[0])
    else:
      nil,
    cNames.len.gsize,
  )
  if isNil(result.handle):
    raise newException(OperationError, "Error creating tensor")

proc nDimensions*(tensor: Tensor): int =
  garrow_tensor_get_n_dimensions(tensor.handle).int

proc shape*(tensor: Tensor): seq[int64] =
  var ndim: gint = 0
  let raw = garrow_tensor_get_shape(tensor.handle, addr ndim)
  result = newSeq[int64](ndim.int)
  let arr = cast[ptr UncheckedArray[gint64]](raw)
  for i in 0 ..< ndim.int:
    result[i] = arr[i].int64
  g_free(raw)

proc strides*(tensor: Tensor): seq[int64] =
  var nstride: gint = 0
  let raw = garrow_tensor_get_strides(tensor.handle, addr nstride)
  if raw == nil or nstride == 0:
    return @[]
  result = newSeq[int64](nstride.int)
  let arr = cast[ptr UncheckedArray[gint64]](raw)
  for i in 0 ..< nstride.int:
    result[i] = arr[i].int64
  g_free(raw)

proc dimensionName*(tensor: Tensor, i: int): string =
  if i < 0 or i >= tensor.nDimensions:
    raise newException(IndexDefect, fmt"dimension {i} out of 0..<{tensor.nDimensions}")
  result = $garrow_tensor_get_dimension_name(tensor.handle, i.gint)

proc size*(tensor: Tensor): int64 =
  garrow_tensor_get_size(tensor.handle).int64

proc isMutable*(tensor: Tensor): bool =
  garrow_tensor_is_mutable(tensor.handle) != 0

proc isContiguous*(tensor: Tensor): bool =
  garrow_tensor_is_contiguous(tensor.handle) != 0

proc isRowMajor*(tensor: Tensor): bool =
  garrow_tensor_is_row_major(tensor.handle) != 0

proc isColumnMajor*(tensor: Tensor): bool =
  garrow_tensor_is_column_major(tensor.handle) != 0

proc valueDataType*(tensor: Tensor): GADType =
  let handle = garrow_tensor_get_value_data_type(tensor.handle)
  result = newGType(handle)

proc valueType*(tensor: Tensor): GArrowType =
  garrow_tensor_get_value_type(tensor.handle)

proc buffer*(tensor: Tensor): GBuffer =
  result = GBuffer(handle: garrow_tensor_get_buffer(tensor.handle))

proc `==`*(a, b: Tensor): bool =
  garrow_tensor_equal(a.handle, b.handle) != 0
