## Union types for heterogeneous nested data.
##
## `SparseUnionArray` and `DenseUnionArray` store values that can be one of
## several types, with a type code per element.
import ../core/[ffi, error, utils]
import ../types/[glist, gtypes]

# ============================================================================
# Union Data Types
# ============================================================================

arcGObject:
  type
    SparseUnionDataType* = object
      ## Data type for a sparse union: each child field has the full length.
      handle*: ptr GArrowSparseUnionDataType

    DenseUnionDataType* = object
      ## Data type for a dense union: child fields store only referenced values.
      handle*: ptr GArrowDenseUnionDataType

proc newSparseUnionDataType*(
    fields: openArray[ptr GArrowField], typeCodes: ptr int8, nTypeCodes: int
): SparseUnionDataType =
  let fieldList = newGList(@fields)
  result.handle = garrow_sparse_union_data_type_new(
    fieldList.list, cast[cstring](typeCodes), nTypeCodes.gsize
  )
  if isNil(result.handle):
    raise newException(OperationError, "Error creating sparse union data type")

proc newDenseUnionDataType*(
    fields: openArray[ptr GArrowField], typeCodes: ptr int8, nTypeCodes: int
): DenseUnionDataType =
  let fieldList = newGList(@fields)
  result.handle = garrow_dense_union_data_type_new(
    fieldList.list, cast[cstring](typeCodes), nTypeCodes.gsize
  )
  if isNil(result.handle):
    raise newException(OperationError, "Error creating dense union data type")

# ============================================================================
# Union Arrays
# ============================================================================

arcGObject:
  type
    SparseUnionArray* = object
      ## An array whose elements can be one of several types, stored sparsely.
      handle*: ptr GArrowSparseUnionArray

    DenseUnionArray* = object
      ## An array whose elements can be one of several types, stored densely with offsets.
      handle*: ptr GArrowDenseUnionArray

proc newSparseUnionArray*(
    typeIds: ptr GArrowInt8Array, fields: openArray[ptr GArrowArray]
): SparseUnionArray =
  let fieldList = newGList(@fields)
  result.handle = verify garrow_sparse_union_array_new(typeIds, fieldList.list)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating sparse union array")

proc newSparseUnionArray*(
    dt: SparseUnionDataType,
    typeIds: ptr GArrowInt8Array,
    fields: openArray[ptr GArrowArray],
): SparseUnionArray =
  let fieldList = newGList(@fields)
  result.handle =
    verify garrow_sparse_union_array_new_data_type(dt.handle, typeIds, fieldList.list)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating sparse union array")

proc newDenseUnionArray*(
    typeIds: ptr GArrowInt8Array,
    valueOffsets: ptr GArrowInt32Array,
    fields: openArray[ptr GArrowArray],
): DenseUnionArray =
  let fieldList = newGList(@fields)
  result.handle =
    verify garrow_dense_union_array_new(typeIds, valueOffsets, fieldList.list)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating dense union array")

proc newDenseUnionArray*(
    dt: DenseUnionDataType,
    typeIds: ptr GArrowInt8Array,
    valueOffsets: ptr GArrowInt32Array,
    fields: openArray[ptr GArrowArray],
): DenseUnionArray =
  let fieldList = newGList(@fields)
  result.handle = verify garrow_dense_union_array_new_data_type(
    dt.handle, typeIds, valueOffsets, fieldList.list
  )
  if isNil(result.handle):
    raise newException(OperationError, "Error creating dense union array")

proc getValueOffset*(arr: DenseUnionArray, i: int64): int32 =
  garrow_dense_union_array_get_value_offset(arr.handle, i)

# ============================================================================
# Union Scalars
# ============================================================================

arcGObject:
  type
    SparseUnionScalar* = object
      handle*: ptr GArrowSparseUnionScalar

    DenseUnionScalar* = object
      handle*: ptr GArrowDenseUnionScalar

proc newSparseUnionScalar*(
    dt: SparseUnionDataType, typeCode: int8, value: ptr GArrowScalar
): SparseUnionScalar =
  result.handle = garrow_sparse_union_scalar_new(dt.handle, typeCode, value)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating sparse union scalar")

proc newDenseUnionScalar*(
    dt: DenseUnionDataType, typeCode: int8, value: ptr GArrowScalar
): DenseUnionScalar =
  result.handle = garrow_dense_union_scalar_new(dt.handle, typeCode, value)
  if isNil(result.handle):
    raise newException(OperationError, "Error creating dense union scalar")

# ============================================================================
# Common Accessors
# ============================================================================

proc len*(arr: SparseUnionArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

proc len*(arr: DenseUnionArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

proc `$`*(arr: SparseUnionArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

proc `$`*(arr: DenseUnionArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)
