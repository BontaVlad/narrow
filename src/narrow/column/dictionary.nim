## Dictionary-encoded arrays for categorical data.
##
## `DictionaryArray` stores values as integer indices into a dictionary,
## reducing memory for repeated values. `DictionaryDataType` pairs an index
## type with a value type.
import ../core/[ffi, error, utils]
import ../types/gtypes
import ./primitive

# ============================================================================
# Dictionary Data Type
# ============================================================================

arcGObject:
  type DictionaryDataType* = object
    ## Pairs an index data type with a value data type for dictionary encoding.
    handle*: ptr GArrowDictionaryDataType

proc newDictionaryDataType*(
    indexDataType: GADType, valueDataType: GADType, ordered: bool = false
): DictionaryDataType =
  result.handle = garrow_dictionary_data_type_new(
    indexDataType.handle, valueDataType.handle, if ordered: 1.gboolean else: 0.gboolean
  )
  if isNil(result.handle):
    raise newException(OperationError, "Error creating dictionary data type")

proc indexDataType*(dt: DictionaryDataType): ptr GArrowDataType =
  garrow_dictionary_data_type_get_index_data_type(dt.handle)

proc valueDataType*(dt: DictionaryDataType): ptr GArrowDataType =
  garrow_dictionary_data_type_get_value_data_type(dt.handle)

proc isOrdered*(dt: DictionaryDataType): bool =
  garrow_dictionary_data_type_is_ordered(dt.handle) != 0

# ============================================================================
# Dictionary Array
# ============================================================================

arcGObject:
  type DictionaryArray* = object
    ## An array of integer indices into a dictionary, encoding categorical data.
    handle*: ptr GArrowDictionaryArray

proc newDictionaryArray*[T, U](
    dataType: DictionaryDataType, indices: Array[T], dictionary: Array[U]
): DictionaryArray =
  result.handle = verify garrow_dictionary_array_new(
    cast[ptr GArrowDataType](dataType.handle),
    cast[ptr GArrowArray](indices.toPtr),
    cast[ptr GArrowArray](dictionary.toPtr),
  )
  if isNil(result.handle):
    raise newException(OperationError, "Error creating dictionary array")

proc indices*(arr: DictionaryArray): ptr GArrowArray =
  garrow_dictionary_array_get_indices(arr.handle)

proc dictionary*(arr: DictionaryArray): ptr GArrowArray =
  garrow_dictionary_array_get_dictionary(arr.handle)

proc dictionaryDataType*(arr: DictionaryArray): ptr GArrowDictionaryDataType =
  garrow_dictionary_array_get_dictionary_data_type(arr.handle)

proc len*(arr: DictionaryArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle)).int

proc isNull*(arr: DictionaryArray, i: int): bool =
  garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i.gint64) != 0

proc `$`*(arr: DictionaryArray): string =
  let cStr = verify garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr, owned = true)

# ============================================================================
# Dictionary Encode Options
# ============================================================================

arcGObject:
  type DictionaryEncodeOptions* = object
    handle*: ptr GArrowDictionaryEncodeOptions

proc newDictionaryEncodeOptions*(): DictionaryEncodeOptions =
  result.handle = garrow_dictionary_encode_options_new()
  if isNil(result.handle):
    raise newException(OperationError, "Error creating dictionary encode options")
