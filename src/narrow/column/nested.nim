import std/[options, strformat, macros, strutils, sequtils]
import ../core/[ffi, error]
import ../types/[gtypes, glist]
import ./[primitive, metadata]

# ============================================================================
# ListArray
# ============================================================================

type ListArray*[T] = object
  handle: ptr GArrowListArray

proc toPtr*[T](arr: ListArray[T]): ptr GArrowListArray {.inline.} =
  arr.handle

proc `=destroy`*[T](arr: ListArray[T]) =
  if not isNil(arr.handle):
    g_object_unref(arr.handle)

proc `=sink`*[T](dest: var ListArray[T], src: ListArray[T]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[T](dest: var ListArray[T], src: ListArray[T]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newListArray*[T](handle: ptr GArrowListArray): ListArray[T] =
  result.handle = handle

proc len*[T](arr: ListArray[T]): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle))

proc valueAt*[T](arr: ListArray[T], idx: int): Array[T] =
  ## Get the array of values at the given index
  if idx < 0 or idx >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  let handle = garrow_list_array_get_value(arr.handle, idx.gint64)
  if handle.isNil:
    raise newException(OperationError, "Failed to get value at index " & $idx)
  result = newArray[T](handle)

proc valueLength*[T](arr: ListArray[T], idx: int): int32 =
  ## Get the length of the list at the given index
  if idx < 0 or idx >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_list_array_get_value_length(arr.handle, idx.gint64)

proc valueOffset*[T](arr: ListArray[T], idx: int): int32 =
  ## Get the offset of the list at the given index
  if idx < 0 or idx >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_list_array_get_value_offset(arr.handle, idx.gint64)

proc isNull*[T](arr: ListArray[T], idx: int): bool =
  ## Check if the value at the given index is null
  if idx < 0 or idx >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_array_is_null(cast[ptr GArrowArray](arr.handle), idx) != 0

proc isValid*[T](arr: ListArray[T], idx: int): bool {.inline.} =
  ## Check if the value at the given index is valid (not null)
  result = not arr.isNull(idx)

proc nNulls*[T](arr: ListArray[T]): int64 =
  ## Count of null values in the array
  result = garrow_array_get_n_nulls(cast[ptr GArrowArray](arr.handle)).int64

proc `$`*[T](arr: ListArray[T]): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr)

# ListValue helper type
type ListValue*[T] = object ## Represents a single list value (an array of T)
  array*: ListArray[T]
  index*: int

proc len*[T](lv: ListValue[T]): int {.inline.} =
  ## Number of elements in this list value
  lv.array.valueLength(lv.index).int

proc `[]`*[T](lv: ListValue[T], idx: int): T =
  ## Get element at idx within this list value
  let arr = lv.array.valueAt(lv.index)
  result = arr[idx]

proc tryGet*[T](arr: ListArray[T], idx: int): Option[ListValue[T]] =
  ## Safely get a list value at index
  if idx < 0 or idx >= arr.len or arr.isNull(idx):
    return none(ListValue[T])
  result = some(ListValue[T](array: arr, index: idx))

iterator items*[T](arr: ListArray[T]): ListValue[T] =
  ## Iterate over all list values
  for i in 0 ..< arr.len:
    if not arr.isNull(i):
      yield ListValue[T](array: arr, index: i)

proc toSeq*[T](arr: ListArray[T]): seq[seq[T]] =
  ## Convert to sequence of sequences
  result = newSeq[seq[T]](arr.len)
  for i in 0 ..< arr.len:
    if arr.isNull(i):
      result[i] = @[]
    else:
      let innerArr = arr.valueAt(i)
      result[i] = @innerArr

proc `@`*[T](arr: ListArray[T]): seq[seq[T]] {.inline.} =
  ## Operator alias for toSeq
  arr.toSeq

proc `==`*[T](a, b: ListArray[T]): bool =
  ## Check equality of two list arrays
  if a.handle == b.handle:
    return true
  if a.handle == nil or b.handle == nil:
    return false
  result =
    garrow_array_equal(cast[ptr GArrowArray](a.handle), cast[ptr GArrowArray](b.handle)) !=
    0

# ============================================================================
# MapArray and MapDataType
# ============================================================================

type
  MapDataType* = object
    handle: ptr GArrowMapDataType

  MapArray*[K, V] = object
    handle: ptr GArrowMapArray

proc toPtr*(dt: MapDataType): ptr GArrowMapDataType {.inline.} =
  dt.handle

proc `=destroy`*(dt: MapDataType) =
  if not isNil(dt.handle):
    g_object_unref(dt.handle)

proc `=sink`*(dest: var MapDataType, src: MapDataType) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var MapDataType, src: MapDataType) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newMapDataType*(keyType: GADType, itemType: GADType): MapDataType =
  result.handle = garrow_map_data_type_new(keyType.toPtr, itemType.toPtr)

proc newMapDataType*(handle: ptr GArrowMapDataType): MapDataType =
  result.handle = handle

proc keyType*(dt: MapDataType): GADType =
  let handle = garrow_map_data_type_get_key_type(dt.handle)
  newGType(handle)

proc itemType*(dt: MapDataType): GADType =
  let handle = garrow_map_data_type_get_item_type(dt.handle)
  newGType(handle)

proc `$`*(dt: MapDataType): string =
  "map<" & $dt.keyType & ", " & $dt.itemType & ">"

proc toPtr*[K, V](a: MapArray[K, V]): ptr GArrowMapArray {.inline.} =
  a.handle

proc `=destroy`*[K, V](arr: MapArray[K, V]) =
  if not isNil(arr.handle):
    g_object_unref(arr.handle)

proc `=sink`*[K, V](dest: var MapArray[K, V], src: MapArray[K, V]) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*[K, V](dest: var MapArray[K, V], src: MapArray[K, V]) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc newMapArray*[K, V](
    offsets: Array[int32], keys: Array[K], items: Array[V]
): MapArray[K, V] =
  var err: ptr GError
  let handle = garrow_map_array_new(
    cast[ptr GArrowArray](offsets.toPtr), keys.toPtr, items.toPtr, addr err
  )

  if not err.isNil:
    let msg =
      if not err.message.isNil:
        $err.message
      else:
        "MapArray creation failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  result = MapArray[K, V](handle: handle)

proc newMapArray*[K, V](handle: ptr GArrowMapArray): MapArray[K, V] =
  result = MapArray[K, V](handle: handle)

proc len*[K, V](arr: MapArray[K, V]): int =
  garrow_array_get_length(cast[ptr GArrowArray](arr.handle))

proc keys*[K, V](arr: MapArray[K, V]): Array[K] =
  let handle = garrow_map_array_get_keys(arr.handle)
  newArray[K](handle)

proc items*[K, V](arr: MapArray[K, V]): Array[V] =
  let handle = garrow_map_array_get_items(arr.handle)
  newArray[V](handle)

proc `[]`*[K, V](arr: MapArray[K, V], i: int): (Array[K], Array[V]) =
  ## Get the key-value arrays for the map at index i
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")

  let allKeys = arr.keys
  let allItems = arr.items
  result = (allKeys, allItems)

proc isNull*[K, V](arr: MapArray[K, V], i: int): bool =
  ## Check if map at index i is null
  if i < 0 or i >= arr.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_array_is_null(cast[ptr GArrowArray](arr.handle), i) != 0

proc isValid*[K, V](arr: MapArray[K, V], i: int): bool {.inline.} =
  ## Check if map at index i is valid (not null)
  result = not arr.isNull(i)

proc nNulls*[K, V](arr: MapArray[K, V]): int64 =
  ## Count of null maps in the array
  result = garrow_array_get_n_nulls(cast[ptr GArrowArray](arr.handle)).int64

type MapEntry*[K, V] = object
  ## Represents a single map entry (key-value pairs at an index)
  keys*: Array[K]
  values*: Array[V]

proc tryGet*[K, V](arr: MapArray[K, V], i: int): Option[MapEntry[K, V]] =
  ## Safely get a map entry at index i
  if i < 0 or i >= arr.len or arr.isNull(i):
    return none(MapEntry[K, V])
  result = some(MapEntry[K, V](keys: arr.keys, values: arr.items))

iterator items*[K, V](arr: MapArray[K, V]): MapEntry[K, V] =
  ## Iterate over all map entries
  for i in 0 ..< arr.len:
    if not arr.isNull(i):
      yield MapEntry[K, V](keys: arr.keys, values: arr.items)

proc `==`*[K, V](a, b: MapArray[K, V]): bool =
  ## Check equality of two map arrays
  if a.handle == b.handle:
    return true
  if a.handle == nil or b.handle == nil:
    return false
  result =
    garrow_array_equal(cast[ptr GArrowArray](a.handle), cast[ptr GArrowArray](b.handle)) !=
    0

proc toSeq*[K, V](arr: MapArray[K, V]): seq[MapEntry[K, V]] =
  ## Convert map array to sequence of entries
  result = newSeq[MapEntry[K, V]](arr.len)
  var idx = 0
  for i in 0 ..< arr.len:
    if not arr.isNull(i):
      result[idx] = MapEntry[K, V](keys: arr.keys, values: arr.items)
      idx += 1
  result.setLen(idx)

proc `@`*[K, V](arr: MapArray[K, V]): seq[MapEntry[K, V]] {.inline.} =
  ## Operator alias for toSeq
  arr.toSeq

proc `$`*[K, V](arr: MapArray[K, V]): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](arr.handle))
  result = $newGString(cStr)

# ============================================================================
# Struct, StructArray, and StructBuilder
# ============================================================================

{.experimental: "dotOperators".}

type
  Struct = object
    handle*: ptr GArrowStructDataType

  StructArray* = object
    handle: ptr GArrowStructArray

  StructBuilder* = object
    handle: ptr GArrowStructArrayBuilder

proc toPtr*(s: Struct): ptr GArrowStructDataType {.inline.} =
  s.handle

proc toPtr*(sa: StructArray): ptr GArrowStructArray {.inline.} =
  sa.handle

proc toPtr*(sb: StructBuilder): ptr GArrowStructArrayBuilder {.inline.} =
  sb.handle

# Memory management for Struct
proc `=destroy`*(s: Struct) =
  if not isNil(s.toPtr):
    g_object_unref(s.toPtr)

proc `=sink`*(dest: var Struct, src: Struct) =
  if not isNil(dest.toPtr) and dest.toPtr != src.toPtr:
    g_object_unref(dest.toPtr)
  dest.handle = src.handle

proc `=copy`*(dest: var Struct, src: Struct) =
  if dest.toPtr != src.toPtr:
    if not isNil(dest.toPtr):
      g_object_unref(dest.toPtr)
    dest.handle = src.handle
    if not isNil(dest.toPtr):
      discard g_object_ref(dest.toPtr)

# Memory management for StructArray
proc `=destroy`*(sa: StructArray) =
  if not isNil(sa.toPtr):
    g_object_unref(sa.toPtr)

proc `=sink`*(dest: var StructArray, src: StructArray) =
  if not isNil(dest.toPtr) and dest.toPtr != src.toPtr:
    g_object_unref(dest.toPtr)
  dest.handle = src.handle

proc `=copy`*(dest: var StructArray, src: StructArray) =
  if dest.toPtr != src.toPtr:
    if not isNil(dest.toPtr):
      g_object_unref(dest.toPtr)
    dest.handle = src.handle
    if not isNil(dest.toPtr):
      discard g_object_ref(dest.toPtr)

# Memory management for StructBuilder
proc `=destroy`*(sb: StructBuilder) =
  if not isNil(sb.toPtr):
    g_object_unref(sb.toPtr)

proc `=sink`*(dest: var StructBuilder, src: StructBuilder) =
  if not isNil(dest.toPtr) and dest.toPtr != src.toPtr:
    g_object_unref(dest.toPtr)
  dest.handle = src.handle

proc `=copy`*(dest: var StructBuilder, src: StructBuilder) =
  if dest.toPtr != src.toPtr:
    if not isNil(dest.toPtr):
      g_object_unref(dest.toPtr)
    dest.handle = src.handle
    if not isNil(dest.toPtr):
      discard g_object_ref(dest.toPtr)

# Struct creators
proc newStruct*(fields: GAList[ptr GArrowField]): Struct =
  result.handle = garrow_struct_data_type_new(fields.toPtr)

proc newStruct*(fields: openArray[Field]): Struct =
  var gFields = newGList[ptr GArrowField]()
  for f in fields:
    gFields.append(f.toPtr)
  newStruct(gFields)

# StructArray creators
proc newStructArray*(
    structType: Struct, fields: varargs[ptr GArrowArray]
): StructArray =
  if fields.len == 0:
    raise newException(ValueError, "Cannot create struct array with no fields")
  let length = garrow_array_get_length(fields[0]).gint64
  var fieldList = newGList[ptr GArrowArray]()
  for f in fields:
    fieldList.append(f)
  result.handle = garrow_struct_array_new(
    cast[ptr GArrowDataType](structType.toPtr), length, fieldList.toPtr, nil, 0
  )
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create StructArray")

# StructBuilder creators
proc newStructBuilder*(structType: Struct): StructBuilder =
  let handle = check garrow_struct_array_builder_new(structType.toPtr)
  if handle.isNil:
    raise newException(OperationError, "Failed to create StructArrayBuilder")
  result.handle = handle

# Struct field access
proc fields*(s: Struct): seq[Field] =
  let gfields = newGList[ptr GArrowField](garrow_struct_data_type_get_fields(s.toPtr))
  for f in gfields:
    result.add(newField(f))

proc `[]`*(s: Struct, name: string): Field =
  let handle = garrow_struct_data_type_get_field_by_name(s.toPtr, name.cstring)
  if handle.isNil:
    raise newException(KeyError, "Field '" & name & "' not found in struct")
  newField(handle)

proc `[]`*(s: Struct, idx: int): Field =
  if idx < 0:
    raise newException(IndexDefect, "Field index cannot be negative")
  let flds = s.fields
  if idx >= flds.len:
    raise newException(IndexDefect, "Field index " & $idx & " out of bounds")
  flds[idx]

macro `.`*(s: Struct, name: untyped): untyped =
  let nameStr = newStrLitNode(name.strVal)
  result = quote:
    `s`[`nameStr`]

proc hasField*(s: Struct, name: string): bool =
  let fieldHandle = garrow_struct_data_type_get_field_by_name(s.toPtr, name.cstring)
  if fieldHandle.isNil:
    return false
  else:
    g_object_unref(fieldHandle)
    return true

proc fieldIndex*(s: Struct, name: string): int =
  let flds = s.fields
  for i, f in flds:
    if f.name == name:
      return i
  return -1

proc fieldCount*(s: Struct): int =
  s.fields.len

# StructArray operations
proc len*(sa: StructArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](sa.toPtr))

proc structType*(sa: StructArray): Struct =
  let dataType = garrow_array_get_value_data_type(cast[ptr GArrowArray](sa.toPtr))
  result.handle = cast[ptr GArrowStructDataType](dataType)

proc fields*(sa: StructArray): seq[Field] =
  result = sa.structType.fields

proc fieldCount*(sa: StructArray): int {.inline.} =
  result = sa.fields.len

proc fieldIndex*(sa: StructArray, name: string): int {.inline.} =
  result = sa.structType.fieldIndex(name)

proc `[]`*(sa: StructArray, idx: int): StructArray =
  if idx < 0 or idx >= sa.len:
    raise newException(IndexDefect, "Index out of bounds")
  result.handle = cast[ptr GArrowStructArray](garrow_array_slice(
    cast[ptr GArrowArray](sa.toPtr), idx.gint64, 1
  ))

proc getField*[T](sa: StructArray, idx: int): Array[T] =
  if idx < 0:
    raise newException(IndexDefect, "Field index cannot be negative")
  let handle = garrow_struct_array_get_field(sa.toPtr, idx.gint)
  if handle.isNil:
    raise newException(KeyError, "Field index " & $idx & " not found")
  result = newArray[T](handle)

# Null handling
proc isNull*(sa: StructArray, i: int): bool =
  if i < 0 or i >= sa.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_array_is_null(cast[ptr GArrowArray](sa.toPtr), i) != 0

proc isValid*(sa: StructArray, i: int): bool {.inline.} =
  result = not sa.isNull(i)

proc nNulls*(sa: StructArray): int64 =
  result = garrow_array_get_n_nulls(cast[ptr GArrowArray](sa.toPtr)).int64

type StructRow* = object ## Represents a single struct row
  array*: StructArray
  index*: int

proc tryGet*(sa: StructArray, i: int): Option[StructRow] =
  if i < 0 or i >= sa.len or sa.isNull(i):
    return none(StructRow)
  result = some(StructRow(array: sa, index: i))

iterator items*(sa: StructArray): StructRow =
  for i in 0 ..< sa.len:
    if not sa.isNull(i):
      yield StructRow(array: sa, index: i)

proc `==`*(a, b: StructArray): bool =
  if a.handle == b.handle:
    return true
  if a.handle == nil or b.handle == nil:
    return false
  result =
    garrow_array_equal(cast[ptr GArrowArray](a.toPtr), cast[ptr GArrowArray](b.toPtr)) !=
    0

proc toSeq*(sa: StructArray): seq[StructRow] =
  result = newSeq[StructRow](sa.len)
  var idx = 0
  for i in 0 ..< sa.len:
    if not sa.isNull(i):
      result[idx] = StructRow(array: sa, index: i)
      idx += 1
  result.setLen(idx)

proc `@`*(sa: StructArray): seq[StructRow] {.inline.} =
  sa.toSeq

# StructRow helper methods
proc len*(row: StructRow): int {.inline.} =
  row.array.fieldCount

proc getField*[T](row: StructRow, idx: int): T =
  let fieldArray = row.array.getField[T](idx)
  result = fieldArray[row.index]

proc getField*[T](row: StructRow, name: string): T =
  let idx = row.array.fieldIndex(name)
  if idx < 0:
    raise newException(KeyError, "Field '" & name & "' not found in struct")
  result = row.getField[T](idx)

proc `$`*(row: StructRow): string =
  result = "StructRow(" & $row.index & "): {"
  let nFields = row.array.fieldCount
  let structFields = row.array.fields
  for i in 0 ..< nFields:
    if i > 0:
      result &= ", "
    if i < structFields.len:
      result &= structFields[i].name & ": ?"
    else:
      result &= "field" & $i & ": ?"
  result &= "}"

# StructBuilder operations
proc append*(sb: StructBuilder) =
  check garrow_struct_array_builder_append(sb.toPtr)

proc appendNull*(sb: StructBuilder) =
  check garrow_struct_array_builder_append_null(sb.toPtr)

proc finish*(sb: StructBuilder): StructArray =
  let handle = check garrow_array_builder_finish(cast[ptr GArrowArrayBuilder](sb.toPtr))
  result.handle = cast[ptr GArrowStructArray](handle)

# String representation
proc `$`*(s: Struct): string =
  let flds = s.fields
  if flds.len <= 3:
    "{ " & flds.mapIt($it).join(", ") & " }"
  else:
    "{\n" & flds.mapIt("  " & $it).join(",\n") & "\n}"

proc `$`*(sa: StructArray): string =
  let cStr = check garrow_array_to_string(cast[ptr GArrowArray](sa.toPtr))
  result = $newGString(cStr)
