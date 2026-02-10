import std/[macros, strutils, sequtils, options]
import ./[ffi, gschema, glist, gtypes, garray, error]

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

# StructArray creators - construct from individual arrays
proc newStructArray*(
    structType: Struct, fields: varargs[ptr GArrowArray]
): StructArray =
  if fields.len == 0:
    raise newException(ValueError, "Cannot create struct array with no fields")
  # Determine array length from first field
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

# Struct field access - define fields first
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
  # Get all fields and access by index
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
  ## Get the index of a field by name, or -1 if not found
  let flds = s.fields
  for i, f in flds:
    if f.name == name:
      return i
  return -1

proc fieldCount*(s: Struct): int =
  ## Get the number of fields in the struct
  s.fields.len

# StructArray operations
proc len*(sa: StructArray): int =
  garrow_array_get_length(cast[ptr GArrowArray](sa.toPtr))

proc structType*(sa: StructArray): Struct =
  ## Get the struct data type for this array
  let dataType = garrow_array_get_value_data_type(cast[ptr GArrowArray](sa.toPtr))
  result.handle = cast[ptr GArrowStructDataType](dataType)

proc fields*(sa: StructArray): seq[Field] =
  ## Get the fields of the struct
  result = sa.structType.fields

proc fieldCount*(sa: StructArray): int {.inline.} =
  ## Get the number of fields
  result = sa.fields.len

proc fieldIndex*(sa: StructArray, name: string): int {.inline.} =
  ## Get field index by name
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
  ## Check if struct at index i is null
  if i < 0 or i >= sa.len:
    raise newException(IndexDefect, "Index out of bounds")
  result = garrow_array_is_null(cast[ptr GArrowArray](sa.toPtr), i) != 0

proc isValid*(sa: StructArray, i: int): bool {.inline.} =
  ## Check if struct at index i is valid (not null)
  result = not sa.isNull(i)

proc nNulls*(sa: StructArray): int64 =
  ## Count of null structs in the array
  result = garrow_array_get_n_nulls(cast[ptr GArrowArray](sa.toPtr)).int64

# Safe getter
type StructRow* = object ## Represents a single struct row
  array*: StructArray
  index*: int

proc tryGet*(sa: StructArray, i: int): Option[StructRow] =
  ## Safely get a struct row at index i
  if i < 0 or i >= sa.len or sa.isNull(i):
    return none(StructRow)
  result = some(StructRow(array: sa, index: i))

# Iteration
iterator items*(sa: StructArray): StructRow =
  ## Iterate over all struct rows
  for i in 0 ..< sa.len:
    if not sa.isNull(i):
      yield StructRow(array: sa, index: i)

# Comparison
proc `==`*(a, b: StructArray): bool =
  ## Check equality of two struct arrays
  if a.handle == b.handle:
    return true
  if a.handle == nil or b.handle == nil:
    return false
  result =
    garrow_array_equal(cast[ptr GArrowArray](a.toPtr), cast[ptr GArrowArray](b.toPtr)) !=
    0

# Sequence conversion
proc toSeq*(sa: StructArray): seq[StructRow] =
  ## Convert struct array to sequence of rows
  result = newSeq[StructRow](sa.len)
  var idx = 0
  for i in 0 ..< sa.len:
    if not sa.isNull(i):
      result[idx] = StructRow(array: sa, index: i)
      idx += 1
  result.setLen(idx)

proc `@`*(sa: StructArray): seq[StructRow] {.inline.} =
  ## Operator alias for toSeq
  sa.toSeq

# StructRow helper methods
proc len*(row: StructRow): int {.inline.} =
  ## Number of fields in the struct row
  row.array.fieldCount

proc getField*[T](row: StructRow, idx: int): T =
  ## Get field value at index from this row
  let fieldArray = row.array.getField[T](idx)
  result = fieldArray[row.index]

proc getField*[T](row: StructRow, name: string): T =
  ## Get field value by name from this row
  # First find the field index using the Struct type's method
  let idx = row.array.fieldIndex(name)
  if idx < 0:
    raise newException(KeyError, "Field '" & name & "' not found in struct")
  result = row.getField[T](idx)

proc `$`*(row: StructRow): string =
  ## String representation of a struct row
  result = "StructRow(" & $row.index & "): {"
  # Use the Struct type's fieldCount method
  let nFields = row.array.fieldCount

  # Get field names from the Struct
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
