import std/[macros, strutils, sequtils]
import ./[ffi, gschema, glist, gtypes, garray, error]

{.experimental: "dotOperators".}

type 
  Struct = object
    handle*: ptr GArrowStructDataType

  StructArray* = object
    handle: ptr GArrowStructArray

  StructBuilder* = object
    handle: ptr GArrowStructArrayBuilder
    fieldBuilders: seq[ptr GArrowArrayBuilder]

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
  dest.fieldBuilders = src.fieldBuilders

proc `=copy`*(dest: var StructBuilder, src: StructBuilder) =
  if dest.toPtr != src.toPtr:
    if not isNil(dest.toPtr):
      g_object_unref(dest.toPtr)
    dest.handle = src.handle
    dest.fieldBuilders = src.fieldBuilders
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
proc newStructArray*(structType: Struct, fields: varargs[ptr GArrowArray]): StructArray =
  if fields.len == 0:
    raise newException(ValueError, "Cannot create struct array with no fields")
  # Determine array length from first field
  let length = garrow_array_get_length(fields[0]).gint64
  var fieldList = newGList[ptr GArrowArray]()
  for f in fields:
    fieldList.append(f)
  result.handle = garrow_struct_array_new(
    cast[ptr GArrowDataType](structType.toPtr),
    length,
    fieldList.toPtr,
    nil,
    0
  )
  if result.handle.isNil:
    raise newException(OperationError, "Failed to create StructArray")

# StructBuilder creators
proc newStructBuilder*(structType: Struct): StructBuilder =
  var err: ptr GError
  let handle = garrow_struct_array_builder_new(structType.toPtr, addr err)
  if not isNil(err):
    let msg = if not isNil(err.message): $err.message else: "Failed to create StructArrayBuilder"
    g_error_free(err)
    raise newException(OperationError, msg)
  if handle.isNil:
    raise newException(OperationError, "Failed to create StructArrayBuilder")
  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)
  
  result.handle = handle
  let fieldBuilders = garrow_struct_array_builder_get_field_builders(handle)
  var current = fieldBuilders
  while current != nil:
    result.fieldBuilders.add(cast[ptr GArrowArrayBuilder](current.data))
    current = current.next

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
  not garrow_struct_data_type_get_field_by_name(s.toPtr, name.cstring).isNil

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

proc `[]`*(sa: StructArray, idx: int): StructArray =
  if idx < 0 or idx >= sa.len:
    raise newException(IndexDefect, "Index out of bounds")
  result.handle = cast[ptr GArrowStructArray](
    garrow_array_slice(cast[ptr GArrowArray](sa.toPtr), idx.gint64, 1)
  )

proc getField*(sa: StructArray, idx: int): ptr GArrowArray =
  if idx < 0:
    raise newException(IndexDefect, "Field index cannot be negative")
  let handle = garrow_struct_array_get_field(sa.toPtr, idx.gint)
  if handle.isNil:
    raise newException(KeyError, "Field index " & $idx & " not found")
  handle

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
