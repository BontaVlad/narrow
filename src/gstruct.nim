import std/[macros, strutils, sequtils]
import ./[ffi, gschema, glist]

{.experimental: "dotOperators".}

type Struct = object
  handle*: ptr GArrowStructDataType

proc toPtr*(s: Struct): ptr GArrowStructDataType {.inline.} =
  s.handle

proc `=destroy`*(s: Struct) =
  if not isNil(s.toPtr):
    g_object_unref(s.toPtr)

proc `=sink`*(dest: var Struct, src: Struct) =
  if not isNil(dest.toPtr) and dest.toPtr != src.toPtr:
    g_object_unref(dest.toPtr)
  # Transfer ownership (move semantics)
  dest.handle = src.handle

proc `=copy`*(dest: var Struct, src: Struct) =
  if dest.toPtr != src.toPtr:
    if not isNil(dest.toPtr):
      g_object_unref(dest.toPtr)
    dest.handle = src.handle
    if not isNil(dest.toPtr):
      discard g_object_ref(dest.toPtr) # bump ref count

proc newStruct*(fields: GAList[ptr GArrowField]): Struct =
  result.handle = garrow_struct_data_type_new(fields.toPtr)

proc newStruct*(fields: openArray[Field]): Struct =
  var gFields = newGList[ptr GArrowField]()
  for f in fields:
    gFields.append(f.toPtr)
  newStruct(gFields)

proc `[]`*(s: Struct, name: string): Field =
  newField(garrow_struct_data_type_get_field_by_name(s.toPtr, name.cstring))

macro `.`*(s: Struct, name: untyped): untyped =
  let nameStr = newStrLitNode(name.strVal)
  result = quote:
    `s`[`nameStr`]

proc ffields*(s: Struct): seq[Field] =
  let fields = newGList[ptr GArrowField](garrow_struct_data_type_get_fields(s.toPtr))
  for f in fields:
    result.add(newField(f))

proc `$`*(s: Struct): string =
  let fields = s.ffields
  if fields.len <= 3:
    "{ " & fields.mapIt($it).join(", ") & " }"
  else:
    "{\n" & fields.mapIt("  " & $it).join(",\n") & "\n}"
