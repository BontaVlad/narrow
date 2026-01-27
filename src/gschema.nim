import std/[strformat, sets]
import ./[ffi, gchunkedarray, garray, glist, gtypes, error]

type
  Field* = object
    handle: ptr GArrowField

  Schema* = object
    handle*: ptr GArrowSchema

proc `=destroy`*(field: Field) =
  if field.handle != nil:
    g_object_unref(field.handle)

proc `=sink`*(dest: var Field, src: Field) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Field, src: Field) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(f: Field): ptr GArrowField {.inline.} =
  f.handle

proc newField*[T](name: string): Field =
  let gType = newGType(T)
  let handle = garrow_field_new(name.cstring, gType.toPtr)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc newField*(handle: ptr GArrowField): Field =
  discard g_object_ref(handle)
  result.handle = handle

proc name*(field: Field): string =
  let cstr = garrow_field_get_name(field.handle)
  if cstr != nil:
    result = $cstr

proc dataType*(field: Field): GADType =
  let handle = garrow_field_get_data_type(field.toPtr)
  result = newGType(handle)

proc `$`*(field: Field): string =
  let cstr = garrow_field_to_string(field.handle)
  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc `==`*(a, b: Field): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_field_equal(a.handle, b.handle).bool

proc `=destroy`*(schema: Schema) =
  if schema.handle != nil:
    g_object_unref(schema.handle)

proc `=sink`*(dest: var Schema, src: Schema) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Schema, src: Schema) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(s: Schema): ptr GArrowSchema {.inline.} =
  s.handle

proc newSchema*(fields: openArray[Field]): Schema =
  var seen = initHashSet[string]()
  for f in fields:
    if f.name in seen:
      raise newException(ValueError, "Duplicate field name: " & f.name)
    seen.incl(f.name)

  var fieldList = newGList[ptr GArrowField]()
  for field in fields:
    fieldList.append(field.handle)

  result.handle = garrow_schema_new(fieldList.list)

proc newSchema*(gptr: pointer): Schema =
  var err: ptr GError
  let handle = garrow_schema_import(gptr, addr err)

  if not isNil(err):
    let msg =
      if not isNil(err.message):
        $err.message
      else:
        "Schema import failed"
    g_error_free(err)
    raise newException(OperationError, msg)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc newSchema*(handle: ptr GArrowSchema): Schema =
  result.handle = handle

proc `$`*(schema: Schema): string =
  let cstr = garrow_schema_to_string(schema.handle)
  if cstr != nil:
    result = $cstr
    g_free(cstr)

proc nFields*(schema: Schema): int =
  garrow_schema_n_fields(schema.handle).int

proc getField*(schema: Schema, idx: int): Field =
  # FIXME: this will segfault with crap idx idx > nFields
  let handle = garrow_schema_get_field(schema.handle, idx.guint)
  result = newField(handle)

proc getFieldByName*(schema: Schema, name: string): Field =
  let handle = garrow_schema_get_field_by_name(schema.handle, name.cstring)
  if handle.isNil:
    raise newException(KeyError, fmt"Field with name: [{name}] does not exist")
  result = newField(handle)

proc getFieldIndex*(schema: Schema, name: string): int =
  result = garrow_schema_get_field_index(schema.handle, name.cstring).int
  if result < 0:
    raise newException(KeyError, fmt"Field with name: [{name}] does not exist")

proc ffields*(schema: Schema): seq[Field] =
  let glistPtr = garrow_schema_get_fields(schema.handle)

  if glistPtr == nil:
    return @[]

  result = newSeq[Field]()
  var current = glistPtr
  while current != nil:
    let item = current.data
    if item != nil:
      let fieldPtr = cast[ptr GArrowField](item)
      result.add(newField(fieldPtr))
    current = current.next

  g_list_free(glistPtr)

iterator items*(schema: Schema): Field =
  for field in schema.ffields:
    yield field

proc `==`*(a, b: Schema): bool =
  if a.handle == nil or b.handle == nil:
    return a.handle == b.handle
  garrow_schema_equal(a.handle, b.handle).bool
