import std/[strformat, sets, options]
import ../core/[ffi, error, utils]
import ../types/[gtypes, glist]
import ./primitive

# ============================================================================
# Field and Schema Definitions
# ============================================================================

arcGObject:
  type
    Field* = object
      handle*: ptr GArrowField

    Schema* = object
      handle*: ptr GArrowSchema

proc newField*[T](name: string): Field =
  let gType = newGType(T)
  let handle = garrow_field_new(name.cstring, gType.toPtr)

  if g_object_is_floating(handle) != 0:
    discard g_object_ref_sink(handle)

  result.handle = handle

proc newField*(handle: ptr GArrowField): Field =
  result.handle = handle

proc newField*(name: string, dataType: GADType): Field =
  let handle = garrow_field_new(name.cstring, dataType.handle)
  if handle.isNil:
    raise newException(OperationError, "Failed to create field")
  result.handle = handle

proc name*(field: Field): string =
  let cstr = garrow_field_get_name(field.handle)
  if cstr != nil:
    result = $cstr

proc dataType*(field: Field): GADType =
  let handle = garrow_field_get_data_type(field.toPtr)
  result = newGType(handle)

proc `$`*(field: Field): string {.inline.} =
  $newGString(garrow_field_to_string(field.handle))

proc `==`*(a, b: Field): bool {.inline.} =
  garrow_field_equal(a.handle, b.handle).bool

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
  let handle = verify garrow_schema_import(gptr)
  result.handle = handle

proc newSchema*(handle: ptr GArrowSchema): Schema =
  result.handle = handle

proc `$`*(schema: Schema): string {.inline.} =
  $newGString(garrow_schema_to_string(schema.handle))

proc nFields*(schema: Schema): int =
  garrow_schema_n_fields(schema.handle).int

proc len*(schema: Schema): int {.inline.} =
  schema.nFields

proc getField*(schema: Schema, idx: int): Field =
  let handle = garrow_schema_get_field(schema.handle, idx.guint)
  result = newField(handle)

proc tryGetField*(schema: Schema, idx: int): Option[Field] =
  if idx < 0 or idx >= schema.nFields:
    return none(Field)
  result = some(schema.getField(idx))

proc getFieldByName*(schema: Schema, name: string): Field =
  let handle = garrow_schema_get_field_by_name(schema.handle, name.cstring)
  if handle.isNil:
    raise newException(KeyError, fmt"Field with name: [{name}] does not exist")
  result = newField(handle)

proc tryGetField*(schema: Schema, name: string): Option[Field] =
  let handle = garrow_schema_get_field_by_name(schema.handle, name.cstring)
  if handle.isNil:
    return none(Field)
  result = some(newField(handle))

proc `[]`*(schema: Schema, idx: int): Field {.inline.} =
  schema.getField(idx)

proc `[]`*(schema: Schema, name: string): Field {.inline.} =
  schema.getFieldByName(name)

proc getFieldIndex*(schema: Schema, name: string): int =
  result = garrow_schema_get_field_index(schema.handle, name.cstring).int
  if result < 0:
    raise newException(KeyError, fmt"Field with name: [{name}] does not exist")

proc ffields*(schema: Schema): seq[Field] =
  let gFields = newGList[ptr GArrowField](garrow_schema_get_fields(schema.handle))
  result = newSeq[Field]()

  if gFields.len == 0:
    return result

  result = newSeqOfCap[Field](gFields.len)
  for gField in gFields:
    result.add(newField(gField))

iterator items*(schema: Schema): Field =
  for field in schema.ffields:
    yield field

proc `==`*(a, b: Schema): bool {.inline.} =
  garrow_schema_equal(a.handle, b.handle).bool
