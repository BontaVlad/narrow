import std/[strformat, sets, options, tables]
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
  result = $newGString(cstr)

proc dataType*(field: Field): GADType =
  let handle = garrow_field_get_data_type(field.toPtr)
  result = newGType(handle)

proc `$`*(field: Field): string {.inline.} =
  let cstr = garrow_field_to_string(field.handle)
  result = $newGString(cstr, owned = true)

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
  let cstr = garrow_schema_to_string(schema.handle)
  result = $newGString(cstr, owned = true)

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

proc getFieldIndex*(schema: Schema, name: string): int =
  result = garrow_schema_get_field_index(schema.handle, name.cstring).int
  if result < 0:
    raise newException(KeyError, fmt"Field with name: [{name}] does not exist")

proc `[]`*(schema: Schema, idx: int): Field {.inline.} =
  schema.getField(idx)

proc `[]`*(schema: Schema, name: string): Field {.inline.} =
  schema.getFieldByName(name)

proc replaceField*(schema: Schema, idx: int, field: Field): Schema =
  let schemaPtr =
    verify garrow_schema_replace_field(schema.handle, idx.guint, field.handle)
  result = newSchema(schemaPtr)

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

# ============================================================================
# Schema Metadata
# ============================================================================

func hasMetadata*(schema: Schema): bool =
  garrow_schema_has_metadata(schema.handle).bool

proc getMetadata*(schema: Schema): Table[string, string] =
  let ht = garrow_schema_get_metadata(schema.handle)
  if ht == nil:
    return initTable[string, string]()
  result = initTable[string, string]()
  var iter: GHashTableIter
  g_hash_table_iter_init(addr iter, ht)
  var key, value: gpointer
  while g_hash_table_iter_next(addr iter, addr key, addr value).bool:
    result[$cast[cstring](key)] = $cast[cstring](value)

proc getMetadataValue*(schema: Schema, key: string): Option[string] =
  let ht = garrow_schema_get_metadata(schema.handle)
  if ht == nil:
    return none(string)
  let val = g_hash_table_lookup(ht, cast[gconstpointer](key.cstring))
  if val == nil:
    result = none(string)
  else:
    result = some($cast[cstring](val))

proc withMetadata*(schema: Schema,
                    kv: openArray[(string, string)]): Schema =
  var ht = g_hash_table_new(
    cast[GHashFunc](g_str_hash), cast[GEqualFunc](g_str_equal))
  for (k, v) in kv:
    discard g_hash_table_insert(ht, cast[gpointer](k.cstring),
                                cast[gpointer](v.cstring))
  let newSchema = garrow_schema_with_metadata(schema.handle, ht)
  g_hash_table_destroy(ht)
  result.handle = newSchema

proc toString*(schema: Schema, showMetadata: bool): string =
  let cstr = garrow_schema_to_string_metadata(
    schema.handle, showMetadata.gboolean)
  result = $cstr

# ============================================================================
# Schema Field Editing
# ============================================================================

proc addField*(schema: Schema, i: int, field: Field): Schema =
  let handle = verify garrow_schema_add_field(
    schema.handle, i.guint, field.handle)
  result.handle = handle

proc removeField*(schema: Schema, i: int): Schema =
  let handle = verify garrow_schema_remove_field(schema.handle, i.guint)
  result.handle = handle
