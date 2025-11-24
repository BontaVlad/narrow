import ./[ffi, gchunkedarray, glist, gtypes, error]

type
  Field* = distinct ptr GArrowField
  Schema* = distinct ptr GArrowSchema
  ArrowTable* = distinct ptr GArrowTable
  RecordBatch* = distinct ptr GArrowRecordBatch

converter toPtr*(f: Field): ptr GArrowField =
  cast[ptr GArrowField](f)

converter toPtr*(s: Schema): ptr GArrowSchema =
  cast[ptr GArrowSchema](s)

converter toPtr*(rb: RecordBatch): ptr GArrowRecordBatch =
  cast[ptr GArrowRecordBatch](rb)

converter toPtr*(tbl: ArrowTable): ptr GArrowTable =
  cast[ptr GArrowTable](tbl)

proc `=destroy`(tbl: ArrowTable) =
  if cast[pointer](tbl) != nil:
    gObjectUnref(cast[gpointer](tbl))

proc `=destroy`(record: RecordBatch) =
  if cast[pointer](record) != nil:
    gObjectUnref(cast[gpointer](record))

proc `=destroy`*(field: Field) =
  if not isNil(field.addr):
    gObjectUnref(cast[pointer](field))

proc `=destroy`(s: Schema) =
  if not isNil(s.addr):
    gObjectUnref(cast[pointer](s))

proc name*(field: Field): string =
  $garrow_field_get_name(field)

proc `$`*(field: Field): string =
  let gStr = garrow_field_to_string(field)
  result = $newGString(gStr)

proc `==`*(a, b: Field): bool =
  garrow_field_equal(a, b).bool

proc newField*[T](name: string): Field =
  let gType = newGType(T)
  Field(garrow_field_new(name.cstring, gType))

proc `$`*(schema: Schema): string =
  let gStr = garrow_schema_to_string(schema)
  result = $newGString(gStr)

proc newSchema*(flds: openArray[Field]): Schema =
  let fList = newGList(flds)
  Schema(garrow_schema_new(fList.list))

proc newSchema*(gptr: pointer): Schema =
  let handle = check garrow_schema_import(cast[gpointer](gptr))
  Schema(handle)

iterator fields*(schema: Schema): lent Field {.inline.} =
  for field in newGList[Field](garrow_schema_get_fields(schema)):
    yield field

iterator items*(schema: Schema): lent Field {.inline.} =
  for field in schema.fields:
    yield field

proc newRecordBatch*(arr: pointer, schema: Schema): RecordBatch =
  let handle = check garrow_record_batch_import(arr, schema)
  RecordBatch(handle)

proc `$`*(record: RecordBatch): string =
  let gStr = check garrow_record_batch_to_string(record)
  result = $newGString(gStr)

proc `$`*(tbl: ArrowTable): string =
  let gStr = check garrow_table_to_string(tbl)
  result = $newGString(gStr)

proc newArrowTable*(schema: Schema, recordBatches: sink seq[RecordBatch]): ArrowTable =
  let handle = check garrow_table_new_record_batches(schema, cast[ptr ptr GArrowRecordBatch](recordBatches[0].addr), gsize(recordBatches.len))
  ArrowTable(handle)

proc schema*(tbl: ArrowTable): Schema =
  Schema(garrow_table_get_schema(tbl))

proc nColumns*(tbl: ArrowTable): int =
  garrow_table_get_n_columns(tbl).int

proc nRows*(tbl: ArrowTable): int64 =
  garrow_table_get_n_rows(tbl).int64

proc addColumn*(tbl: ArrowTable, idx: int, field: Field, column: pointer): ArrowTable =
  let handle = check garrow_table_add_column(tbl, guint(idx), field, cast[ptr GArrowChunkedArray](column))
  ArrowTable(handle)

proc removeColumn*(tbl: ArrowTable, idx: int): ArrowTable =
  let handle = check garrow_table_remove_column(tbl, guint(idx))
  ArrowTable(handle)

proc replaceColumn*(
    tbl: ArrowTable, idx: int, field: Field, column: pointer
): ArrowTable =
  let handle = check garrow_table_replace_column(tbl, guint(idx), field, cast[ptr GArrowChunkedArray](column))
  ArrowTable(handle)

proc equal*(a, b: ArrowTable): bool =
  garrow_table_equal(a, b).bool

proc equalMetadata*(a, b: ArrowTable, checkMetadata: bool): bool =
  garrow_table_equal_metadata(a, b, checkMetadata.gboolean).bool

proc slice*(tbl: ArrowTable, offset, length: int64): ArrowTable =
  ArrowTable(garrow_table_slice(tbl, gint64(offset), gint64(length)))

proc combineChunks*(tbl: ArrowTable): ArrowTable =
  let handle = check garrow_table_combine_chunks(tbl)
  ArrowTable(handle)

proc validate*(tbl: ArrowTable): bool =
  # TODO: check here is not working because it threats bool ret value as error Status
  var err: ptr GError
  garrow_table_validate(tbl, addr err).bool

proc validateFull*(tbl: ArrowTable): bool =
  # TODO: check here is not working because it threats bool ret value as error Status
  var err: ptr GError
  garrow_table_validate_full(tbl, addr err).bool

proc concatenate*(tbl: ArrowTable, others: seq[ArrowTable]): ArrowTable =
  var err: ptr GError
  var gList = newGList(others)
  let handle = check garrow_table_concatenate(tbl, gList.list, garrow_table_concatenate_options_new())
  ArrowTable(handle)

proc getColumnData*(tbl: ArrowTable, idx: int): ChunkedArray =
  let handle = garrow_table_get_column_data(tbl, idx.gint)
  result = newChunkedArray(handle)

proc `[]`*(tbl: ArrowTable, idx: int): ChunkedArray =
  result = tbl.getColumnData(idx)

proc `[]`*(tbl: ArrowTable, key: string): ChunkedArray =
  let idx = garrow_schema_get_field_index(tbl.schema, key.cstring)
  result = tbl.getColumnData(idx)

# proc columns*(tbl: ArrowTable)

iterator keys*(tbl: ArrowTable): string =
  for field in tbl.schema:
    yield $field.name
