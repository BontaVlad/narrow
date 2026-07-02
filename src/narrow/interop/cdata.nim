import ../core/[ffi, error]
import ../column/metadata
import ../tabular/batch

proc exportSchema*(schema: Schema): pointer =
  ## Exports a schema to the Arrow C Data Interface format.
  ## Returns a pointer to an `ArrowSchema` C struct (owned by the caller).
  result = verify garrow_schema_export(schema.toPtr)

proc exportRecordBatch*(batch: RecordBatch): (pointer, pointer) =
  ## Exports a record batch to the Arrow C Data Interface format.
  ## Returns `(cAbiArray, cAbiSchema)` — pointers to `ArrowArray` and
  ## `ArrowSchema` C structs (owned by the caller).
  var cArray: gpointer
  var cSchema: gpointer
  verify garrow_record_batch_export(batch.toPtr, addr cArray, addr cSchema)
  result = (cArray, cSchema)

proc importRecordBatchReader*(cAbiArrayStream: pointer): RecordBatchReader =
  ## Imports a RecordBatchReader from an Arrow C Data Interface
  ## `ArrowArrayStream`.
  let handle = verify garrow_record_batch_reader_import(cAbiArrayStream)
  result = RecordBatchReader(handle: handle)

proc exportRecordBatchReader*(reader: RecordBatchReader): pointer =
  ## Exports a RecordBatchReader to the Arrow C Data Interface format.
  ## Returns a pointer to an `ArrowArrayStream` C struct (owned by the caller).
  result = verify garrow_record_batch_reader_export(reader.toPtr)
