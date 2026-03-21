import ../core/[ffi, error]
import ../tabular/[table, batch]
import ../column/[metadata]
import ./filesystem

# ============================================================================
# Type Definitions
# ============================================================================

type
  IpcFileReader* = object
    ## Random-access IPC file format reader
    ## Wraps GArrowRecordBatchFileReader (separate from RecordBatchReader hierarchy)
    handle: ptr GArrowRecordBatchFileReader
    stream: SeekableInputStream  # Keep stream alive as long as reader exists

  IpcStreamWriter* = object
    ## Streaming IPC format writer
    ## Wraps GArrowRecordBatchStreamWriter
    handle: ptr GArrowRecordBatchStreamWriter

  IpcFileWriter* = object
    ## File IPC format writer (with footer)
    ## Wraps GArrowRecordBatchFileWriter (inherits from StreamWriter)
    handle: ptr GArrowRecordBatchFileWriter

  IpcWriteOptions* = object
    handle: ptr GArrowWriteOptions

  IpcReadOptions* = object
    handle: ptr GArrowReadOptions

# ============================================================================
# IpcFileReader - ARC Hooks (separate type, NOT a RecordBatchReader)
# ============================================================================

proc `=destroy`*(reader: IpcFileReader) =
  if reader.handle != nil:
    g_object_unref(reader.handle)
  `=destroy`(reader.stream)

proc `=sink`*(dest: var IpcFileReader, src: IpcFileReader) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle
  `=sink`(dest.stream, src.stream)

proc `=copy`*(dest: var IpcFileReader, src: IpcFileReader) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)
  `=copy`(dest.stream, src.stream)

proc toPtr*(reader: IpcFileReader): ptr GArrowRecordBatchFileReader {.inline.} =
  reader.handle

proc toPtr*(opt: IpcWriteOptions): ptr GArrowWriteOptions {.inline.} =
  opt.handle

proc toPtr*(rdr: IpcReadOptions): ptr GArrowReadOptions {.inline.} =
  rdr.handle

# ============================================================================
# IpcStreamWriter - ARC Hooks
# ============================================================================

proc `=destroy`*(writer: IpcStreamWriter) =
  if writer.handle != nil:
    g_object_unref(writer.handle)

proc `=sink`*(dest: var IpcStreamWriter, src: IpcStreamWriter) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var IpcStreamWriter, src: IpcStreamWriter) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(writer: IpcStreamWriter): ptr GArrowRecordBatchStreamWriter {.inline.} =
  writer.handle

# ============================================================================
# IpcFileWriter - ARC Hooks
# ============================================================================

proc `=destroy`*(writer: IpcFileWriter) =
  if writer.handle != nil:
    g_object_unref(writer.handle)

proc `=sink`*(dest: var IpcFileWriter, src: IpcFileWriter) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var IpcFileWriter, src: IpcFileWriter) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(writer: IpcFileWriter): ptr GArrowRecordBatchFileWriter {.inline.} =
  writer.handle

# ============================================================================
# Options - ARC Hooks
# ============================================================================

proc `=destroy`*(options: IpcWriteOptions) =
  if options.handle != nil:
    g_object_unref(options.handle)

proc `=sink`*(dest: var IpcWriteOptions, src: IpcWriteOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var IpcWriteOptions, src: IpcWriteOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(options: IpcReadOptions) =
  if options.handle != nil:
    g_object_unref(options.handle)

proc `=sink`*(dest: var IpcReadOptions, src: IpcReadOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var IpcReadOptions, src: IpcReadOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

# ============================================================================
# Options - Constructors
# ============================================================================

proc newIpcWriteOptions*(): IpcWriteOptions =
  let handle = garrow_write_options_new()
  if handle.isNil:
    raise newException(IOError, "Failed to create IpcWriteOptions")
  result.handle = handle

proc newIpcReadOptions*(): IpcReadOptions =
  let handle = garrow_read_options_new()
  if handle.isNil:
    raise newException(IOError, "Failed to create IpcReadOptions")
  result.handle = handle

# ============================================================================
# IpcStreamReader - Uses existing RecordBatchReader type
# ============================================================================

proc newIpcStreamReader*(stream: InputStream): RecordBatchReader =
  ## Create a streaming IPC reader from an input stream
  let handle = check garrow_record_batch_stream_reader_new(stream.handle)
  result.handle = cast[ptr GArrowRecordBatchReader](handle)
  result.streamHandle = stream.handle  # Store stream to keep it alive
  discard g_object_ref(result.streamHandle)  # Increment ref count

proc newIpcStreamReader*(fs: FileSystem, path: string): RecordBatchReader =
  ## Create a streaming IPC reader from a filesystem path
  let stream = fs.openInputStream(path)
  result = newIpcStreamReader(stream)

# ============================================================================
# IpcFileReader - Random access file format
# ============================================================================

proc newIpcFileReader*(stream: SeekableInputStream): IpcFileReader =
  ## Create a file IPC reader from a seekable input stream
  let handle = check garrow_record_batch_file_reader_new(stream.handle)
  result.handle = handle
  result.stream = stream  # Store stream to keep it alive

proc newIpcFileReader*(fs: FileSystem, path: string): IpcFileReader =
  ## Create a file IPC reader from a filesystem path
  let stream = fs.openInputFile(path)
  result = newIpcFileReader(stream)

proc schema*(reader: IpcFileReader): Schema =
  ## Get the schema from the file
  let handle = garrow_record_batch_file_reader_get_schema(reader.handle)
  result = newSchema(handle)

proc nRecordBatches*(reader: IpcFileReader): int =
  ## Get the number of record batches in the file
  int(garrow_record_batch_file_reader_get_n_record_batches(reader.handle))

proc version*(reader: IpcFileReader): GArrowMetadataVersion =
  ## Get the IPC format version
  garrow_record_batch_file_reader_get_version(reader.handle)

proc readRecordBatch*(reader: IpcFileReader, index: int): RecordBatch =
  ## Read a specific record batch by index (random access)
  let handle = check garrow_record_batch_file_reader_read_record_batch(
    reader.handle, guint(index)
  )
  result = newRecordBatch(handle)

# ============================================================================
# IpcStreamWriter - Streaming format
# ============================================================================

proc newIpcStreamWriter*(stream: OutputStream, schema: Schema): IpcStreamWriter =
  ## Create a streaming IPC writer
  let handle = check garrow_record_batch_stream_writer_new(stream.handle, schema.handle)
  result.handle = handle

proc newIpcStreamWriter*(fs: FileSystem, path: string, schema: Schema): IpcStreamWriter =
  ## Create a streaming IPC writer to a filesystem path
  let stream = fs.openOutputStream(path)
  result = newIpcStreamWriter(stream, schema)


proc readAll*(reader: IpcFileReader): ArrowTable =
  ## Read all record batches as a table
  var batches: seq[RecordBatch] = @[]
  for i in 0 ..< reader.nRecordBatches:
    batches.add(reader.readRecordBatch(i))
  
  result = newArrowTable(reader.schema, batches)

proc writeTable*(writer: IpcFileWriter | IpcStreamWriter, table: ArrowTable) =
  ## Write a table to the stream
  check garrow_record_batch_writer_write_table(
    cast[ptr GArrowRecordBatchWriter](writer.handle), table.toPtr
  )

proc writeRecordBatch*(writer: IpcFileWriter, batch: RecordBatch) =
  ## Write a record batch to the file
  check garrow_record_batch_writer_write_record_batch(
    cast[ptr GArrowRecordBatchWriter](writer.handle), batch.handle
  )

proc close*(writer: IpcFileWriter | IpcStreamWriter) =
  ## Close the writer
  check garrow_record_batch_writer_close(
    cast[ptr GArrowRecordBatchWriter](writer.handle)
  )

proc isClosed*(writer: IpcFileWriter | IpcStreamWriter): bool =
  ## Check if the writer is closed
  bool(garrow_record_batch_writer_is_closed(
    cast[ptr GArrowRecordBatchWriter](writer.handle)
  ))

# ============================================================================
# IpcFileWriter - File format with footer
# ============================================================================

proc newIpcFileWriter*(stream: OutputStream, schema: Schema): IpcFileWriter =
  ## Create a file IPC writer
  let handle = check garrow_record_batch_file_writer_new(stream.handle, schema.handle)
  result.handle = handle

proc newIpcFileWriter*(fs: FileSystem, path: string, schema: Schema): IpcFileWriter =
  ## Create a file IPC writer to a filesystem path
  let stream = fs.openOutputStream(path)
  result = newIpcFileWriter(stream, schema)

# ============================================================================
# High-level Convenience API
# ============================================================================

proc readIpcFile*(uri: string): ArrowTable =
  ## Read an entire IPC file into a table
  let fs = newFileSystem(uri)
  let reader = newIpcFileReader(fs, uri)
  result = reader.readAll()

proc readIpcStream*(uri: string): ArrowTable =
  ## Read an entire IPC stream file into a table
  let fs = newFileSystem(uri)

  with fs.openInputStream(uri), stream:
    let reader = newIpcStreamReader(stream)
    result = reader.readAll()

proc writeIpcFile*(uri: string, table: ArrowTable) =
  ## Write a table to an IPC file format
  let fs = newFileSystem(uri)
  let writer = newIpcFileWriter(fs, uri, table.schema)
  defer: writer.close()
  writer.writeTable(table)

proc writeIpcFile*(uri: string, batch: RecordBatch) =
  ## Write a record batch to an IPC file format
  let fs = newFileSystem(uri)
  let writer = newIpcFileWriter(fs, uri, batch.schema)
  defer: writer.close()
  writer.writeRecordBatch(batch)

proc writeIpcStream*(uri: string, table: ArrowTable) =
  ## Write a table to an IPC stream file
  let fs = newFileSystem(uri)
  let writer = newIpcStreamWriter(fs, uri, table.schema)
  defer: writer.close()
  writer.writeTable(table)
