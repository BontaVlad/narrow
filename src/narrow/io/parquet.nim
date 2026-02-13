import std/[options, sugar, sequtils]
import ../core/[ffi, error]
import ./filesystem
import ../column/[metadata, primitive]
import ../tabular/[table, batch]

type
  FileReader* = object
    handle: ptr GParquetArrowFileReader

  FileWriter* = object
    handle: ptr GParquetArrowFileWriter

  WriterProperties* = object
    handle: ptr GParquetWriterProperties

  Writable* =
    concept w
        w.schema is Schema
        w.toPtr is ptr GArrowTable | ptr GArrowRecordBatch

proc `=destroy`*(pfr: FileReader) =
  if pfr.handle != nil:
    g_object_unref(pfr.handle)

proc `=sink`*(dest: var FileReader, src: FileReader) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FileReader, src: FileReader) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(wp: WriterProperties) =
  if not isNil(wp.handle):
    g_object_unref(wp.handle)

proc `=sink`*(dest: var WriterProperties, src: WriterProperties) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var WriterProperties, src: WriterProperties) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

proc `=destroy`*(fw: FileWriter) =
  if not isNil(fw.handle):
    g_object_unref(fw.handle)

proc `=sink`*(dest: var FileWriter, src: FileWriter) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FileWriter, src: FileWriter) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

proc toPtr*(fr: FileReader): ptr GParquetArrowFileReader {.inline.} =
  fr.handle

proc toPtr*(fw: FileWriter): ptr GParquetArrowFileWriter {.inline.} =
  fw.handle

proc toPtr*(wp: WriterProperties): ptr GParquetWriterProperties {.inline.} =
  wp.handle

proc schema*(pfr: FileReader): Schema =
  let handle = check gparquet_arrow_file_reader_get_schema(pfr.toPtr)
  result = newSchema(handle)

proc nRowGroups*(pfr: FileReader): int =
  gparquet_arrow_file_reader_get_n_row_groups(pfr.toPtr)

proc nRows*(pfr: FileReader): int64 =
  gparquet_arrow_file_reader_get_n_rows(pfr.toPtr)

proc close*(pfr: FileReader) =
  gparquet_arrow_file_reader_close(pfr.toPtr)

proc readRowGroup*(pfr: FileReader, rowGroupIndex: int): ArrowTable =
  # TODO: Support reading specific columns - requires handling column_indices parameter
  # For now, read all columns by passing nil for column_indices
  let handle = check gparquet_arrow_file_reader_read_row_group(
    pfr.toPtr, rowGroupIndex.gint, nil, 0
  )
  result = newArrowTable(handle)

proc readColumnData*(pfr: FileReader, columnIndex: int): ChunkedArray[void] =
  let handle =
    check gparquet_arrow_file_reader_read_column_data(pfr.toPtr, columnIndex.gint)
  result = newChunkedArray[void](handle)

proc `useThreads=`*(pfr: FileReader, useThreads: bool) =
  gparquet_arrow_file_reader_set_use_threads(pfr.toPtr, useThreads.gboolean)

proc nColumns*(pfr: FileReader): int =
  schema(pfr).nFields

proc newFileReader*(sis: SeekableInputStream): FileReader =
  result.handle = check gparquet_arrow_file_reader_new_arrow(sis.toPtr)

proc newFileReader*(uri: string): FileReader =
  result.handle = check gparquet_arrow_file_reader_new_path(uri)

proc newFileWriter*(uri: string, schema: Schema, wp: WriterProperties): FileWriter =
  result.handle =
    check gparquet_arrow_file_writer_new_path(schema.toPtr, uri.cstring, wp.toPtr)

proc newFileWriter*(
    snk: OutputStream, schema: Schema, wp: WriterProperties
): FileWriter =
  result.handle =
    check gparquet_arrow_file_writer_new_arrow(schema.toPtr, snk.toPtr, wp.toPtr)

proc newWriterProperties*(): WriterProperties =
  result.handle = gparquet_writer_properties_new()

# FileWriter methods
proc close*(fw: FileWriter) =
  check gparquet_arrow_file_writer_close(fw.toPtr)

proc newRowGroup*(fw: FileWriter) =
  check gparquet_arrow_file_writer_new_row_group(fw.toPtr)

proc schema*(fw: FileWriter): Schema =
  let handle = gparquet_arrow_file_writer_get_schema(fw.toPtr)
  result = newSchema(handle)

# Property getters and setters for WriterProperties

proc dictionaryPageSizeLimit*(wp: WriterProperties): int64 =
  gparquet_writer_properties_get_dictionary_page_size_limit(wp.handle)

proc `dictionaryPageSizeLimit=`*(wp: var WriterProperties, limit: int64) =
  gparquet_writer_properties_set_dictionary_page_size_limit(wp.handle, limit)

proc batchSize*(wp: WriterProperties): int64 =
  gparquet_writer_properties_get_batch_size(wp.handle)

proc `batchSize=`*(wp: var WriterProperties, size: int64) =
  gparquet_writer_properties_set_batch_size(wp.handle, size)

proc maxRowGroupLength*(wp: WriterProperties): int64 =
  gparquet_writer_properties_get_max_row_group_length(wp.handle)

proc `maxRowGroupLength=`*(wp: var WriterProperties, length: int64) =
  gparquet_writer_properties_set_max_row_group_length(wp.handle, length)

proc dataPageSize*(wp: WriterProperties): int64 =
  gparquet_writer_properties_get_data_page_size(wp.handle)

proc `dataPageSize=`*(wp: var WriterProperties, size: int64) =
  gparquet_writer_properties_set_data_page_size(wp.handle, size)

proc setCompression*(
    wp: WriterProperties, path: string, compression: GArrowCompressionType
) =
  gparquet_writer_properties_set_compression(wp.handle, compression, path.cstring)

proc compression*(
    wp: WriterProperties, path: string
): GArrowCompressionType {.inline.} =
  gparquet_writer_properties_get_compression_path(wp.handle, path.cstring)

proc enableDictionary*(wp: WriterProperties, path: string) =
  gparquet_writer_properties_enable_dictionary(wp.handle, path.cstring)

proc disableDictionary*(wp: WriterProperties, path: string) =
  gparquet_writer_properties_disable_dictionary(wp.handle, path.cstring)

proc isDictionaryEnabled*(wp: WriterProperties, path: string): bool =
  gparquet_writer_properties_is_dictionary_enabled(wp.handle, path.cstring) == 1

proc readTable*(uri: string): ArrowTable =
  let pfr = newFileReader(uri)
  let handle = check gparquet_arrow_file_reader_read_table(pfr.toPtr)
  result = newArrowTable(handle)

proc readTable*(uri: string, columns: sink seq[string]): ArrowTable =
  let pfr = newFileReader(uri)
  let schema = pfr.schema

  # 1. Map column names to (index, field) tuples, filtering out missing ones
  let fieldsInfo = columns.filterIt(schema.tryGetField(it).isSome).mapIt(
      (index: schema.getFieldIndex(it), field: schema.tryGetField(it).get())
    )

  var chunkedArrays = newSeq[ChunkedArray[void]]()
  for info in fieldsInfo:
    chunkedArrays.add(pfr.readColumnData(info.index))

  var data = newSeq[ptr GArrowChunkedArray]()
  for arr in chunkedArrays:
    data.add(arr.toPtr)

  let tSchema = newSchema(fieldsInfo.mapIt(it.field))

  result = newArrowTableFromChunkedArrays(tSchema, data)

proc writeTable*[T: Writable](writable: T, uri: string, chunk_size: int = 65536) =
  let wp = newWriterProperties()
  let writer = newFileWriter(uri, writable.schema, wp)
  defer:
    writer.close()
  when writable is ArrowTable:
    check gparquet_arrow_file_writer_write_table(
      writer.toPtr, writable.toPtr, chunk_size.gsize
    )
  elif writable is RecordBatch:
    check gparquet_arrow_file_writer_write_record_batch(writer.toPtr, writable.toPtr)
