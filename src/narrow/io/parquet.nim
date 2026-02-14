import std/[options, sequtils]
import ../core/[ffi, error]
import ./filesystem
import ../column/[metadata, primitive]
import ../tabular/[table, batch]

# Type 1: FileReader
type
  FileReader* = object
    handle*: ptr GParquetArrowFileReader

# FileReader ARC/ORC hooks
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

proc toPtr*(fr: FileReader): ptr GParquetArrowFileReader {.inline.} =
  fr.handle

# Type 2: FileWriter
type
  FileWriter* = object
    handle*: ptr GParquetArrowFileWriter

# FileWriter ARC/ORC hooks
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

proc toPtr*(fw: FileWriter): ptr GParquetArrowFileWriter {.inline.} =
  fw.handle

# Type 3: WriterProperties
type
  WriterProperties* = object
    handle*: ptr GParquetWriterProperties

# WriterProperties ARC/ORC hooks
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

proc toPtr*(wp: WriterProperties): ptr GParquetWriterProperties {.inline.} =
  wp.handle

# Writable concept (defined after all toPtr procs)
type Writable* =
  concept w
      w.schema is Schema
      w.toPtr is ptr GArrowTable | ptr GArrowRecordBatch

# FileReader methods
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
  let handle = check gparquet_arrow_file_reader_read_row_group(
    pfr.toPtr, rowGroupIndex.gint, nil, 0
  )
  result = newArrowTable(handle)

proc readColumnData*(pfr: FileReader, columnIndex: int): ChunkedArray[void] =
  let handle = check gparquet_arrow_file_reader_read_column_data(pfr.toPtr, columnIndex.gint)
  result = newChunkedArray[void](handle)

proc `useThreads=`*(pfr: FileReader, useThreads: bool) =
  gparquet_arrow_file_reader_set_use_threads(pfr.toPtr, useThreads.gboolean)

proc nColumns*(pfr: FileReader): int =
  schema(pfr).nFields

proc newFileReader*(sis: SeekableInputStream): FileReader =
  result.handle = check gparquet_arrow_file_reader_new_arrow(sis.toPtr)

proc newFileReader*(uri: string): FileReader =
  result.handle = check gparquet_arrow_file_reader_new_path(uri)

# FileWriter methods
proc newFileWriter*(uri: string, schema: Schema, wp: WriterProperties): FileWriter =
  result.handle = check gparquet_arrow_file_writer_new_path(schema.toPtr, uri.cstring, wp.toPtr)

proc newFileWriter*(snk: OutputStream, schema: Schema, wp: WriterProperties): FileWriter =
  result.handle = check gparquet_arrow_file_writer_new_arrow(schema.toPtr, snk.toPtr, wp.toPtr)

proc close*(fw: FileWriter) =
  check gparquet_arrow_file_writer_close(fw.toPtr)

proc newRowGroup*(fw: FileWriter) =
  check gparquet_arrow_file_writer_new_row_group(fw.toPtr)

proc schema*(fw: FileWriter): Schema =
  let handle = gparquet_arrow_file_writer_get_schema(fw.toPtr)
  result = newSchema(handle)

# WriterProperties methods
proc newWriterProperties*(): WriterProperties =
  result.handle = gparquet_writer_properties_new()

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

proc setCompression*(wp: WriterProperties, path: string, compression: GArrowCompressionType) =
  gparquet_writer_properties_set_compression(wp.handle, compression, path.cstring)

proc compression*(wp: WriterProperties, path: string): GArrowCompressionType {.inline.} =
  gparquet_writer_properties_get_compression_path(wp.handle, path.cstring)

proc enableDictionary*(wp: WriterProperties, path: string) =
  gparquet_writer_properties_enable_dictionary(wp.handle, path.cstring)

proc disableDictionary*(wp: WriterProperties, path: string) =
  gparquet_writer_properties_disable_dictionary(wp.handle, path.cstring)

proc isDictionaryEnabled*(wp: WriterProperties, path: string): bool =
  gparquet_writer_properties_is_dictionary_enabled(wp.handle, path.cstring) == 1

# High-level I/O procs
proc readTable*(uri: string): ArrowTable =
  let pfr = newFileReader(uri)
  let handle = check gparquet_arrow_file_reader_read_table(pfr.toPtr)
  result = newArrowTable(handle)

proc readTable*(uri: string, columns: sink seq[string]): ArrowTable =
  let pfr = newFileReader(uri)
  let schema = pfr.schema
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
  defer: writer.close()
  when writable is ArrowTable:
    check gparquet_arrow_file_writer_write_table(writer.toPtr, writable.toPtr, chunk_size.gsize)
  elif writable is RecordBatch:
    check gparquet_arrow_file_writer_write_record_batch(writer.toPtr, writable.toPtr)

type
  Statistics* = object
    handle*: ptr GParquetStatistics

  BooleanStatistics* = object
    handle*: ptr GParquetBooleanStatistics

  Int32Statistics* = object
    handle*: ptr GParquetInt32Statistics

  Int64Statistics* = object
    handle*: ptr GParquetInt64Statistics

  FloatStatistics* = object
    handle*: ptr GParquetFloatStatistics

  DoubleStatistics* = object
    handle*: ptr GParquetDoubleStatistics

  ByteArrayStatistics* = object
    handle*: ptr GParquetByteArrayStatistics

  FixedLengthByteArrayStatistics* = object
    handle*: ptr GParquetFixedLengthByteArrayStatistics

  ColumnChunkMetadata* = object
    handle*: ptr GParquetColumnChunkMetadata

  RowGroupMetadata* = object
    handle*: ptr GParquetRowGroupMetadata

  FileMetadata* = object
    handle*: ptr GParquetFileMetadata

# toPtr helpers
proc toPtr*(s: Statistics): ptr GParquetStatistics {.inline.} =
  s.handle

proc toPtr*(s: BooleanStatistics): ptr GParquetBooleanStatistics {.inline.} =
  s.handle

proc toPtr*(s: Int32Statistics): ptr GParquetInt32Statistics {.inline.} =
  s.handle

proc toPtr*(s: Int64Statistics): ptr GParquetInt64Statistics {.inline.} =
  s.handle

proc toPtr*(s: FloatStatistics): ptr GParquetFloatStatistics {.inline.} =
  s.handle

proc toPtr*(s: DoubleStatistics): ptr GParquetDoubleStatistics {.inline.} =
  s.handle

proc toPtr*(s: ByteArrayStatistics): ptr GParquetByteArrayStatistics {.inline.} =
  s.handle

proc toPtr*(
    s: FixedLengthByteArrayStatistics
): ptr GParquetFixedLengthByteArrayStatistics {.inline.} =
  s.handle

proc toPtr*(m: ColumnChunkMetadata): ptr GParquetColumnChunkMetadata {.inline.} =
  m.handle

proc toPtr*(m: RowGroupMetadata): ptr GParquetRowGroupMetadata {.inline.} =
  m.handle

proc toPtr*(m: FileMetadata): ptr GParquetFileMetadata {.inline.} =
  m.handle

# Statistics constructors
proc newStatistics*(handle: ptr GParquetStatistics): Statistics =
  result.handle = handle

proc newBooleanStatistics*(handle: ptr GParquetBooleanStatistics): BooleanStatistics =
  result.handle = handle

proc newInt32Statistics*(handle: ptr GParquetInt32Statistics): Int32Statistics =
  result.handle = handle

proc newInt64Statistics*(handle: ptr GParquetInt64Statistics): Int64Statistics =
  result.handle = handle

proc newFloatStatistics*(handle: ptr GParquetFloatStatistics): FloatStatistics =
  result.handle = handle

proc newDoubleStatistics*(handle: ptr GParquetDoubleStatistics): DoubleStatistics =
  result.handle = handle

proc newByteArrayStatistics*(
    handle: ptr GParquetByteArrayStatistics
): ByteArrayStatistics =
  result.handle = handle

proc newFixedLengthByteArrayStatistics*(
    handle: ptr GParquetFixedLengthByteArrayStatistics
): FixedLengthByteArrayStatistics =
  result.handle = handle

# Metadata constructors
proc newColumnChunkMetadata*(
    handle: ptr GParquetColumnChunkMetadata
): ColumnChunkMetadata =
  result.handle = handle

proc newRowGroupMetadata*(handle: ptr GParquetRowGroupMetadata): RowGroupMetadata =
  result.handle = handle

proc newFileMetadata*(handle: ptr GParquetFileMetadata): FileMetadata =
  result.handle = handle

# Statistics methods
proc `==`*(a, b: Statistics): bool =
  gparquet_statistics_equal(a.toPtr, b.toPtr) == 1

proc hasNulls*(s: Statistics): bool =
  gparquet_statistics_has_n_nulls(s.toPtr) == 1

proc nullCount*(s: Statistics): int64 =
  gparquet_statistics_get_n_nulls(s.toPtr)

proc hasDistinctValues*(s: Statistics): bool =
  gparquet_statistics_has_n_distinct_values(s.toPtr) == 1

proc distinctValueCount*(s: Statistics): int64 =
  gparquet_statistics_get_n_distinct_values(s.toPtr)

proc valueCount*(s: Statistics): int64 =
  gparquet_statistics_get_n_values(s.toPtr)

proc hasMinMax*(s: Statistics): bool =
  gparquet_statistics_has_min_max(s.toPtr) == 1

# BooleanStatistics methods
proc min*(s: BooleanStatistics): bool =
  gparquet_boolean_statistics_get_min(s.toPtr) == 1

proc max*(s: BooleanStatistics): bool =
  gparquet_boolean_statistics_get_max(s.toPtr) == 1

# Int32Statistics methods
proc min*(s: Int32Statistics): int32 =
  gparquet_int32_statistics_get_min(s.toPtr)

proc max*(s: Int32Statistics): int32 =
  gparquet_int32_statistics_get_max(s.toPtr)

# Int64Statistics methods
proc min*(s: Int64Statistics): int64 =
  gparquet_int64_statistics_get_min(s.toPtr)

proc max*(s: Int64Statistics): int64 =
  gparquet_int64_statistics_get_max(s.toPtr)

# FloatStatistics methods
proc min*(s: FloatStatistics): float32 =
  gparquet_float_statistics_get_min(s.toPtr)

proc max*(s: FloatStatistics): float32 =
  gparquet_float_statistics_get_max(s.toPtr)

# DoubleStatistics methods
proc min*(s: DoubleStatistics): float64 =
  gparquet_double_statistics_get_min(s.toPtr)

proc max*(s: DoubleStatistics): float64 =
  gparquet_double_statistics_get_max(s.toPtr)

# ByteArrayStatistics methods
proc min*(s: ByteArrayStatistics): seq[byte] =
  let bytes = gparquet_byte_array_statistics_get_min(s.toPtr)
  if not isNil(bytes):
    let size = g_bytes_get_size(bytes)
    let data = g_bytes_get_data(bytes, nil)
    result = newSeq[byte](size)
    if size > 0:
      copyMem(result[0].addr, data, size)

proc max*(s: ByteArrayStatistics): seq[byte] =
  let bytes = gparquet_byte_array_statistics_get_max(s.toPtr)
  if not isNil(bytes):
    let size = g_bytes_get_size(bytes)
    let data = g_bytes_get_data(bytes, nil)
    result = newSeq[byte](size)
    if size > 0:
      copyMem(result[0].addr, data, size)

proc toString*(bytes: seq[byte]): string =
  if bytes.len > 0:
    result = newString(bytes.len)
    copyMem(result[0].addr, bytes[0].unsafeAddr, bytes.len)

# FixedLengthByteArrayStatistics methods
proc min*(s: FixedLengthByteArrayStatistics): seq[byte] =
  let bytes = gparquet_fixed_length_byte_array_statistics_get_min(s.toPtr)
  if not isNil(bytes):
    let size = g_bytes_get_size(bytes)
    let data = g_bytes_get_data(bytes, nil)
    result = newSeq[byte](size)
    if size > 0:
      copyMem(result[0].addr, data, size)

proc max*(s: FixedLengthByteArrayStatistics): seq[byte] =
  let bytes = gparquet_fixed_length_byte_array_statistics_get_max(s.toPtr)
  if not isNil(bytes):
    let size = g_bytes_get_size(bytes)
    let data = g_bytes_get_data(bytes, nil)
    result = newSeq[byte](size)
    if size > 0:
      copyMem(result[0].addr, data, size)

# ColumnChunkMetadata methods
proc `==`*(a, b: ColumnChunkMetadata): bool =
  gparquet_column_chunk_metadata_equal(a.toPtr, b.toPtr) == 1

proc totalSize*(m: ColumnChunkMetadata): int64 =
  gparquet_column_chunk_metadata_get_total_size(m.toPtr)

proc totalCompressedSize*(m: ColumnChunkMetadata): int64 =
  gparquet_column_chunk_metadata_get_total_compressed_size(m.toPtr)

proc fileOffset*(m: ColumnChunkMetadata): int64 =
  gparquet_column_chunk_metadata_get_file_offset(m.toPtr)

proc canDecompress*(m: ColumnChunkMetadata): bool =
  gparquet_column_chunk_metadata_can_decompress(m.toPtr) == 1

proc statistics*(m: ColumnChunkMetadata): Statistics =
  let handle = gparquet_column_chunk_metadata_get_statistics(m.toPtr)
  result.handle = handle

# RowGroupMetadata methods
proc `==`*(a, b: RowGroupMetadata): bool =
  gparquet_row_group_metadata_equal(a.toPtr, b.toPtr) == 1

proc nColumns*(m: RowGroupMetadata): int =
  gparquet_row_group_metadata_get_n_columns(m.toPtr)

proc columnChunk*(m: RowGroupMetadata, index: int): ColumnChunkMetadata =
  result.handle =
    check gparquet_row_group_metadata_get_column_chunk(m.toPtr, index.gint)

proc nRows*(m: RowGroupMetadata): int64 =
  gparquet_row_group_metadata_get_n_rows(m.toPtr)

proc totalSize*(m: RowGroupMetadata): int64 =
  gparquet_row_group_metadata_get_total_size(m.toPtr)

proc totalCompressedSize*(m: RowGroupMetadata): int64 =
  gparquet_row_group_metadata_get_total_compressed_size(m.toPtr)

proc fileOffset*(m: RowGroupMetadata): int64 =
  gparquet_row_group_metadata_get_file_offset(m.toPtr)

proc canDecompress*(m: RowGroupMetadata): bool =
  gparquet_row_group_metadata_can_decompress(m.toPtr) == 1

# FileMetadata methods
proc `==`*(a, b: FileMetadata): bool =
  gparquet_file_metadata_equal(a.toPtr, b.toPtr) == 1

proc nColumns*(m: FileMetadata): int =
  gparquet_file_metadata_get_n_columns(m.toPtr)

proc nSchemaElements*(m: FileMetadata): int =
  gparquet_file_metadata_get_n_schema_elements(m.toPtr)

proc nRows*(m: FileMetadata): int64 =
  gparquet_file_metadata_get_n_rows(m.toPtr)

proc nRowGroups*(m: FileMetadata): int =
  gparquet_file_metadata_get_n_row_groups(m.toPtr)

# TODO: leaks memory
proc rowGroup*(m: FileMetadata, index: int): RowGroupMetadata =
  newRowGroupMetadata(check gparquet_file_metadata_get_row_group(m.toPtr, index.gint))

proc createdBy*(m: FileMetadata): string =
  $gparquet_file_metadata_get_created_by(m.toPtr)

proc size*(m: FileMetadata): uint32 =
  gparquet_file_metadata_get_size(m.toPtr)

proc canDecompress*(m: FileMetadata): bool =
  gparquet_file_metadata_can_decompress(m.toPtr) == 1

# FileReader metadata access
proc metadata*(reader: FileReader): FileMetadata =
  let handle = gparquet_arrow_file_reader_get_metadata(reader.toPtr)
  result.handle = handle

# Statistics ARC/ORC hooks
proc `=destroy`*(s: Statistics) =
  if not isNil(s.handle):
    g_object_unref(s.handle)

proc `=sink`*(dest: var Statistics, src: Statistics) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Statistics, src: Statistics) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# BooleanStatistics hooks
proc `=destroy`*(s: BooleanStatistics) =
  if not isNil(s.handle):
    g_object_unref(s.handle)

proc `=sink`*(dest: var BooleanStatistics, src: BooleanStatistics) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var BooleanStatistics, src: BooleanStatistics) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# Int32Statistics hooks
proc `=destroy`*(s: Int32Statistics) =
  if not isNil(s.handle):
    g_object_unref(s.handle)

proc `=sink`*(dest: var Int32Statistics, src: Int32Statistics) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Int32Statistics, src: Int32Statistics) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# Int64Statistics hooks
proc `=destroy`*(s: Int64Statistics) =
  if not isNil(s.handle):
    g_object_unref(s.handle)

proc `=sink`*(dest: var Int64Statistics, src: Int64Statistics) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Int64Statistics, src: Int64Statistics) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# FloatStatistics hooks
proc `=destroy`*(s: FloatStatistics) =
  if not isNil(s.handle):
    g_object_unref(s.handle)

proc `=sink`*(dest: var FloatStatistics, src: FloatStatistics) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FloatStatistics, src: FloatStatistics) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# DoubleStatistics hooks
proc `=destroy`*(s: DoubleStatistics) =
  if not isNil(s.handle):
    g_object_unref(s.handle)

proc `=sink`*(dest: var DoubleStatistics, src: DoubleStatistics) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var DoubleStatistics, src: DoubleStatistics) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# ByteArrayStatistics hooks
proc `=destroy`*(s: ByteArrayStatistics) =
  if not isNil(s.handle):
    g_object_unref(s.handle)

proc `=sink`*(dest: var ByteArrayStatistics, src: ByteArrayStatistics) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var ByteArrayStatistics, src: ByteArrayStatistics) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# FixedLengthByteArrayStatistics hooks
proc `=destroy`*(s: FixedLengthByteArrayStatistics) =
  if not isNil(s.handle):
    g_object_unref(s.handle)

proc `=sink`*(
    dest: var FixedLengthByteArrayStatistics, src: FixedLengthByteArrayStatistics
) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(
    dest: var FixedLengthByteArrayStatistics, src: FixedLengthByteArrayStatistics
) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# ColumnChunkMetadata hooks
proc `=destroy`*(m: ColumnChunkMetadata) =
  if not isNil(m.handle):
    g_object_unref(m.handle)

proc `=sink`*(dest: var ColumnChunkMetadata, src: ColumnChunkMetadata) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var ColumnChunkMetadata, src: ColumnChunkMetadata) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# RowGroupMetadata hooks
proc `=destroy`*(m: RowGroupMetadata) =
  if not isNil(m.handle):
    g_object_unref(m.handle)

proc `=sink`*(dest: var RowGroupMetadata, src: RowGroupMetadata) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var RowGroupMetadata, src: RowGroupMetadata) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

# FileMetadata hooks
proc `=destroy`*(m: FileMetadata) =
  if not isNil(m.handle):
    g_object_unref(m.handle)

proc `=sink`*(dest: var FileMetadata, src: FileMetadata) =
  if not isNil(dest.handle) and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FileMetadata, src: FileMetadata) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)
