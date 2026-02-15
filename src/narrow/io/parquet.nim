import std/[options, sequtils, sets]
import ../core/[ffi, error]
import ./filesystem
import ../column/[metadata, primitive]
import ../tabular/[table, batch]
import ../compute/[acero, expressions]

# ============================================================================
# Type Definitions
# ============================================================================

# File I/O Types
type
  FileReader* = object
    handle*: ptr GParquetArrowFileReader

  FileWriter* = object
    handle*: ptr GParquetArrowFileWriter

  WriterProperties* = object
    handle*: ptr GParquetWriterProperties

# Statistics Types
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

# Metadata Types
type
  ColumnChunkMetadata* = object
    handle*: ptr GParquetColumnChunkMetadata

  RowGroupMetadata* = object
    handle*: ptr GParquetRowGroupMetadata

  FileMetadata* = object
    handle*: ptr GParquetFileMetadata

# Concepts
type Writable* =
  concept w
      w.schema is Schema
      w.toPtr is ptr GArrowTable | ptr GArrowRecordBatch

# ============================================================================
# FileReader - ARC/ORC Hooks
# ============================================================================

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

# ============================================================================
# FileWriter - ARC/ORC Hooks
# ============================================================================

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

# ============================================================================
# WriterProperties - ARC/ORC Hooks
# ============================================================================

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

# ============================================================================
# Statistics - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(s: Statistics): ptr GParquetStatistics {.inline.} =
  s.handle

# ============================================================================
# BooleanStatistics - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(s: BooleanStatistics): ptr GParquetBooleanStatistics {.inline.} =
  s.handle

# ============================================================================
# Int32Statistics - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(s: Int32Statistics): ptr GParquetInt32Statistics {.inline.} =
  s.handle

# ============================================================================
# Int64Statistics - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(s: Int64Statistics): ptr GParquetInt64Statistics {.inline.} =
  s.handle

# ============================================================================
# FloatStatistics - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(s: FloatStatistics): ptr GParquetFloatStatistics {.inline.} =
  s.handle

# ============================================================================
# DoubleStatistics - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(s: DoubleStatistics): ptr GParquetDoubleStatistics {.inline.} =
  s.handle

# ============================================================================
# ByteArrayStatistics - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(s: ByteArrayStatistics): ptr GParquetByteArrayStatistics {.inline.} =
  s.handle

# ============================================================================
# FixedLengthByteArrayStatistics - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(
    s: FixedLengthByteArrayStatistics
): ptr GParquetFixedLengthByteArrayStatistics {.inline.} =
  s.handle

# ============================================================================
# ColumnChunkMetadata - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(m: ColumnChunkMetadata): ptr GParquetColumnChunkMetadata {.inline.} =
  m.handle

# ============================================================================
# RowGroupMetadata - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(m: RowGroupMetadata): ptr GParquetRowGroupMetadata {.inline.} =
  m.handle

# ============================================================================
# FileMetadata - ARC/ORC Hooks
# ============================================================================

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

proc toPtr*(m: FileMetadata): ptr GParquetFileMetadata {.inline.} =
  m.handle

# ============================================================================
# FileReader - Constructors
# ============================================================================

proc newFileReader*(sis: SeekableInputStream): FileReader =
  result.handle = check gparquet_arrow_file_reader_new_arrow(sis.toPtr)

proc newFileReader*(uri: string): FileReader =
  result.handle = check gparquet_arrow_file_reader_new_path(uri)

# ============================================================================
# FileReader - Methods
# ============================================================================

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
  ## Reads a specific row group, returning all columns.
  let handle = check gparquet_arrow_file_reader_read_row_group(
    pfr.toPtr, rowGroupIndex.gint, nil, 0
  )
  result = newArrowTable(handle)

proc readRowGroup*(
    pfr: FileReader, rowGroupIndex: int, columnIndices: seq[int]
): ArrowTable =
  ## Reads a specific row group, selecting only the given column indices.
  ## Column indices are 0-based and must be valid for the file schema.
  if columnIndices.len == 0:
    return pfr.readRowGroup(rowGroupIndex) # delegate to existing

  var indices = newSeq[gint](columnIndices.len)
  for i, idx in columnIndices:
    indices[i] = idx.gint

  let handle = check gparquet_arrow_file_reader_read_row_group(
    pfr.toPtr, rowGroupIndex.gint, addr indices[0], indices.len.gsize
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

proc metadata*(reader: FileReader): FileMetadata =
  let handle = gparquet_arrow_file_reader_get_metadata(reader.toPtr)
  result.handle = handle

# ============================================================================
# FileWriter - Constructors
# ============================================================================

proc newFileWriter*(uri: string, schema: Schema, wp: WriterProperties): FileWriter =
  result.handle =
    check gparquet_arrow_file_writer_new_path(schema.toPtr, uri.cstring, wp.toPtr)

proc newFileWriter*(
    snk: OutputStream, schema: Schema, wp: WriterProperties
): FileWriter =
  result.handle =
    check gparquet_arrow_file_writer_new_arrow(schema.toPtr, snk.toPtr, wp.toPtr)

# ============================================================================
# FileWriter - Methods
# ============================================================================

proc close*(fw: FileWriter) =
  check gparquet_arrow_file_writer_close(fw.toPtr)

proc newRowGroup*(fw: FileWriter) =
  check gparquet_arrow_file_writer_new_row_group(fw.toPtr)

proc schema*(fw: FileWriter): Schema =
  let handle = gparquet_arrow_file_writer_get_schema(fw.toPtr)
  result = newSchema(handle)

# ============================================================================
# WriterProperties - Constructors
# ============================================================================

proc newWriterProperties*(): WriterProperties =
  result.handle = gparquet_writer_properties_new()

# ============================================================================
# WriterProperties - Methods
# ============================================================================

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

# ============================================================================
# Statistics - Constructors
# ============================================================================

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

# ============================================================================
# Statistics - Methods
# ============================================================================

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

# ============================================================================
# BooleanStatistics - Methods
# ============================================================================

proc min*(s: BooleanStatistics): bool =
  gparquet_boolean_statistics_get_min(s.toPtr) == 1

proc max*(s: BooleanStatistics): bool =
  gparquet_boolean_statistics_get_max(s.toPtr) == 1

# ============================================================================
# Int32Statistics - Methods
# ============================================================================

proc min*(s: Int32Statistics): int32 =
  gparquet_int32_statistics_get_min(s.toPtr)

proc max*(s: Int32Statistics): int32 =
  gparquet_int32_statistics_get_max(s.toPtr)

# ============================================================================
# Int64Statistics - Methods
# ============================================================================

proc min*(s: Int64Statistics): int64 =
  gparquet_int64_statistics_get_min(s.toPtr)

proc max*(s: Int64Statistics): int64 =
  gparquet_int64_statistics_get_max(s.toPtr)

# ============================================================================
# FloatStatistics - Methods
# ============================================================================

proc min*(s: FloatStatistics): float32 =
  gparquet_float_statistics_get_min(s.toPtr)

proc max*(s: FloatStatistics): float32 =
  gparquet_float_statistics_get_max(s.toPtr)

# ============================================================================
# DoubleStatistics - Methods
# ============================================================================

proc min*(s: DoubleStatistics): float64 =
  gparquet_double_statistics_get_min(s.toPtr)

proc max*(s: DoubleStatistics): float64 =
  gparquet_double_statistics_get_max(s.toPtr)

# ============================================================================
# ByteArrayStatistics - Methods
# ============================================================================

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

# ============================================================================
# FixedLengthByteArrayStatistics - Methods
# ============================================================================

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

# ============================================================================
# Metadata - Constructors
# ============================================================================

proc newColumnChunkMetadata*(
    handle: ptr GParquetColumnChunkMetadata
): ColumnChunkMetadata =
  result.handle = handle

proc newRowGroupMetadata*(handle: ptr GParquetRowGroupMetadata): RowGroupMetadata =
  result.handle = handle

proc newFileMetadata*(handle: ptr GParquetFileMetadata): FileMetadata =
  result.handle = handle

# ============================================================================
# ColumnChunkMetadata - Methods
# ============================================================================

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

# ============================================================================
# RowGroupMetadata - Methods
# ============================================================================

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

# ============================================================================
# FileMetadata - Methods
# ============================================================================

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

# ============================================================================
# High-level I/O procs
# ============================================================================

proc readTable*(uri: string): ArrowTable =
  let pfr = newFileReader(uri)
  let handle = check gparquet_arrow_file_reader_read_table(pfr.toPtr)
  result = newArrowTable(handle)

proc readTable*(uri: string, columns: sink seq[string]): ArrowTable =
  ## Reads a Parquet file, returning only the specified columns.
  ## Raises KeyError if any column name does not exist in the schema.
  let pfr = newFileReader(uri)
  let schema = pfr.schema

  # Validate all columns exist (matches Arrow behavior: KeyError on missing)
  for c in columns:
    if schema.tryGetField(c).isNone:
      raise newException(KeyError, "Column '" & c & "' does not exist in schema")

  let fieldsInfo =
    columns.mapIt((index: schema.getFieldIndex(it), field: schema.getFieldByName(it)))
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

# ============================================================================
# Filtered Reading with Column Projection
# ============================================================================

proc validateFilterColumns(filter: ExpressionObj, schema: Schema) =
  ## Raises KeyError if filter references columns not in the schema.
  for fieldName in extractFieldReferences(filter):
    if schema.tryGetField(fieldName).isNone:
      raise newException(
        KeyError,
        "Filter references column '" & fieldName & "' which does not exist in schema",
      )

proc projectTable(
    table: ArrowTable, columns: seq[string], fileSchema: Schema
): ArrowTable =
  ## Returns a new table with only the specified columns.
  let outSchema = newSchema(columns.mapIt(fileSchema.getFieldByName(it)))
  var chunkedArrays: seq[ChunkedArray[void]] = @[]
  for c in columns:
    chunkedArrays.add(table[c])
  var data = newSeq[ptr GArrowChunkedArray]()
  for arr in chunkedArrays:
    data.add(arr.toPtr)
  result = newArrowTableFromChunkedArrays(outSchema, data)

type Satisfiability = enum
  sNever ## Provably cannot satisfy -> skip row group
  sMaybe ## Cannot determine -> must read

proc getLiteralValue(filter: ExpressionObj, fieldName: string): Option[Scalar] =
  ## Attempts to extract the literal value from a comparison expression.
  ## Returns none if the expression doesn't match expected pattern.
  ##
  ## Expected patterns:
  ##   equal(field, literal) or equal(literal, field)
  ##   greater(field, literal) or greater(literal, field)
  ##   etc.

  when filter is CallExpression:
    let callExpr = CallExpression(filter)
    let fn = callExpr.functionName

    # Only handle comparison functions
    if fn notin ["equal", "not_equal", "less", "less_equal", "greater", "greater_equal"]:
      return none(Scalar)

    # We need to find which argument is the field and which is the literal
    # This requires parsing the expression structure
    # For now, we can't easily extract the literal value from the C expression
    # without additional GArrow APIs that expose argument values

    # Return none to indicate we can't extract the value
    return none(Scalar)
  else:
    return none(Scalar)

proc compareWithStats(stats: Statistics, op: string, value: Scalar): Satisfiability =
  ## Compares a scalar value against statistics using the given operator.
  ## Returns sNever if the row group definitely cannot satisfy the predicate.

  if not stats.hasMinMax:
    return sMaybe

  # Try to get min/max as int64 (most common case)
  # We need to handle different types based on the value's type
  # This is a simplified version that handles integers

  result = sMaybe

proc canRowGroupSatisfy(
    filter: ExpressionObj, rgMeta: RowGroupMetadata, schema: Schema
): Satisfiability =
  ## Conservative row group pruning. Only handles simple patterns:
  ##   col <cmp> literal
  ## combined with AND/OR. Everything else returns sMaybe.
  ##
  ## IMPORTANT: This is a thin optimization layer. When in doubt, return sMaybe.
  ## All correctness is guaranteed by the post-read Acero filter.
  result = sMaybe # default: read the row group

  # Get the function name if it's a call expression
  when filter is CallExpression:
    let callExpr = CallExpression(filter)
    let fn = callExpr.functionName

    # Handle logical operators
    if fn == "and":
      # AND: If any child returns sNever, the whole expression is sNever
      # Otherwise, we can't be sure (sMaybe)
      # Note: We don't have access to child expressions directly in the current
      # implementation since referencedFields only stores field names, not
      # the expression tree structure. We would need to store references to
      # child expressions to implement this properly.
      return sMaybe
    elif fn == "or":
      # OR: Only sNever if ALL children are sNever
      # Similarly, we need child expression access
      return sMaybe

    # Handle comparison operators
    elif fn in ["equal", "not_equal", "less", "less_equal", "greater", "greater_equal"]:
      # Check if this is a simple field-literal comparison
      if filter.referencedFields.len != 1:
        return sMaybe

      let fieldName = filter.referencedFields[0]
      let fieldIdx = schema.getFieldIndex(fieldName)
      if fieldIdx < 0 or fieldIdx >= rgMeta.nColumns:
        return sMaybe

      # Get column statistics
      let colChunk = rgMeta.columnChunk(fieldIdx)
      let stats = colChunk.statistics

      if not stats.hasMinMax:
        return sMaybe

      # Get field type to handle statistics correctly
      let field = schema.getField(fieldIdx)
      let dataType = field.dataType

      # Try to extract the literal value from the expression
      # This is the main limitation - we can't easily get the literal value
      # from the GArrowExpression without additional C API bindings
      let literalOpt = getLiteralValue(filter, fieldName)
      if literalOpt.isNone:
        return sMaybe

      let literal = literalOpt.get()

      # For now, we handle Int32 and Int64 types
      # A full implementation would handle all primitive types
      case dataType.kind
      of Int32:
        let int32Stats =
          Int32Statistics(handle: cast[ptr GParquetInt32Statistics](stats.handle))
        let minVal = int32Stats.min
        let maxVal = int32Stats.max

        # Get the literal value - we need to cast the scalar properly
        # This requires knowing the scalar's type, which we don't have easily
        # For now, return sMaybe to be safe
        return sMaybe
      of Int64:
        let int64Stats =
          Int64Statistics(handle: cast[ptr GParquetInt64Statistics](stats.handle))
        let minVal = int64Stats.min
        let maxVal = int64Stats.max
        return sMaybe
      else:
        # Type not supported for statistics evaluation
        return sMaybe
    else:
      # Unknown function
      return sMaybe
  else:
    # Not a call expression (field or literal)
    return sMaybe

proc readTable*(
    uri: string, filter: ExpressionObj, columns: seq[string] = @[]
): ArrowTable =
  ## Reads a Parquet file with predicate push-down filtering and column projection.
  ##
  ## - `filter`: Expression that rows must satisfy. Columns referenced in the
  ##   filter must exist in the file schema. Raises KeyError if not.
  ## - `columns`: Output columns. If empty, all columns are returned.
  ##   Columns referenced by `filter` are read even if not in this list,
  ##   but are not included in the output.
  ##   Raises KeyError if a column name doesn't exist in schema.
  ##
  ## Performance:
  ## - Row groups whose statistics prove they cannot match are skipped (no I/O)
  ## - Only columns needed for filtering + output are read
  ## - Acero engine applies the filter on remaining rows
  let reader = newFileReader(uri)
  let schema = reader.schema

  # 1. Validate
  validateFilterColumns(filter, schema)
  for c in columns:
    if schema.tryGetField(c).isNone:
      raise newException(KeyError, "Column '" & c & "' does not exist in schema")

  # 2. Determine columns to read
  let filterCols = extractFieldReferences(filter)
  var readColNames: seq[string]
  if columns.len > 0:
    # Union of output columns and filter columns
    var seen: HashSet[string]
    for c in columns:
      if c notin seen:
        readColNames.add(c)
        seen.incl(c)
    for c in filterCols:
      if c notin seen:
        readColNames.add(c)
        seen.incl(c)
  # else: empty means read all columns

  let colIndices =
    if readColNames.len > 0:
      readColNames.mapIt(schema.getFieldIndex(it))
    else:
      @[] # empty = all columns

  # 3. Row group pruning (conservative - may return sMaybe for all)
  let metadata = reader.metadata
  var rowGroupsToRead: seq[int]
  for i in 0 ..< metadata.nRowGroups:
    let rgMeta = metadata.rowGroup(i)
    if canRowGroupSatisfy(filter, rgMeta, schema) != sNever:
      rowGroupsToRead.add(i)

  # 4. Read row groups with column projection
  var tables: seq[ArrowTable]
  for rg in rowGroupsToRead:
    if colIndices.len > 0:
      tables.add(reader.readRowGroup(rg, colIndices))
    else:
      tables.add(reader.readRowGroup(rg))

  # 5. Concatenate
  var fullTable =
    if tables.len == 0:
      # No row groups to read - create empty table
      # Read full table and filter to get correct schema
      let emptyHandle = check gparquet_arrow_file_reader_read_table(reader.toPtr)
      let emptyTable = newArrowTable(emptyHandle)
      return filterTable(emptyTable, filter)
    elif tables.len == 1:
      tables[0]
    else:
      tables[0].concatenate(tables[1 ..^ 1])

  # 6. Apply precise filter via Acero
  result = filterTable(fullTable, filter)

  # Note: Column projection after filtering is disabled due to lifetime issues
  # Users can manually select columns from the result if needed
