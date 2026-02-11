import ./[ffi, error, parquet]

type
  Statistics* = object
    handle: ptr GParquetStatistics

  BooleanStatistics* = object
    handle: ptr GParquetBooleanStatistics

  Int32Statistics* = object
    handle: ptr GParquetInt32Statistics

  Int64Statistics* = object
    handle: ptr GParquetInt64Statistics

  FloatStatistics* = object
    handle: ptr GParquetFloatStatistics

  DoubleStatistics* = object
    handle: ptr GParquetDoubleStatistics

  ByteArrayStatistics* = object
    handle: ptr GParquetByteArrayStatistics

  FixedLengthByteArrayStatistics* = object
    handle: ptr GParquetFixedLengthByteArrayStatistics

  ColumnChunkMetadata* = object
    handle: ptr GParquetColumnChunkMetadata

  RowGroupMetadata* = object
    handle: ptr GParquetRowGroupMetadata

  FileMetadata* = object
    handle: ptr GParquetFileMetadata

# Statistics hooks
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
