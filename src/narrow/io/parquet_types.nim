import ../core/[ffi]

type
  FileReader* = object
    handle*: ptr GParquetArrowFileReader

  FileWriter* = object
    handle*: ptr GParquetArrowFileWriter

  WriterProperties* = object
    handle*: ptr GParquetWriterProperties

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

proc toPtr*(fr: FileReader): ptr GParquetArrowFileReader {.inline.} =
  fr.handle

proc toPtr*(fw: FileWriter): ptr GParquetArrowFileWriter {.inline.} =
  fw.handle

proc toPtr*(wp: WriterProperties): ptr GParquetWriterProperties {.inline.} =
  wp.handle

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
