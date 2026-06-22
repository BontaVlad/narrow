## Parquet file reading and writing.
##
## Parquet is a columnar storage format with efficient compression and
## encoding. Use `readTable`/`writeTable` for simple cases, or `FileReader`/
## `FileWriter` for fine-grained control over row groups and columns.
import std/[options, sequtils, sets]
import ../core/[ffi, error, utils]
import ../types/[gtypes]
import ./filesystem
import ../column/[metadata, primitive]
import ../tabular/[table, batch]
import ../compute/[acero, expressions, statistics]

arcGObject:
  type
    FileReader* = object
      ## Reader for Parquet files. Supports reading full tables, individual row groups, or single columns.
      handle*: ptr GParquetArrowFileReader

    FileWriter* = object
      ## Writer for Parquet files. Supports configurable compression via `WriterProperties`.
      handle*: ptr GParquetArrowFileWriter

    WriterProperties* = object
      ## Configuration for Parquet writing: compression codec, row group size, etc.
      handle*: ptr GParquetWriterProperties

    # Metadata Types
    ColumnChunkMetadata* = object
      ## Metadata for a single column chunk within a row group.
      handle*: ptr GParquetColumnChunkMetadata

    FileMetadata* = object ## Metadata for an entire Parquet file.
      handle*: ptr GParquetFileMetadata

# RowGroupMetadata manually managed to work around Arrow GLib bug:
# dispose is never assigned (only finalize), so owner FileMetadata
# is never unreffed and raw C++ pointer is never freed.
type RowGroupMetadata* = object ## Metadata for a single row group.
  handle*: ptr GParquetRowGroupMetadata

proc getRowGroupPrivateOwner*(
    handle: ptr GParquetRowGroupMetadata
): ptr GParquetFileMetadata =
  # G_DEFINE_TYPE_WITH_PRIVATE puts private data at offset -16 from object pointer
  let priv = cast[pointer](cast[uint](handle) - 16)
  # Private struct layout: metadata (ptr), owner (ptr)
  # Owner is at offset sizeof(pointer) = 8 on 64-bit
  result = cast[ptr ptr GParquetFileMetadata](cast[uint](priv) + 8)[]

proc `=destroy`*(rgm: RowGroupMetadata) =
  g_object_unref(rgm.handle)
  # if not isNil(rgm.handle):
  #   # Workaround: manually unref the owner that Arrow GLib leaks
  #   let owner = getRowGroupPrivateOwner(rgm.handle)
  #   if not isNil(owner):
  #     g_object_unref(owner)
  #   g_object_unref(rgm.handle)

proc `=wasMoved`*(rgm: var RowGroupMetadata) =
  rgm.handle = nil

proc `=dup`*(rgm: RowGroupMetadata): RowGroupMetadata {.nodestroy.} =
  result.handle = rgm.handle
  if not isNil(rgm.handle):
    discard g_object_ref(rgm.handle)

proc `=copy`*(dest: var RowGroupMetadata, src: RowGroupMetadata) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(dest.handle):
      discard g_object_ref(dest.handle)

proc toPtr*(rgm: RowGroupMetadata): ptr GParquetRowGroupMetadata {.inline.} =
  rgm.handle

type Writable* = concept w
  w.schema is Schema
  w.toPtr is ptr GArrowTable | ptr GArrowRecordBatch

proc newFileReader*(sis: SeekableInputStream): FileReader =
  ## Creates a Parquet file reader from a seekable input stream.
  result.handle = verify gparquet_arrow_file_reader_new_arrow(sis.toPtr)

proc newFileReader*(uri: string): FileReader =
  ## Creates a Parquet file reader from a file URI.
  result.handle = verify gparquet_arrow_file_reader_new_path(uri)

proc schema*(pfr: FileReader): Schema =
  let handle = verify gparquet_arrow_file_reader_get_schema(pfr.toPtr)
  result = newSchema(handle)

func nRowGroups*(pfr: FileReader): int {.inline.} =
  gparquet_arrow_file_reader_get_n_row_groups(pfr.toPtr)

func nRows*(pfr: FileReader): int64 {.inline.} =
  ## Returns the number of rows in the file.
  gparquet_arrow_file_reader_get_n_rows(pfr.toPtr)

proc close*(pfr: FileReader) =
  ## Closes the reader.
  gparquet_arrow_file_reader_close(pfr.toPtr)

proc readRowGroup*(pfr: FileReader, rowGroupIndex: int): ArrowTable =
  ## Reads a specific row group, returning all columns.
  let handle = verify gparquet_arrow_file_reader_read_row_group(
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

  let handle = verify gparquet_arrow_file_reader_read_row_group(
    pfr.toPtr, rowGroupIndex.gint, addr indices[0], indices.len.gsize
  )
  result = newArrowTable(handle)

proc readColumnData*(pfr: FileReader, columnIndex: int): ChunkedArray[void] =
  let handle =
    verify gparquet_arrow_file_reader_read_column_data(pfr.toPtr, columnIndex.gint)
  result = newChunkedArray[void](handle)

proc `useThreads=`*(pfr: FileReader, useThreads: bool) =
  gparquet_arrow_file_reader_set_use_threads(pfr.toPtr, useThreads.gboolean)

proc nColumns*(pfr: FileReader): int =
  schema(pfr).nFields

proc metadata*(reader: FileReader): FileMetadata =
  let handle = gparquet_arrow_file_reader_get_metadata(reader.toPtr)
  result.handle = handle
  # if not isNil(handle):
  #   discard g_object_ref(handle)

proc newFileWriter*(uri: string, schema: Schema, wp: WriterProperties): FileWriter =
  ## Creates a Parquet file writer writing to a file URI.
  result.handle =
    verify gparquet_arrow_file_writer_new_path(schema.toPtr, uri.cstring, wp.toPtr)

proc newFileWriter*(
    snk: OutputStream, schema: Schema, wp: WriterProperties
): FileWriter =
  ## Creates a Parquet file writer writing to an output stream.
  result.handle =
    verify gparquet_arrow_file_writer_new_arrow(schema.toPtr, snk.toPtr, wp.toPtr)

proc close*(fw: FileWriter) =
  ## Closes the writer, finalizing the file footer. Must be called.
  verify gparquet_arrow_file_writer_close(fw.toPtr)

proc newRowGroup*(fw: FileWriter) =
  verify gparquet_arrow_file_writer_new_row_group(fw.toPtr)

proc newBufferedRowGroup*(fw: FileWriter) =
  verify gparquet_arrow_file_writer_new_buffered_row_group(fw.toPtr)

proc writeChunkedArray*(fw: FileWriter, chunkedArray: ChunkedArray) =
  verify gparquet_arrow_file_writer_write_chunked_array(fw.toPtr, chunkedArray.handle)

proc writeRecordBatch*(fw: FileWriter, rb: RecordBatch) =
  verify gparquet_arrow_file_writer_write_record_batch(fw.toPtr, rb.toPtr)

proc schema*(fw: FileWriter): Schema =
  let handle = gparquet_arrow_file_writer_get_schema(fw.toPtr)
  result = newSchema(handle)

proc newWriterProperties*(): WriterProperties =
  result.handle = gparquet_writer_properties_new()

func dictionaryPageSizeLimit*(wp: WriterProperties): int64 {.inline.} =
  gparquet_writer_properties_get_dictionary_page_size_limit(wp.handle)

proc `dictionaryPageSizeLimit=`*(wp: var WriterProperties, limit: int64) =
  gparquet_writer_properties_set_dictionary_page_size_limit(wp.handle, limit)

func batchSize*(wp: WriterProperties): int64 {.inline.} =
  gparquet_writer_properties_get_batch_size(wp.handle)

proc `batchSize=`*(wp: var WriterProperties, size: int64) =
  gparquet_writer_properties_set_batch_size(wp.handle, size)

func maxRowGroupLength*(wp: WriterProperties): int64 {.inline.} =
  gparquet_writer_properties_get_max_row_group_length(wp.handle)

proc `maxRowGroupLength=`*(wp: var WriterProperties, length: int64) =
  gparquet_writer_properties_set_max_row_group_length(wp.handle, length)

func dataPageSize*(wp: WriterProperties): int64 {.inline.} =
  gparquet_writer_properties_get_data_page_size(wp.handle)

proc `dataPageSize=`*(wp: var WriterProperties, size: int64) =
  gparquet_writer_properties_set_data_page_size(wp.handle, size)

proc setCompression*(
    wp: WriterProperties, path: string, compression: GArrowCompressionType
) =
  ## Sets the compression codec for a specific column.
  gparquet_writer_properties_set_compression(wp.handle, compression, path.cstring)

func compression*(
    wp: WriterProperties, path: string
): GArrowCompressionType {.inline.} =
  gparquet_writer_properties_get_compression_path(wp.handle, path.cstring)

proc enableDictionary*(wp: WriterProperties, path: string) =
  gparquet_writer_properties_enable_dictionary(wp.handle, path.cstring)

proc disableDictionary*(wp: WriterProperties, path: string) =
  gparquet_writer_properties_disable_dictionary(wp.handle, path.cstring)

func isDictionaryEnabled*(wp: WriterProperties, path: string): bool {.inline.} =
  gparquet_writer_properties_is_dictionary_enabled(wp.handle, path.cstring) == 1

proc newColumnChunkMetadata*(
    handle: ptr GParquetColumnChunkMetadata
): ColumnChunkMetadata =
  result.handle = handle

proc newRowGroupMetadata*(handle: ptr GParquetRowGroupMetadata): RowGroupMetadata =
  result.handle = handle

proc newFileMetadata*(handle: ptr GParquetFileMetadata): FileMetadata =
  result.handle = handle

func `==`*(a, b: ColumnChunkMetadata): bool {.inline.} =
  gparquet_column_chunk_metadata_equal(a.toPtr, b.toPtr) == 1

func totalSize*(m: ColumnChunkMetadata): int64 {.inline.} =
  gparquet_column_chunk_metadata_get_total_size(m.toPtr)

func totalCompressedSize*(m: ColumnChunkMetadata): int64 {.inline.} =
  gparquet_column_chunk_metadata_get_total_compressed_size(m.toPtr)

func fileOffset*(m: ColumnChunkMetadata): int64 {.inline.} =
  gparquet_column_chunk_metadata_get_file_offset(m.toPtr)

func canDecompress*(m: ColumnChunkMetadata): bool {.inline.} =
  gparquet_column_chunk_metadata_can_decompress(m.toPtr) == 1

proc statistics*(m: ColumnChunkMetadata): Statistics =
  let handle = gparquet_column_chunk_metadata_get_statistics(m.toPtr)
  result.handle = handle

func `==`*(a, b: RowGroupMetadata): bool {.inline.} =
  gparquet_row_group_metadata_equal(a.toPtr, b.toPtr) == 1

func nColumns*(m: RowGroupMetadata): int {.inline.} =
  gparquet_row_group_metadata_get_n_columns(m.toPtr)

proc columnChunk*(m: RowGroupMetadata, index: int): ColumnChunkMetadata =
  result.handle =
    verify gparquet_row_group_metadata_get_column_chunk(m.toPtr, index.gint)

func nRows*(m: RowGroupMetadata): int64 {.inline.} =
  gparquet_row_group_metadata_get_n_rows(m.toPtr)

func totalSize*(m: RowGroupMetadata): int64 {.inline.} =
  gparquet_row_group_metadata_get_total_size(m.toPtr)

func totalCompressedSize*(m: RowGroupMetadata): int64 {.inline.} =
  gparquet_row_group_metadata_get_total_compressed_size(m.toPtr)

func fileOffset*(m: RowGroupMetadata): int64 {.inline.} =
  gparquet_row_group_metadata_get_file_offset(m.toPtr)

func canDecompress*(m: RowGroupMetadata): bool {.inline.} =
  gparquet_row_group_metadata_can_decompress(m.toPtr) == 1

func `==`*(a, b: FileMetadata): bool {.inline.} =
  gparquet_file_metadata_equal(a.toPtr, b.toPtr) == 1

func nColumns*(m: FileMetadata): int {.inline.} =
  gparquet_file_metadata_get_n_columns(m.toPtr)

func nSchemaElements*(m: FileMetadata): int {.inline.} =
  gparquet_file_metadata_get_n_schema_elements(m.toPtr)

func nRows*(m: FileMetadata): int64 {.inline.} =
  gparquet_file_metadata_get_n_rows(m.toPtr)

func nRowGroups*(m: FileMetadata): int {.inline.} =
  gparquet_file_metadata_get_n_row_groups(m.toPtr)

proc rowGroup*(m: FileMetadata, index: int): RowGroupMetadata =
  newRowGroupMetadata(verify gparquet_file_metadata_get_row_group(m.toPtr, index.gint))

func createdBy*(m: FileMetadata): string {.inline.} =
  $gparquet_file_metadata_get_created_by(m.toPtr)

func size*(m: FileMetadata): uint32 {.inline.} =
  gparquet_file_metadata_get_size(m.toPtr)

func canDecompress*(m: FileMetadata): bool {.inline.} =
  gparquet_file_metadata_can_decompress(m.toPtr) == 1

proc rowGroupGuarantee*(
    rowGroupMeta: RowGroupMetadata, referencedFields: HashSet[string], schema: Schema
): Expression =
  ## Builds a combined guarantee expression for a row group by
  ## AND-ing together the statistics expressions for each referenced column.
  ##
  ## Only columns that appear in the predicate are considered.
  ## 
  var guarantee: Expression = nil
  for fieldName in referencedFields:
    let colIdx = schema.getFieldIndex(fieldName)

    let columnMeta = rowGroupMeta.columnChunk(colIdx)
    let stats = columnMeta.statistics

    let statsExpr = statisticsAsExpression(fieldName, stats)
    if statsExpr.isSome:
      if guarantee.isNil:
        guarantee = statsExpr.get()
      else:
        guarantee = guarantee and statsExpr.get()

  if guarantee.isNil:
    guarantee = newLiteralExpression(true)

  result = guarantee

proc filterRowGroups*(
    fileMeta: FileMetadata, schema: Schema, predicate: Expression
): seq[int] =
  ## Evaluates the predicate against each row group's statistics and
  ## returns the indices of row groups that might contain matching data.
  ##
  ## This is the equivalent of Arrow's `ParquetFileFragment::FilterRowGroups`.

  let fields = referencedFields(predicate)
  let numRowGroups = fileMeta.nRowGroups

  result = @[]

  result = newSeqOfCap[int](numRowGroups)
  for i in 0 ..< numRowGroups:
    let rowGroupMeta = fileMeta.rowGroup(i)

    # Build the guarantee expression from this row group's statistics
    let guarantee = rowGroupGuarantee(rowGroupMeta, fields, schema)

    # Simplify the predicate with the guarantee
    let simplified = simplifyWithGuarantee(predicate, guarantee)

    # If the simplified expression is satisfiable, keep this row group
    if simplified.isSatisfiable:
      result.add(i)

proc readTable*(uri: string): ArrowTable =
  ## Reads a Parquet file into an `ArrowTable`.
  let reader = newFileReader(uri)
  let handle = verify gparquet_arrow_file_reader_read_table(reader.toPtr)
  result = newArrowTable(handle)

proc readTable*(uri: string, columns: sink seq[string]): ArrowTable =
  ## Reads a Parquet file, returning only the specified columns.
  ## Raises KeyError if any column name does not exist in the schema.
  let reader = newFileReader(uri)
  let schema = reader.schema

  var data = newSeqOfCap[ChunkedArray[void]](columns.len)
  var fields = newSeqOfCap[Field](columns.len)
  for c in columns:
    let idx = schema.getFieldIndex(c)
    data.add reader.readColumnData(idx)
    fields.add schema[idx]
  let tableSchema = newSchema(fields)
  result = newArrowTable(tableSchema, data)

proc readTable*(
    uri: string, filter: Expression, columns: sink seq[string] = @[]
): ArrowTable =
  let reader = newFileReader(uri)
  let schema = reader.schema

  let schemaColumns = toHashSet(schema.ffields.mapIt(it.name))
  let filterCols = extractFieldReferences(filter)

  let readColumns =
    if columns.len > 0:
      toHashSet(columns)
    else:
      schemaColumns

  let missingRead = readColumns - schemaColumns
  if missingRead.len > 0:
    raise newException(KeyError, "Requested columns not in schema: " & $(missingRead))

  let missingFilter = filterCols - schemaColumns
  if missingFilter.len > 0:
    raise
      newException(KeyError, "Filter references missing columns: " & $(missingFilter))

  let metadata = reader.metadata
  let rowGroupIndices = filterRowGroups(metadata, schema, filter)
  var tables = newSeqOfCap[ArrowTable](rowGroupIndices.len)

  if rowGroupIndices.len == 0:
    # No row groups match the filter, return an empty table with the requested schema
    let resultSchema =
      if columns.len > 0:
        newSchema(columns.mapIt(schema[it]))
      else:
        schema
    result = newArrowTable(resultSchema, newSeq[ChunkedArray[void]]())
  else:
    # Build deterministic column indices: requested columns first (in order),
    # then filter-only columns (in schema order)
    var columnIndices = newSeq[int]()
    var columnNames = newSeq[string]()

    if columns.len > 0:
      for c in columns:
        columnIndices.add(schema.getFieldIndex(c))
        columnNames.add(c)
      for f in schema.ffields:
        if f.name in filterCols and f.name notin readColumns:
          columnIndices.add(schema.getFieldIndex(f.name))
          columnNames.add(f.name)
    else:
      for f in schema.ffields:
        columnIndices.add(schema.getFieldIndex(f.name))
        columnNames.add(f.name)

    for rgi in rowGroupIndices:
      tables.add(reader.readRowGroup(rgi, columnIndices))

    let filtered = filterTable(tables[0].concatenate(tables[1 ..^ 1]), filter)

    # Project to only requested columns
    if columns.len > 0:
      var selectedFields = newSeqOfCap[Field](columns.len)
      var selectedData = newSeqOfCap[ChunkedArray[void]](columns.len)
      for c in columns:
        selectedFields.add(schema.tryGetField(c).get())
        selectedData.add(filtered[c])
      result = newArrowTable(newSchema(selectedFields), selectedData)
    else:
      result = filtered

proc writeTable*[T: Writable](
    writable: T,
    uri: string,
    chunk_size: int = 65536,
    wp: WriterProperties = newWriterProperties(),
) =
  ## Writes a table to a Parquet file.
  let writer = newFileWriter(uri, writable.schema, wp)
  defer:
    writer.close()
  when writable is ArrowTable:
    verify gparquet_arrow_file_writer_write_table(
      writer.toPtr, writable.toPtr, chunk_size.gsize
    )
  elif writable is RecordBatch:
    verify gparquet_arrow_file_writer_write_record_batch(writer.toPtr, writable.toPtr)
