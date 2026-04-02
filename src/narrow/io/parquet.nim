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
      handle*: ptr GParquetArrowFileReader

    FileWriter* = object
      handle*: ptr GParquetArrowFileWriter

    WriterProperties* = object
      handle*: ptr GParquetWriterProperties

    # Metadata Types
    ColumnChunkMetadata* = object
      handle*: ptr GParquetColumnChunkMetadata

    RowGroupMetadata* = object
      handle*: ptr GParquetRowGroupMetadata

    FileMetadata* = object
      handle*: ptr GParquetFileMetadata

type Writable* =
  concept w
      w.schema is Schema
      w.toPtr is ptr GArrowTable | ptr GArrowRecordBatch

proc newFileReader*(sis: SeekableInputStream): FileReader =
  result.handle = check gparquet_arrow_file_reader_new_arrow(sis.toPtr)

proc newFileReader*(uri: string): FileReader =
  result.handle = check gparquet_arrow_file_reader_new_path(uri)

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

proc newFileWriter*(uri: string, schema: Schema, wp: WriterProperties): FileWriter =
  result.handle =
    check gparquet_arrow_file_writer_new_path(schema.toPtr, uri.cstring, wp.toPtr)

proc newFileWriter*(
    snk: OutputStream, schema: Schema, wp: WriterProperties
): FileWriter =
  result.handle =
    check gparquet_arrow_file_writer_new_arrow(schema.toPtr, snk.toPtr, wp.toPtr)

proc close*(fw: FileWriter) =
  check gparquet_arrow_file_writer_close(fw.toPtr)

proc newRowGroup*(fw: FileWriter) =
  check gparquet_arrow_file_writer_new_row_group(fw.toPtr)

proc schema*(fw: FileWriter): Schema =
  let handle = gparquet_arrow_file_writer_get_schema(fw.toPtr)
  result = newSchema(handle)

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

proc newColumnChunkMetadata*(
    handle: ptr GParquetColumnChunkMetadata
): ColumnChunkMetadata =
  result.handle = handle

proc newRowGroupMetadata*(handle: ptr GParquetRowGroupMetadata): RowGroupMetadata =
  result.handle = handle

proc newFileMetadata*(handle: ptr GParquetFileMetadata): FileMetadata =
  result.handle = handle

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

proc rowGroup*(m: FileMetadata, index: int): RowGroupMetadata =
  newRowGroupMetadata(check gparquet_file_metadata_get_row_group(m.toPtr, index.gint))

proc createdBy*(m: FileMetadata): string {.inline.} =
  $gparquet_file_metadata_get_created_by(m.toPtr)

proc size*(m: FileMetadata): uint32 {.inline.} =
  gparquet_file_metadata_get_size(m.toPtr)

proc canDecompress*(m: FileMetadata): bool {.inline.} =
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
  let reader = newFileReader(uri)
  let handle = check gparquet_arrow_file_reader_read_table(reader.toPtr)
  result = newArrowTable(handle)

proc readTable*(uri: string, columns: sink seq[string]): ArrowTable =
  ## Reads a Parquet file, returning only the specified columns.
  ## Raises KeyError if any column name does not exist in the schema.
  let reader = newFileReader(uri)
  let schema = reader.schema

  var fields = newSeq[Field]()
  var data = newSeq[ChunkedArray[void]]()
  for c in columns:
    let fld = schema.tryGetField(c)
    if fld.isNone:
      raise newException(KeyError, "Column '" & c & "' does not exist in schema")
    let index = schema.getFieldIndex(c)
    data.add(reader.readColumnData(index))

  let tableSchema = newSchema(fields)
  result = newArrowTable(tableSChema, data)

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

  let neededColumns = readColumns + filterCols
  let metadata = reader.metadata
  let columnIndices = neededColumns.mapIt(schema.getFieldIndex(it))
  var tables: seq[ArrowTable]

  let rowGroupIndices = filterRowGroups(metadata, schema, filter)

  if rowGroupIndices.len == 0:
    # No row groups match the filter, return an empty table with the requested schema
    result = newArrowTableFromArrays(newSchema(neededColumns.mapIt(schema[it])), @[])
  else:
    for rgi in rowGroupIndices:
      tables.add(reader.readRowGroup(rgi, columnIndices))
    result = filterTable(tables[0].concatenate(tables[1 ..^ 1]), filter)

proc writeTable*[T: Writable](
    writable: T,
    uri: string,
    chunk_size: int = 65536,
    wp: WriterProperties = newWriterProperties(),
) =
  let writer = newFileWriter(uri, writable.schema, wp)
  defer:
    writer.close()
  when writable is ArrowTable:
    check gparquet_arrow_file_writer_write_table(
      writer.toPtr, writable.toPtr, chunk_size.gsize
    )
  elif writable is RecordBatch:
    check gparquet_arrow_file_writer_write_record_batch(writer.toPtr, writable.toPtr)
