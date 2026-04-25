import ../core/[ffi, error]
import ../tabular/table
import ./filesystem

# ============================================================================
# Type Definitions
# ============================================================================

type
  FeatherReader* = object
    ## Reader for Feather file format
    ## Wraps GArrowFeatherFileReader
    handle: ptr GArrowFeatherFileReader
    stream: SeekableInputStream

  FeatherWriteProperties* = object
    ## Properties for writing Feather files (compression, etc.)
    ## Wraps GArrowFeatherWriteProperties
    handle: ptr GArrowFeatherWriteProperties

# ============================================================================
# FeatherReader - ARC Hooks
# ============================================================================

proc `=destroy`*(reader: FeatherReader) =
  if not isNil(reader.handle):
    g_object_unref(reader.handle)
  `=destroy`(reader.stream)

proc `=wasMoved`*(reader: var FeatherReader) =
  reader.handle = nil
  `=wasMoved`(reader.stream)

proc `=dup`*(reader: FeatherReader): FeatherReader =
  result.handle = reader.handle
  result.stream = reader.stream
  if not isNil(reader.handle):
    discard g_object_ref(reader.handle)

proc `=copy`*(dest: var FeatherReader, src: FeatherReader) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)
  `=copy`(dest.stream, src.stream)

proc toPtr*(reader: FeatherReader): ptr GArrowFeatherFileReader {.inline.} =
  reader.handle

# ============================================================================
# FeatherWriteProperties - ARC Hooks
# ============================================================================

proc `=destroy`*(props: FeatherWriteProperties) =
  if not isNil(props.handle):
    g_object_unref(props.handle)

proc `=wasMoved`*(props: var FeatherWriteProperties) =
  props.handle = nil

proc `=dup`*(props: FeatherWriteProperties): FeatherWriteProperties =
  result.handle = props.handle
  if not isNil(props.handle):
    discard g_object_ref(props.handle)

proc `=copy`*(dest: var FeatherWriteProperties, src: FeatherWriteProperties) =
  if dest.handle != src.handle:
    if not isNil(dest.handle):
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if not isNil(src.handle):
      discard g_object_ref(dest.handle)

proc toPtr*(
    props: FeatherWriteProperties
): ptr GArrowFeatherWriteProperties {.inline.} =
  props.handle

# ============================================================================
# FeatherReader - Constructors
# ============================================================================

proc newFeatherReader*(stream: SeekableInputStream): FeatherReader =
  ## Create a Feather file reader from a seekable input stream
  let handle = verify garrow_feather_file_reader_new(stream.handle)
  result.handle = handle
  result.stream = stream

proc newFeatherReader*(fs: FileSystem, path: string): FeatherReader =
  ## Create a Feather file reader from a filesystem path
  let stream = fs.openInputFile(path)
  result = newFeatherReader(stream)

# ============================================================================
# FeatherReader - Methods
# ============================================================================

proc version*(reader: FeatherReader): int =
  ## Get the Feather format version
  int(garrow_feather_file_reader_get_version(reader.handle))

proc read*(reader: FeatherReader): ArrowTable =
  ## Read the entire table from the Feather file
  let handle = verify garrow_feather_file_reader_read(reader.handle)
  result = newArrowTable(handle)

proc readIndices*(reader: FeatherReader, indices: openArray[int]): ArrowTable =
  ## Read specific columns by their indices (0-based)
  ## This allows selective column reading for better performance
  if indices.len == 0:
    return reader.read()

  var cIndices = newSeq[gint](indices.len)
  for i, idx in indices:
    cIndices[i] = idx.gint

  let handle = verify garrow_feather_file_reader_read_indices(
    reader.handle, addr cIndices[0], indices.len.guint
  )
  result = newArrowTable(handle)

proc readNames*(reader: FeatherReader, names: openArray[string]): ArrowTable =
  ## Read specific columns by their names
  ## This allows selective column reading for better performance
  if names.len == 0:
    return reader.read()

  var cNames = newSeq[cstring](names.len)
  for i, name in names:
    cNames[i] = name.cstring

  let handle = verify garrow_feather_file_reader_read_names(
    reader.handle, addr cNames[0], names.len.guint
  )
  result = newArrowTable(handle)

# ============================================================================
# FeatherWriteProperties - Properties
# ============================================================================

proc `compression=`*(
    props: FeatherWriteProperties, compression: GArrowCompressionType
) =
  ## Set the compression type for writing Feather files
  ## Only UNCOMPRESSED, LZ4, and ZSTD are supported by Feather format
  g_object_set(props.handle, "compression", compression.cint, nil)

proc compression*(props: FeatherWriteProperties): GArrowCompressionType =
  ## Get the current compression type
  var value: cint
  g_object_get(props.handle, "compression", addr value, nil)
  result = value.GArrowCompressionType

# ============================================================================
# FeatherWriteProperties - Constructors
# ============================================================================

proc newFeatherWriteProperties*(): FeatherWriteProperties =
  ## Create new write properties with default settings
  ## Default compression is LZ4 if available, otherwise uncompressed
  let handle = garrow_feather_write_properties_new()
  if handle.isNil:
    raise newException(IOError, "Failed to create FeatherWriteProperties")
  result.handle = handle

# ============================================================================
# High-level Convenience API
# ============================================================================

proc readFeatherFile*(uri: string): ArrowTable =
  ## Read an entire Feather file into a table
  let fs = newFileSystem(uri)
  let reader = newFeatherReader(fs, uri)
  result = reader.read()

proc readFeatherFile*(uri: string, columns: openArray[string]): ArrowTable =
  ## Read specific columns from a Feather file
  ## Raises KeyError if any column name does not exist
  let fs = newFileSystem(uri)
  let reader = newFeatherReader(fs, uri)
  result = reader.readNames(columns)

proc readFeatherFile*(uri: string, columnIndices: openArray[int]): ArrowTable =
  ## Read specific columns by index from a Feather file
  let fs = newFileSystem(uri)
  let reader = newFeatherReader(fs, uri)
  result = reader.readIndices(columnIndices)

proc writeFeatherFile*(uri: string, table: ArrowTable) =
  ## Write a table to a Feather file
  ## Uses default compression (LZ4 if available, otherwise uncompressed)
  let fs = newFileSystem(uri)
  let stream = fs.openOutputStream(uri)
  let props = newFeatherWriteProperties()
  verify garrow_table_write_as_feather(table.toPtr, stream.handle, props.handle)

proc writeFeatherFile*(uri: string, table: ArrowTable, props: FeatherWriteProperties) =
  ## Write a table to a Feather file with custom write properties
  let fs = newFileSystem(uri)
  let stream = fs.openOutputStream(uri)
  verify garrow_table_write_as_feather(table.toPtr, stream.handle, props.handle)
