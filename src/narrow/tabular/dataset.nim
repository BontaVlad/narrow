import ../core/[ffi, error, utils]
import ../compute/expressions
import ../column/metadata
import ../io/filesystem
import ../types/gtypes
import ./table
import ./batch

## Collection of data fragments and potentially child datasets.
##
## Arrow Datasets allow you to query against data that has been split across
## multiple files. This sharding of data may indicate partitioning, which
## can accelerate queries that only touch some partitions (files).
arcGObject:
  type
    Dataset* = object
      handle: ptr GADatasetDataset

    FileSystemDataset* = object
      handle: ptr GADatasetFileSystemDataset

type
  Fragment* {.inheritable.} = object
    handle: ptr GADatasetFragment

  InMemoryFragment* = object of Fragment

arcGObject:
  type
    Scanner* = object
      handle: ptr GADatasetScanner

    ScannerBuilder* = object
      handle: ptr GADatasetScannerBuilder

type
  FileFormatTp* = enum
    CSV
    IPC
    Parquet

  FileFormat* = object
    handle: ptr GADatasetFileFormat
    kind: FileFormatTp

type SegmentEncoding* = enum
  None
  URI

type
  DatasetFactory* {.inheritable.} = object
    handle: ptr GADatasetDatasetFactory

  FileSystemDatasetFactory* = object of DatasetFactory

arcGObject:
  type FinishOptions* = object
    handle: ptr GADatasetFinishOptions

type
  Partitioning* {.inheritable.} = object
    handle: ptr GADatasetPartitioning

  DirectoryPartitioning* = object of Partitioning

  HivePartitioning* = object of Partitioning

arcGObject:
  type
    KeyValuePartitioningOptions* = object
      handle: ptr GADatasetKeyValuePartitioningOptions

    HivePartitioningOptions* = object
      handle: ptr GADatasetHivePartitioningOptions

arcGObject:
  type
    FileWriter* = object
      handle: ptr GADatasetFileWriter

    FileWriteOptions* = object
      handle: ptr GADatasetFileWriteOptions

arcGObject:
  type PartitioningFactoryOptions* = object
    ## Options for discovering partitioning from file paths
    handle: ptr GADatasetPartitioningFactoryOptions

arcGObject:
  type FileSystemDatasetWriteOptions* = object
    handle*: ptr GADatasetFileSystemDatasetWriteOptions

ensureComputeInitialized()

proc `=destroy`*(frag: Fragment) =
  if frag.handle != nil:
    g_object_unref(frag.handle)

proc `=wasMoved`*(frag: var Fragment) =
  frag.handle = nil

proc `=dup`*(frag: Fragment): Fragment =
  result.handle = frag.handle
  if frag.handle != nil:
    discard g_object_ref(frag.handle)

proc `=copy`*(dest: var Fragment, src: Fragment) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(format: FileFormat) =
  if format.handle != nil:
    g_object_unref(format.handle)

proc `=wasMoved`*(format: var FileFormat) =
  format.handle = nil

proc `=dup`*(format: FileFormat): FileFormat =
  result.handle = format.handle
  if format.handle != nil:
    discard g_object_ref(format.handle)

proc `=copy`*(dest: var FileFormat, src: FileFormat) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(factory: DatasetFactory) =
  if factory.handle != nil:
    g_object_unref(factory.handle)

proc `=wasMoved`*(factory: var DatasetFactory) =
  factory.handle = nil

proc `=dup`*(factory: DatasetFactory): DatasetFactory =
  result.handle = factory.handle
  if factory.handle != nil:
    discard g_object_ref(factory.handle)

proc `=copy`*(dest: var DatasetFactory, src: DatasetFactory) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(factory: FileSystemDatasetFactory) =
  if factory.handle != nil:
    g_object_unref(factory.handle)

proc `=wasMoved`*(factory: var FileSystemDatasetFactory) =
  factory.handle = nil

proc `=dup`*(factory: FileSystemDatasetFactory): FileSystemDatasetFactory =
  result.handle = factory.handle
  if factory.handle != nil:
    discard g_object_ref(factory.handle)

proc `=copy`*(dest: var FileSystemDatasetFactory, src: FileSystemDatasetFactory) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(partitioning: Partitioning) =
  if partitioning.handle != nil:
    g_object_unref(partitioning.handle)

proc `=wasMoved`*(partitioning: var Partitioning) =
  partitioning.handle = nil

proc `=dup`*(partitioning: Partitioning): Partitioning =
  result.handle = partitioning.handle
  if partitioning.handle != nil:
    discard g_object_ref(partitioning.handle)

proc `=copy`*(dest: var Partitioning, src: Partitioning) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(frag: Fragment): ptr GADatasetFragment {.inline.} =
  frag.handle

proc toPtr*(format: FileFormat): ptr GADatasetFileFormat {.inline.} =
  format.handle

proc toPtr*(factory: DatasetFactory): ptr GADatasetDatasetFactory {.inline.} =
  factory.handle

proc toPtr*(partitioning: Partitioning): ptr GADatasetPartitioning {.inline.} =
  partitioning.handle

proc toPtr*(pt: HivePartitioning): ptr GADatasetHivePartitioning {.inline.} =
  cast[ptr GADatasetHivePartitioning](pt.handle)

# =======================================================
# Dataset and stuff
# =======================================================
proc toTable*(ds: Dataset): ArrowTable =
  ## Converts the dataset to an ArrowTable by reading all fragments
  let handle = verify gadataset_dataset_to_table(ds.toPtr)
  result = newArrowTable(handle)

proc newFileFormat*(format: FileFormatTp): FileFormat =
  case format
  of CSV:
    result.handle = cast[ptr GADatasetFileFormat](gadataset_csv_file_format_new())
  of IPC:
    result.handle = cast[ptr GADatasetFileFormat](gadataset_ipc_file_format_new())
  of Parquet:
    result.handle = cast[ptr GADatasetFileFormat](gadataset_parquet_file_format_new())
  result.kind = format

proc kind*(fmt: FileFormat): FileFormatTp =
  fmt.kind

proc newPartitioningFactoryOptions*(): PartitioningFactoryOptions =
  ## Creates default options for discovering partitioning from paths
  result.handle = gadataset_partitioning_factory_options_new()

proc getTypeName*(partitioning: Partitioning): string =
  ## Returns the type name of the partitioning (e.g., "directory", "hive")
  if partitioning.handle == nil:
    return ""
  result = $gadataset_partitioning_get_type_name(partitioning.toPtr)

proc newDefaultPartitioning*(): Partitioning =
  ## Creates a default partitioning scheme (no partitioning)
  result.handle = gadataset_partitioning_create_default()

proc newDirectoryPartitioning*(schema: Schema): DirectoryPartitioning =
  # TODO: impplement dictionaries for partitioning
  result.handle = cast[ptr GADatasetPartitioning](verify gadataset_directory_partitioning_new(
    schema.toPtr, nil, nil
  ))

proc newHivePartitioningOptions*(): HivePartitioningOptions =
  result.handle = gadataset_hive_partitioning_options_new()

proc newHivePartitioning*(schema: Schema): HivePartitioning =
  # TODO: impplement dictionaries for partitioning
  let opts = newHivePartitioningOptions()
  result.handle = cast[ptr GADatasetPartitioning](verify gadataset_hive_partitioning_new(
    schema.toPtr, nil, opts.toPtr
  ))

proc newKeyValuePartitioningOptions*(): KeyValuePartitioningOptions =
  ## Creates default options for key-value partitioning
  result.handle = gadataset_key_value_partitioning_options_new()

proc newDirectoryPartitioning*(
    schema: Schema, options: KeyValuePartitioningOptions
): DirectoryPartitioning =
  ## Creates a directory partitioning scheme with custom options
  # TODO: implement dictionaries for partitioning
  result.handle = cast[ptr GADatasetPartitioning](verify gadataset_directory_partitioning_new(
    schema.toPtr, nil, options.toPtr
  ))

proc newHivePartitioning*(
    schema: Schema, options: HivePartitioningOptions
): HivePartitioning =
  ## Creates a Hive partitioning scheme with custom options
  # TODO: implement dictionaries for partitioning
  result.handle = cast[ptr GADatasetPartitioning](verify gadataset_hive_partitioning_new(
    schema.toPtr, nil, options.toPtr
  ))

proc segmentEncoding*(opts: KeyValuePartitioningOptions): SegmentEncoding =
  ## Gets the segment encoding for path component decoding
  var encoding: cint
  g_object_get(opts.toPtr, "segment-encoding", addr encoding, nil)
  result = SegmentEncoding(encoding)

proc `segmentEncoding=`*(
    opts: var KeyValuePartitioningOptions, encoding: SegmentEncoding
) =
  ## Sets the segment encoding for path component decoding
  g_object_set(opts.toPtr, "segment-encoding", encoding.cint, nil)

proc nullFallback*(opts: HivePartitioningOptions): string =
  ## Gets the fallback string for null values in Hive partitioning
  var gstr = newGString(nil)
  g_object_get(opts.toPtr, "null-fallback", addr gstr.handle, nil)
  if gstr.handle != nil:
    result = $move(gstr)

proc `nullFallback=`*(opts: var HivePartitioningOptions, fallback: string) =
  ## Sets the fallback string for null values in Hive partitioning
  g_object_set(opts.toPtr, "null-fallback", fallback.cstring, nil)

proc segmentEncoding*(opts: HivePartitioningOptions): SegmentEncoding =
  ## Gets the segment encoding (inherited from KeyValuePartitioningOptions)
  var encoding: cint
  g_object_get(opts.toPtr, "segment-encoding", addr encoding, nil)
  result = SegmentEncoding(encoding)

proc `segmentEncoding=`*(opts: var HivePartitioningOptions, encoding: SegmentEncoding) =
  ## Sets the segment encoding (inherited from KeyValuePartitioningOptions)
  g_object_set(opts.toPtr, "segment-encoding", encoding.cint, nil)

proc nullFallback*(partitioning: HivePartitioning): string =
  ## Gets the fallback string for null values from a Hive partitioning
  if partitioning.handle == nil:
    return ""
  let gstr =
    newGString(gadataset_hive_partitioning_get_null_fallback(partitioning.toPtr))
  if gstr.handle != nil:
    result = $gstr

proc inferDictionary*(opts: PartitioningFactoryOptions): bool =
  ## Gets whether to infer dictionary-encoded types for partition fields
  var infer: gboolean
  g_object_get(opts.toPtr, "infer-dictionary", addr infer, nil)
  result = infer != 0

proc `inferDictionary=`*(opts: var PartitioningFactoryOptions, infer: bool) =
  ## Sets whether to infer dictionary-encoded types for partition fields
  g_object_set(opts.toPtr, "infer-dictionary", infer.gboolean, nil)

proc schema*(opts: PartitioningFactoryOptions): Schema =
  ## Gets the expected schema for partitioning inference
  var schemaPtr: ptr GArrowSchema
  g_object_get(opts.toPtr, "schema", addr schemaPtr, nil)
  if schemaPtr != nil:
    result.handle = schemaPtr

proc `schema=`*(opts: var PartitioningFactoryOptions, s: Schema) =
  ## Sets the expected schema for partitioning inference
  g_object_set(opts.toPtr, "schema", s.toPtr, nil)

proc segmentEncoding*(opts: PartitioningFactoryOptions): SegmentEncoding =
  ## Gets the segment encoding for path component decoding
  var encoding: cint
  g_object_get(opts.toPtr, "segment-encoding", addr encoding, nil)
  result = SegmentEncoding(encoding)

proc `segmentEncoding=`*(
    opts: var PartitioningFactoryOptions, encoding: SegmentEncoding
) =
  ## Sets the segment encoding for path component decoding
  g_object_set(opts.toPtr, "segment-encoding", encoding.cint, nil)

proc newFinishOptions*(): FinishOptions =
  ## Creates default finish options for dataset factories
  result.handle = gadataset_finish_options_new()

proc newInMemoryFragment*(
    schema: Schema, recordBatches: openArray[RecordBatch]
): InMemoryFragment =
  ## Creates an in-memory fragment from a schema and record batches
  if recordBatches.len == 0:
    raise newException(
      ValueError, "Cannot create InMemoryFragment from empty record batches"
    )
  var ptrArray = newSeq[ptr GArrowRecordBatch](recordBatches.len)
  for i, rb in recordBatches:
    ptrArray[i] = rb.toPtr
  let handle =
    gadataset_in_memory_fragment_new(schema.toPtr, addr ptrArray[0], ptrArray.len.gsize)
  if handle == nil:
    raise newException(OperationError, "Failed to create InMemoryFragment")
  result.handle = cast[ptr GADatasetFragment](handle)

proc newFileSystemDatasetFactory*(format: FileFormat): FileSystemDatasetFactory =
  ## Creates a new factory for building FileSystemDataset from files
  let handle = gadataset_file_system_dataset_factory_new(format.toPtr)
  if handle == nil:
    raise newException(OperationError, "Failed to create FileSystemDatasetFactory")
  result.handle = cast[ptr GADatasetDatasetFactory](handle)

proc setFileSystem*(
    factory: var FileSystemDatasetFactory, fs: FileSystem
): var FileSystemDatasetFactory =
  ## Sets the filesystem to use (local, S3, etc.)
  ## Returns self for method chaining.
  verify gadataset_file_system_dataset_factory_set_file_system(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), fs.handle
  )
  return factory

proc setFileSystemUri*(
    factory: var FileSystemDatasetFactory, uri: string
): var FileSystemDatasetFactory =
  ## Sets the filesystem from a URI (e.g., "file:///data", "s3://bucket/path")
  ## Returns self for method chaining.
  verify gadataset_file_system_dataset_factory_set_file_system_uri(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), uri.cstring
  )
  return factory

proc addPath*(
    factory: var FileSystemDatasetFactory, path: string
): var FileSystemDatasetFactory =
  ## Adds a path to scan for files
  ## Can be called multiple times to add multiple paths.
  ## Returns self for method chaining.
  verify gadataset_file_system_dataset_factory_add_path(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), path.cstring
  )
  return factory

proc inspectNFragments*(opts: FinishOptions): int =
  ## Gets the number of fragments to inspect for schema inference
  var n: cint
  g_object_get(opts.toPtr, "inspect-n-fragments", addr n, nil)
  result = n.int

proc `inspectNFragments=`*(opts: var FinishOptions, n: int) =
  ## Sets the number of fragments to inspect for schema inference
  g_object_set(opts.toPtr, "inspect-n-fragments", n.cint, nil)

proc schema*(opts: FinishOptions): Schema =
  ## Gets the schema to use for the dataset (if set)
  var schemaPtr: ptr GArrowSchema
  g_object_get(opts.toPtr, "schema", addr schemaPtr, nil)
  if schemaPtr != nil:
    result.handle = schemaPtr

proc `schema=`*(opts: var FinishOptions, s: Schema) =
  ## Sets the schema to use for the dataset
  g_object_set(opts.toPtr, "schema", s.toPtr, nil)

proc validateFragments*(opts: FinishOptions): bool =
  ## Gets whether to validate fragments against the schema
  var validate: gboolean
  g_object_get(opts.toPtr, "validate-fragments", addr validate, nil)
  result = validate != 0

proc `validateFragments=`*(opts: var FinishOptions, validate: bool) =
  ## Sets whether to validate fragments against the schema
  g_object_set(opts.toPtr, "validate-fragments", validate.gboolean, nil)

proc finish*(factory: DatasetFactory, schema: Schema): Dataset =
  ## Builds the Dataset from the configured factory
  var opts = newFinishOptions()
  opts.schema = schema
  let handle = verify gadataset_dataset_factory_finish(factory.toPtr, opts.toPtr)
  result.handle = cast[ptr GADatasetDataset](handle)

proc finish*(
    factory: FileSystemDatasetFactory, opts: FinishOptions = newFinishOptions()
): FileSystemDataset =
  ## Builds the FileSystemDataset from the configured paths
  let handle = verify gadataset_file_system_dataset_factory_finish(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), opts.toPtr
  )
  result.handle = handle

proc `fileSystem=`*(ds: var Dataset, fs: FileSystem) =
  ## Sets the filesystem associated with the dataset
  g_object_set(ds.toPtr, "file-system", fs.handle, nil)

proc fileSystem*(ds: Dataset): FileSystem =
  ## Gets the filesystem associated with the dataset (if any)
  var fsPtr: ptr GArrowFileSystem
  g_object_get(ds.toPtr, "file-system", addr fsPtr, nil)
  echo repr fsPtr
  if fsPtr != nil:
    result = newFileSystem(fsPtr)

proc `format=`*(ds: var Dataset, fmt: FileFormat) =
  ## Sets the file format associated with the dataset
  g_object_set(ds.toPtr, "format", fmt.handle, nil)

proc format*(ds: Dataset): FileFormat =
  ## Gets the file format associated with the dataset (if any)
  var formatPtr: ptr GADatasetFileFormat
  g_object_get(ds.toPtr, "format", addr formatPtr, nil)
  if formatPtr != nil:
    let typeName = $gadataset_file_format_get_type_name(formatPtr)
    let kind =
      case typeName
      of "GArrowCSVFileFormat": CSV
      of "GArrowIPCFileFormat": IPC
      of "GArrowParquetFileFormat": Parquet
      else: Parquet
    result = newFileFormat(kind)
    result.handle = formatPtr

proc `partitioning=`*(ds: var Dataset, part: Partitioning) =
  ## Sets the partitioning scheme associated with the dataset
  g_object_set(ds.toPtr, "partitioning", part.handle, nil)

proc partitioning*(ds: Dataset): Partitioning =
  ## Gets the partitioning scheme associated with the dataset (if any)
  var partPtr: ptr GADatasetPartitioning
  g_object_get(ds.toPtr, "partitioning", addr partPtr, nil)
  if partPtr != nil:
    result.handle = partPtr

proc files*(ds: Dataset): seq[FileInfo] =
  echo repr ds
  result = @[]

proc newDataset*(path: string, formatType: FileFormatTp = Parquet): Dataset =
  let fmt = newFileFormat(formatType)
  let fs = newFileSystem(path)
  var factory = newFileSystemDatasetFactory(fmt)
  # Capture specific type first, then upcast to base Dataset type
  var ds = factory.setFileSystem(fs).addPath(path).finish()
  result.handle = cast[ptr GADatasetDataset](ds.handle)
  ds.handle = nil

proc newScannerBuilder*(ds: Dataset): ScannerBuilder =
  ## Creates a scanner builder from a dataset
  result.handle = verify gadataset_scanner_builder_new(ds.toPtr)

proc newScannerBuilder*(reader: RecordBatchReader): ScannerBuilder =
  ## Creates a scanner builder from a record batch reader
  result.handle = gadataset_scanner_builder_new_record_batch_reader(reader.toPtr)

proc `filter=`*(sb: var ScannerBuilder, filter: Expression) =
  ## Sets a filter expression for push-down filtering.
  verify gadataset_scanner_builder_set_filter(sb.toPtr, filter.toPtr)

proc setFilter*(sb: ScannerBuilder, filter: Expression): ScannerBuilder =
  ## Sets a filter expression for push-down filtering.
  ## Returns self for method chaining.
  verify gadataset_scanner_builder_set_filter(sb.toPtr, filter.toPtr)
  result = sb

proc finish*(sb: ScannerBuilder): Scanner =
  ## Builds the scanner from the builder
  result.handle = verify gadataset_scanner_builder_finish(sb.toPtr)

proc toTable*(scanner: Scanner): ArrowTable =
  ## Executes the scan and returns results as a table
  let handle = verify gadataset_scanner_to_table(scanner.toPtr)
  result = newArrowTable(handle)

proc toRecordBatchReader*(scanner: Scanner): RecordBatchReader =
  ## Converts the scanner to a record batch reader for iteration
  let handle = verify gadataset_scanner_to_record_batch_reader(scanner.toPtr)
  result.handle = handle

iterator scan*(scanner: Scanner): RecordBatch =
  ## Iterates over all record batches from the scanner
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let scanner = dataset.newScannerBuilder().setFilter(filter).finish()
  ##     for batch in scanner.scan():
  ##       echo batch.len
  let reader = scanner.toRecordBatchReader()
  for batch in batches(reader):
    yield batch

proc getDefaultWriteOptions*(format: FileFormat): FileWriteOptions =
  ## Gets the default write options for a file format
  let handle = gadataset_file_format_get_default_write_options(format.toPtr)
  if handle == nil:
    raise newException(OperationError, "Failed to get default write options")
  result.handle = handle

proc openFileWriter*(
    format: FileFormat,
    destination: OutputStream,
    fs: FileSystem,
    path: string,
    schema: Schema,
    options: FileWriteOptions,
): FileWriter =
  ## Opens a file writer for writing record batches
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let format = newFileFormat(Parquet)
  ##     let localFs = newLocalFileSystem()
  ##     let outputStream = localFs.openOutputStream("/data/output.parquet")
  ##     let opts = format.getDefaultWriteOptions()
  ##     let writer = openFileWriter(format, outputStream, localFs, "/data/output.parquet", schema, opts)
  ##     writer.writeRecordBatch(batch)
  ##     writer.finish()
  let handle = verify gadataset_file_format_open_writer(
    format.toPtr, destination.handle, fs.handle, path.cstring, schema.toPtr,
    options.toPtr,
  )
  result.handle = handle

proc writeRecordBatch*(writer: FileWriter, batch: RecordBatch) =
  ## Writes a single record batch to the file
  verify gadataset_file_writer_write_record_batch(writer.toPtr, batch.toPtr)

proc writeRecordBatchReader*(writer: FileWriter, reader: RecordBatchReader) =
  ## Writes all record batches from a reader to the file
  verify gadataset_file_writer_write_record_batch_reader(writer.toPtr, reader.toPtr)

proc finish*(writer: FileWriter) =
  ## Finishes writing and closes the file
  verify gadataset_file_writer_finish(writer.toPtr)

proc writeDatasetFromScanner*(
    scanner: Scanner, path: string, format: FileFormat, options: FileWriteOptions
) =
  ## Writes data from a scanner to a single file.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let scanner = dataset.newScannerBuilder().setFilter(filter).finish()
  ##     let format = newFileFormat(Parquet)
  ##     writeDatasetFromScanner(scanner, "/data/filtered.parquet", format, options=format.getDefaultWriteOptions())
  let filesystem = newLocalFileSystem()
  let outputStream = filesystem.openOutputStream(path)
  let reader = scanner.toRecordBatchReader()
  let schema = reader.schema
  let writer = openFileWriter(format, outputStream, filesystem, path, schema, options)
  writer.writeRecordBatchReader(reader)
  writer.finish()

proc writeDatasetFromScanner*(scanner: Scanner, path: string, format: FileFormat) =
  ## Writes data from a scanner to a single file.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let scanner = dataset.newScannerBuilder().setFilter(filter).finish()
  ##     let format = newFileFormat(Parquet)
  ##     writeDatasetFromScanner(scanner, "/data/filtered.parquet", format))
  writeDatasetFromScanner(scanner, path, format, format.getDefaultWriteOptions())

proc `baseDir=`*(opts: var FileSystemDatasetWriteOptions, dir: string) =
  ## Sets the root output directory for the dataset write.
  g_object_set(opts.toPtr, "base-dir", dir.cstring, nil)

proc baseDir*(opts: FileSystemDatasetWriteOptions): string =
  ## Gets the root output directory for the dataset write.
  var gstr = newGString(nil)
  g_object_get(opts.toPtr, "base-dir", addr gstr.handle, nil)
  if gstr.handle != nil:
    result = $move(gstr)

proc `baseNameTemplate=`*(opts: var FileSystemDatasetWriteOptions, tpl: string) =
  ## Sets the basename template (e.g. `"part-{i}.parquet"`).
  g_object_set(opts.toPtr, "base-name-template", tpl.cstring, nil)

proc baseNameTemplate*(opts: FileSystemDatasetWriteOptions): string =
  ## Gets the basename template.
  var gstr = newGString(nil)
  g_object_get(opts.toPtr, "base-name-template", addr gstr.handle, nil)
  if gstr.handle != nil:
    result = $move(gstr)

proc `fileSystem=`*(opts: var FileSystemDatasetWriteOptions, fs: FileSystem) =
  ## Sets the filesystem to write into.
  g_object_set(opts.toPtr, "file-system", fs.handle, nil)

proc `fileWriteOptions=`*(
    opts: var FileSystemDatasetWriteOptions, fwo: FileWriteOptions
) =
  ## Sets the format-specific write options.
  g_object_set(opts.toPtr, "file-write-options", fwo.toPtr, nil)

proc `maxPartitions=`*(opts: var FileSystemDatasetWriteOptions, max: uint) =
  ## Sets the maximum number of partitions any batch may be written into.
  g_object_set(opts.toPtr, "max-partitions", max.guint, nil)

proc maxPartitions*(opts: FileSystemDatasetWriteOptions): uint =
  ## Gets the maximum number of partitions any batch may be written into.
  var max: guint
  g_object_get(opts.toPtr, "max-partitions", addr max, nil)
  result = max.uint

proc `partitioning=`*(opts: var FileSystemDatasetWriteOptions, part: Partitioning) =
  ## Sets the partitioning scheme used to generate fragment paths.
  g_object_set(opts.toPtr, "partitioning", part.handle, nil)

proc writeDataset*(scanner: Scanner, options: FileSystemDatasetWriteOptions) =
  ## Writes a scanner to a dataset using the given options.
  verify gadataset_file_system_dataset_write_scanner(scanner.toPtr, options.toPtr)

proc writeDataset*(
    table: ArrowTable,
    path: string,
    format: FileFormat,
    partitioning: Partitioning = Partitioning(),
    options: FileWriteOptions = FileWriteOptions(),
) =
  ## Writes a table to a partitioned dataset.
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let schema = newSchema([newField[int32]("year"), newField[int32]("value")])
  ##     let table = newArrowTable(schema, years, values)
  ##     let part = newHivePartitioning(newSchema([newField[int32]("year")]))
  ##     writeDataset(table, "/data/partitioned", newFileFormat(Parquet), partitioning=part)
  let fs = newLocalFileSystem()
  let reader = newRecordBatchReader(table)
  let scanner = newScannerBuilder(reader).finish()
  var writeOpts = FileSystemDatasetWriteOptions(
    handle: gadataset_file_system_dataset_write_options_new()
  )
  writeOpts.baseDir = path
  writeOpts.fileSystem = fs
  writeOpts.fileWriteOptions =
    if options.handle.isNil:
      format.getDefaultWriteOptions()
    else:
      options
  let ext =
    case format.kind
    of Parquet: "parquet"
    of IPC: "arrow"
    of CSV: "csv"
  writeOpts.baseNameTemplate = "part-{i}." & ext
  if partitioning.handle != nil:
    writeOpts.partitioning = partitioning
  writeDataset(scanner, writeOpts)
