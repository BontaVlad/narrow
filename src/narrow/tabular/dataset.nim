import ../core/[ffi, error]
import ../compute/expressions
import ../column/metadata
import ../io/filesystem
import ./table
import ./batch

## Collection of data fragments and potentially child datasets.
##
## Arrow Datasets allow you to query against data that has been split across
## multiple files. This sharding of data may indicate partitioning, which
## can accelerate queries that only touch some partitions (files).
type
  Dataset* = object
    handle: ptr GADatasetDataset

  FileSystemDataset* = object
    handle: ptr GADatasetFileSystemDataset

type
  Fragment* {.inheritable.} = object
    handle: ptr GADatasetFragment

  InMemoryFragment* = object of Fragment

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

type
  DatasetFactory* {.inheritable.} = object
    handle: ptr GADatasetDatasetFactory

  FileSystemDatasetFactory* = object of DatasetFactory

  FinishOptions* = object
    handle: ptr GADatasetFinishOptions

type
  Partitioning* {.inheritable.} = object
    handle: ptr GADatasetPartitioning

  DirectoryPartitioning* = object of Partitioning

  HivePartitioning* = object of Partitioning

  HivePartitioningOptions* = object
    handle: ptr GADatasetHivePartitioningOptions

type
  FileWriter* = object
    handle: ptr GADatasetFileWriter

  FileWriteOptions* = object
    handle: ptr GADatasetFileWriteOptions

type PartitioningFactoryOptions* = object
  ## Options for discovering partitioning from file paths
  handle: ptr GADatasetPartitioningFactoryOptions

var computeInitialized {.global.} = false

proc ensureComputeInitialized() =
  ## Ensures compute functions are registered. Thread-safe one-time initialization.
  once:
    var err = newError()
    if not garrow_compute_initialize(err.toPtr).bool or err:
      raise newException(OperationError, "Failed to initialize compute: " & $err)

    computeInitialized = true

ensureComputeInitialized()

proc `=destroy`*(ds: Dataset) =
  if ds.handle != nil:
    g_object_unref(ds.handle)

proc `=sink`*(dest: var Dataset, src: Dataset) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Dataset, src: Dataset) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(frag: Fragment) =
  if frag.handle != nil:
    g_object_unref(frag.handle)

proc `=sink`*(dest: var Fragment, src: Fragment) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Fragment, src: Fragment) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(scanner: Scanner) =
  if scanner.handle != nil:
    g_object_unref(scanner.handle)

proc `=sink`*(dest: var Scanner, src: Scanner) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Scanner, src: Scanner) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(sb: ScannerBuilder) =
  if sb.handle != nil:
    g_object_unref(sb.handle)

proc `=sink`*(dest: var ScannerBuilder, src: ScannerBuilder) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var ScannerBuilder, src: ScannerBuilder) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(format: FileFormat) =
  if format.handle != nil:
    g_object_unref(format.handle)

proc `=sink`*(dest: var FileFormat, src: FileFormat) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

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

proc `=sink`*(dest: var DatasetFactory, src: DatasetFactory) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

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

proc `=sink`*(dest: var FileSystemDatasetFactory, src: FileSystemDatasetFactory) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FileSystemDatasetFactory, src: FileSystemDatasetFactory) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(opts: FinishOptions) =
  if opts.handle != nil:
    g_object_unref(opts.handle)

proc `=sink`*(dest: var FinishOptions, src: FinishOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FinishOptions, src: FinishOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(partitioning: Partitioning) =
  if partitioning.handle != nil:
    g_object_unref(partitioning.handle)

proc `=sink`*(dest: var Partitioning, src: Partitioning) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var Partitioning, src: Partitioning) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(opts: HivePartitioningOptions) =
  if opts.handle != nil:
    g_object_unref(opts.handle)

proc `=sink`*(dest: var HivePartitioningOptions, src: HivePartitioningOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var HivePartitioningOptions, src: HivePartitioningOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(writer: FileWriter) =
  if writer.handle != nil:
    g_object_unref(writer.handle)

proc `=sink`*(dest: var FileWriter, src: FileWriter) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FileWriter, src: FileWriter) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(opts: FileWriteOptions) =
  if opts.handle != nil:
    g_object_unref(opts.handle)

proc `=sink`*(dest: var FileWriteOptions, src: FileWriteOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var FileWriteOptions, src: FileWriteOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc `=destroy`*(opts: PartitioningFactoryOptions) =
  if opts.handle != nil:
    g_object_unref(opts.handle)

proc `=sink`*(dest: var PartitioningFactoryOptions, src: PartitioningFactoryOptions) =
  if dest.handle != nil and dest.handle != src.handle:
    g_object_unref(dest.handle)
  dest.handle = src.handle

proc `=copy`*(dest: var PartitioningFactoryOptions, src: PartitioningFactoryOptions) =
  if dest.handle != src.handle:
    if dest.handle != nil:
      g_object_unref(dest.handle)
    dest.handle = src.handle
    if src.handle != nil:
      discard g_object_ref(dest.handle)

proc toPtr*(ds: Dataset): ptr GADatasetDataset {.inline.} =
  ds.handle

proc toPtr*(frag: Fragment): ptr GADatasetFragment {.inline.} =
  frag.handle

proc toPtr*(scanner: Scanner): ptr GADatasetScanner {.inline.} =
  scanner.handle

proc toPtr*(sb: ScannerBuilder): ptr GADatasetScannerBuilder {.inline.} =
  sb.handle

proc toPtr*(format: FileFormat): ptr GADatasetFileFormat {.inline.} =
  format.handle

proc toPtr*(factory: DatasetFactory): ptr GADatasetDatasetFactory {.inline.} =
  factory.handle

proc toPtr*(opts: FinishOptions): ptr GADatasetFinishOptions {.inline.} =
  opts.handle

proc toPtr*(partitioning: Partitioning): ptr GADatasetPartitioning {.inline.} =
  partitioning.handle

proc toPtr*(writer: FileWriter): ptr GADatasetFileWriter {.inline.} =
  writer.handle

proc toPtr*(opts: FileWriteOptions): ptr GADatasetFileWriteOptions {.inline.} =
  opts.handle

proc toPtr*(
    opts: PartitioningFactoryOptions
): ptr GADatasetPartitioningFactoryOptions {.inline.} =
  opts.handle

proc toPtr*(pt: HivePartitioning): ptr GADatasetHivePartitioning {.inline.} =
  cast[ptr GADatasetHivePartitioning](pt.handle)

proc toPtr*(
    pt: HivePartitioningOptions
): ptr GADatasetHivePartitioningOptions {.inline.} =
  pt.handle

# =======================================================
# Dataset and stuff
# =======================================================
proc toTable*(ds: Dataset): ArrowTable =
  ## Converts the dataset to an ArrowTable by reading all fragments
  let handle = check gadataset_dataset_to_table(ds.toPtr)
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
  result.handle = cast[ptr GADatasetPartitioning](check gadataset_directory_partitioning_new(
    schema.toPtr, nil, nil
  ))

proc newHivePartitioningOptions*(): HivePartitioningOptions =
  result.handle = gadataset_hive_partitioning_options_new()

proc newHivePartitioning*(schema: Schema): HivePartitioning =
  # TODO: impplement dictionaries for partitioning
  let opts = newHivePartitioningOptions()
  result.handle = cast[ptr GADatasetPartitioning](check gadataset_hive_partitioning_new(
    schema.toPtr, nil, opts.toPtr
  ))

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
  check gadataset_file_system_dataset_factory_set_file_system(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), fs.handle
  )
  return factory

proc setFileSystemUri*(
    factory: var FileSystemDatasetFactory, uri: string
): var FileSystemDatasetFactory =
  ## Sets the filesystem from a URI (e.g., "file:///data", "s3://bucket/path")
  ## Returns self for method chaining.
  check gadataset_file_system_dataset_factory_set_file_system_uri(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), uri.cstring
  )
  return factory

proc addPath*(
    factory: var FileSystemDatasetFactory, path: string
): var FileSystemDatasetFactory =
  ## Adds a path to scan for files
  ## Can be called multiple times to add multiple paths.
  ## Returns self for method chaining.
  check gadataset_file_system_dataset_factory_add_path(
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
  let handle = check gadataset_dataset_factory_finish(factory.toPtr, opts.toPtr)
  result.handle = cast[ptr GADatasetDataset](handle)

proc finish*(
    factory: FileSystemDatasetFactory, opts: FinishOptions = newFinishOptions()
): FileSystemDataset =
  ## Builds the FileSystemDataset from the configured paths
  let handle = check gadataset_file_system_dataset_factory_finish(
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
  let ds = factory.setFileSystem(fs).addPath(path).finish()
  result = Dataset(handle: cast[ptr GADatasetDataset](ds.handle))

proc newScannerBuilder*(ds: Dataset): ScannerBuilder =
  ## Creates a scanner builder from a dataset
  result.handle = check gadataset_scanner_builder_new(ds.toPtr)

proc `filter=`*(sb: var ScannerBuilder, filter: Expression) =
  ## Sets a filter expression for push-down filtering.
  check gadataset_scanner_builder_set_filter(sb.toPtr, filter.toPtr)

proc setFilter*(sb: ScannerBuilder, filter: Expression): ScannerBuilder =
  ## Sets a filter expression for push-down filtering.
  ## Returns self for method chaining.
  check gadataset_scanner_builder_set_filter(sb.toPtr, filter.toPtr)
  result = sb

proc finish*(sb: ScannerBuilder): Scanner =
  ## Builds the scanner from the builder
  result.handle = check gadataset_scanner_builder_finish(sb.toPtr)

proc toTable*(scanner: Scanner): ArrowTable =
  ## Executes the scan and returns results as a table
  let handle = check gadataset_scanner_to_table(scanner.toPtr)
  result = newArrowTable(handle)

proc toRecordBatchReader*(scanner: Scanner): RecordBatchReader =
  ## Converts the scanner to a record batch reader for iteration
  let handle = check gadataset_scanner_to_record_batch_reader(scanner.toPtr)
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
  ##     let format = newParquetFileFormat()
  ##     let localFs = newLocalFileSystem()
  ##     let outputStream = localFs.openOutputStream("/data/output.parquet")
  ##     let opts = format.getDefaultWriteOptions()
  ##     let writer = openFileWriter(format, outputStream, localFs, "/data/output.parquet", schema, opts)
  ##     writer.writeRecordBatch(batch)
  ##     writer.finish()
  let handle = check gadataset_file_format_open_writer(
    format.toPtr, destination.handle, fs.handle, path.cstring, schema.toPtr,
    options.toPtr,
  )
  result.handle = handle

proc writeRecordBatch*(writer: FileWriter, batch: RecordBatch) =
  ## Writes a single record batch to the file
  check gadataset_file_writer_write_record_batch(writer.toPtr, batch.toPtr)

proc writeRecordBatchReader*(writer: FileWriter, reader: RecordBatchReader) =
  ## Writes all record batches from a reader to the file
  check gadataset_file_writer_write_record_batch_reader(writer.toPtr, reader.toPtr)

proc finish*(writer: FileWriter) =
  ## Finishes writing and closes the file
  check gadataset_file_writer_finish(writer.toPtr)

proc writeDatasetFromScanner*(
    scanner: Scanner,
    path: string,
    format: FileFormat,
    options: FileWriteOptions = FileWriteOptions(),
) =
  ## Writes data from a scanner to a file
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let scanner = dataset.newScannerBuilder().setFilter(filter).finish()
  ##     let opts = newParquetFileFormat().getDefaultWriteOptions()
  ##     writeDatasetFromScanner(scanner, "/data/filtered.parquet", newParquetFileFormat(), options=opts)
  discard
  # let filesystem =
  #   if fs == nil:
  #     newLocalFileSystem()
  #   else:
  #     fs
  # let outputStream = filesystem.openOutputStream(path)
  # let reader = scanner.toRecordBatchReader()
  # let schema = reader.schema
  # let opts =
  #   if options.handle.isNil:
  #     format.getDefaultWriteOptions()
  #   else:
  #     options
  # let writer = openFileWriter(format, outputStream, filesystem, path, schema, opts)
  # writer.writeRecordBatchReader(reader)
  # writer.finish()
