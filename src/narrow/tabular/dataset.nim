import ../core/[ffi, error]
import ../compute/expressions
import ../column/metadata
import ../io/filesystem
import ./table
import ./batch

# ============================================================================
# Core Dataset Types
# ============================================================================

type
  Dataset* {.inheritable.} = object
    handle: ptr GADatasetDataset

  FileSystemDataset* = object of Dataset
    ## A dataset backed by files in a filesystem

# ============================================================================
# Fragment Types
# ============================================================================

# A Fragment in Apache Arrow Dataset is a granular, independently scannable piece of a Dataset, typically representing an individual file or an in-memory collection of RecordBatches dataset.h:148-156 . It encapsulates:

#     A physical schema (the writer/on-disk schema), which may differ from the Dataset’s unified schema dataset.h:161-166 .
#     A partition expression that is guaranteed true for all rows in the fragment, enabling partition pruning dataset.h:203-207 .
#     Methods to scan data asynchronously (ScanBatchesAsync), inspect metadata (InspectFragment), and count rows using metadata only (CountRows) dataset.h:169-191 .

type
  Fragment* {.inheritable.} = object ## Base fragment type representing a chunk of data
    handle: ptr GADatasetFragment

  InMemoryFragment* = object of Fragment
    ## A fragment that wraps data already in memory (e.g., record batches)

# ============================================================================
# Scanning Types
# ============================================================================

type
  Scanner* = object ## A scanner that iterates over fragments and yields record batches
    handle: ptr GADatasetScanner

  ScannerBuilder* = object ## Builder for configuring and creating a Scanner
    handle: ptr GADatasetScannerBuilder

# ============================================================================
# File Format Types
# ============================================================================

type FileFormat* = object
  ## File format handler for dataset discovery
  ## Supports CSV, IPC, and Parquet formats
  handle: ptr GADatasetFileFormat

# ============================================================================
# Factory Types 
# ============================================================================

type
  DatasetFactory* {.inheritable.} = object
    ## Base factory for creating datasets from various sources
    handle: ptr GADatasetDatasetFactory

  FileSystemDatasetFactory* = object of DatasetFactory
    ## Factory for creating FileSystemDataset from files/directories
    ## Inherits handle from DatasetFactory

  FinishOptions* = object ## Options for finishing dataset factory construction
    handle: ptr GADatasetFinishOptions

# ============================================================================
# Partitioning Types 
# ============================================================================

type
  Partitioning* {.inheritable.} = object
    ## Base partitioning type for organizing data in a dataset
    handle: ptr GADatasetPartitioning

  DirectoryPartitioning* = object of Partitioning
    ## Partitioning based on directory structure (e.g., /year=2024/month=01/)

  HivePartitioning* = object of Partitioning
    ## Hive-style partitioning (key=value directory naming)

  HivePartitioningOptions* = object ## Options for creating HivePartitioning
    handle: ptr GADatasetHivePartitioningOptions

# ============================================================================
# File Writer Types (Phase 3)
# ============================================================================

type
  FileWriter* = object ## Writer for writing record batches to files in various formats
    handle: ptr GADatasetFileWriter

  FileWriteOptions* = object ## Options for configuring file writes (format-specific)
    handle: ptr GADatasetFileWriteOptions

# ============================================================================
# Partitioning Discovery Types (Phase 3)
# ============================================================================

type PartitioningFactoryOptions* = object
  ## Options for discovering partitioning from file paths
  handle: ptr GADatasetPartitioningFactoryOptions

# ============================================================================
# ARC Hooks - Dataset (base type)
# ============================================================================

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

# ============================================================================
# ARC Hooks - Fragment (base type)
# ============================================================================

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

# ============================================================================
# ARC Hooks - Scanner
# ============================================================================

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

# ============================================================================
# ARC Hooks - ScannerBuilder
# ============================================================================

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

# ============================================================================
# ARC Hooks - FileFormat
# ============================================================================

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

# ============================================================================
# ARC Hooks - DatasetFactory (base type)
# ============================================================================

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

# ============================================================================
# ARC Hooks - FinishOptions
# ============================================================================

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

# ============================================================================
# ARC Hooks - Partitioning (base type)
# ============================================================================

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

# ============================================================================
# ARC Hooks - HivePartitioningOptions
# ============================================================================

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

# ============================================================================
# ARC Hooks - FileWriter
# ============================================================================

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

# ============================================================================
# ARC Hooks - FileWriteOptions
# ============================================================================

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

# ============================================================================
# ARC Hooks - PartitioningFactoryOptions
# ============================================================================

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

# ============================================================================
# Pointer Converters
# ============================================================================

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

# ============================================================================
# Dataset Operations
# ============================================================================

proc toTable*(ds: Dataset): ArrowTable =
  ## Converts the dataset to an ArrowTable by reading all fragments
  let handle = check gadataset_dataset_to_table(ds.toPtr)
  result = newArrowTable(handle)

# # ============================================================================
# # File Format Constructors
# # ============================================================================

# proc newCSVFileFormat*(): FileFormat =
#   ## Creates a CSV file format handler for dataset discovery
#   result.handle = cast[ptr GADatasetFileFormat](gadataset_csv_file_format_new())

# proc newIPCFileFormat*(): FileFormat =
#   ## Creates an Arrow IPC file format handler for dataset discovery  
#   result.handle = cast[ptr GADatasetFileFormat](gadataset_ipc_file_format_new())

# proc newParquetFileFormat*(): FileFormat =
#   ## Creates a Parquet file format handler for dataset discovery
#   result.handle = cast[ptr GADatasetFileFormat](gadataset_parquet_file_format_new())

# # ============================================================================
# # Partitioning Constructors
# # ============================================================================

# proc newDirectoryPartitioning*(schema: Schema): DirectoryPartitioning =
#   ## Creates a directory partitioning scheme from a schema
#   ## Schema field names become partition keys (e.g., year, month, day)
#   result.handle = cast[ptr GADatasetPartitioning](check gadataset_directory_partitioning_new(
#     schema.toPtr, nil, nil
#   ))

# proc newHivePartitioning*(schema: Schema): HivePartitioning =
#   ## Creates a Hive partitioning scheme from a schema
#   ## Uses key=value directory naming convention
#   let opts = gadataset_hive_partitioning_options_new()
#   result.handle = cast[ptr GADatasetPartitioning](check gadataset_hive_partitioning_new(
#     schema.toPtr, nil, opts
#   ))
#   g_object_unref(opts)

# proc newHivePartitioningOptions*(): HivePartitioningOptions =
#   ## Creates default Hive partitioning options
#   result.handle = gadataset_hive_partitioning_options_new()

# ============================================================================
# FinishOptions Constructor
# ============================================================================

proc newFinishOptions*(): FinishOptions =
  ## Creates default finish options for dataset factories
  result.handle = gadataset_finish_options_new()

# ============================================================================
# Fragment Constructors
# ============================================================================

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

# ============================================================================
# FileSystemDatasetFactory Constructors and Methods
# ============================================================================

proc newFileSystemDatasetFactory*(format: FileFormat): FileSystemDatasetFactory =
  ## Creates a new factory for building FileSystemDataset from files
  let handle = gadataset_file_system_dataset_factory_new(format.toPtr)
  if handle == nil:
    raise newException(OperationError, "Failed to create FileSystemDatasetFactory")
  result.handle = cast[ptr GADatasetDatasetFactory](handle)

proc setFileSystem*(
    factory: FileSystemDatasetFactory, fs: FileSystem
): FileSystemDatasetFactory =
  ## Sets the filesystem to use (local, S3, etc.)
  ## Returns self for method chaining.
  check gadataset_file_system_dataset_factory_set_file_system(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), fs.handle
  )
  result = factory

proc setFileSystemUri*(
    factory: FileSystemDatasetFactory, uri: string
): FileSystemDatasetFactory =
  ## Sets the filesystem from a URI (e.g., "file:///data", "s3://bucket/path")
  ## Returns self for method chaining.
  check gadataset_file_system_dataset_factory_set_file_system_uri(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), uri.cstring
  )
  result = factory

proc addPath*(
    factory: FileSystemDatasetFactory, path: string
): FileSystemDatasetFactory =
  ## Adds a path to scan for files
  ## Can be called multiple times to add multiple paths.
  ## Returns self for method chaining.
  check gadataset_file_system_dataset_factory_add_path(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), path.cstring
  )
  result = factory

proc finish*(
    factory: FileSystemDatasetFactory, opts: FinishOptions = newFinishOptions()
): FileSystemDataset =
  ## Builds the FileSystemDataset from the configured paths
  let handle = check gadataset_file_system_dataset_factory_finish(
    cast[ptr GADatasetFileSystemDatasetFactory](factory.handle), opts.toPtr
  )
  result.handle = cast[ptr GADatasetDataset](handle)

# ============================================================================
# High-level Convenience Functions
# ============================================================================

proc newFileSystemDataset*(
    paths: openArray[string], format: FileFormat, fs: FileSystem = nil
): FileSystemDataset =
  ## High-level convenience function to create a FileSystemDataset from paths
  ## Uses local filesystem by default if none specified
  ## 
  ## Example:
  ##   .. code-block:: nim
  ##     let format = newParquetFileFormat()
  ##     let dataset = newFileSystemDataset(@["/data/part1.parquet", "/data/part2.parquet"], format)
  ##     let table = dataset.toTable()
  var factory = newFileSystemDatasetFactory(format)
  if fs != nil:
    discard factory.setFileSystem(fs)
  else:
    # Use local filesystem by default
    let localFs = newLocalFileSystem()
    discard factory.setFileSystem(localFs)
  for path in paths:
    discard factory.addPath(path)
  result = factory.finish()

proc newDatasetFromDirectory*(
    path: string, format: FileFormat, fs: FileSystem = nil
): FileSystemDataset =
  ## Creates a dataset from a directory path (scans all files recursively)
  ## Uses local filesystem by default if none specified
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let format = newParquetFileFormat()
  ##     let dataset = newDatasetFromDirectory("/data/parquet_files", format)
  var factory = newFileSystemDatasetFactory(format)
  if fs != nil:
    discard factory.setFileSystem(fs)
  else:
    # Use local filesystem by default
    let localFs = newLocalFileSystem()
    discard factory.setFileSystem(localFs)
  discard factory.addPath(path)
  result = factory.finish()

# ============================================================================
# Scanner Building
# ============================================================================

proc newScannerBuilder*(ds: Dataset): ScannerBuilder =
  ## Creates a scanner builder from a dataset
  result.handle = check gadataset_scanner_builder_new(ds.toPtr)

proc setFilter*(sb: ScannerBuilder, filter: ExpressionObj): ScannerBuilder =
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

# ============================================================================
# Scanner Iteration (Phase 2)
# ============================================================================

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

# ============================================================================
# FileWriter and Writing Operations (Phase 3)
# ============================================================================

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

# ============================================================================
# High-level Write Convenience Functions (Phase 3)
# ============================================================================

proc writeDatasetFromScanner*(
    scanner: Scanner,
    path: string,
    format: FileFormat,
    fs: FileSystem = nil,
    options: FileWriteOptions = FileWriteOptions(),
) =
  ## Writes data from a scanner to a file
  ##
  ## Example:
  ##   .. code-block:: nim
  ##     let scanner = dataset.newScannerBuilder().setFilter(filter).finish()
  ##     let opts = newParquetFileFormat().getDefaultWriteOptions()
  ##     writeDatasetFromScanner(scanner, "/data/filtered.parquet", newParquetFileFormat(), options=opts)
  let filesystem =
    if fs == nil:
      newLocalFileSystem()
    else:
      fs
  let outputStream = filesystem.openOutputStream(path)
  let reader = scanner.toRecordBatchReader()
  let schema = reader.schema
  let opts =
    if options.isNil:
      format.getDefaultWriteOptions()
    else:
      options
  let writer = openFileWriter(format, outputStream, filesystem, path, schema, opts)
  writer.writeRecordBatchReader(reader)
  writer.finish()

# ============================================================================
# Partitioning Discovery (Phase 3)
# ============================================================================

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
