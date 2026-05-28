# Narrow Feature Manual

Practical use cases for every recently added feature, with code examples.

---

## 1. Hash Join

**Use case:** Combine two tables by matching keys — think SQL `JOIN`.

- **Analytics:** Merge customer data with purchase history on `customer_id`.
- **ETL:** Enrich logs with dimension tables.
- **Data cleaning:** Cross-reference two datasets to find matches/differences.

```nim
let customers = newArrowTable(
  newSchema([newField[int32]("id"), newField[string]("name")]),
  newArray(@[1'i32, 2, 3]),
  newArray(@["Alice", "Bob", "Carol"]))

let orders = newArrowTable(
  newSchema([newField[int32]("customer_id"), newField[float64]("amount")]),
  newArray(@[1'i32, 1, 2]),
  newArray(@[99.0, 150.0, 200.0]))

let joined = joinTables(customers, orders, "id", "customer_id",
  jtLeftOuter)
```

**Key types:** `JoinType` (8 values), `HashJoinNodeOptions`, `joinTables()`.

---

## 2. Binary Arrays

**Use case:** Store arbitrary byte sequences — protobuf blobs, image thumbnails, cryptographic hashes, raw sensor data.

```nim
var builder = newArrayBuilder[seq[byte]]()
builder.append(@[0xDE'u8, 0xAD, 0xBE, 0xEF])
builder.append(@[0x01'u8, 0x02])
let arr = builder.finish()
echo arr[0]  # @[0xDE, 0xAD, 0xBE, 0xEF]
```

---

## 3. Duration & Interval Arrays

**Use case:** Represent time spans — event durations, timeouts, scheduling offsets, timezone-aware intervals.

```nim
# Duration: a fixed-length time span (seconds, millis, micros, nanos)
var durBuilder = newDurationArrayBuilder(MILLISECOND)
durBuilder.append(some(5000'i64))  # 5000 ms
let durArr = durBuilder.finish()

# MonthInterval: calendar months (variable days)
var miBuilder = newMonthIntervalArrayBuilder()
miBuilder.append(some(3'i32))  # 3 months
let miArr = miBuilder.finish()

# DayTimeInterval: days + ms (for timezone DST-safe spans)
var dtiBuilder = newDayTimeIntervalArrayBuilder()
dtiBuilder.append(some(newDayTimeInterval(1'i32, 3600000'i32)))
let dtiArr = dtiBuilder.finish()
```

**Key types:** `DurationArray`, `MonthIntervalArray`, `DayTimeIntervalArray`, `MonthDayNanoIntervalArray`.

---

## 4. Decimal Arrays

**Use case:** Exact decimal arithmetic for financial calculations, accounting, billing.

```nim
# Decimal128 — up to 38 significant digits
var builder = newDecimal128ArrayBuilder(10, 2)  # precision=10, scale=2
builder.append("12345678.90")
builder.append(42)  # stored as 0.42
let arr = builder.finish()
echo arr.formatValue(0)  # "12345678.90"

# Value-level arithmetic
let a = newDecimal128(42, 10, 2)   # 0.42
let b = newDecimal128(100, 10, 2)  # 1.00
echo a + b   # "142" (raw digits, scale=2 → 1.42)
```

**Key types:** `Decimal128`, `Decimal256`, `Decimal128Array`, `Decimal256Array`.

---

## 5. Compressed I/O

**Use case:** Read/write compressed Parquet/CSV/JSON files directly without external decompression.

```nim
# Auto-detect compression from file extension
let input = openCompressedInputStream("/data/logs.csv.gz")
let batch = readCSV(input)
input.close()

# Explicit codec
let output = newCompressedOutputStream(
  openOutputStream("/data/output.parquet.zst"),
  newCodec("zstd"))
writeParquet(output, table)
output.close()
```

Supported: gzip (`.gz`), zstd (`.zst`), bz2 (`.bz2`). Snappy/LZ4/Brotli only for reading.

**Key types:** `Codec`, `codecFromExtension()`.

---

## 6. Half-Float Arrays

**Use case:** Store float data at 16-bit precision — ML model weights, GPU interop, memory-constrained edge devices.

```nim
var builder = newHalfFloatArrayBuilder()
builder.append(HalfFloat(0x3C00))   # 1.0 in IEEE 754 half-precision
builder.append(HalfFloat(0x4000))   # 2.0
let arr = builder.finish()
echo arr[0]  # HalfFloat(0x3C00)
```

Values are raw IEEE 754 bit patterns (`distinct uint16`). Convert to/from `float32` with an external helper or `cast`.

**Key types:** `HalfFloat`, `HGFloatArray`, `HGFloatArrayBuilder`.

---

## 7. S3 Filesystem

**Use case:** Read/write Arrow data directly from Amazon S3 without downloading files first.

```nim
initializeS3()

let fs = openS3Filesystem()  # via FileSystem API
let dataset = fs.openDataset("s3://my-bucket/data/")
let result = dataset.scan()
  .filter(col("year") == 2024'i32)
  .readAll()

finalizeS3()
```

Requires S3 credentials (environment variables or IAM role). `isS3Enabled()` probes availability.

**Key types:** `S3GlobalOptions`, `initializeS3()`, `finalizeS3()`.

---

## 8. Schema Metadata

**Use case:** Attach arbitrary key-value metadata to schemas — schema version, data provenance, domain annotations.

```nim
let schema = newSchema([newField[int32]("id")])
  .withMetadata({"version": "2.1", "source": "data-lake-3"})

echo schema.getMetadataValue("version").get()  # "2.1"

# Add/remove fields (returns new schema — immutable)
let updated = schema.addField(1, newField[string]("tag"))
let stripped = updated.removeField(0)
```

**Key types:** Methods on `Schema` — `hasMetadata`, `getMetadata`, `getMetadataValue`, `withMetadata`, `addField`, `removeField`.

---

## 9. RecordBatch Sort / Take / Filter

**Use case:** Reorder, slice, or filter record batches — building blocks for query engines.

```nim
let batch = newRecordBatch(
  newSchema([newField[int32]("id"), newField[string]("name")]),
  newArray(@[3'i32, 1, 2]),
  newArray(@["c", "a", "b"]))

# Sort by column
let sorted = batch.sortBy("id")
# id: [1, 2, 3], name: ["a", "b", "c"]

# Slice by index
let indices = newArray(@[0'u64, 2])
let subset = batch.take(indices)
# id: [3, 2], name: ["c", "b"]

# Filter with expression
let expr = col("id") > 1'i32
let filtered = batch.filter(expr)
```

Requires `ensureComputeInitialized()` (dispatches through Arrow's compute function registry).

---

## 10. Acero Project Node

**Use case:** Select, rename, or derive columns in an Acero execution plan — columnar transformations in a pipeline.

```nim
# Select and rename columns
let result = projectTable(table,
  expressions = [col("name"), col("age")],
  names = ["person_name", "person_age"])

# Derive a new column
let result = projectTable(table,
  expressions = [col("name"), col("age") + 10'i32],
  names = ["name", "age_plus_10"])
```

Fluent pipeline: `source → filter → project → sink`.

**Key types:** `ProjectNodeOptions`, `buildProjectNode()`, `projectTable()`.

---

## 11. Parquet Column-Level Writing

**Use case:** Write columns individually to Parquet — streaming writes, column-by-column ETL, lazy materialization.

```nim
let schema = newSchema([newField[int32]("id"), newField[string]("name")])
let writer = newParquetFileWriter(openOutputStream("/tmp/out.parquet"), schema)

# Option A: Write a pre-built RecordBatch directly
let batch = newRecordBatch(schema, ids, names)
writer.writeRecordBatch(batch)

# Option B: Write column by column (streaming)
let group = writer.newBufferedRowGroup()
group.writeChunkedArray(chunkedIds)
group.writeChunkedArray(chunkedNames)
group.close()

writer.close()
```

**Key types:** `newBufferedRowGroup()`, `writeRecordBatch()`.

---

## 12. Array Statistics

**Use case:** Profile data quality — null counts, distinct value counts, approximate cardinality.

```nim
let arr = newArray(@[1'i32, 2, 2, 3, 3, 3])
let stats = newArrayStatistics(arr)
echo stats.nullCount      # 0
echo stats.distinctCount  # 3 (approximate)
echo stats.isNullCountExact  # true
```

Useful for data profiling pipelines, query planners (knowing distinct count helps with join strategies), and data quality dashboards.

**Key types:** `ArrayStatistics`, `newArrayStatistics()`, `nullCount`, `distinctCount`.

---

## 13. Null Arrays

**Use case:** Placeholder columns in schema templates, fully-null sentinel values, schema evolution scaffolding.

```nim
# A column that is entirely null — all 100 rows are null
var builder = newNullArrayBuilder()
builder.appendNulls(100)
let nullCol = builder.finish()

# Or create directly
let nullCol = newNullArray(100)
echo nullCol.len       # 100
echo nullCol.isNull(0) # true
```

**Key types:** `NullArray`, `NullArrayBuilder`, `NullScalar`.

---

## 14. Acero Plan Introspection

**Use case:** Debug execution plans — inspect node types and output schemas in complex pipelines.

```nim
let plan = newExecutePlan(ctx)
let source = plan.buildSourceNode(newSourceNodeOptions(table))
let filterNode = plan.buildFilterNode(source,
  newFilterNodeOptions(col("id") > 2'i32))
discard plan.buildSinkNode(filterNode, sinkOpts)

# Inspect the plan
let nodes = plan.getNodes()
var current = nodes
while current != nil:
  let node = cast[ptr GArrowExecuteNode](current.data)
  echo getKindName(ExecuteNode(handle: node))  # "SourceNode", "FilterNode", "SinkNode"
  echo node.outputSchema
  current = current.next
```

**Key types:** `getKindName()`, `plan.getNodes()`, `node.outputSchema`.

---

## 15. Fixed-Size Binary Arrays

**Use case:** Store fixed-width byte sequences — IP addresses (4 or 16 bytes), UUIDs (16 bytes), hash digests, routing keys.

```nim
# Store SHA-256 hashes (32 bytes each)
var builder = newFixedSizeBinaryArrayBuilder(32)
builder.append(@[0x00'u8, 0x01, ...])  # exactly 32 bytes
builder.append(@[0xFF'u8, 0xEE, ...])
let arr = builder.finish()
echo arr.byteWidth  # 32
echo arr[0][0..3]   # first 4 bytes
```

More compact than variable-length binary (no offset array overhead). Arrow optimizes fixed-width access.

**Key types:** `FixedSizeBinaryArray`, `FixedSizeBinaryArrayBuilder`, `FixedSizeBinaryDataType`.

---

## 16. Tensor

**Use case:** Store multi-dimensional numeric data — ML feature vectors, image data, time-series windows.

```nim
# 2D tensor: 2 rows x 3 cols of float64
let values = @[1'f64, 2, 3, 4, 5, 6]
let buf = newBuffer(cast[pointer](values[0].unsafeAddr), 48)
let tensor = newTensor(newGType(float64), buf, [2'i64, 3],
  dimNames = ["rows", "cols"])

echo tensor.nDimensions   # 2
echo tensor.shape         # @[2, 3]
echo tensor.isRowMajor    # true
echo tensor.dimensionName(0)  # "rows"
```

**Key types:** `Tensor`, `newTensor()`, `shape`, `strides`, `isContiguous`.

---

## 17. BinaryView / StringView

**Use case:** Consume Arrow IPC data that uses the new view-based string/binary layout (zero-copy substring operations).

```nim
# View arrays are read-only in GLib — they typically come from
# Arrow IPC files using the newer binary/string view format.
# They save memory by avoiding separate offset buffers.

let reader = openIPCFile("/data/view_arrays.ipc")
let batch = reader.read()
# batch columns may contain StringViewArray or BinaryViewArray
```

These types are created by Arrow's IPC reader when data uses the view format.
Construction from Nim is not supported (no GLib builders).

**Key types:** `StringViewArray`, `BinaryViewArray`.

---

## 18. Dictionary-Encoded Arrays

**Use case:** Compress low-cardinality string columns — country codes, status fields, categories.

```nim
# 5 rows, only 3 unique values
let indices = newArray(@[0'i32, 1, 2, 0, 1])
let dictionary = newArray(@["alpha", "beta", "gamma"])
let dt = newDictionaryDataType(newGType(int32), newGType(string))
let arr = newDictionaryArray(dt, indices, dictionary)

# 5 logical rows, but strings stored once in dictionary
echo arr.len  # 5

# Decode back via Arrow's encode kernel with DictionaryEncodeOptions
let opts = newDictionaryEncodeOptions()
```

10× to 100× compression for low-cardinality categorical data. Arrow's compute kernels natively understand dictionary arrays.

**Key types:** `DictionaryArray`, `DictionaryDataType`, `DictionaryEncodeOptions`.

---

## 19. Run-End Encoded Arrays

**Use case:** Compress repeated-value runs — time series with constant periods, sorted columns with duplicates, grid data.

```nim
# "a" repeats 3 times, "b" repeats 2, "c" repeats 2 → 7 logical values
let runEnds = newArray(@[3'i32, 5, 7])
let values = newArray(@["a", "b", "c"])
let dt = newRunEndEncodedDataType(newGType(int32), newGType(string))
let arr = newRunEndEncodedArray(dt, 7, runEnds, values)

echo arr.len  # 7 (logical length)

# Decode to regular array
let decoded = arr.decode  # ["a", "a", "a", "b", "b", "c", "c"]

# Physical → logical mapping
echo arr.findPhysicalOffset  # offset into physical arrays
```

Excellent compression when data changes infrequently. Arrow's compute kernels work directly on the compressed representation.

**Key types:** `RunEndEncodedArray`, `RunEndEncodedDataType`, `decode()`.

---

## 20. Union Arrays

**Use case:** Store heterogeneous rows — variant/one-of types, tagged unions, event logs with different field schemas.

```nim
# Two member types: int32 (field "a") and string (field "b")
let f1 = newField[int32]("a")
let f2 = newField[string]("b")
var codes = [0'i8, 1]

let dt = newSparseUnionDataType([f1.toPtr, f2.toPtr], addr codes[0], 2)

# Sparse union: each row is either int32 OR string
let typeIds = newArray(@[0'i8, 1, 0])
let intArr = newArray(@[10'i32, 20, 30])
let strArr = newArray(@["x", "y", "z"])
let arr = newSparseUnionArray(dt,
  cast[ptr GArrowInt8Array](typeIds.toPtr),
  [intArr.toPtr, strArr.toPtr])

# Dense union: similar but uses offset array for value packing
let offsets = newArray(@[0'i32, 0, 1])
let denseArr = newDenseUnionArray(dt,
  cast[ptr GArrowInt8Array](typeIds.toPtr),
  cast[ptr GArrowInt32Array](offsets.toPtr),
  [intArr.toPtr, strArr.toPtr])
```

Useful for event sourcing, semi-structured log formats, GraphQL-style responses. Sparse unions are simpler; dense unions pack values tightly.

**Key types:** `SparseUnionArray`, `DenseUnionArray`, `SparseUnionDataType`, `DenseUnionDataType`, `getValueOffset()`.

---

## 21. Compute Function Options

**Use case:** Pass typed option objects to Arrow compute kernels — sort ordering, variance parameters, rounding mode, etc.

```nim
# ArraySortOptions: control sort direction per array
let opts = newArraySortOptions(GARROW_SORT_ORDER_DESCENDING)

# VarianceOptions: control variance algorithm
let varianceOpts = newVarianceOptions()

# RoundOptions: control rounding behavior
let roundOpts = newRoundOptions()

# Pass to generic function execution
let result = execute("sort_indices", [datum(arr)], opts.handle)

# For sorting with per-column direction, use SortKey + SortOptions:
let keys = [newSortKey("age", GARROW_SORT_ORDER_DESCENDING)]
let sortOpts = newSortOptions(keys)
```

Previously options were created as `FunctionOptions()` with raw handles only. Typed wrappers add type safety and discoverability.

**Key types:** `ArraySortOptions`, `VarianceOptions`, `RoundOptions`, `IndexOptions`, `JoinOptions`, `WinsorizeOptions`, `ScalarAggregateOptions`, `EqualOptions`, `SetLookupOptions`.

---

## 22. Cloud Filesystems (GCS/Azure/HDFS)

**Use case:** Provide type shells for GCS, Azure, and HDFS filesystem objects — used by Arrow Dataset's FileSystem discovery.

```nim
# Types exist for type introspection and forward compatibility.
# Instances are created by Arrow Dataset when resolving URIs like
# "gs://bucket/path", "abfs://container/path", "hdfs://namenode/path"

let fs: GcsFileSystem = ...  # obtained from Arrow Dataset discovery
```

No Nim constructors exist (Arrow GLib only exposes `get_type` for these). Useful for type-casting when Arrow Dataset returns a generic `FileSystem`.

**Key types:** `GcsFileSystem`, `AzureFileSystem`, `HdfsFileSystem`.

---

## 23. Arrow C Data Interface

**Use case:** Zero-copy data exchange across language/runtime boundaries — share Arrow data between Python (PyArrow), Rust (arrow-rs), C++, and Nim without serialization.

```nim
# Export a RecordBatch for another runtime to read
let batch = newRecordBatch(schema, col1, col2)
let (arrayPtr, schemaPtr) = batch.exportRecordBatch()
# Pass arrayPtr and schemaPtr to Python via ctypes/cffi

# Import a RecordBatchReader streamed from another runtime
let reader = importRecordBatchReader(externalArrayStreamPointer)
while true:
  let batch = reader.readNext()
  if batch.isNil:
    break
  echo batch.nRows
```

The standard for interop with other Arrow implementations. Pointers follow the Arrow C Data Interface spec with release callbacks.

**Key types:** `exportSchema()`, `exportRecordBatch()`, `importRecordBatchReader()`, `exportRecordBatchReader()`.

---

## 24. Partitioning Dictionaries (Hive/Directory)

**Use case:** Hive and directory partitioning already work correctly — dictionary support for partition fields is available through the C API when needed.

```nim
# Partitioning works without dictionaries (nil is the correct default)
let partitioning = newDirectoryPartitioning(schema)
let hivePart = newHivePartitioning(schema)

# With custom segment encoding options
let opts = newKeyValuePartitioningOptions()
opts.segmentEncoding = None  # no URI encoding
let part = newDirectoryPartitioning(schema, opts)
```

Partitioning dictionaries allow customizing how partition values are encoded. `nil` means "use defaults" which handles most cases. Explicit dictionary support can be added on demand.

**Key types:** `newDirectoryPartitioning()`, `newHivePartitioning()`, `KeyValuePartitioningOptions`.

---

## Quick Reference: When to Use What

| Your need | Use |
|-----------|-----|
| Join two tables | `joinTables()` |
| Store protobuf/image bytes | `Array[seq[byte]]` / Binary |
| Store time spans | `DurationArray`, `MonthIntervalArray` |
| Exact currency amounts | `Decimal128Array` |
| Read `.csv.gz` files | `openCompressedInputStream()` |
| Store ML weights at 16-bit | `HGFloatArray` |
| Read from S3 | `initializeS3()`, S3 filesystem |
| Tag schema with version info | `schema.withMetadata()` |
| Slice/filter a RecordBatch | `batch.take()`, `batch.filter()` |
| Select/rename columns in pipeline | `projectTable()` |
| Write Parquet column by column | `newBufferedRowGroup()` |
| Profile null/distinct counts | `newArrayStatistics()` |
| Scaffold null-only columns | `NullArray` |
| Debug an Acero plan | `plan.getNodes()` |
| Fixed-width byte records | `FixedSizeBinaryArray` |
| Multi-dimensional data | `Tensor` |
| Consume IPC with view strings | `StringViewArray` |
| Compress categorical strings | `DictionaryArray` |
| Compress repeated runs | `RunEndEncodedArray` |
| Heterogeneous row types | `UnionArray` |
| Typed compute options | `newArraySortOptions()`, etc. |
| Zero-copy cross-language data | `exportRecordBatch()`, `importRecordBatchReader()` |
