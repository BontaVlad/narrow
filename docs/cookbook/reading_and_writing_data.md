# Reading and Writing Data

Recipes related to reading and writing data from disk using [narrow](https://github.com/your-org/narrow) — a Nim wrapper over Apache Arrow.

.. contents::
----

## Write a Parquet file

Given an array with 100 numbers, from 0 to 99, to write it to a Parquet file
we must first create an `ArrowTable` — Parquet is a columnar format that
requires named columns. We then pass the table to `writeTable`:

```nim test
import std/[sequtils, os]
import narrow

let arr    = newArray(toSeq(0 .. 99))
let schema = newSchema([newField[int]("col1")])
let table  = newArrowTable(schema, arr)

let path = getTempDir() / "example.parquet"
writeTable(table, path)

echo $arr[0] & " .. " & $arr[99]
```

```
0 .. 99
```

## Reading a Parquet file

A Parquet file is read back to an `ArrowTable` using `readTable`. The
resulting table exposes every column as a `ChunkedArray`:

```nim test
import std/[sequtils, os]
import narrow

let path = getTempDir() / "example.parquet"
block:
  let arr    = newArray(toSeq(0 .. 99))
  let schema = newSchema([newField[int]("col1")])
  writeTable(newArrowTable(schema, arr), path)

let table = readTable(path)
echo table
```

```
col1: int64
----
col1:
  [
    [
      0,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      ...
      90,
      91,
      92,
      93,
      94,
      95,
      96,
      97,
      98,
      99
    ]
  ]
```

## Reading a subset of Parquet data

`readTable` accepts `columns` and `filter` arguments to limit which data is
loaded into memory. Filters are built with the `compute/expressions` module
using the `col` helper:

```nim test
import std/[sequtils, os]
import narrow

let path = getTempDir() / "example.parquet"
block:
  let arr    = newArray(toSeq(0'i64 .. 99'i64))
  let schema = newSchema([newField[int64]("col1")])
  writeTable(newArrowTable(schema, arr), path)

let filter = col("col1") > 5'i64 and col("col1") < 10'i64
let table  = readTable(path,
               columns = @["col1"],
               filter  = filter)
echo table

```

```
col1: int64
----
col1:
  [
    [
      6,
      7,
      8,
      9
    ]
  ]
```

## Saving Arrow Arrays to disk

Apart from Parquet, data can be saved in the raw Arrow IPC format, which
allows direct memory mapping from disk. We wrap the array in a `RecordBatch`
and write it through an `IpcFileWriter`:

```nim test
import std/[sequtils, os]
import narrow

let arr    = newArray(toSeq(0'i64 .. 99'i64))
let schema = newSchema([newField[int64]("nums")])
let batch  = newRecordBatch(schema, arr)
let path   = getTempDir() / "arraydata.arrow"
let fs     = newFileSystem(path)

with fs.openOutputStream(path), stream:
  with newIpcFileWriter(stream, schema), writer:
    writer.writeRecordBatch(batch)

let restored = readIpcFile(path)
echo $restored["nums", int64][0] & " .. " & $restored["nums", int64][99]

```

```
0 .. 99
```

## Memory Mapping Arrow Arrays from disk

IPC files can be memory-mapped back directly from disk without copying into
heap memory using `newMemoryMappedInputStream`:

```nim test
import std/[sequtils, os]
import narrow

let path   = getTempDir() / "arraydata.arrow"
let arr    = newArray(toSeq(0'i64 .. 99'i64))
let schema = newSchema([newField[int64]("nums")])
let batch  = newRecordBatch(schema, arr)
let fs     = newFileSystem(path)

with fs.openOutputStream(path), stream:
  with newIpcFileWriter(stream, schema), writer:
    writer.writeRecordBatch(batch)

with newMemoryMappedInputStream(path), stream:
  let reader   = newIpcFileReader(stream)
  let restored = reader.readAll()
  let col      = restored["nums", int64]
  echo $col[0] & " .. " & $col[99]

```

```
0 .. 99
```

## Writing CSV files

An `ArrowTable` is written to CSV with `writeCsv`. Pass `newWriteOptions` to
control whether a header row is included:

```nim test
import std/[sequtils, os]
import narrow

let arr    = newArray(toSeq(0 .. 99))
let schema = newSchema([newField[int]("col1")])
let table  = newArrowTable(schema, arr)
let path   = getTempDir() / "table.csv"

writeCsv(path, table, newWriteOptions(includeHeader = true))

let restored = readCSV(path)
echo restored.nRows
echo restored.nColumns

```

```
100
1
```

## Writing CSV files incrementally

To write CSV incrementally — chunk by chunk as data is generated — without
keeping the whole table in memory, call `writeCsv` once per chunk:

```nim test
import std/os
import narrow

let schema = newSchema([newField[int32]("col1")])
let path   = getTempDir() / "incremental.csv"

for chunk in 0 ..< 10:
  var chunkData: seq[int32]
  for j in chunk * 10 ..< (chunk + 1) * 10:
    chunkData.add(j.int32)
  let chunkTable = newArrowTable(schema, newArray(chunkData))
  writeCsv(path, chunkTable, newWriteOptions(batchSize = 10))

let restored = readCSV(path)
echo restored.nRows
```

```
10
```

----

## Reading CSV files

`readCSV` reads an `ArrowTable` from a CSV file, inferring column types
automatically:

```nim test
import std/[sequtils, os]
import narrow

let path = getTempDir() / "table.csv"
block:
  let arr    = newArray(toSeq(0'i64 .. 99'i64))
  let schema = newSchema([newField[int64]("col1")])
  writeCsv(path, newArrowTable(schema, arr), newWriteOptions(includeHeader = true))

let table = readCSV(path)
echo table
```

```
col1: int64
----
col1:
  [
    [
      0,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      ...
      90,
      91,
      92,
      93,
      94,
      95,
      96,
      97,
      98,
      99
    ]
  ]
```

## Writing Partitioned Datasets

When your dataset is large it makes sense to split it into multiple files.
Partition by any column by writing each subset to its own subdirectory:

```nim test
import std/[os, strformat]
import narrow

let schema = newSchema([
  newField[int32]("day"),
  newField[int32]("month"),
  newField[int32]("year"),
])

var days, months, years: seq[int32]
for i in 0 ..< 100:
  days.add((i mod 30 + 1).int32)
  months.add((i mod 12 + 1).int32)
  years.add((2000 + i div 10).int32)

let table   = newArrowTable(schema, newArray(days), newArray(months), newArray(years))
let baseDir = getTempDir() / "partitioned"

for year in 2000 .. 2009:
  let dir = baseDir / $year
  createDir(dir)
  writeTable(table, dir / "part-0.parquet")

let localfs = newLocalFileSystem()
for year in 2000 .. 2009:
  let path = baseDir / $year / "part-0.parquet"
  echo localfs.getFileInfo(path).exists
```

```
true
true
true
true
true
true
true
true
true
true
```

## Reading Partitioned Datasets

`newDataset` discovers all Parquet files under a directory and exposes them
as a single logical dataset.

For example given a layout like:

```
examples/
├── dataset0.parquet
├── dataset1.parquet
└── dataset2.parquet
```

```nim test
import std/[os, strformat]
import narrow

let schema  = newSchema([newField[int32]("id"), newField[string]("name")])
let baseDir = getTempDir() / "examples"
createDir(baseDir)

for i in 0 ..< 3:
  var ids: seq[int32]
  var names: seq[string]
  for j in 0 ..< 10:
    ids.add((i * 10 + j).int32)
    names.add(fmt"name_{i}_{j}")
  writeTable(newArrowTable(schema, newArray(ids), newArray(names)),
             baseDir / fmt"dataset{i}.parquet")

let ds    = newDataset(baseDir)
let table = ds.toTable()
echo table.nRows
echo table.nColumns

```

```
30
2
```

```nim test
import std/[os, strformat]
import narrow

let schema  = newSchema([newField[int32]("id")])
let baseDir = getTempDir() / "batched"
createDir(baseDir)

for i in 0 ..< 3:
  var ids: seq[int32]
  for j in 0 ..< 10:
    ids.add((i * 10 + j).int32)
  writeTable(newArrowTable(schema, newArray(ids)),
             baseDir / fmt"dataset{i}.parquet")

#for batch in newDataset(baseDir).toBatches():
#  let col = batch.column("id")
#  echo col.name & " = " & $col[0] & " .. " & $col[^1]

```

```

```

## Reading Partitioned Data from S3

> **Note:** S3 support is not yet implemented in narrow. This recipe is a
> placeholder — track progress on the issue tracker.

When S3 support lands the API will mirror the local filesystem API: pass an
S3 URI to `newDataset` and iterate with `toTable` or `toBatches` exactly as
you would for a local dataset.

> **Warning:** AWS credentials must be configured in
> `~/.aws/credentials` (macOS / Linux) or
> `C:\Users\<USERNAME>\.aws\credentials` (Windows):
>
> ```
> [default]
> aws_access_key_id=<YOUR_AWS_ACCESS_KEY_ID>
> aws_secret_access_key=<YOUR_AWS_SECRET_ACCESS_KEY>
> ```

----

## Write a Feather file

In narrow, Feather is the Arrow IPC file format on disk — `writeIpcFile` is
the equivalent of `pyarrow.feather.write_feather`:

```nim test
import std/[sequtils, os]
import narrow

let arr    = newArray(toSeq(0'i64 .. 99'i64))
let schema = newSchema([newField[int64]("col1")])
let table  = newArrowTable(schema, arr)
let path   = getTempDir() / "example.feather"

writeIpcFile(path, table)

let localfs = newLocalFileSystem()
echo localfs.getFileInfo(path).exists
echo $arr[0] & " .. " & $arr[99]

```

```
true
0 .. 99
```

## Reading a Feather file

A Feather file is read back with `readIpcFile`. The resulting table exposes
columns as `ChunkedArray`:

```nim test
import std/[sequtils, os]
import narrow

let path = getTempDir() / "example.feather"
block:
  let arr    = newArray(toSeq(0'i64 .. 99'i64))
  let schema = newSchema([newField[int64]("col1")])
  writeIpcFile(path, newArrowTable(schema, arr))

let table = readIpcFile(path)
echo table.nRows
echo table.nColumns
echo table["col1", int64][0]
echo table["col1", int64][99]

```

```
100
1
0
99
```

----

## Reading Line Delimited JSON

narrow has built-in support for line-delimited JSON (JSONL / NDJSON) via
`readJSON`. Each line must be a JSON object representing one row:

```nim test
import std/os
import narrow

let path = getTempDir() / "data.json"
writeFile(path, """{"a": 1, "b": 2.0, "c": 1}
{"a": 3, "b": 3.0, "c": 2}
{"a": 5, "b": 4.0, "c": 3}
{"a": 7, "b": 5.0, "c": 4}""")

let table = readJSON(path)
echo table.nRows
echo table.nColumns

let aCol = table["a", int64]
echo aCol[0]
echo aCol[1]
echo aCol[2]
echo aCol[3]
```

```
4
3
1
3
5
7
```

## Writing Compressed Data

narrow supports writing Parquet files with compression via `WriterProperties`.
Parquet uses Snappy by default when no properties are specified:

```nim test
import std/os
import narrow

let schema = newSchema([newField[int64]("numbers")])
let data   = newArray(@[1'i64, 2, 3, 4, 5])
let table  = newArrowTable(schema, data)

let pathSnappy = getTempDir() / "compressed.parquet"
let pathGzip   = getTempDir() / "compressed_gzip.parquet"
let pathZstd   = getTempDir() / "compressed_zstd.parquet"

writeTable(table, pathSnappy)

var propsGzip = newWriterProperties()
propsGzip.setCompression("numbers", GARROW_COMPRESSION_TYPE_GZIP)
writeTable(table, pathGzip, wp = propsGzip)

var propsZstd = newWriterProperties()
propsZstd.setCompression("numbers", GARROW_COMPRESSION_TYPE_ZSTD)
writeTable(table, pathZstd, wp = propsZstd)

echo readTable(pathSnappy) == readTable(pathGzip)
echo readTable(pathSnappy) == readTable(pathZstd)

```

```
true
true
```

## Reading Compressed Data

Reading compressed Parquet files requires no special handling — `readTable`
detects and decompresses automatically regardless of the codec used when
writing:

```nim test
import std/os
import narrow

let schema = newSchema([newField[int64]("numbers")])
let data   = newArray(@[1'i64, 2, 3, 4, 5])
let table  = newArrowTable(schema, data)

let pathSnappy = getTempDir() / "read_snappy.parquet"
let pathGzip   = getTempDir() / "read_gzip.parquet"
let pathZstd   = getTempDir() / "read_zstd.parquet"

writeTable(table, pathSnappy)

var propsGzip = newWriterProperties()
propsGzip.setCompression("numbers", GARROW_COMPRESSION_TYPE_GZIP)
writeTable(table, pathGzip, wp = propsGzip)

var propsZstd = newWriterProperties()
propsZstd.setCompression("numbers", GARROW_COMPRESSION_TYPE_ZSTD)
writeTable(table, pathZstd, wp = propsZstd)

let restoredSnappy = readTable(pathSnappy)
let restoredGzip   = readTable(pathGzip)
let restoredZstd   = readTable(pathZstd)

assert restoredSnappy["numbers", int64] == newChunkedArray(@[data])
assert restoredGzip["numbers", int64]   == newChunkedArray(@[data])
assert restoredZstd["numbers", int64]   == newChunkedArray(@[data])
echo restoredSnappy

```

```
numbers: int64
----
numbers:
  [
    [
      1,
      2,
      3,
      4,
      5
    ]
  ]
```
