import std/[os]
import unittest2
import testfixture
import ../src/narrow/[column/primitive, tabular/table, column/metadata, io/parquet, io/parquet_statistics]

# TODO: Low quality tests - just checking we can access properties without crashing. Need to add more thorough tests for statistics values and edge cases.
suite "Parquet Statistics and Metadata":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_io/parquet_statistics")

  test "read metadata from parquet file":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let reader = newFileReader(uri)
    let metadata = reader.metadata

    check metadata.nRows > 0
    check metadata.nRowGroups > 0
    check metadata.nColumns > 0
    check metadata.size > 0

  # test "access row group metadata":
  #   let uri = getCurrentDir() & "/tests/fatboy.parquet"
  #   let reader = newFileReader(uri)
  #   let metadata = reader.metadata

  #   if metadata.nRowGroups > 0:
  #     let rowGroup = metadata.rowGroup(0)
  #     check rowGroup.nColumns > 0
  #     check rowGroup.nRows > 0

  # test "access column chunk metadata and statistics":
  #   let uri = getCurrentDir() & "/tests/fatboy.parquet"
  #   let reader = newFileReader(uri)
  #   let metadata = reader.metadata

  #   if metadata.nRowGroups > 0 and metadata.nColumns > 0:
  #     let rowGroup = metadata.rowGroup(0)
  #     let columnChunk = rowGroup.columnChunk(0)

  #     check columnChunk.totalSize >= 0
  #     check columnChunk.canDecompress or not columnChunk.canDecompress

  #     let stats = columnChunk.statistics
  #     check stats.valueCount >= 0

  # test "statistics properties are accessible":
  #   let uri = getCurrentDir() & "/tests/fatboy.parquet"
  #   let reader = newFileReader(uri)
  #   let metadata = reader.metadata

  #   if metadata.nRowGroups > 0 and metadata.nColumns > 0:
  #     let rowGroup = metadata.rowGroup(0)
  #     let columnChunk = rowGroup.columnChunk(0)
  #     let stats = columnChunk.statistics

  #     discard stats.hasNulls
  #     discard stats.nullCount
  #     discard stats.hasDistinctValues
  #     discard stats.distinctValueCount
  #     discard stats.hasMinMax

  test "write and read parquet preserves row counts":
    let
      schema = newSchema([newField[int32]("value")])
      values = newArray(@[1i32, 2i32, 3i32, 4i32, 5i32])
      table = newArrowTable(schema, values)

    let uri = fixture / "stats.parquet"
    writeTable(table, uri)

    let reader = newFileReader(uri)
    let metadata = reader.metadata

    check metadata.nRows == 5
    check metadata.nColumns == 1

  test "file metadata createdBy is not empty":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let reader = newFileReader(uri)
    let metadata = reader.metadata

    check metadata.createdBy.len > 0

  test "equality comparison for metadata":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let reader1 = newFileReader(uri)
    let reader2 = newFileReader(uri)

    let metadata1 = reader1.metadata
    let metadata2 = reader2.metadata

    check metadata1 == metadata2
