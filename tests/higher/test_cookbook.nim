import std/[os, sequtils, strformat]
import unittest2
import ../testfixture

import ../../src/narrow/[core/ffi, column/primitive, column/metadata, tabular/table, tabular/batch, tabular/dataset, io/parquet, io/ipc, io/csv, io/json, io/filesystem, compute/expressions]

suite "Reading and Writing Data":

  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_parquet")

  teardown:
    fixture.cleanup()

  test "Write a parquet file":
    let schema = newSchema([
      newField[int]("id"),
    ])
    let data = newArray(toSeq(0 .. 99))

    let table = newArrowTable(schema, data)
    let uri = fixture / "table.parquet"
    writeTable(table, uri)

  test "Reading a Parquet file":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let table = readTable(uri)
    check table["int64_col"].len == 10

  test "Reading a subset of Parquet data":
    let uri = getCurrentDir() & "/tests/fatboy.parquet"
    let filter = col("int64_col") >  1000000'i64 and col("int64_col") < 5000000'i64
    let table = readTable(uri, columns = @["int64_col", "string_col"], filter=filter)
    check table["int64_col"].len == 3

  test "Saving Arrow Arrays to disk":
    let
      schema = newSchema([newField[int64]("nums"),])
      uri = fixture / "arraydata.arrow"
      fs = newFileSystem(uri)
      arr = newArray(toSeq(0 .. 99))
      batch = newRecordBatch(schema, arr)

    with fs.openOutputStream(uri), stream:
      with newIpcFileWriter(stream, schema), writer:
        writer.writeRecordBatch(batch)

    let restored = readIpcFile(uri)
    check restored["nums", int64] == newChunkedArray(@[arr])


  test "Memory Mapping Arrow Arrays from disk":
    let
      schema = newSchema([newField[int64]("nums"),])
      uri = fixture / "arraydata.arrow"
      fs = newFileSystem(uri)
      arr = newArray(toSeq(0 .. 99))
      batch = newRecordBatch(schema, arr)

    writeIpcFile(uri, batch)
    with newMemoryMappedInputStream(uri), stream:
      let reader = newIpcFileReader(stream)
      let restored = reader.readAll()
      check restored["nums", int64] == newChunkedArray(@[arr])

  test "Writing CSV files":
    let schema = newSchema([
      newField[int]("col1"),
    ])
    let data = newArray(toSeq(0 .. 99))
    let table = newArrowTable(schema, data)
    let uri = fixture / "table.csv"
    
    let opts = newWriteOptions(includeHeader=true)
    writeCsv(uri, table, opts)
    
    # Verify file was created and can be read back
    let restored = readCSV(uri)
    check restored.nRows == 100
    check restored.nColumns == 1

  test "Writing CSV files incrementally":
    let schema = newSchema([
      newField[int32]("col1"),
    ])
    let uri = fixture / "incremental.csv"
    
    # Write data in chunks using batchSize option
    for chunk in 0 ..< 10:
      var chunkData: seq[int32]
      for j in (chunk * 10) ..< ((chunk + 1) * 10):
        chunkData.add(j.int32)
      let chunkArr = newArray(chunkData)
      let chunkTable = newArrowTable(schema, chunkArr)
      
      # Use small batch sizes to write incrementally
      let opts = newWriteOptions(batchSize=10)
      writeCsv(uri, chunkTable, opts)
    
    # Verify last chunk was written
    let restored = readCSV(uri)
    check restored.nRows == 10

  test "Reading CSV files":
    # First create a CSV file
    let schema = newSchema([
      newField[int64]("col1"),
    ])
    var data: seq[int64]
    for i in 0 ..< 100:
      data.add(i.int64)
    let arr = newArray(data)
    let table = newArrowTable(schema, arr)
    let uri = fixture / "input.csv"
    writeCsv(uri, table, newWriteOptions())
    
    # Now read it back
    let restored = readCSV(uri)
    check restored.nRows == 100
    check restored.nColumns == 1

  test "Writing Partitioned Datasets":
    let schema = newSchema([
      newField[int32]("day"),
      newField[int32]("month"),
      newField[int32]("year"),
    ])
    
    # Create sample data with years 2000-2009
    var days, months, years: seq[int32]
    for i in 0 ..< 100:
      days.add((i mod 30 + 1).int32)
      months.add((i mod 12 + 1).int32)
      years.add((2000 + i div 10).int32)
    
    let table = newArrowTable(schema, 
      newArray(days), newArray(months), newArray(years))
    
    let partitionDir = fixture / "partitioned"
    createDir(partitionDir)
    
    # Write each year as a separate partition
    for year in 2000 .. 2009:
      let uri = partitionDir / fmt"{year}.parquet"
      writeTable(table, uri)
    
    # Verify partitions were created
    let fs = newLocalFileSystem()
    for year in 2000 .. 2009:
      let uri = partitionDir / fmt"{year}.parquet"
      check fs.getFileInfo(uri).exists

  test "Reading Partitioned Datasets":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    
    # Create multiple parquet files
    let partitionDir = fixture / "multi_file_dataset"
    createDir(partitionDir)
    
    for i in 0 ..< 3:
      var ids: seq[int32]
      var names: seq[string]
      for j in 0 ..< 10:
        ids.add((i * 10 + j).int32)
        names.add(fmt"name_{i}_{j}")
      let idArr = newArray(ids)
      let nameArr = newArray(names)
      let table = newArrowTable(schema, idArr, nameArr)
      let uri = partitionDir / fmt"dataset{i}.parquet"
      writeTable(table, uri)
    
    # Read as a dataset
    let ds = newDataset(partitionDir)
    let combinedTable = ds.toTable()
    check combinedTable.nRows == 30
    check combinedTable.nColumns == 2

  test "Reading Partitioned Data from S3":
    skip()

  test "Write a Feather file":
    # Feather format is essentially the IPC format
    let schema = newSchema([
      newField[int64]("col1"),
    ])
    var data: seq[int64]
    for i in 0 ..< 100:
      data.add(i.int64)
    let arr = newArray(data)
    let table = newArrowTable(schema, arr)
    
    # Write as Feather (using IPC format)
    let uri = fixture / "example.feather"
    writeIpcFile(uri, table)
    
    # Verify file exists
    let fs = newLocalFileSystem()
    check fs.getFileInfo(uri).exists

  test "Reading a Feather file":
    # First write a Feather file (using IPC format)
    let schema = newSchema([
      newField[int64]("col1"),
    ])
    var data: seq[int64]
    for i in 0 ..< 100:
      data.add(i.int64)
    let arr = newArray(data)
    let table = newArrowTable(schema, arr)
    let uri = fixture / "example.feather"
    writeIpcFile(uri, table)
    
    # Read it back
    let restored = readIpcFile(uri)
    check restored.nRows == 100
    check restored.nColumns == 1
    check restored["col1", int64][0] == 0
    check restored["col1", int64][99] == 99

  test "Reading Line Delimited JSON":
    # Create a JSON file with line-delimited JSON objects
    let jsonPath = fixture / "data.json"
    let jsonContent = """{"a": 1, "b": 2.0, "c": 1}
{"a": 3, "b": 3.0, "c": 2}
{"a": 5, "b": 4.0, "c": 3}
{"a": 7, "b": 5.0, "c": 4}"""
    writeFile(jsonPath, jsonContent)
    
    # Read the JSON file
    let table = readJSON(jsonPath)
    check table.nRows == 4
    check table.nColumns == 3
    
    let aCol = table["a", int64]
    check aCol[0] == 1
    check aCol[1] == 3
    check aCol[2] == 5
    check aCol[3] == 7

  test "Writing Compressed Data":
    let schema = newSchema([
      newField[int64]("numbers"),
    ])
    let data = newArray(@[1'i64, 2, 3, 4, 5])
    let table = newArrowTable(schema, data)
    
    # Write Parquet file - compression is default (SNAPPY)
    let uri = fixture / "compressed.parquet"
    writeTable(table, uri)
    
    # Verify file exists
    let fs = newLocalFileSystem()
    check fs.getFileInfo(uri).exists
    
    # # Write with GZIP compression using WriterProperties
    let uri2 = fixture / "compressed_gzip.parquet"
    var props2 = newWriterProperties()
    props2.setCompression("numbers", GARROW_COMPRESSION_TYPE_GZIP)
    writeTable(table, uri2, wp=props2)

    check fs.getFileInfo(uri2).exists
    check readTable(uri) == readTable(uri2)

  test "Reading Compressed Data":
    let schema = newSchema([
      newField[int64]("numbers"),
    ])
    let data = newArray(@[1'i64, 2, 3, 4, 5])
    let table = newArrowTable(schema, data)
    
    # Write compressed Parquet using ZSTD
    let uri = fixture / "read_compressed.parquet"
    var props = newWriterProperties()
    props.setCompression("numbers", GARROW_COMPRESSION_TYPE_ZSTD)
    writeTable(table, uri, wp=props)
    
    # Read it back - compression is handled automatically
    let restored = readTable(uri)
    check restored.nRows == 5
    check restored.nColumns == 1
    check restored["numbers", int64] == newChunkedArray(@[data])
    

suite "Creating Arrow Objects":
  test "Creating Arrays":
    let arr = @[1, 2, 3, 4, 5]
    let garr = newArray[int](arr)
    check garr == arr

  test "Creating Arrays with mask to speccify which values should be considered null":
    let arr = @[1, 2, 3, 4, 5]
    let mask = @[true, false, true, false, true] # Mask to specify null values
    let garr = newArray[int](arr, mask)
    for i in 0 ..< arr.len:
      if mask[i]:
        check garr.isNull(i) # Check if the value is considered null
      else:
        check garr.isValid(i)
        check garr[i] == arr[i] # Check if the value is correctly included

  test "Creating Tables":
    let schema = newSchema([
      newField[int]("id"),
      newField[string]("name"),
      newField[float64]("value")
    ])

    let idArr = newArray(@[1, 2, 3, 4, 5])
    let nameArr = newArray(@["a", "b", "c", "d", "e"])
    let valueArr = newArray(@[1.0, 2.0, 3.0, 4.0, 5.0])
    
    let table = newArrowTable(schema, idArr, nameArr, valueArr)
    check table["id"] == newChunkedArray(@[idArr])
    check table["name"] == newChunkedArray(@[nameArr])
    check table[2] == newChunkedArray(@[valueArr])

  test "Creating Tables from tuples":
    let data = @[
        (col1: 1, col2: "a"),
        (col1: 2, col2: "b"),
        (col1: 3, col2: "c"),
        (col1: 4, col2: "d"),
        (col1: 5, col2: "e")
    ]
    let table = newArrowTable(data)
    check table["col1"] == newChunkedArray(@[newArray(@[1, 2, 3, 4, 5])])
    check table["col2"] == newChunkedArray(@[newArray(@["a", "b", "c", "d", "e"])])

