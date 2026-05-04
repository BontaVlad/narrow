import std/os
import unittest2
import testfixture
import ../src/narrow


suite "Dataset":

  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_dataset/parquet")

  teardown:
    fixture.cleanup()

  test "FinishOptions - inspectNFragments":
    var opts = newFinishOptions()
    unittest2.check opts.inspectNFragments == 1

    opts.inspectNFragments = 5
    unittest2.check opts.inspectNFragments == 5

    opts.inspectNFragments = -1
    unittest2.check opts.inspectNFragments == -1

  test "Create dataset from Parquet files":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[bool]("active")
    ])
    let ids1 = newArray(@[1'i32, 2, 3, 4])
    let names1 = newArray(@["alpha", "beta", "gamma", "delta"])
    let actives1 = newArray(@[true, false, true, false])
    
    let table1 = newArrowTable(schema, ids1, names1, actives1)
    let uri1 = fixture / "one.parquet"
    
    writeTable(table1, uri1)

    let ids2 = newArray(@[9'i32, 1, 1, 0])
    let names2 = newArray(@["Some", "things", "don't", "die"])
    let actives2 = newArray(@[false, true, true, false])
    
    let table2 = newArrowTable(schema, ids2, names2, actives2)
    let uri2 = fixture / "two.paruqet"

    writeTable(table2, uri2)

    let ds = newDataset(fixture / ".")
    let tbl = ds.toTable()
    unittest2.check tbl.nRows == 8
    unittest2.check tbl.nColumns == 3
    unittest2.check tbl["id"] == newChunkedArray([newArray(@[1'i32, 2, 3, 4, 9, 1, 1, 0]), ])


  test "Create dataset from Parquet files with filter":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[bool]("active")
    ])
    let ids1 = newArray(@[1'i32, 2, 3, 4])
    let names1 = newArray(@["alpha", "beta", "gamma", "delta"])
    let actives1 = newArray(@[true, false, true, false])
    
    let table1 = newArrowTable(schema, ids1, names1, actives1)
    let uri1 = fixture / "one.parquet"
    
    writeTable(table1, uri1)

    let ids2 = newArray(@[9'i32, 1, 1, 0])
    let names2 = newArray(@["Some", "things", "don't", "die"])
    let actives2 = newArray(@[false, true, true, false])
    
    let table2 = newArrowTable(schema, ids2, names2, actives2)
    let uri2 = fixture / "two.paruqet"

    writeTable(table2, uri2)

    let ds = newDataset(fixture / ".")
    var builder = ds.newScannerBuilder()
    builder.filter = col("id") > 2'i32
    let scanner = builder.finish()
    let tbl = scanner.toTable()
    unittest2.check tbl.nRows == 3
    unittest2.check tbl.nColumns == 3
    unittest2.check tbl["id"] == newChunkedArray([newArray(@[3'i32, 4]), newArray(@[9'i32])])
    unittest2.check tbl["name"] == newChunkedArray([newArray(@["gamma", "delta"]), newArray(@["Some"])])

  test "Write dataset from scanner to single file":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3, 4])
    let names = newArray(@["alpha", "beta", "gamma", "delta"])
    let table = newArrowTable(schema, ids, names)
    let uri = fixture / "written.parquet"

    let ds = newDataset(fixture / ".")
    let scanner = ds.newScannerBuilder().finish()
    writeDatasetFromScanner(scanner, uri, newFileFormat(Parquet))

    let fs = newLocalFileSystem()
    check fs.getFileInfo(uri).exists

  test "Write partitioned dataset from table":
    let schema = newSchema([
      newField[int32]("year"),
      newField[int32]("value"),
    ])
    var years, values: seq[int32]
    for i in 0 ..< 100:
      years.add((2000 + i div 10).int32)
      values.add(i.int32)

    let table = newArrowTable(schema, newArray(years), newArray(values))
    let partitionDir = fixture / "partitioned_write"
    createDir(partitionDir)

    let partSchema = newSchema([newField[int32]("year")])
    let partitioning = newDirectoryPartitioning(partSchema)
    writeDataset(table, partitionDir, newFileFormat(Parquet), partitioning=partitioning)

    let ds = newDataset(partitionDir)
    let readBack = ds.toTable()
    check readBack.nRows == 100
    # Partition columns are stored in directory paths, not in data files,
    # so newDataset (without partitioning discovery) sees only data columns
    check readBack.nColumns == 1

  test "Write dataset with custom basename template":
    let schema = newSchema([
      newField[int64]("nums"),
    ])
    let data = newArray(@[1'i64, 2, 3, 4, 5])
    let table = newArrowTable(schema, data)
    let outDir = fixture / "templated"
    createDir(outDir)

    writeDataset(table, outDir, newFileFormat(Parquet))

    let fs = newLocalFileSystem()
    check fs.getFileInfo(outDir / "part-0.parquet").exists

  # test "Dataset files - get file list":
  #   let schema = newSchema([
  #     newField[int32]("id"),
  #     newField[string]("name"),
  #   ])
  #   let ids1 = newArray(@[1'i32, 2, 3, 4])
  #   let names1 = newArray(@["alpha", "beta", "gamma", "delta"])
    
  #   let table1 = newArrowTable(schema, ids1, names1)
  #   let uri1 = fixture / "one.parquet"
    
  #   writeTable(table1, uri1)

  #   let ids2 = newArray(@[9'i32, 1, 1, 0])
  #   let names2 = newArray(@["Some", "things", "don't", "die"])
    
  #   let table2 = newArrowTable(schema, ids2, names2)
  #   let uri2 = fixture / "two.paruqet"

  #   writeTable(table2, uri2)

  #   let ds = newDataset(fixture / ".")
  #   let files = ds.files
  #   unittest2.check files.len == 2

  # test "Dataset - fileSystem getter and setter":
  #   let schema = newSchema([
  #     newField[int32]("id"),
  #     newField[string]("name"),
  #   ])
  #   let ids = newArray(@[1'i32, 2, 3])
  #   let names = newArray(@["a", "b", "c"])
  #   let table = newArrowTable(schema, ids, names)
  #   let uri = fixture / "fs_test.parquet"
  #   writeTable(table, uri)

  #   let ds = newDataset(fixture / ".")
  #   let fs = ds.fileSystem
  #   unittest2.check fs != nil

  #   var ds2 = ds
  #   let newFs = newFileSystem(fixture / "newpath")
  #   ds2.fileSystem = newFs

  # test "Dataset - format getter and setter":
  #   let schema = newSchema([
  #     newField[int32]("id"),
  #     newField[string]("name"),
  #   ])
  #   let ids = newArray(@[1'i32, 2, 3])
  #   let names = newArray(@["a", "b", "c"])
  #   let table = newArrowTable(schema, ids, names)
  #   let uri = fixture / "format_test.parquet"
  #   writeTable(table, uri)

  #   let ds = newDataset(fixture / ".")
  #   let fmt = ds.format
  #   unittest2.check fmt.kind == Parquet

  #   var ds2 = ds
  #   let newFormat = newFileFormat(IPC)
  #   ds2.format = newFormat

  # test "Dataset - partitioning getter and setter":
  #   let schema = newSchema([
  #     newField[int32]("id"),
  #     newField[string]("name"),
  #   ])
  #   let ids = newArray(@[1'i32, 2, 3])
  #   let names = newArray(@["a", "b", "c"])
  #   let table = newArrowTable(schema, ids, names)
  #   let uri = fixture / "part_test.parquet"
  #   writeTable(table, uri)

  #   let ds = newDataset(fixture / ".")
  #   let part = ds.partitioning
  #   unittest2.check part.toPtr != nil

  #   let newPart = newDefaultPartitioning()
  #   var ds2 = ds
  #   ds2.partitioning = newPart


suite "Partitioning":

  test "Default partitioning type name":
    let part = newDefaultPartitioning()
    check part.getTypeName == "directory"

  test "Directory partitioning type name":
    let schema = newSchema([newField[uint16]("year")])
    let part = newDirectoryPartitioning(schema)
    check part.getTypeName == "directory"

  test "Directory partitioning with options":
    let schema = newSchema([newField[uint16]("year")])
    var opts = newKeyValuePartitioningOptions()
    opts.segmentEncoding = None
    let part = newDirectoryPartitioning(schema, opts)
    check part.getTypeName == "directory"

  # test "Hive partitioning type name":
  #   let schema = newSchema([newField[uint16]("year")])
  #   # Known issue: newHivePartitioning segfaults in Arrow GLib 22.0.0
  #   # on this system (SIGSEGV inside gadataset_hive_partitioning_new).
  #   # This is an upstream bug, not a narrow wrapper issue.
  #   var opts = newHivePartitioningOptions()
  #   let part = newHivePartitioning(schema, opts)
  #   check part.getTypeName == "hive"

  # test "Hive partitioning with options":
  #   let schema = newSchema([newField[uint16]("year")])
  #   var opts = newHivePartitioningOptions()
  #   opts.segmentEncoding = None
  #   opts.nullFallback = "NULL"
  #   let part = newHivePartitioning(schema, opts)
  #   check part.nullFallback == "NULL"

  test "Hive partitioning options null fallback":
    var opts = newHivePartitioningOptions()
    # Default is empty string because GObject property starts unset;
    # C++ default "__HIVE_DEFAULT_PARTITION__" is in the param spec only.
    check opts.nullFallback == ""
    opts.nullFallback = "NULL"
    check opts.nullFallback == "NULL"

  test "Hive partitioning options segment encoding":
    var opts = newHivePartitioningOptions()
    check opts.segmentEncoding == None
    opts.segmentEncoding = URI
    check opts.segmentEncoding == URI

  test "KeyValue partitioning options segment encoding":
    var opts = newKeyValuePartitioningOptions()
    check opts.segmentEncoding == None
    opts.segmentEncoding = URI
    check opts.segmentEncoding == URI

  test "PartitioningFactoryOptions infer dictionary":
    var opts = newPartitioningFactoryOptions()
    check not opts.inferDictionary
    opts.inferDictionary = true
    check opts.inferDictionary

  test "PartitioningFactoryOptions schema":
    var opts = newPartitioningFactoryOptions()
    let schema = newSchema([newField[uint16]("year")])
    opts.schema = schema
    let retrieved = opts.schema
    check retrieved.nFields == 1
    check retrieved[0].name == "year"

  test "PartitioningFactoryOptions segment encoding":
    var opts = newPartitioningFactoryOptions()
    check opts.segmentEncoding == None
    opts.segmentEncoding = URI
    check opts.segmentEncoding == URI
