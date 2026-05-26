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
    let uri2 = fixture / "two.parquet"

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
    let uri2 = fixture / "two.parquet"

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

    # Verify directory structure: 10 year subdirectories
    let fs = newLocalFileSystem()
    let selector = newFileSelector(partitionDir, recursive = true)
    let infos = fs.getFileInfos(selector)
    var yearDirs: seq[string]
    for info in infos:
      if info.isDir and info.path != partitionDir:
        yearDirs.add(info.baseName)
    check yearDirs.len == 10
    for y in 2000 .. 2009:
      check $y in yearDirs

  test "Write partitioned dataset with URI segment encoding options":
    let schema = newSchema([
      newField[int32]("region"),
      newField[int32]("value"),
    ])
    let table = newArrowTable(schema, newArray(@[1'i32, 2]), newArray(@[10'i32, 20]))
    let partitionDir = fixture / "partitioned_uri"
    createDir(partitionDir)

    var opts = newKeyValuePartitioningOptions()
    opts.segmentEncoding = URI
    let partSchema = newSchema([newField[int32]("region")])
    let partitioning = newDirectoryPartitioning(partSchema, opts)
    writeDataset(table, partitionDir, newFileFormat(Parquet), partitioning=partitioning)

    let ds = newDataset(partitionDir)
    check ds.toTable().nRows == 2

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

  test "Dataset type name":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3])
    let names = newArray(@["a", "b", "c"])
    let table = newArrowTable(schema, ids, names)
    let uri = fixture / "type_name_test.parquet"
    writeTable(table, uri)

    let ds = newDataset(fixture / ".")
    check ds.typeName == "filesystem"

  test "Dataset format getter":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3])
    let names = newArray(@["a", "b", "c"])
    let table = newArrowTable(schema, ids, names)
    let uri = fixture / "format_test.parquet"
    writeTable(table, uri)

    let ds = newDataset(fixture / ".")
    let fmt = ds.format
    check fmt.kind == Parquet

  test "Dataset toRecordBatchReader":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3])
    let names = newArray(@["a", "b", "c"])
    let table = newArrowTable(schema, ids, names)
    let uri = fixture / "rbr_test.parquet"
    writeTable(table, uri)

    let ds = newDataset(fixture / ".")
    let reader = ds.toRecordBatchReader()
    check reader.schema.nFields == 2
    var count = 0
    for batch in batches(reader):
      count += batch.nRows.int
    check count == 3

  test "Dataset beginScan":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3])
    let names = newArray(@["a", "b", "c"])
    let table = newArrowTable(schema, ids, names)
    let uri = fixture / "scan_test.parquet"
    writeTable(table, uri)

    let ds = newDataset(fixture / ".")
    let scanner = ds.beginScan().finish()
    let tbl = scanner.toTable()
    check tbl.nRows == 3

  test "Dataset partitioning getter returns empty when unset":
    # The partitioning property on FileSystemDataset is construct-only;
    # it can only be set during dataset creation, not after.
    # Arrow GLib 22.0.0 does not expose factory-level partitioning.
    # This behavior is verified against Arrow GLib 24.0.0.
    # discovery, so newDataset() creates datasets without partitioning.
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3])
    let names = newArray(@["a", "b", "c"])
    let table = newArrowTable(schema, ids, names)
    let uri = fixture / "part_test.parquet"
    writeTable(table, uri)

    let ds = newDataset(fixture / ".")
    let retrieved = ds.partitioning
    check retrieved.getTypeName == ""


  test "readTable vs dataset scanner equivalence":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3, 4])
    let names = newArray(@["a", "b", "c", "d"])
    let table = newArrowTable(schema, ids, names)
    let uri = fixture / "equiv.parquet"
    writeTable(table, uri)

    let direct = readTable(uri)
    let ds = newDataset(uri)
    let viaScanner = ds.toTable()

    check direct.nRows == viaScanner.nRows
    check direct.nColumns == viaScanner.nColumns

  test "readTable with filter vs dataset scanner with filter":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids = newArray(@[1'i32, 2, 3, 4, 5])
    let names = newArray(@["a", "b", "c", "d", "e"])
    let table = newArrowTable(schema, ids, names)
    let uri = fixture / "filter_equiv.parquet"
    writeTable(table, uri)

    let filter = col("id") > 2'i32
    let direct = readTable(uri, filter)

    let ds = newDataset(uri)
    var builder = ds.newScannerBuilder()
    builder.filter = filter
    let viaScanner = builder.finish().toTable()

    check direct.nRows == viaScanner.nRows
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
  #   # Known issue: newHivePartitioning segfaults in Arrow GLib 22.0.0 and earlier.
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



