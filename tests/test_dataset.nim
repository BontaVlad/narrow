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

  test "Dataset files - get file list":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
    ])
    let ids1 = newArray(@[1'i32, 2, 3, 4])
    let names1 = newArray(@["alpha", "beta", "gamma", "delta"])
    
    let table1 = newArrowTable(schema, ids1, names1)
    let uri1 = fixture / "one.parquet"
    
    writeTable(table1, uri1)

    let ids2 = newArray(@[9'i32, 1, 1, 0])
    let names2 = newArray(@["Some", "things", "don't", "die"])
    
    let table2 = newArrowTable(schema, ids2, names2)
    let uri2 = fixture / "two.paruqet"

    writeTable(table2, uri2)

    let ds = newDataset(fixture / ".")
    let files = ds.files
    unittest2.check files.len == 2
