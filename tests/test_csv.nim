import std/[os, options, sets, sequtils]
import unittest2
import testfixture
import ../src/narrow/[core/ffi, io/filesystem, tabular/table, io/csv, types/gtypes, column/metadata, column/primitive, tabular/batch]

suite "Reading CSV":

  test "read csv file localFileSystem":
    let uri = getCurrentDir() & "/tests/customers-100.csv"
    let table = readCSV(uri)
    check table.nRows == 100

  test "read csv file with full uri":
    let uri = "file://" & getCurrentDir() & "/tests/customers-100.csv"
    let table = readCSV(uri)
    check table.nRows == 100

  test "read csv file with custom delimiter":
    let uri = getCurrentDir() & "/tests/email.csv"
    var options = newCsvReadOptions(delimiter=some(';'))

    let table = readCSV(uri, options)
    check table.nRows == 4

  test "read csv file with custom delimiter and column filtering":
    let uri = getCurrentDir() & "/tests/email.csv"
    var options = newCsvReadOptions(delimiter=some(';'))

    let schema = newSchema(@[newField[string]("First name"), newField[string]("Last name")])
    options.addSchema(schema)

    let table = readCSV(uri, options)
    check table.nRows == 4
    let tblKeys = toHashSet(table.keys.toSeq)
    check len(tblKeys) == 2
    check "First name" in tblKeys
    check "Last name" in tblKeys

suite "Writing CSV - ArrowTable":
  let schema = newSchema([
    newField[bool]("alive"),
    newField[string]("name")
  ])

  let
    alive = newArray(@[true, true, false])
    name = newArray(@["a", "b", "c"])
    opt = newWriteOptions(batchSize=1)

  var fixture: TestFixture

  setup:
    # Auto-detect test name - will create isolated directory per test
    fixture = newTestFixture("test_io/csv")

  teardown:
    fixture.cleanup()

  test "write table to io/csv file localFileSystem":
    let table = newArrowTable(schema, alive, name)
    let uri = fixture / "written_table.csv"

    writeCsv(uri, table, opt)
    let inTable = readCSV(uri)
    check table.equal(inTable)
    check table == inTable

  test "write table with multiple data types":
    let multiSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[float64]("score"),
      newField[bool]("active")
    ])
    let ids = newArray(@[1'i32, 2'i32, 3'i32])
    let names = newArray(@["alice", "bob", "charlie"])
    let scores = newArray(@[95.5'f64, 87.2'f64, 92.1'f64])
    let actives = newArray(@[true, false, true])
    
    let table = newArrowTable(multiSchema, ids, names, scores, actives)
    let uri = fixture / "written_multi.csv"
    
    writeCsv(uri, table, newWriteOptions())
    let inTable = readCSV(uri)
    check table.nRows == inTable.nRows
    check table.nColumns == inTable.nColumns

  test "write empty table":
    let emptySchema = newSchema([
      newField[int32]("col1"),
      newField[string]("col2")
    ])
    let emptyTable = newArrowTable(emptySchema, newArray[int32](@[]), newArray[string](@[]))
    let uri = fixture / "written_empty.csv"
    
    writeCsv(uri, emptyTable, newWriteOptions())
    let inTable = readCSV(uri)
    check inTable.nRows == 0

suite "Writing CSV - RecordBatch":
  let schema = newSchema([
    newField[bool]("alive"),
    newField[string]("name")
  ])

  let
    alive = newArray(@[true, true, false])
    name = newArray(@["a", "b", "c"])
    opt = newWriteOptions(batchSize=1)

  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_io/csv")

  teardown:
    fixture.cleanup()

  test "write record batch to io/csv file":
    let rb = newRecordBatch(schema, alive, name)
    let uri = fixture / "written_batch.csv"

    writeCsv(uri, rb, opt)
    let inTable = readCSV(uri)
    check rb.nRows == inTable.nRows
    check rb.nColumns == inTable.nColumns

  test "write record batch with multiple types":
    let multiSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[float64]("value")
    ])
    let ids = newArray(@[10'i32, 20'i32, 30'i32])
    let names = newArray(@["x", "y", "z"])
    let values = newArray(@[1.1'f64, 2.2'f64, 3.3'f64])
    
    let rb = newRecordBatch(multiSchema, ids, names, values)
    let uri = fixture / "written_batch_multi.csv"
    
    writeCsv(uri, rb, newWriteOptions())
    let inTable = readCSV(uri)
    check rb.nRows == inTable.nRows
    check rb.nColumns == inTable.nColumns

  test "write large record batch with batching":
    var ids: seq[int32]
    var names: seq[string]
    for i in 0..<100:
      ids.add(i.int32)
      names.add("name_" & $i)
    
    let largeSchema = newSchema([
      newField[int32]("id"),
      newField[string]("name")
    ])
    let rb = newRecordBatch(largeSchema, newArray(ids), newArray(names))
    let uri = fixture / "written_batch_large.csv"
    
    writeCsv(uri, rb, newWriteOptions(batchSize=25))
    let inTable = readCSV(uri)
    check rb.nRows == inTable.nRows
    check rb.nColumns == inTable.nColumns

suite "WriteOptions":
  test "default write options":
    let opts = newWriteOptions()
    check opts.includeHeader == true
    check opts.batchSize == 1024
    check opts.delimiter == ','
    check opts.nullString == ""
    check opts.eol == "\n"

  test "custom write options":
    let opts = newWriteOptions(
      includeHeader=false,
      batchSize=100,
      delimiter=';',
      nullString="NULL",
      eol="\r\n"
    )
    check opts.includeHeader == false
    check opts.batchSize == 100
    check opts.delimiter == ';'
    check opts.nullString == "NULL"
    check opts.eol == "\r\n"

suite "CSV Round-trip":
  var fixture: TestFixture

  setup:
    fixture = newTestFixture("test_io/csv")

  teardown:
    fixture.cleanup()

  test "write and read back preserves data":
    let schema = newSchema([
      newField[int32]("id"),
      newField[string]("name"),
      newField[bool]("active")
    ])
    let ids = newArray(@[1'i32, 2'i32, 3'i32, 4'i32])
    let names = newArray(@["alpha", "beta", "gamma", "delta"])
    let actives = newArray(@[true, false, true, false])
    
    let table = newArrowTable(schema, ids, names, actives)
    let uri = fixture / "written_roundtrip.csv"
    
    writeCsv(uri, table, newWriteOptions())
    let restored = readCSV(uri)
    
    check table.nRows == restored.nRows
    check table.nColumns == restored.nColumns
