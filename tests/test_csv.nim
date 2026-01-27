import std/[os, options, sets, sequtils]
import unittest2
import ../src/[ffi, filesystem, gtables, csv, gtypes, gschema, garray]

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

suite "Writing CSV":
  let uri = getCurrentDir() & "/tests/written.csv"
  let schema = newSchema([
    newField[bool]("alive"),
    newField[string]("name")
  ])

  let
    alive = newArray(@[true, true, false])
    name = newArray(@["a", "b", "c"])
    opt = newWriteOptions(batchSize=1)

  test "write table to csv file localFileSystem":
    let table = newArrowTable(schema, alive, name)

    writeCsv(uri, table, opt)
    let inTable = readCSV(uri)
    check table.equal(inTable)
    check table == inTable

  # test "write record batch to csv file localFileSystem":
  #   let rb = newRecordBatch(schema, alive, name)

  #   writeCsv(uri, rb, opt)
  #   let inTable = readCSV(uri)
  #   check rb == inTable

